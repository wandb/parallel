package parallel

import (
	"context"
	"errors"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

// This constant can be anything, and only allows for optimization. None of our
// channels are guaranteed not to block. This can be reduced to 0 and the tests
// will still pass with (usually) full coverage.
const bufferSize = 8

const misuseMessage = "parallel executor misuse: don't reuse executors"

var (
	errPanicked       = errors.New("panicked")
	errGroupDone      = errors.New("executor done")
	errGroupAbandoned = errors.New("executor abandoned")

	// Contexts are canceled with this error when executors are awaited.
	GroupDoneError = errGroupDone
)

// WorkerPanic represents a panic value propagated from a task within a parallel
// executor, and is the main type of panic that you might expect to receive.
type WorkerPanic struct {
	// Panic contains the originally panic()ed value.
	Panic any
	// Stacktraces contains the stacktraces of the panics. The stack trace of
	// the line that threw the original Panic value appears first, and any other
	// stack traces from other parallel groups that received this panic and re-
	// threw it appear in order afterwards.
	Stacktraces []string
}

// NOTE: If you want to really get crazy with it, it IS permissible and safe to
// call Go(...) from multiple threads without additional synchronization, on
// every kind of executor. HOWEVER: the caller always assumes full
// responsibility for making sure that Wait() definitely was not called on that
// executor yet, and any goroutine calling an executor's functions may receive a
// panic if any of the group's goroutines panicked. (Go() may hoist the panic
// opportunistically, and Wait() will reliably always hoist a panic if one
// occurred.)

// Executor that runs the given functions and can wait for all of them to
// finish.
type Executor interface {
	// Go submits a task to the Executor, to be run at some point in the future.
	//
	// Panics if Wait() has already been called.
	// May panic if any submitted task has already panicked.
	Go(func(context.Context))
	// Wait waits until all submitted tasks have completed.
	//
	// After waiting, panics if any submitted task panicked.
	Wait()

	// internal
	getContext() (context.Context, context.CancelCauseFunc)
	// Waits without canceling the context with errGroupDone. The caller of this
	// function promises that they will be responsible for canceling the context
	quietWait()
}

// Creates a basic executor which runs all the functions given in one goroutine
// each. Composes starting the goroutines, safe usage of WaitGroups, and as a
// bonus any panic that happens in one of the provided functions will be ferried
// over and re-panicked in the thread that owns the executor (that is, the code
// calling Wait() and Go()), so the whole process doesn't die.
func Unlimited(ctx context.Context) Executor {
	return makeGroup(context.WithCancelCause(ctx))
}

// Creates a parallelism-limited executor which starts up to a given number of
// goroutines, which each run the provided functions until done.
//
// These executors are even best-effort safe against misuse: if the owner panics
// or otherwise forgets to call Wait(), the goroutines started by this executor
// should still be cleaned up.
func Limited(ctx context.Context, maxGoroutines int) Executor {
	if maxGoroutines < 1 {
		gctx, cancel := context.WithCancelCause(ctx)
		g := &runner{ctx: gctx, cancel: cancel}
		runtime.SetFinalizer(g, func(doomed *runner) {
			doomed.cancel(errGroupAbandoned)
		})
		return g
	}
	making := &limitedGroup{
		g:   makeGroup(context.WithCancelCause(ctx)),
		ops: make(chan func(context.Context), bufferSize),
		max: uint64(maxGoroutines),
	}
	runtime.SetFinalizer(making, func(doomed *limitedGroup) {
		close(doomed.ops)
		doomed.g.cancel(errGroupAbandoned)
	})
	return making
}

// Base executor with an interface that runs everything serially.
type runner struct {
	ctx     context.Context         // Closed when we panic or get garbage collected
	cancel  context.CancelCauseFunc // Only close the dying channel one time
	awaited atomic.Bool             // Set when Wait() is called
}

func (n *runner) Go(op func(context.Context)) {
	if n.awaited.Load() {
		panic(misuseMessage)
	}
	select {
	case <-n.ctx.Done():
		return
	default:
	}
	op(n.ctx)
}

func (n *runner) Wait() {
	n.quietWait()
	n.cancel(errGroupDone)
}

func (n *runner) quietWait() {
	n.awaited.Store(true)
	runtime.SetFinalizer(n, nil)
}

func (n *runner) getContext() (context.Context, context.CancelCauseFunc) {
	return n.ctx, n.cancel
}

func makeGroup(ctx context.Context, cancel context.CancelCauseFunc) *group {
	g := &group{runner: runner{ctx: ctx, cancel: cancel}}
	runtime.SetFinalizer(g, func(doomed *group) {
		doomed.cancel(errGroupAbandoned)
	})
	return g
}

// Base concurrent executor
type group struct {
	runner
	wg       sync.WaitGroup
	panicked atomic.Pointer[WorkerPanic] // Stores panic values
}

func (g *group) Go(op func(context.Context)) {
	if g.awaited.Load() {
		panic(misuseMessage)
	}
	g.checkPanic()
	select {
	case <-g.ctx.Done():
		return
	default:
	}
	g.wg.Add(1)
	go func() {
		returnedNormally := false
		defer func() {
			if !returnedNormally {
				// When the function call has exited without returning, hoist
				// the recover()ed panic value and a stack trace so it can be
				// re-panicked.
				p := recover()
				if p == nil {
					// This is a runtime.Goexit(), such as from a process
					// termination or a test failure; let that propagate instead
					g.cancel(context.Canceled)
				} else {
					// If we are propagating a panic that is already a
					// WorkerPanic (for example, if we have panics propagating
					// through multiple parallel groups), just add our
					// stacktrace onto the end of the slice; otherwise make a
					// new WorkerPanic value.
					var wp WorkerPanic
					switch tp := p.(type) {
					case WorkerPanic:
						wp = WorkerPanic{
							Panic:       tp.Panic,
							Stacktraces: append(tp.Stacktraces, string(debug.Stack())),
						}
					default:
						wp = WorkerPanic{
							Panic:       p,
							Stacktraces: []string{string(debug.Stack())},
						}
					}

					g.panicked.CompareAndSwap(nil, &wp)
					g.cancel(errPanicked)
				}
			}
			g.wg.Done()
		}()
		op(g.ctx)
		returnedNormally = true // op returned, don't store a panic
	}()
}

func (g *group) Wait() {
	defer g.cancel(errGroupDone)
	g.quietWait()
}

func (g *group) quietWait() {
	g.awaited.Store(true)
	runtime.SetFinalizer(g, nil)
	g.wg.Wait()
	g.checkPanic()
}

func (g *group) checkPanic() {
	// Safely propagate any panic a worker encountered into the owning goroutine
	if p := g.panicked.Load(); p != nil {
		panic(*p)
	}
}

// Executor that starts a limited number of worker goroutines.
//
// Note that here, as well in our collectors, many things are stored
// out-of-line through pointers, interfaces, etc. This is required so that we
// can avoid retaining *any* reference to the managing struct in values captured
// by the goroutines we run, ensuring that the struct will get garbage collected
// if it is forgotten rather than being kept alive forever by its sleeping
// goroutines, which lets us guarantee that it will be shut down with the
// registered runtime finalizer.
type limitedGroup struct {
	g       *group                     // The actual executor.
	ops     chan func(context.Context) // These are the functions the workers will run
	max     uint64                     // Maximum number of worker goroutines we start
	started uint64                     // Counter of how many goroutines we've started or almost-started so far
	awaited atomic.Bool                // Set when Wait() is called, so we don't close ops twice
}

func (lg *limitedGroup) Go(op func(context.Context)) {
	if lg.awaited.Load() {
		panic(misuseMessage)
	}
	// The first "max" ops started kick off a new worker goroutine
	if atomic.LoadUint64(&lg.started) < lg.max && atomic.AddUint64(&lg.started, 1) <= lg.max {
		ops, dying := lg.ops, lg.g.ctx.Done() // Don't capture a pointer to the group
		lg.g.Go(func(ctx context.Context) {
			// Worker bee function. We take and execute ops from the channel
			// until we are done or the group is dead.
			for {
				// First do a non-blocking check on dying. If work remains but
				// the context has ended, we only have a ~50% chance to stop
				// each iteration unless we specifically check this one first,
				// because go's "select" chooses among available channels
				// semi-randomly.
				select {
				case <-dying:
					return
				default:
				}
				// Then wait for either submitted work, end, or death
				select {
				case <-dying:
					return
				case thisOp, stillOpen := <-ops:
					if !stillOpen {
						return
					}
					thisOp(ctx)
				}
			}
		})
	}
	select {
	case lg.ops <- op:
		return
	case <-lg.g.ctx.Done():
		lg.g.checkPanic()
	}
}

func (lg *limitedGroup) Wait() {
	if !lg.awaited.Swap(true) {
		close(lg.ops)
		runtime.SetFinalizer(lg, nil) // Don't try to close this chan again :)
	}
	lg.g.Wait()
}

func (lg *limitedGroup) quietWait() {
	if !lg.awaited.Swap(true) {
		close(lg.ops)
		runtime.SetFinalizer(lg, nil) // Don't try to close this chan again :)
	}
	lg.g.quietWait()
}

func (lg *limitedGroup) getContext() (context.Context, context.CancelCauseFunc) {
	return lg.g.getContext()
}
