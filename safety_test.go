//go:build !race

package parallel

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

// The tests in this file can be detected as racy by the race condition checker
// because we are reaching under the hood to look at the group's channel, so we
// can see when the group's functions have started running. There's no good
// reason make those channels otherwise accessible, since they are completely
// owned by the group and making this work in a "non-racy" way would require
// extra complexity and overhead.

func TestLimitedGroupCleanup(t *testing.T) {
	t.Parallel()
	var counter int64
	var leak contextLeak

	opsQueue := func() chan func(context.Context) {
		g := Limited(context.Background(), 10)
		for i := 0; i < 100; i++ {
			g.Go(func(ctx context.Context) {
				defer func() {
					p := recover()
					if p != nil {
						println(p)
					}
				}()
				atomic.AddInt64(&counter, 1)
				leak.leak(ctx)
			})
		}
		return g.(*limitedGroup).ops
		// leak the un-awaited group
	}()
	assert.NotNil(t, opsQueue)
	runtime.GC() // Trigger cleanups for leaked resources
	for op := range opsQueue {
		op(nil) // have mercy and run those ops anyway, just so we get a full count
	}
	// The channel should get closed!
	assert.Equal(t, int64(100), counter)
	leak.assertAllCanceled(t, errGroupAbandoned)
}

func TestCollectorCleanup(t *testing.T) {
	t.Parallel()
	var leak contextLeak
	valuePipe := func() chan int {
		g := Collect[int](Unlimited(context.Background()))
		g.Go(func(ctx context.Context) (int, error) {
			leak.leak(ctx)
			return 1, nil
		})
		return g.(collectingGroup[int]).pipe
		// leak the un-awaited group
	}()
	assert.NotNil(t, valuePipe)
	runtime.GC() // Trigger cleanup of the collector
	runtime.GC() // Trigger cleanup of the executor it owned
	runtime.GC() // One more for good measure
	for range valuePipe {
		// The channel should get closed!
	}
	leak.assertAllCanceled(t) // the cancelation error is inconsistent here
}

func TestFeederCleanup(t *testing.T) {
	t.Parallel()
	var leak contextLeak
	valuePipe := func() chan int {
		g := Feed[int](Unlimited(context.Background()), func(context.Context, int) error { return nil })
		g.Go(func(ctx context.Context) (int, error) {
			leak.leak(ctx)
			return 1, nil
		})
		return g.(feedingGroup[int]).pipe
		// leak the un-awaited group
	}()
	assert.NotNil(t, valuePipe)
	runtime.GC() // Trigger cleanup of the feeder
	runtime.GC() // Trigger cleanup of the executor it owned
	runtime.GC() // One more for good measure
	for range valuePipe {
		// The channel should get closed!
	}
	leak.assertAllCanceled(t) // the cancelation error is inconsistent here
}

func TestGatherErrCleanup(t *testing.T) {
	t.Parallel()
	var leak contextLeak
	valuePipe := func() chan error {
		g := GatherErrs(Unlimited(context.Background()))
		g.Go(func(ctx context.Context) error {
			leak.leak(ctx)
			return nil
		})
		return g.(multiErrGroup).pipe
		// leak the un-awaited group
	}()
	assert.NotNil(t, valuePipe)
	runtime.GC() // Trigger cleanup of the gatherer
	runtime.GC() // Trigger cleanup of the executor it owned
	runtime.GC() // One more for good measure
	for range valuePipe {
		// The channel should get closed!
	}
	leak.assertAllCanceled(t) // the cancelation error is inconsistent here
}

func TestCollectWithErrsCleanup(t *testing.T) {
	t.Parallel()
	var leak contextLeak
	valuePipe := func() chan withErr[int] {
		g := CollectWithErrs[int](Unlimited(context.Background()))
		g.Go(func(ctx context.Context) (int, error) {
			leak.leak(ctx)
			return 1, nil
		})
		return g.(collectingMultiErrGroup[int]).pipe
		// leak the un-awaited group
	}()
	assert.NotNil(t, valuePipe)
	runtime.GC() // Trigger cleanup of the collector
	runtime.GC() // Trigger cleanup of the executor it owned
	runtime.GC() // One more for good measure
	for range valuePipe {
		// The channel should get closed!
	}
	leak.assertAllCanceled(t) // the cancelation error is inconsistent here
}

func TestFeedWithErrsCleanup(t *testing.T) {
	t.Parallel()
	var leak contextLeak
	valuePipe := func() chan withErr[int] {
		g := FeedWithErrs(Unlimited(context.Background()),
			func(context.Context, int) error { return nil })
		g.Go(func(ctx context.Context) (int, error) {
			leak.leak(ctx)
			return 1, nil
		})
		return g.(feedingMultiErrGroup[int]).pipe
		// leak the un-awaited group
	}()
	assert.NotNil(t, valuePipe)
	runtime.GC() // Trigger cleanup of the collector
	runtime.GC() // Trigger cleanup of the executor it owned
	runtime.GC() // One more for good measure
	for range valuePipe {
		// The channel should get closed!
	}
	leak.assertAllCanceled(t) // the cancelation error is inconsistent here
}

func TestPanicGroup(t *testing.T) {
	t.Parallel()
	var leak contextLeak
	g := Unlimited(context.Background())
	var blocker sync.WaitGroup
	blocker.Add(1)
	g.Go(func(ctx context.Context) {
		leak.leak(ctx)
		blocker.Wait()
		panic("wow")
	})
	g.Go(func(context.Context) {
		blocker.Done()
	})
	// Wait for the group to "die" when the panic hits
	ctx, _ := g.getContext()
	<-ctx.Done()
	assert.PanicsWithValue(t, "wow", func() {
		g.Wait()
	})
	leak.assertAllCanceled(t, errPanicked)
}

func TestPanicGroupSecondPath(t *testing.T) {
	t.Parallel()
	var leak contextLeak
	g := Unlimited(context.Background())
	var blocker sync.WaitGroup
	blocker.Add(1)
	g.Go(func(ctx context.Context) {
		leak.leak(ctx)
		blocker.Wait()
		panic("wow")
	})
	g.Go(func(context.Context) {
		blocker.Done()
	})
	// Wait for the group to "die" when the panic hits
	ctx, _ := g.getContext()
	<-ctx.Done()
	assert.PanicsWithValue(t, "wow", func() {
		g.Go(func(context.Context) {
			t.Fatal("this op should never run")
		})
	})
	leak.assertAllCanceled(t, errPanicked)
}

func TestPanicLimitedGroup(t *testing.T) {
	t.Parallel()
	var leak contextLeak
	var waitForNonPanic, unblockInnocent, block sync.WaitGroup
	waitForNonPanic.Add(1)
	unblockInnocent.Add(1)
	block.Add(1)
	g := Limited(context.Background(), 10)
	g.Go(func(ctx context.Context) { // Innocent function
		leak.leak(ctx)
		waitForNonPanic.Done()
		unblockInnocent.Wait()
	})
	g.Go(func(context.Context) { // Panicking function
		block.Wait()
		unblockInnocent.Done()
		panic("lol")
	})
	waitForNonPanic.Wait()
	block.Done()
	assert.PanicsWithValue(t, "lol", func() {
		g.Wait()
	})
	leak.assertAllCanceled(t, errPanicked)
}

func TestPanicLimitedGroupSecondPath(t *testing.T) {
	t.Parallel()
	var leak contextLeak
	var waitForNonPanic, unblockInnocent, block sync.WaitGroup
	waitForNonPanic.Add(1)
	unblockInnocent.Add(1)
	block.Add(1)
	g := Limited(context.Background(), 10)
	g.Go(func(ctx context.Context) { // Innocent function
		leak.leak(ctx)
		waitForNonPanic.Done()
		unblockInnocent.Wait()
	})
	g.Go(func(context.Context) { // Panicking function
		block.Wait()
		unblockInnocent.Done()
		panic("lol")
	})
	waitForNonPanic.Wait()
	block.Done()
	assert.PanicsWithValue(t, "lol", func() {
		// Eventually :)
		for {
			g.Go(func(context.Context) {})
		}
	})
	leak.assertAllCanceled(t, errPanicked)
}

func TestPanicFeedFunction(t *testing.T) {
	t.Parallel()
	var leak contextLeak
	g := Feed(Unlimited(context.Background()), func(ctx context.Context, _ int) error {
		leak.leak(ctx)
		panic("oh no!")
	})
	g.Go(func(context.Context) (int, error) {
		return 1, nil
	})
	assert.PanicsWithValue(t, "oh no!", func() { _ = g.Wait() })
	leak.assertAllCanceled(t) // the cancelation error is inconsistent here
}

func TestPanicFeedWork(t *testing.T) {
	t.Parallel()
	var leak contextLeak
	g := Feed(Unlimited(context.Background()), func(context.Context, int) error {
		t.Fatal("should not get called")
		return nil
	})
	g.Go(func(ctx context.Context) (int, error) {
		leak.leak(ctx)
		panic("oh no!")
	})
	assert.PanicsWithValue(t, "oh no!", func() { _ = g.Wait() })
	leak.assertAllCanceled(t, errPanicked)
}

func TestPanicFeedWorkSecondPath(t *testing.T) {
	t.Parallel()
	var leak contextLeak
	g := Feed(Unlimited(context.Background()), func(context.Context, int) error {
		t.Fatal("should not get called")
		return nil
	})
	g.Go(func(ctx context.Context) (int, error) {
		leak.leak(ctx)
		panic("oh no!")
	})
	ctx, _ := g.(feedingGroup[int]).g.getContext()
	<-ctx.Done()
	assert.PanicsWithValue(t, "oh no!", func() {
		g.Go(func(context.Context) (int, error) { return 2, nil })
	})
	leak.assertAllCanceled(t, errPanicked)
}

func TestPanicFeedFunctionNotCalled(t *testing.T) {
	t.Parallel()
	var leak contextLeak
	g := Feed(Unlimited(context.Background()), func(context.Context, int) error {
		panic("oh no!")
	})
	g.Go(func(ctx context.Context) (int, error) {
		leak.leak(ctx)
		return 0, errors.New("foo")
	})
	assert.NotPanics(t, func() {
		assert.Errorf(t, g.Wait(), "foo")
	})
	leak.assertAllCanceled(t)
}

func TestPanicFeedErrFunction(t *testing.T) {
	t.Parallel()
	var leak contextLeak
	g := FeedWithErrs(Unlimited(context.Background()), func(context.Context, int) error {
		panic("oh no!")
	})
	g.Go(func(ctx context.Context) (int, error) {
		leak.leak(ctx)
		return 1, nil
	})
	assert.PanicsWithValue(t, "oh no!", func() { _ = g.Wait() })
	leak.assertAllCanceled(t) // the cancelation error is inconsistent here
}

func TestPanicFeedErrWork(t *testing.T) {
	t.Parallel()
	var leak contextLeak
	g := FeedWithErrs(Unlimited(context.Background()), func(context.Context, int) error {
		t.Fatal("should not get a value")
		return nil
	})
	g.Go(func(ctx context.Context) (int, error) {
		leak.leak(ctx)
		panic("oh no!")
	})
	assert.PanicsWithValue(t, "oh no!", func() { _ = g.Wait() })
	leak.assertAllCanceled(t, errPanicked)
}

func TestPanicFeedErrWorkSecondPath(t *testing.T) {
	t.Parallel()
	var leak contextLeak
	g := FeedWithErrs(Unlimited(context.Background()), func(context.Context, int) error {
		t.Fatal("should not get a value")
		return nil
	})
	g.Go(func(ctx context.Context) (int, error) {
		leak.leak(ctx)
		panic("oh no!")
	})
	ctx, _ := g.(feedingMultiErrGroup[int]).g.getContext()
	<-ctx.Done()
	assert.PanicsWithValue(t, "oh no!", func() {
		g.Go(func(context.Context) (int, error) { return 2, nil })
	})
	leak.assertAllCanceled(t, errPanicked)
}

func TestPanicFeedErrFunctionNoValues(t *testing.T) {
	t.Parallel()
	var leak contextLeak
	g := FeedWithErrs(Unlimited(context.Background()), func(context.Context, int) error {
		t.Fatal("should not get a value")
		return nil
	})
	g.Go(func(ctx context.Context) (int, error) {
		leak.leak(ctx)
		return 0, errors.New("regular error")
	})
	assert.Errorf(t, g.Wait(), "regular error")
	leak.assertAllCanceled(t, errGroupDone)
}

func TestMisuseReuse(t *testing.T) {
	t.Parallel()
	limitedWithAllWorkers := Limited(context.Background(), 10)
	for i := 0; i < 10; i++ {
		limitedWithAllWorkers.Go(func(context.Context) {})
	}
	for _, testCase := range []struct {
		name string
		g    Executor
	}{
		{"Unlimited", Unlimited(context.Background())},
		{"Limited", Limited(context.Background(), 10)},
		{"Serial", Limited(context.Background(), 0)},
		{"Limited with all workers", limitedWithAllWorkers},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			testCase.g.Wait()
			assert.PanicsWithValue(
				t,
				"parallel executor misuse: don't reuse executors",
				func() {
					testCase.g.Go(func(context.Context) {
						t.Fatal("this should never run")
					})
				},
			)
		})
	}
}

func TestMisuseReuseCollector(t *testing.T) {
	t.Parallel()
	g := Collect[int](Unlimited(context.Background()))
	res, err := g.Wait()
	assert.NoError(t, err)
	assert.Equal(t, []int(nil), res)
	assert.PanicsWithValue(
		t,
		"parallel executor misuse: don't reuse executors",
		func() {
			g.Go(func(context.Context) (int, error) {
				t.Fatal("this should never run")
				return 1, nil
			})
		},
	)
}

func TestGroupsPanicAgain(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		g    func() Executor
	}{
		{"Unlimited", func() Executor { return Unlimited(context.Background()) }},
		{"Limited", func() Executor { return Limited(context.Background(), 10) }},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			innerGroup := test.g()
			outerGroup := test.g()
			outerGroup.Go(func(context.Context) {
				innerGroup.Go(func(context.Context) { panic("at the disco") })
				innerGroup.Wait()
			})
			assert.PanicsWithValue(t, "at the disco", outerGroup.Wait)
			assert.PanicsWithValue(t, "at the disco", innerGroup.Wait)
			assert.PanicsWithValue(t, "at the disco", outerGroup.Wait)
			assert.PanicsWithValue(t, "at the disco", innerGroup.Wait)
		})
	}
}

func TestPipeGroupPanicsAgain(t *testing.T) {
	t.Parallel()
	g := Feed(Unlimited(context.Background()), func(context.Context, int) error { return nil })
	g.Go(func(context.Context) (int, error) { panic("at the disco") })
	assert.PanicsWithValue(t, "at the disco", func() { _ = g.Wait() })
	assert.PanicsWithValue(t, "at the disco", func() { _ = g.Wait() })
}

func TestForgottenPipeLegiblePanic(t *testing.T) {
	t.Parallel()
	exec := Unlimited(context.Background())
	var blocker sync.WaitGroup
	blocker.Add(1)
	valuePipe := func() chan int {
		g := Collect[int](exec)
		g.Go(func(context.Context) (int, error) {
			blocker.Wait()
			return 1, nil
		})
		return g.(collectingGroup[int]).pipe
		// leak the un-awaited group
	}()
	assert.NotNil(t, valuePipe)
	runtime.GC() // Trigger cleanups for leaked resources
	for range valuePipe {
	}
	// The collector's pipe is now closed. Unblock the task we submitted to the
	// collector now, so its value will be sent to the closed pipe. When this
	// happens the panic will be stored in the executor, so we re-panic that
	// specific error with a more diagnostic message.
	blocker.Done()
	assert.PanicsWithValue(t, "parallel executor pipe error: a "+
		"collector using this same executor was probably not awaited", exec.Wait)
}

func TestPanicNil(t *testing.T) {
	// Read what is actually thrown when we call panic(nil)
	nilPanic := func() (p any) {
		defer func() {
			p = recover()
		}()
		panic(nil)
	}()
	if nilPanic != nil {
		// We are probably on go1.21 or later, where panic(nil) is transformed
		// into a runtime.PanicNilError.
		assert.Equal(t,
			"PanicNilError",
			reflect.TypeOf(nilPanic).Elem().Name())
		return
	}

	t.Parallel()
	g := Unlimited(context.Background())
	g.Go(func(context.Context) {
		// Panics that are literally `nil` should also be caught, even though
		// they aren't detectable without some trickery (prior to go1.21)
		panic(nil)
	})
	assert.PanicsWithValue(t, nil, g.Wait)
}
