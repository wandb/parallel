package parallel

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGroup(t *testing.T) {
	for _, test := range []struct {
		name     string
		makeExec func(context.Context) Executor
	}{
		{"Unlimited", Unlimited},
		{"Limited", func(ctx context.Context) Executor { return Limited(ctx, 10) }},
		{"serial", func(ctx context.Context) Executor { return Limited(ctx, 0) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			testGroup(t, test.makeExec)
		})
	}
}

func testGroup(t *testing.T, makeExec func(context.Context) Executor) {
	t.Parallel()
	t.Run("do nothing", func(t *testing.T) {
		t.Parallel()
		g := makeExec(context.Background())
		g.Wait()
	})
	t.Run("do nothing canceled", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		g := makeExec(ctx)
		cancel()
		g.Wait()
	})
	t.Run("sum 100", func(t *testing.T) {
		t.Parallel()
		var counter int64
		g := makeExec(context.Background())
		for i := 0; i < 100; i++ {
			g.Go(func(context.Context) {
				atomic.AddInt64(&counter, 1)
			})
		}
		g.Wait()
		assert.Equal(t, int64(100), counter)
	})
	t.Run("sum canceled", func(t *testing.T) {
		t.Parallel()
		var counter int64
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		g := makeExec(ctx)
		for i := 0; i < 100; i++ {
			if i == 50 {
				cancel()
			}
			g.Go(func(context.Context) {
				atomic.AddInt64(&counter, 1)
			})
		}
		g.Wait()
		// Work submitted after the context has been canceled does not happen.
		// We cannot guarantee that the counter isn't less than 50, because some
		// of the original 50 work units might not have started yet. We also
		// cannot guarantee that the counter isn't *more* than 50 because in the
		// limited executor, some of the worker functions may select a work item
		// instead of seeing the done signal on their final loop.
		var maxSum int64 = 50
		if lg, ok := g.(*limitedGroup); ok {
			maxSum += int64(lg.max) // limitedGroup may run up to 1 more per worker
		}
		assert.LessOrEqual(t, counter, maxSum)
	})
	t.Run("wait multiple times", func(t *testing.T) {
		t.Parallel()
		g := makeExec(context.Background())
		assert.NotPanics(t, g.Wait)
		assert.NotPanics(t, g.Wait)
	})
}

func testLimitedGroupMaxConcurrency(t *testing.T, name string, g Executor, limit int, shouldSucceed bool) {
	// Testing that some process can work with *at least* N parallelism is easy:
	// we run N jobs that cannot make progress, and unblock them when they have
	// all arrived at that blocker.
	//
	// Coming up with a way to validate that something runs with *NO MORE THAN*
	// N parallelism is HARD.
	//
	// We can't just time.Sleep and wait for everything to catch up, because
	// that simply isn't how concurrency works, especially in test environments:
	// there's no amount of time we can choose that will actually guarantee
	// another thread has caught up. So instead, we first assert that exactly N
	// jobs are running in the executor in parallel, and then we insert lots and
	// lots of poison pills into the work queue and *footrace* with any other
	// worker threads that might have started that could be trying to run jobs,
	// while also reaching under the hood and discarding those work units
	// ourselves. Golang channels are sufficiently fair such that if there are
	// multiple waiters all of them will get at least *some* of the items in the
	// channel eventually, which gives us a very high probability that any such
	// worker will choke on a poison pill if it exists.
	t.Run(name, func(t *testing.T) {
		t.Parallel()
		var blocker, barrier sync.WaitGroup
		// Blocker stops the workers from progressing
		blocker.Add(1)
		// Barrier lets us know when all the workers have arrived. If this
		// test hangs, probably it's because not enough workers started.
		barrier.Add(limit)

		jobInserter := Unlimited(context.Background())
		jobInserter.Go(func(context.Context) {
			// We fully loop over the ops channel in the test to empty it. The
			// channel is only closed when the group is awaited or forgotten but
			// not when it panics, and just guaranteeing we await it takes the
			// least code, so we do that.
			defer g.Wait()

			for i := 0; i < limit; i++ {
				g.Go(func(context.Context) {
					barrier.Done()
					blocker.Wait()
				})
			}

			// Now we insert a whole buttload of jobs that should never be picked
			// up and run by the executor. We will go through and consume these
			// from the channel ourselves in the main thread, but if there were
			// any workers taking from that channel chances are they would get
			// and run at least one of these jobs, failing the test.
			for i := 0; i < 10000; i++ {
				g.Go(func(context.Context) {
					panic("poison pill")
				})
			}

			g.Wait()
		})
		barrier.Wait()
		// All the workers we *expect* to see have shown up now. Throw away all
		// the poison pills in the ops queue
		for poisonPill := range g.(*limitedGroup).ops {
			time.Sleep(0) // Trigger preemption as much as we can
			assert.NotNil(t, poisonPill)
			time.Sleep(0) // Trigger preemption as much as we can
		}
		blocker.Done() // unblock the workers
		if shouldSucceed {
			assert.NotPanics(t, jobInserter.Wait)
		} else {
			assert.PanicsWithValue(t, "poison pill", jobInserter.Wait)
		}
	})
}

func TestLimitedGroupMaxConcurrency(t *testing.T) {
	t.Parallel()
	testLimitedGroupMaxConcurrency(t, "100", Limited(context.Background(), 100), 100, true)
	testLimitedGroupMaxConcurrency(t, "50", Limited(context.Background(), 50), 50, true)
	testLimitedGroupMaxConcurrency(t, "5", Limited(context.Background(), 5), 5, true)
	testLimitedGroupMaxConcurrency(t, "1", Limited(context.Background(), 1), 1, true)
	// Validate the test
	testLimitedGroupMaxConcurrency(t, "fail", Limited(context.Background(), 6), 5, false)
}
