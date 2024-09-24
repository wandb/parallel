package main

import (
	"context"
	"os"
	"runtime"
	"time"

	"github.com/wandb/parallel"
)

func main() {
	const cycles = 100
	const batchSize = 100

	ctx := context.Background()

	// This demonstrates the library's cleanup functionality, where forgotten
	// executors that own goroutines and have been discarded without being awaited
	// will clean themselves up without permanently leaking goroutines. This is
	// part of the library's panic safety test suite.
	//
	// To see the counterexample of this test's success, comment out the contents
	// of the thunks that are registered with `runtime.SetFinalizer()` in
	// group.go and collect.go, which close channels and call cancel functions.

	println("leaking goroutines from group executors...")

	// Dependent contexts are always canceled, too
	leakDependent := func(ctx context.Context) {
		ctx, cancel := context.WithCancel(ctx)
		_ = cancel
		go func() {
			<-ctx.Done()
		}()
	}

	// Leak just a crazy number of goroutines
	for i := 0; i < cycles; i++ {
		func() {
			g := parallel.Collect[int](parallel.Limited(ctx, batchSize))
			for j := 0; j < batchSize; j++ {
				g.Go(func(ctx context.Context) (int, error) {
					leakDependent(ctx)
					return 1, nil
				})
			}
			// Leak the group without awaiting it
		}()

		func() {
			defer func() { _ = recover() }()
			g := parallel.Feed(parallel.Unlimited(ctx), func(context.Context, int) error {
				panic("feed function panics")
			})
			g.Go(func(ctx context.Context) (int, error) {
				leakDependent(ctx)
				return 1, nil
			})
			// Leak the group without awaiting it
		}()

		func() {
			defer func() { _ = recover() }()
			g := parallel.Collect[int](parallel.Unlimited(ctx))
			g.Go(func(ctx context.Context) (int, error) {
				leakDependent(ctx)
				panic("op panics")
			})
			// Leak the group without awaiting it
		}()

		// Start some executors that complete normally without error
		{
			g := parallel.Unlimited(ctx)
			g.Go(func(ctx context.Context) {
				leakDependent(ctx)
			})
			g.Wait()
		}
		{
			g := parallel.Limited(ctx, 0)
			g.Go(func(ctx context.Context) {
				leakDependent(ctx)
			})
			g.Wait()
		}
		{
			g := parallel.Collect[int](parallel.Limited(ctx, batchSize))
			g.Go(func(ctx context.Context) (int, error) {
				return 1, nil
			})
			_, err := g.Wait()
			if err != nil {
				panic(err)
			}
		}
		{
			g := parallel.Feed(parallel.Unlimited(ctx), func(context.Context, int) error {
				return nil
			})
			g.Go(func(ctx context.Context) (int, error) {
				leakDependent(ctx)
				return 1, nil
			})
			err := g.Wait()
			if err != nil {
				panic(err)
			}
		}
	}

	println("monitoring and running GC...")

	numGoroutines := runtime.NumGoroutine()
	lastGoroutineCount := numGoroutines
	noProgressFor := 0
	for {
		println("number of goroutines:", numGoroutines)
		if numGoroutines == 1 {
			break
		}

		if numGoroutines >= lastGoroutineCount {
			noProgressFor++ // no progress was made
			if noProgressFor > 3 {
				println("GC is not making progress! :(")
				os.Exit(1)
			}
			// Don't update lastGoroutineCount if the value went *up* (why would it
			// do that? no idea, but might as well guard against it)
		} else {
			noProgressFor = 0 // progress was made
			lastGoroutineCount = numGoroutines
		}

		<-time.After(50 * time.Millisecond)
		runtime.GC() // keep looking for garbage
		numGoroutines = runtime.NumGoroutine()
	}
	println("tadaa! \U0001f389")
	os.Exit(0)
}
