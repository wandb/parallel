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

	// This demonstrates the library's cleanup functionality, where forgotten
	// executors that own goroutines and have been discarded without being awaited
	// will clean themselves up without permanently leaking goroutines. This is
	// part of the library's panic safety test suite.
	//
	// To see the counterexample of this test's success, comment out the contents
	// of the thunks that are registered with `runtime.SetFinalizer()` in
	// group.go and collect.go, which close channels and call cancel functions.

	println("leaking ~", cycles*(batchSize+2), "goroutines from abandoned group executors...")

	// Leak just a crazy number of goroutines
	for i := 0; i < cycles; i++ {
		func() {
			g := parallel.Collect[int](parallel.Limited(context.Background(), batchSize))
			for j := 0; j < batchSize; j++ {
				g.Go(func(context.Context) (int, error) {
					return 1, nil
				})
			}
			// Leak the group without awaiting it
		}()

		func() {
			defer func() { _ = recover() }()
			g := parallel.Feed(parallel.Unlimited(context.Background()), func(context.Context, int) error {
				panic("feed function panics")
			})
			g.Go(func(context.Context) (int, error) {
				return 1, nil
			})
			// Leak the group without awaiting it
		}()

		func() {
			defer func() { _ = recover() }()
			g := parallel.Collect[int](parallel.Unlimited(context.Background()))
			g.Go(func(context.Context) (int, error) {
				panic("op panics")
			})
			// Leak the group without awaiting it
		}()
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
