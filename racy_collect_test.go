//go:build !race

package parallel

import (
	"context"
	"errors"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

// The tests in this file are detected as racy by the race condition checker
// because we are reaching under the hood to look at the group's result (see
// "Hack" comments) so we can see when the group's functions have started
// running. There's no good reason make those fields otherwise accessible, since
// they are completely owned by the group and making this work in a "non-racy"
// way would require extra complexity and overhead.

func TestGatherErrCanceled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	g := GatherErrs(Unlimited(ctx))
	g.Go(func(context.Context) error {
		return errors.New("shadowed")
	})
	// Hack: wait for the error to be definitely collected
	for len(*g.(multiErrGroup).res) == 0 {
		runtime.Gosched()
	}
	cancel()
	err := g.Wait()
	assert.Errorf(t, err, "context canceled\nshadowed")
	// Because MultiError implements Unwrap() []error, errors.Is() will detect
	// context.Canceled
	assert.ErrorIs(t, err, context.Canceled)
	// Canceled will be the first error
	assert.Equal(t, []error{context.Canceled, errors.New("shadowed")}, err.Unwrap())
}

func TestCollectWithErrsCanceled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	// Use a dummy executor so we ensure these run in order
	g := CollectWithErrs[int](Unlimited(ctx))
	g.Go(func(context.Context) (int, error) {
		return 0, errors.New("oh no")
	})
	// Hack: wait for the error to be definitely collected
	for len(g.(collectingMultiErrGroup[int]).res.errs) == 0 {
		runtime.Gosched()
	}
	cancel()
	_, err := g.Wait()
	// Because MultiError implements Unwrap() []error, errors.Is() will detect
	// context.Canceled
	assert.ErrorIs(t, err, context.Canceled)
	// Canceled will be the first error
	assert.Equal(t, []error{context.Canceled, errors.New("oh no")}, err.Unwrap())
}

func TestFeedWithErrsCanceled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	// Use a dummy executor so we ensure these run in order
	g := FeedWithErrs[int](Unlimited(ctx), func(context.Context, int) error { return nil })
	g.Go(func(context.Context) (int, error) {
		return 0, errors.New("oh no")
	})
	// Hack: wait for the error to be definitely collected
	for len(*g.(feedingMultiErrGroup[int]).res) == 0 {
		runtime.Gosched()
	}
	cancel()
	err := g.Wait()
	// Because MultiError implements Unwrap() []error, errors.Is() will detect
	// context.Canceled
	assert.ErrorIs(t, err, context.Canceled)
	// Canceled will be the first error
	assert.Equal(t, []error{context.Canceled, errors.New("oh no")}, err.Unwrap())
}
