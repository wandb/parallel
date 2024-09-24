package parallel

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrGroup(t *testing.T) {
	for _, test := range []struct {
		name     string
		makeExec func(context.Context) Executor
	}{
		{"Unlimited", Unlimited},
		{"Limited", func(ctx context.Context) Executor { return Limited(ctx, 10) }},
		{"serial", func(ctx context.Context) Executor { return Limited(ctx, 0) }},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			testErrGroup(t, test.makeExec)
		})
	}
}

func testErrGroup(t *testing.T, makeExec func(context.Context) Executor) {
	t.Parallel()
	t.Run("nothing", func(t *testing.T) {
		t.Parallel()
		g := ErrGroup(makeExec(context.Background()))
		assert.NoError(t, g.Wait())
	})
	t.Run("some", func(t *testing.T) {
		t.Parallel()
		g := ErrGroup(makeExec(context.Background()))
		flag := 0
		g.Go(func(context.Context) error {
			flag = 1
			return nil
		})
		assert.NoError(t, g.Wait())
		assert.Equal(t, 1, flag)

	})
	t.Run("failing", func(t *testing.T) {
		t.Parallel()
		g := ErrGroup(makeExec(context.Background()))
		g.Go(func(context.Context) error {
			return errors.New("failed")
		})
		assert.Errorf(t, g.Wait(), "failed")
	})
	t.Run("canceled", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		g := ErrGroup(makeExec(ctx))
		cancel()
		g.Go(func(context.Context) error {
			return errors.New("failed")
		})
		// context cancelation overrides other errors as long as the group isn't
		// stopped by an error first
		assert.ErrorIs(t, g.Wait(), context.Canceled)
	})
}

func TestCollectNothing(t *testing.T) {
	t.Parallel()
	g := Collect[int](Unlimited(context.Background()))
	res, err := g.Wait()
	assert.NoError(t, err)
	assert.Nil(t, res)
}

func TestCollectSome(t *testing.T) {
	t.Parallel()
	g := Collect[int](Unlimited(context.Background()))
	g.Go(func(context.Context) (int, error) { return 1, nil })
	g.Go(func(context.Context) (int, error) { return 1, nil })
	g.Go(func(context.Context) (int, error) { return 1, nil })
	res, err := g.Wait()
	assert.NoError(t, err)
	assert.Equal(t, []int{1, 1, 1}, res)
}

func TestCollectFailed(t *testing.T) {
	t.Parallel()
	g := Collect[int](Unlimited(context.Background()))
	g.Go(func(context.Context) (int, error) { return 1, nil })
	g.Go(func(context.Context) (int, error) { return 1, nil })
	g.Go(func(context.Context) (int, error) { return 1, errors.New("nvm") })
	res, err := g.Wait()
	assert.Errorf(t, err, "nvm")
	assert.Nil(t, res)
}

func TestCollectCanceled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	g := Collect[int](Unlimited(ctx))
	cancel()
	g.Go(func(context.Context) (int, error) { return 1, errors.New("nvm") })
	g.Go(func(context.Context) (int, error) { return 1, nil })
	g.Go(func(context.Context) (int, error) { return 1, nil })
	res, err := g.Wait()
	// context cancellation overrides other errors as long as the group isn't
	// stopped by an error first
	assert.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, res)
}

func TestFeedNothing(t *testing.T) {
	t.Parallel()
	g := Feed[int](Unlimited(context.Background()), func(context.Context, int) error {
		t.Fatal("never runs")
		return nil
	})
	assert.NoError(t, g.Wait())
}

func TestFeedSome(t *testing.T) {
	t.Parallel()
	res := make(map[int]bool)
	g := Feed(Unlimited(context.Background()), func(ctx context.Context, val int) error {
		res[val] = true
		return nil
	})
	g.Go(func(context.Context) (int, error) { return 1, nil })
	g.Go(func(context.Context) (int, error) { return 2, nil })
	g.Go(func(context.Context) (int, error) { return 3, nil })
	assert.NoError(t, g.Wait())
	assert.Equal(t, map[int]bool{1: true, 2: true, 3: true}, res)
}

func TestFeedErroring(t *testing.T) {
	t.Parallel()
	var res []int
	g := Feed(Unlimited(context.Background()), func(ctx context.Context, val int) error {
		res = append(res, val)
		return nil
	})
	g.Go(func(context.Context) (int, error) { return 1, nil })
	g.Go(func(context.Context) (int, error) { return 2, nil })
	g.Go(func(context.Context) (int, error) { return 3, nil })
	g.Go(func(context.Context) (int, error) { return 4, errors.New("oops") })
	assert.Errorf(t, g.Wait(), "oops")
	assert.Subset(t, []int{1, 2, 3}, res)
}

func TestFeedLastReceiverErrs(t *testing.T) {
	t.Parallel()
	// Even when the very very last item through the pipe group causes an error,
	// the group's context shouldn't be canceled yet and it should still be able
	// to set the error.
	g := Feed(Limited(context.Background(), 0), func(ctx context.Context, val int) error {
		if val == 10 {
			return errors.New("boom")
		} else {
			return nil
		}
	})
	for i := 1; i <= 10; i++ {
		g.Go(func(ctx context.Context) (int, error) {
			return i, nil
		})
	}
	require.Error(t, g.Wait())
}

func TestFeedErroringInReceiver(t *testing.T) {
	t.Parallel()
	g := Feed(Unlimited(context.Background()), func(ctx context.Context, val int) error {
		if val%2 == 1 {
			return errors.New("odd numbers are unacceptable")
		}
		return nil
	})
	for i := 0; i < 100; i++ {
		i := i
		g.Go(func(context.Context) (int, error) { return i, nil })
	}
	assert.Errorf(t, g.Wait(), "odd numbers are unacceptable")
}

func TestFeedCanceled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g := Feed(Unlimited(ctx), func(ctx context.Context, val int) error {
		return errors.New("error from receiver")
	})
	cancel()
	g.Go(func(context.Context) (int, error) { return 1, errors.New("error from work") })
	g.Go(func(context.Context) (int, error) { return 2, nil })
	// context cancelation overrides other errors as long as the group isn't
	// stopped by an error first.
	assert.ErrorIs(t, g.Wait(), context.Canceled)
}

func TestGatherErrNothing(t *testing.T) {
	t.Parallel()
	g := GatherErrs(Unlimited(context.Background()))
	assert.NoError(t, g.Wait())
}

func TestGatherNoErrs(t *testing.T) {
	t.Parallel()
	var res int64
	g := GatherErrs(Unlimited(context.Background()))
	g.Go(func(context.Context) error {
		atomic.AddInt64(&res, 1)
		return nil
	})
	g.Go(func(context.Context) error {
		atomic.AddInt64(&res, 1)
		return nil
	})
	g.Go(func(context.Context) error {
		atomic.AddInt64(&res, 1)
		return nil
	})
	assert.NoError(t, g.Wait())
	assert.Equal(t, int64(3), res)
}

func TestGatherErrSome(t *testing.T) {
	t.Parallel()
	// Use a dummy executor so we ensure these run in order
	g := GatherErrs(Limited(context.Background(), 0))
	flag := 0
	g.Go(func(context.Context) error {
		return errors.New("oh no")
	})
	g.Go(func(context.Context) error {
		flag = 1
		return nil
	})
	g.Go(func(context.Context) error {
		return NewMultiError(errors.New("another one"), errors.New("even more"))
	})
	err := g.Wait()
	assert.Errorf(t, err, "oh no\nanother one\neven more")
	assert.Equal(t, []error{
		errors.New("oh no"),
		errors.New("another one"),
		errors.New("even more"),
	}, err.Unwrap())
	assert.Equal(t, 1, flag)
}

func TestCollectWithErrsNothing(t *testing.T) {
	t.Parallel()
	g := CollectWithErrs[int](Unlimited(context.Background()))
	res, err := g.Wait()
	assert.NoError(t, err)
	assert.Nil(t, res)
}

func TestCollectWithErrsSome(t *testing.T) {
	t.Parallel()
	// Use a dummy executor so we ensure these run in order
	g := CollectWithErrs[int](Limited(context.Background(), 0))
	g.Go(func(context.Context) (int, error) {
		return 0, errors.New("oh no")
	})
	g.Go(func(context.Context) (int, error) {
		return 1, nil
	})
	g.Go(func(context.Context) (int, error) {
		return 2, nil
	})
	g.Go(func(context.Context) (int, error) {
		return 3, NewMultiError(errors.New("more"), errors.New("yet more"))
	})
	res, err := g.Wait()
	assert.Equal(t, []int{1, 2}, res)
	assert.Errorf(t, err, "oh no\nmore\nyet more")
	// Multierrors get flattened :)
	assert.Equal(t, []error{
		errors.New("oh no"),
		errors.New("more"),
		errors.New("yet more"),
	}, err.Unwrap())
}

func TestFeedWithErrsNothing(t *testing.T) {
	t.Parallel()
	g := FeedWithErrs(Unlimited(context.Background()), func(context.Context, int) error {
		return nil
	})
	assert.NoError(t, g.Wait())
}

func TestFeedWithErrsSome(t *testing.T) {
	t.Parallel()
	res := make(map[int]bool)
	// Use a dummy executor so we ensure these run in order
	g := FeedWithErrs(Limited(context.Background(), 0), func(ctx context.Context, val int) error {
		res[val] = true
		return nil
	})
	g.Go(func(context.Context) (int, error) {
		return 0, errors.New("oh no")
	})
	g.Go(func(context.Context) (int, error) {
		return 1, nil
	})
	g.Go(func(context.Context) (int, error) {
		return 2, nil
	})
	g.Go(func(context.Context) (int, error) {
		return 3, NewMultiError(errors.New("more"), errors.New("yet more"))
	})
	err := g.Wait()
	assert.Equal(t, map[int]bool{1: true, 2: true}, res)
	assert.Errorf(t, err, "oh no\nmore\nyet more")
	// Multierrors get flattened :)
	assert.Equal(t, []error{
		errors.New("oh no"),
		errors.New("more"),
		errors.New("yet more"),
	}, err.Unwrap())
}

func TestFeedWithErrsInReceiver(t *testing.T) {
	t.Parallel()
	var res []int
	// Use a dummy executor so we ensure these run in order
	g := FeedWithErrs(Limited(context.Background(), 0), func(ctx context.Context, val int) error {
		if val%5 == 0 {
			return errors.New("buzz")
		}
		res = append(res, val)
		return nil
	})
	for i := 1; i <= 10; i++ {
		i := i
		g.Go(func(context.Context) (int, error) {
			if i%3 == 0 {
				return 0, errors.New("fizz")
			}
			return i, nil
		})
	}
	err := g.Wait()
	assert.Equal(t, []int{1, 2, 4, 7, 8}, res)
	assert.Error(t, err)
	assert.Equal(t, []error{
		errors.New("fizz"),
		errors.New("buzz"),
		errors.New("fizz"),
		errors.New("fizz"),
		errors.New("buzz"),
	}, err.Unwrap())
}

func TestMultipleUsageOfExecutor(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name     string
		executor Executor
	}{
		{"group", Unlimited(context.Background())},
		{"limited", Limited(context.Background(), 10)},
		{"serial", Limited(context.Background(), 0)},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			collector := Collect[int](testCase.executor)
			feedResult := make(map[string]bool)
			feeder := Feed(testCase.executor, func(ctx context.Context, val string) error {
				feedResult[val] = true
				return nil
			})
			errorer := GatherErrs(testCase.executor)
			sender := Unlimited(context.Background())
			sender.Go(func(context.Context) {
				collector.Go(func(context.Context) (int, error) {
					return 1, nil
				})
				collector.Go(func(context.Context) (int, error) {
					return 1, nil
				})
				collector.Go(func(context.Context) (int, error) {
					return 1, nil
				})
			})
			sender.Go(func(context.Context) {
				feeder.Go(func(context.Context) (string, error) {
					return "abc", nil
				})
				feeder.Go(func(context.Context) (string, error) {
					return "foo", nil
				})
				feeder.Go(func(context.Context) (string, error) {
					return "bar", nil
				})
			})
			sender.Go(func(context.Context) {
				errorer.Go(func(context.Context) error {
					return nil
				})
				errorer.Go(func(context.Context) error {
					return errors.New("kaboom")
				})
			})
			sender.Wait()
			collected, err := collector.Wait()
			assert.NoError(t, err)
			assert.Equal(t, []int{1, 1, 1}, collected)
			assert.NoError(t, feeder.Wait())
			assert.Equal(t, map[string]bool{"abc": true, "foo": true, "bar": true}, feedResult)
			assert.Errorf(t, errorer.Wait(), "kaboom")
		})
	}
}

func TestWaitPipeGroupMultipleTimes(t *testing.T) {
	t.Parallel()
	g := Feed(Unlimited(context.Background()), func(context.Context, int) error { return nil })
	assert.NotPanics(t, func() { assert.NoError(t, g.Wait()) })
	assert.NotPanics(t, func() { assert.NoError(t, g.Wait()) })
}
