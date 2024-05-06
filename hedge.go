package parallel

import (
	"context"
	"time"
)

type HedgedRequestConfig struct {
	delay             time.Duration // time to wait before issuing hedged requests
	numHedgedRequests int           // the maximum permitted number of outstanding hedged requests
}

func (h *HedgedRequestConfig) Apply(opts ...HedgedRequestOpt) {
	for _, opt := range opts {
		opt(h)
	}
}

type HedgedRequestOpt func(config *HedgedRequestConfig)

func WithDelay(delay time.Duration) HedgedRequestOpt {
	return func(config *HedgedRequestConfig) {
		config.delay = delay
	}
}

func WithNumHedgedRequests(numHedgedRequests int) HedgedRequestOpt {
	return func(config *HedgedRequestConfig) {
		config.numHedgedRequests = numHedgedRequests
	}
}

func HedgedRequest[T any](
	ctx context.Context,
	requester func(context.Context) (T, error),
	opts ...HedgedRequestOpt,
) (T, error) {
	cfg := HedgedRequestConfig{
		delay:             50 * time.Millisecond,
		numHedgedRequests: 2,
	}
	cfg.Apply(opts...)

	hedgeSignal := make(chan struct{}) // closed when hedged requests should fire
	responses := make(chan T)          // unbuffered, we only expect one response
	ctx, cancel := context.WithCancelCause(ctx)

	group := ErrGroup(Limited(ctx, cfg.numHedgedRequests+1))
	for i := 0; i < cfg.numHedgedRequests+1; i++ {
		i := i
		group.Go(func(ctx context.Context) (rerr error) {
			defer func() {
				cancel(rerr)
			}()

			if i == 0 {
				// Initial request case: if this does not complete within the hedge delay, we signal the
				// hedge requests to fire off.
				time.AfterFunc(cfg.delay, func() {
					close(hedgeSignal)
				})
			} else {
				// Hedged request case: wait for the go-ahead for hedged requests first.
				select {
				case <-ctx.Done():
					return context.Cause(ctx)
				case <-hedgeSignal:
					// good to proceed
				}
			}

			res, err := requester(ctx)
			if err != nil {
				return err
			}

			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case responses <- res:
				return nil
			}
		})
	}

	go func() {
		_ = group.Wait()
		close(responses)
	}()

	for response := range responses {
		return response, nil
	}

	var empty T
	return empty, context.Cause(ctx)
}
