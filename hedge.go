package parallel

import (
	"context"
	"time"
)

type HedgedRequestConfig struct {
	delay                  time.Duration // time to wait before issuing hedged requests
	maxOutstandingRequests int           // the maximum permitted number of outstanding requests
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

func WithMaxOutstandingRequests(maxOutstandingRequests int) HedgedRequestOpt {
	return func(config *HedgedRequestConfig) {
		config.maxOutstandingRequests = maxOutstandingRequests
	}
}

func HedgedRequest[T any](
	ctx context.Context,
	requester func(context.Context) (T, error),
	opts ...HedgedRequestOpt,
) (T, error) {
	cfg := HedgedRequestConfig{
		delay:                  50 * time.Millisecond,
		maxOutstandingRequests: 3,
	}
	cfg.Apply(opts...)

}
