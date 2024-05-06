package parallel

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHedgedRequestBasic(t *testing.T) {
	ctx := context.Background()
	count := 0
	expectedResult := "success"
	requester := func(ctx context.Context) (string, error) {
		count++
		if count == 1 {
			return expectedResult, nil
		} else {
			return "fail", fmt.Errorf("should not trigger hedged request")
		}
	}

	result, err := HedgedRequest[string](ctx, requester)

	require.NoError(t, err)
	assert.Equal(t, expectedResult, result)
}

func TestHedgedRequestHedgingTriggered(t *testing.T) {
	ctx := context.Background()
	count := 0
	delay := 50 * time.Millisecond
	expectedResult := "success"
	requester := func(ctx context.Context) (string, error) {
		count++
		if count == 0 {
			select {
			case <-time.After(2 * delay):
				return "fail", fmt.Errorf("original request slow")
			case <-ctx.Done():
				return "fail", ctx.Err()
			}
		} else {
			return "success", nil
		}
	}

	result, err := HedgedRequest[string](ctx, requester, WithDelay(delay), WithNumHedgedRequests(1))

	require.NoError(t, err)
	assert.Equal(t, expectedResult, result)
}

func TestHedgedRequestErrorPropagation(t *testing.T) {
	ctx := context.Background()
	expectedError := errors.New("failure")
	requester := func(ctx context.Context) (string, error) {
		return "fail", expectedError
	}

	_, err := HedgedRequest[string](ctx, requester)

	require.ErrorIs(t, err, expectedError)
}

func TestHedgedRequestCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	delay := 100 * time.Millisecond
	requester := func(ctx context.Context) (string, error) {
		<-time.After(delay)
		return "fail", fmt.Errorf("context should be canceled")
	}

	go func() {
		// Cancel context before the requester completes
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	_, err := HedgedRequest[string](ctx, requester)
	require.ErrorIs(t, err, context.Canceled)
}
