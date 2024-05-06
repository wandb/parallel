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
	expected := "success"
	requester := func(ctx context.Context) (string, error) {
		count++
		if count == 1 {
			return expected, nil
		} else {
			return "fail", fmt.Errorf("should not trigger hedged request")
		}
	}

	actual, err := HedgedRequest[string](ctx, requester)

	require.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func TestHedgedRequestHedgingTriggered(t *testing.T) {
	ctx := context.Background()
	count := 0
	delay := 50 * time.Millisecond
	expected := "success"
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
			return expected, nil
		}
	}

	actual, err := HedgedRequest[string](ctx, requester, WithDelay(delay), WithNumHedgedRequests(1))

	require.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func TestHedgedRequestMultipleSuccess(t *testing.T) {
	ctx := context.Background()
	expected := "success"
	delay := 5 * time.Millisecond
	done := make(chan struct{})
	requester := func(ctx context.Context) (string, error) {
		// Synchronize on the done channel. The original request and
		// all hedged requests will line up and block here.
		select {
		case <-done:
			return expected, nil
		case <-ctx.Done():
			return "fail", ctx.Err()
		}
	}

	// Wait for 2x the delay to ensure that all hedged requests have
	// fired alongside the original request. Then close the done channel
	// so the original and hedged requests complete almost simultaneously.
	time.AfterFunc(2*delay, func() {
		close(done)
	})

	actual, err := HedgedRequest[string](
		ctx,
		requester,
		WithDelay(delay),
	)

	require.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func TestHedgedRequestErrorPropagation(t *testing.T) {
	ctx := context.Background()
	expectedErr := errors.New("failure")
	requester := func(ctx context.Context) (string, error) {
		return "fail", expectedErr
	}

	_, err := HedgedRequest[string](ctx, requester)

	require.ErrorIs(t, err, expectedErr)
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
