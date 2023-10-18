package parallel_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wandb/parallel"
)

func TestMultiErrorMessage(t *testing.T) {
	assert.Equal(t,
		parallel.NewMultiError(),
		nil,
	)
	assert.Equal(t,
		parallel.NewMultiError(
			errors.New("foo"),
		).Error(),
		"foo",
	)
	assert.Equal(t,
		parallel.NewMultiError(
			errors.New("foo"),
			errors.New("bar"),
		).Error(),
		"foo\nbar",
	)
	assert.Equal(t,
		parallel.NewMultiError(
			errors.New("foo"),
			errors.New("bar"),
			nil,
			errors.New("baz"),
		).Error(),
		"foo\nbar\nbaz",
	)
	// Naively created MultiErrors with nils in them should still contain those
	// nils; only Combine tries to strip those out.
	assert.Equal(t,
		parallel.NewMultiError(
			errors.New("foo"),
			errors.New("bar"),
			nil,
			errors.New("baz"),
		).Unwrap(),
		[]error{
			errors.New("foo"),
			errors.New("bar"),
			nil,
			errors.New("baz"),
		},
	)
}

func TestMultiErrorOne(t *testing.T) {
	assert.Equal(t,
		parallel.NewMultiError(
			errors.New("foo"),
		).One(),
		errors.New("foo"),
	)
	assert.Equal(t,
		parallel.NewMultiError(
			errors.New("foo"),
			errors.New("bar"),
		).One(),
		errors.New("foo"),
	)
}

func TestCombineErrors(t *testing.T) {
	// Nils shouldn't produce an error value
	assert.Equal(t,
		parallel.CombineErrors(),
		nil,
	)
	assert.Equal(t,
		parallel.CombineErrors(nil, nil),
		nil,
	)
	// Some errors should produce multierrors containing them
	assert.Equal(t,
		parallel.CombineErrors(
			nil,
			errors.New("foo"),
		).Unwrap(),
		[]error{errors.New("foo")},
	)
	assert.Equal(t,
		parallel.CombineErrors(
			nil,
			errors.New("foo"),
			errors.New("bar"),
		).Unwrap(),
		[]error{
			errors.New("foo"),
			errors.New("bar"),
		},
	)
	// Should unwrap MultiErrors
	assert.Equal(t,
		parallel.CombineErrors(
			parallel.NewMultiError(
				errors.New("foo"),
				errors.New("bar"),
			),
			nil,
			errors.New("baz"),
		).Unwrap(),
		[]error{
			errors.New("foo"),
			errors.New("bar"),
			errors.New("baz"),
		},
	)
}
