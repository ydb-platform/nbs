package errors

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

////////////////////////////////////////////////////////////////////////////////

func TestRetriableAndNonRetriableErrorsShouldUnwrapCorrectly(t *testing.T) {
	err := assert.AnError

	assert.True(
		t,
		Is(
			NewNonRetriableError(NewRetriableError(err)),
			NewEmptyNonRetriableError(),
		),
	)
	assert.True(
		t,
		Is(
			NewNonRetriableError(NewRetriableError(err)),
			err,
		),
	)
	assert.True(
		t,
		Is(
			NewNonRetriableError(NewRetriableError(err)),
			NewEmptyRetriableError(),
		),
	)
	assert.True(
		t,
		Is(
			NewRetriableError(NewNonRetriableError(err)),
			NewEmptyNonRetriableError(),
		),
	)
	assert.True(
		t,
		Is(
			NewRetriableError(NewNonRetriableError(err)),
			NewEmptyNonRetriableError(),
		),
	)
	assert.True(
		t,
		Is(
			NewRetriableError(NewNonRetriableError(err)),
			err,
		),
	)
	assert.True(
		t,
		Is(
			NewNonRetriableError(NewNonCancellableError(err)),
			NewEmptyNonCancellableError(),
		),
	)

	assert.False(
		t,
		Is(
			NewNonRetriableError(NewNonRetriableError(err)),
			NewEmptyRetriableError(),
		),
	)
	assert.False(
		t,
		Is(
			NewRetriableError(NewRetriableError(err)),
			NewEmptyNonRetriableError(),
		),
	)
	assert.False(
		t,
		Is(
			NewNonRetriableError(NewRetriableErrorf("other error")),
			err,
		),
	)
	assert.False(
		t,
		Is(
			NewNonRetriableError(NewRetriableError(err)),
			NewEmptyNonCancellableError(),
		),
	)
}

func TestFoundErrorShouldUnwrapCorrectly(t *testing.T) {
	assert.True(
		t,
		Is(
			NewNonRetriableError(NewEmptyNotFoundError()),
			NewEmptyNotFoundError(),
		),
	)
	assert.True(
		t,
		Is(
			NewNonRetriableError(NewNotFoundErrorWithTaskID("id")),
			NewEmptyNotFoundError(),
		),
	)
}

func TestObtainDetailsFromDetailedError(t *testing.T) {
	details := &ErrorDetails{
		Code:     1,
		Message:  "message",
		Internal: true,
	}

	e := NewEmptyDetailedError()

	// Details should be obtained correctly even if DetailedError is wrapped
	// into some other error.
	assert.True(
		t,
		As(
			NewNonRetriableError(NewDetailedError(assert.AnError, details)),
			&e,
		),
	)

	assert.Equal(t, assert.AnError, e.Err)
	assert.Equal(t, details, e.Details)
}

func TestCanRetry(t *testing.T) {
	err := assert.AnError

	assert.True(t, CanRetry(NewRetriableError(err)))

	assert.False(t, CanRetry(err))
	assert.False(t, CanRetry(NewRetriableError(NewNonCancellableError(err))))
	assert.False(t, CanRetry(NewRetriableError(NewNonRetriableError(err))))
}

func TestNonRetriableErrorSilent(t *testing.T) {
	err := NewSilentNonRetriableError(assert.AnError)

	e := NewEmptyNonRetriableError()
	assert.True(t, As(err, &e))
	assert.True(t, e.Silent)
}

func TestNonRetriableErrorSilentUnwrapsCorrectly(t *testing.T) {
	err := NewRetriableError(NewSilentNonRetriableError(assert.AnError))

	e := NewEmptyNonRetriableError()
	assert.True(t, As(err, &e))
	assert.True(t, e.Silent)
}

func TestRetriableErrorIgnoreRetryLimitUnwrapsCorrectly(t *testing.T) {
	err := NewRetriableErrorWithIgnoreRetryLimit(
		NewNonRetriableError(assert.AnError),
	)

	e := NewEmptyRetriableError()
	assert.True(t, As(err, &e))
	assert.True(t, e.IgnoreRetryLimit)
}

////////////////////////////////////////////////////////////////////////////////

type simpleError struct{}

func (simpleError) Error() string {
	return "Simple error"
}

func TestSimpleErrorUnwrapsCorrectly(t *testing.T) {
	assert.True(t, Is(NewNonRetriableError(&simpleError{}), &simpleError{}))
}
