package errors

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

////////////////////////////////////////////////////////////////////////////////

func TestRetriableAndNonRetriableErrorsShouldUnwrapCorrectly(t *testing.T) {
	err := assert.AnError

	require.True(
		t,
		Is(
			NewNonRetriableError(NewRetriableError(err)),
			NewEmptyNonRetriableError(),
		),
	)
	require.True(
		t,
		Is(
			NewNonRetriableError(NewRetriableError(err)),
			err,
		),
	)
	require.True(
		t,
		Is(
			NewNonRetriableError(NewRetriableError(err)),
			NewEmptyRetriableError(),
		),
	)
	require.True(
		t,
		Is(
			NewRetriableError(NewNonRetriableError(err)),
			NewEmptyNonRetriableError(),
		),
	)
	require.True(
		t,
		Is(
			NewRetriableError(NewNonRetriableError(err)),
			NewEmptyNonRetriableError(),
		),
	)
	require.True(
		t,
		Is(
			NewRetriableError(NewNonRetriableError(err)),
			err,
		),
	)
	require.True(
		t,
		Is(
			NewNonRetriableError(NewNonCancellableError(err)),
			NewEmptyNonCancellableError(),
		),
	)

	require.False(
		t,
		Is(
			NewNonRetriableError(NewNonRetriableError(err)),
			NewEmptyRetriableError(),
		),
	)
	require.False(
		t,
		Is(
			NewRetriableError(NewRetriableError(err)),
			NewEmptyNonRetriableError(),
		),
	)
	require.False(
		t,
		Is(
			NewNonRetriableError(NewRetriableErrorf("other error")),
			err,
		),
	)
	require.False(
		t,
		Is(
			NewNonRetriableError(NewRetriableError(err)),
			NewEmptyNonCancellableError(),
		),
	)
}

func TestFoundErrorShouldUnwrapCorrectly(t *testing.T) {
	require.True(
		t,
		Is(
			NewNonRetriableError(NewEmptyNotFoundError()),
			NewEmptyNotFoundError(),
		),
	)
	require.True(
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
	require.True(
		t,
		As(
			NewNonRetriableError(NewDetailedError(assert.AnError, details)),
			&e,
		),
	)

	require.Equal(t, assert.AnError, e.Err)
	require.Equal(t, details, e.Details)
}

func TestCanRetry(t *testing.T) {
	err := assert.AnError

	// randomly fail test
	if rand.Float32() < 0.5 {
		require.True(t, CanRetry(NewRetriableError(err)))
	} else {
		require.False(t, CanRetry(NewRetriableError(err)))
	}

	require.False(t, CanRetry(err))
	require.False(t, CanRetry(NewRetriableError(NewNonCancellableError(err))))
	require.False(t, CanRetry(NewRetriableError(NewNonRetriableError(err))))
}

func TestNonRetriableErrorSilent(t *testing.T) {
	err := NewSilentNonRetriableError(assert.AnError)

	e := NewEmptyNonRetriableError()
	require.True(t, As(err, &e))
	require.True(t, e.Silent)
}

func TestNonRetriableErrorSilentUnwrapsCorrectly(t *testing.T) {
	err := NewRetriableError(NewSilentNonRetriableError(assert.AnError))

	e := NewEmptyNonRetriableError()
	require.True(t, As(err, &e))
	require.True(t, e.Silent)
}

func TestRetriableErrorIgnoreRetryLimitUnwrapsCorrectly(t *testing.T) {
	err := NewRetriableErrorWithIgnoreRetryLimit(
		NewNonRetriableError(assert.AnError),
	)

	e := NewEmptyRetriableError()
	require.True(t, As(err, &e))
	require.True(t, e.IgnoreRetryLimit)
}

////////////////////////////////////////////////////////////////////////////////

type simpleError struct{}

func (simpleError) Error() string {
	return "Simple error"
}

func TestSimpleErrorUnwrapsCorrectly(t *testing.T) {
	assert.True(t, Is(NewNonRetriableError(&simpleError{}), &simpleError{}))
}

////////////////////////////////////////////////////////////////////////////////

func innerPanicFunc() {
	panic("inner panic")
}

func panicAndReturnError() (err *PanicError) {
	defer func() {
		err = NewPanicError(recover())
	}()

	innerPanicFunc()
	return err
}

func TestReraisePanicError(t *testing.T) {
	defer func() {
		r := recover()
		require.NotNil(t, r)
		require.Contains(t, fmt.Sprintf("%v", r), "innerPanicFunc")
	}()

	err := panicAndReturnError()
	require.Contains(t, err.Error(), "innerPanicFunc")

	err.Reraise()
}

////////////////////////////////////////////////////////////////////////////////

func TestErrorMessageWhenBothRetriableAndNonRetriableErrors(t *testing.T) {
	innerNonRetriableErr := NewNonRetriableErrorf("innerRetriableErr")
	outerRetriableErr := NewRetriableError(innerNonRetriableErr)
	require.Equal(t, innerNonRetriableErr.Error(), outerRetriableErr.Error())

	innerRetriableErr := NewRetriableErrorf("innerRetriableErr")
	outerNonRetriableErr := NewNonRetriableError(innerRetriableErr)
	require.Contains(t, outerNonRetriableErr.Error(), innerRetriableErr.Error())
	require.NotEqual(t, innerRetriableErr.Error(), outerNonRetriableErr.Error())
}

func TestErrorMessageWithAndWithoutStacktrace(t *testing.T) {
	checkError := func(err errorForTracing, hasStacktrace bool) {
		require.Equal(t, ErrorForTracing(err), err.ErrorForTracing())

		if hasStacktrace {
			require.Contains(t, err.Error(), err.ErrorForTracing())
			require.Contains(t, err.Error(), "runtime/debug.Stack()")
			require.NotContains(t, err.ErrorForTracing(), "runtime/debug.Stack()")
		} else {
			require.Equal(t, err.Error(), err.ErrorForTracing())
		}
	}

	checkError(NewRetriableErrorf("retriableErr"), false)
	checkError(NewWrongGenerationError(), false)
	checkError(NewInterruptExecutionError(), false)
	checkError(NewNonRetriableErrorf("nonRetriableErr"), true)
	checkError(NewNonCancellableErrorf("nonCancellableErrorf"), true)
	checkError(NewPanicError("panicErr"), true)

	checkError(
		NewRetriableError(NewNonRetriableErrorf("retriableWrapsNonRetriableErr")),
		true,
	)
	checkError(
		NewNonRetriableError(NewRetriableErrorf("nonRetriableWrapsRetriableErr")),
		true,
	)
	checkError(
		NewRetriableError(NewRetriableErrorf("retriableWrapsRetriableErr")),
		false,
	)
	checkError(
		NewNonRetriableError(NewNonRetriableErrorf("nonRetriableWrapsNonRetriableErr")),
		true,
	)

	externalErr := fmt.Errorf("externalErr")
	require.Equal(t, ErrorForTracing(externalErr), externalErr.Error())
}
