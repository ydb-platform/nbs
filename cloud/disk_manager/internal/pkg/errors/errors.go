package errors

import (
	"errors"
	"fmt"
	"runtime/debug"
)

////////////////////////////////////////////////////////////////////////////////

func New(text string) error {
	return errors.New(text)
}

func As(err error, target interface{}) bool {
	return errors.As(err, target)
}

func Is(err, target error) bool {
	return errors.Is(err, target)
}

////////////////////////////////////////////////////////////////////////////////

type RetriableError struct {
	Err              error
	IgnoreRetryLimit bool
}

func NewRetriableError(err error) *RetriableError {
	return &RetriableError{
		Err: err,
	}
}

func NewRetriableErrorf(format string, a ...any) *RetriableError {
	return NewRetriableError(fmt.Errorf(format, a...))
}

func NewRetriableErrorWithIgnoreRetryLimit(err error) *RetriableError {
	return &RetriableError{
		Err:              err,
		IgnoreRetryLimit: true,
	}
}

func NewRetriableErrorWithIgnoreRetryLimitf(format string, a ...any) *RetriableError {
	return NewRetriableErrorWithIgnoreRetryLimit(fmt.Errorf(format, a...))
}

func NewEmptyRetriableError() *RetriableError {
	return &RetriableError{}
}

func (e *RetriableError) Error() string {
	return fmt.Sprintf("Retriable error, IgnoreRetryLimit=%v: %v", e.IgnoreRetryLimit, e.Err)
}

func (e *RetriableError) Unwrap() error {
	return e.Err
}

func (e *RetriableError) Is(target error) bool {
	t, ok := target.(*RetriableError)
	if !ok {
		return false
	}

	return t.Err == nil || (e.Err == t.Err)
}

////////////////////////////////////////////////////////////////////////////////

type NonRetriableError struct {
	Err        error
	Silent     bool
	stackTrace []byte
}

func NewNonRetriableError(err error) *NonRetriableError {
	return newNonRetriableError(err, false)
}

func NewNonRetriableErrorf(format string, a ...any) *NonRetriableError {
	return newNonRetriableError(fmt.Errorf(format, a...), false)
}

func NewSilentNonRetriableError(err error) *NonRetriableError {
	return newNonRetriableError(err, true)
}

func NewSilentNonRetriableErrorf(format string, a ...any) *NonRetriableError {
	return newNonRetriableError(fmt.Errorf(format, a...), true)
}

func NewEmptyNonRetriableError() *NonRetriableError {
	return newNonRetriableError(nil, false)
}

func newNonRetriableError(err error, silent bool) *NonRetriableError {
	return &NonRetriableError{
		Err:        err,
		Silent:     silent,
		stackTrace: debug.Stack(),
	}
}

func (e *NonRetriableError) Error() string {
	msg := fmt.Sprintf("Non retriable error, Silent=%v: %v", e.Silent, e.Err)
	return appendStackTrace(msg, e.stackTrace)
}

func (e *NonRetriableError) Unwrap() error {
	return e.Err
}

func (e *NonRetriableError) Is(target error) bool {
	t, ok := target.(*NonRetriableError)
	if !ok {
		return false
	}

	return t.Err == nil || (e.Err == t.Err)
}

////////////////////////////////////////////////////////////////////////////////

func appendStackTrace(errorMessage string, stackTrace []byte) string {
	return fmt.Sprintf("%s\n%s", errorMessage, stackTrace)
}
