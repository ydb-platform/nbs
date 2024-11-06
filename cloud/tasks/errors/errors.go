package errors

import (
	"errors"
	"fmt"
	"runtime/debug"

	"github.com/ydb-platform/nbs/cloud/tasks/common/protos"
	grpc_status "google.golang.org/grpc/status"
)

////////////////////////////////////////////////////////////////////////////////

type errorForTracing interface {
	error
	ErrorForTracing() string
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
	// We don't want to mention retriable errors created over non-retriable
	// errors.
	nonRetriableErr := NewEmptyNonRetriableError()
	if errors.As(e, &nonRetriableErr) {
		return nonRetriableErr.Error()
	}

	return fmt.Sprintf("%v: %v", e.message(), e.Err.Error())
}

func (e *RetriableError) ErrorForTracing() string {
	nonRetriableErr := NewEmptyNonRetriableError()
	if errors.As(e, &nonRetriableErr) {
		return ErrorForTracing(nonRetriableErr)
	}

	return fmt.Sprintf("%v: %v", e.message(), ErrorForTracing(e.Err))
}

func (e *RetriableError) message() string {
	return fmt.Sprintf(
		"Retriable error, IgnoreRetryLimit=%v",
		e.IgnoreRetryLimit,
	)
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
	msg := fmt.Sprintf("%v: %v", e.message(), e.Err.Error())
	return appendStackTrace(msg, e.stackTrace)
}

func (e *NonRetriableError) ErrorForTracing() string {
	return fmt.Sprintf("%v: %v", e.message(), ErrorForTracing(e.Err))
}

func (e *NonRetriableError) message() string {
	return fmt.Sprintf("Non retriable error, Silent=%v", e.Silent)
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

// Used to indicate that task should be restarted from the beginning.
type AbortedError struct {
	Err error
}

func NewAbortedError(err error) *AbortedError {
	return &AbortedError{Err: err}
}

func NewAbortedErrorf(format string, a ...any) *AbortedError {
	return NewAbortedError(fmt.Errorf(format, a...))
}

func NewEmptyAbortedError() *AbortedError {
	return &AbortedError{}
}

func (e *AbortedError) Error() string {
	return fmt.Sprintf("%v: %v", e.message(), e.Err.Error())
}

func (e *AbortedError) ErrorForTracing() string {
	return fmt.Sprintf("%v: %v", e.message(), ErrorForTracing(e.Err))
}

func (e *AbortedError) message() string {
	return "Aborted error"
}

func (e *AbortedError) Unwrap() error {
	return e.Err
}

func (e *AbortedError) Is(target error) bool {
	t, ok := target.(*AbortedError)
	if !ok {
		return false
	}

	return t.Err == nil || (e.Err == t.Err)
}

////////////////////////////////////////////////////////////////////////////////

type NonCancellableError struct {
	Err        error
	stackTrace []byte
}

func NewNonCancellableError(err error) *NonCancellableError {
	return &NonCancellableError{
		Err:        err,
		stackTrace: debug.Stack(),
	}
}

func NewNonCancellableErrorf(format string, a ...any) *NonCancellableError {
	return NewNonCancellableError(fmt.Errorf(format, a...))
}

func NewEmptyNonCancellableError() *NonCancellableError {
	return NewNonCancellableError(nil)
}

func (e *NonCancellableError) Error() string {
	msg := fmt.Sprintf("%v: %v", e.message(), e.Err.Error())
	return appendStackTrace(msg, e.stackTrace)
}

func (e *NonCancellableError) ErrorForTracing() string {
	return fmt.Sprintf("%v: %v", e.message(), ErrorForTracing(e.Err))
}

func (e *NonCancellableError) message() string {
	return "Non cancellable error"
}

func (e *NonCancellableError) Unwrap() error {
	return e.Err
}

func (e *NonCancellableError) Is(target error) bool {
	t, ok := target.(*NonCancellableError)
	if !ok {
		return false
	}

	return t.Err == nil || (e.Err == t.Err)
}

////////////////////////////////////////////////////////////////////////////////

type WrongGenerationError struct{}

func NewWrongGenerationError() *WrongGenerationError {
	return &WrongGenerationError{}
}

func (e WrongGenerationError) Error() string {
	return "Wrong generation"
}

func (e WrongGenerationError) ErrorForTracing() string {
	return e.Error()
}

////////////////////////////////////////////////////////////////////////////////

type InterruptExecutionError struct{}

func NewInterruptExecutionError() *InterruptExecutionError {
	return &InterruptExecutionError{}
}

func (e InterruptExecutionError) Error() string {
	return "Interrupt execution"
}

func (e InterruptExecutionError) ErrorForTracing() string {
	return e.Error()
}

////////////////////////////////////////////////////////////////////////////////

type PanicError struct {
	value      any
	stackTrace []byte
}

func NewPanicError(value any) *PanicError {
	return &PanicError{
		value:      value,
		stackTrace: debug.Stack(),
	}
}

func (e PanicError) Reraise() {
	msg := fmt.Sprintf("%v", e.value)
	msg = appendStackTrace(msg, e.stackTrace)
	panic(msg)
}

func (e PanicError) Error() string {
	msg := e.message()
	return appendStackTrace(msg, e.stackTrace)
}

func (e PanicError) ErrorForTracing() string {
	return e.message()
}

func (e PanicError) message() string {
	return fmt.Sprintf("panic: %v", e.value)
}

func IsPanicError(err error) bool {
	panicErr := &PanicError{}
	return errors.As(err, &panicErr)
}

////////////////////////////////////////////////////////////////////////////////

type NotFoundError struct {
	TaskID         string
	IdempotencyKey string
}

func NewNotFoundErrorWithTaskID(taskID string) *NotFoundError {
	return newNotFoundError(taskID, "")
}

func NewNotFoundErrorWithIdempotencyKey(idempotencyKey string) *NotFoundError {
	return newNotFoundError("", idempotencyKey)
}

func NewEmptyNotFoundError() *NotFoundError {
	return newNotFoundError("", "")
}

func newNotFoundError(taskID, idempotencyKey string) *NotFoundError {
	return &NotFoundError{
		TaskID:         taskID,
		IdempotencyKey: idempotencyKey,
	}
}

func (e NotFoundError) Error() string {
	return fmt.Sprintf(
		"No task with ID=%v, IdempotencyKey=%v",
		e.TaskID,
		e.IdempotencyKey,
	)
}

func (e NotFoundError) ErrorForTracing() string {
	return e.Error()
}

// HACK: Need to avoid default comparator that uses inner fields.
func (e NotFoundError) Is(target error) bool {
	_, ok := target.(*NotFoundError)
	return ok
}

////////////////////////////////////////////////////////////////////////////////

type ErrorDetails = protos.ErrorDetails

type DetailedError struct {
	Err     error
	Details *ErrorDetails
	Silent  bool
}

func NewDetailedError(err error, details *ErrorDetails) *DetailedError {
	silent := false
	if isPublic(details) {
		// Public errors should be silent.
		silent = true
	}

	return NewDetailedErrorFull(err, details, silent)
}

func NewDetailedErrorFull(
	err error,
	details *ErrorDetails,
	silent bool,
) *DetailedError {

	return &DetailedError{
		Err:     err,
		Details: details,
		Silent:  silent,
	}
}

func NewEmptyDetailedError() *DetailedError {
	return NewDetailedErrorFull(nil, nil, false)
}

func (e *DetailedError) Error() string {
	return fmt.Sprintf("%v: %v", e.message(), e.Err.Error())
}

func (e *DetailedError) ErrorForTracing() string {
	return fmt.Sprintf("%v: %v", e.message(), ErrorForTracing(e.Err))
}

func (e *DetailedError) message() string {
	return fmt.Sprintf(
		"Detailed error, Details=%v, Silent=%v",
		e.Details,
		e.Silent,
	)
}

func (e *DetailedError) Unwrap() error {
	return e.Err
}

func (e *DetailedError) Is(target error) bool {
	t, ok := target.(*DetailedError)
	if !ok {
		return false
	}

	return t.Err == nil || (e.Err == t.Err)
}

func (e *DetailedError) GRPCStatus() *grpc_status.Status {
	status, _ := grpc_status.FromError(e.Err)
	return status
}

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

func CanRetry(err error) bool {
	if Is(err, NewWrongGenerationError()) || Is(err, NewInterruptExecutionError()) {
		return true
	}

	return !Is(err, NewEmptyNonCancellableError()) &&
		!Is(err, NewEmptyNonRetriableError()) &&
		Is(err, NewEmptyRetriableError())
}

////////////////////////////////////////////////////////////////////////////////

func isPublic(details *ErrorDetails) bool {
	return details != nil && !details.Internal
}

func IsPublic(err error) bool {
	detailedError := NewEmptyDetailedError()
	if !As(err, &detailedError) {
		return false
	}

	return isPublic(detailedError.Details)
}

func IsSilent(err error) bool {
	nonRetriableError := NewEmptyNonRetriableError()
	if As(err, &nonRetriableError) {
		return nonRetriableError.Silent
	}

	detailedError := NewEmptyDetailedError()
	if As(err, &detailedError) {
		return detailedError.Silent
	}

	return false
}

////////////////////////////////////////////////////////////////////////////////

func ErrorForTracing(err error) string {
	if e, ok := err.(errorForTracing); ok {
		return e.ErrorForTracing()
	}
	return err.Error()
}

////////////////////////////////////////////////////////////////////////////////

func appendStackTrace(errorMessage string, stackTrace []byte) string {
	return fmt.Sprintf("%s\n%s", errorMessage, stackTrace)
}
