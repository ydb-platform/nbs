package errors

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/errors"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/logging"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/persistence"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/common/protos"
	grpc_codes "google.golang.org/grpc/codes"
	grpc_status "google.golang.org/grpc/status"
)

////////////////////////////////////////////////////////////////////////////////

type RetriableError = errors.RetriableError

func NewRetriableError(err error) *RetriableError {
	return errors.NewRetriableError(err)
}

func NewRetriableErrorf(format string, a ...any) *RetriableError {
	return errors.NewRetriableErrorf(format, a...)
}

func NewRetriableErrorWithIgnoreRetryLimit(err error) *RetriableError {
	return errors.NewRetriableErrorWithIgnoreRetryLimit(err)
}

func NewRetriableErrorWithIgnoreRetryLimitf(format string, a ...any) *RetriableError {
	return errors.NewRetriableErrorWithIgnoreRetryLimitf(format, a...)
}

func NewEmptyRetriableError() *RetriableError {
	return errors.NewEmptyRetriableError()
}

////////////////////////////////////////////////////////////////////////////////

type NonRetriableError = errors.NonRetriableError

func NewNonRetriableError(err error) *NonRetriableError {
	return errors.NewNonRetriableError(err)
}

func NewNonRetriableErrorf(format string, a ...any) *NonRetriableError {
	return errors.NewNonRetriableErrorf(format, a...)
}

func NewSilentNonRetriableError(err error) *NonRetriableError {
	return errors.NewSilentNonRetriableError(err)
}

func NewSilentNonRetriableErrorf(format string, a ...any) *NonRetriableError {
	return errors.NewSilentNonRetriableErrorf(format, a...)
}

func NewEmptyNonRetriableError() *NonRetriableError {
	return errors.NewEmptyNonRetriableError()
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
	return fmt.Sprintf("Aborted error: %v", e.Err)
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
	msg := fmt.Sprintf("Non cancellable error: %v", e.Err)
	return appendStackTrace(msg, e.stackTrace)
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

func (WrongGenerationError) Error() string {
	return "Wrong generation"
}

////////////////////////////////////////////////////////////////////////////////

type InterruptExecutionError struct{}

func NewInterruptExecutionError() *InterruptExecutionError {
	return &InterruptExecutionError{}
}

func (InterruptExecutionError) Error() string {
	return "Interrupt execution"
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
	msg := fmt.Sprintf("panic: %v", e.value)
	return appendStackTrace(msg, e.stackTrace)
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
	return fmt.Sprintf(
		"Detailed error, Details=%v, Silent=%v: %v",
		e.Details,
		e.Silent,
		e.Err,
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

func isCancelledError(err error) bool {
	switch {
	case
		errors.Is(err, context.Canceled),
		persistence.IsTransportError(err, grpc_codes.Canceled):
		return true
	default:
		return false
	}
}

func LogError(
	ctx context.Context,
	err error,
	format string,
	args ...interface{},
) {

	description := fmt.Sprintf(format, args...)

	if Is(err, NewWrongGenerationError()) ||
		Is(err, NewInterruptExecutionError()) ||
		isCancelledError(err) {

		logging.Debug(logging.AddCallerSkip(ctx, 1), "%v: %v", description, err)
	} else {
		logging.Warn(logging.AddCallerSkip(ctx, 1), "%v: %v", description, err)
	}
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

func appendStackTrace(errorMessage string, stackTrace []byte) string {
	return fmt.Sprintf("%s\n%s", errorMessage, stackTrace)
}
