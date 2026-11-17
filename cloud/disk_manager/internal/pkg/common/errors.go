package common

import (
	"fmt"

	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client/codes"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

func NewNonRetriableAbortedError(format string, args ...interface{}) error {
	message := fmt.Sprintf(format, args...)
	return errors.NewDetailedError(
		errors.NewNonRetriableErrorf("%s", message),
		&errors.ErrorDetails{
			Code:     codes.Aborted,
			Message:  message,
			Internal: true,
		},
	)
}

func NewInvalidArgumentError(format string, args ...interface{}) error {
	message := fmt.Sprintf(format, args...)

	return errors.NewDetailedError(
		errors.NewNonRetriableErrorf(message),
		&errors.ErrorDetails{
			Code:     codes.InvalidArgument,
			Message:  message,
			Internal: true,
		},
	)
}
