package common

import (
	"fmt"

	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client/codes"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

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
