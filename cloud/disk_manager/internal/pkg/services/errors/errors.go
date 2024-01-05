package errors

import (
	"fmt"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client/codes"
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
