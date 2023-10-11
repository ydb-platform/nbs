package common

import (
	"fmt"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/errors"
	error_codes "github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client/codes"
)

////////////////////////////////////////////////////////////////////////////////

func NewSourceNotFoundError(format string, args ...interface{}) error {
	return errors.NewDetailedError(
		fmt.Errorf(format, args...),
		&errors.ErrorDetails{
			Code:     error_codes.BadSource,
			Message:  "url source not found",
			Internal: false,
		},
	)
}

func NewSourceInvalidError(format string, args ...interface{}) error {
	return errors.NewDetailedError(
		fmt.Errorf(format, args...),
		&errors.ErrorDetails{
			Code:     error_codes.BadSource,
			Message:  "url source invalid",
			Internal: false,
		},
	)
}
