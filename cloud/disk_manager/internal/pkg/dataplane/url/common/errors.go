package common

import (
	"fmt"

	error_codes "github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client/codes"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
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

func NewSourceForbiddenError(format string, args ...interface{}) error {
	return errors.NewDetailedError(
		fmt.Errorf(format, args...),
		&errors.ErrorDetails{
			Code:     error_codes.BadSource,
			Message:  "url source forbidden",
			Internal: false,
		},
	)
}

func NewWrongETagError(format string, args ...interface{}) error {
	return errors.NewDetailedError(
		fmt.Errorf(format, args...),
		&errors.ErrorDetails{
			Code:     error_codes.Aborted,
			Message:  "data from url source was changed during image creation",
			Internal: false,
		},
	)
}

func NewRequestedRangeNotSatisfiableError(
	format string,
	args ...interface{},
) error {

	return errors.NewDetailedError(
		fmt.Errorf(format, args...),
		&errors.ErrorDetails{
			Code:     error_codes.Aborted,
			Message:  "data from url source was changed during image creation",
			Internal: false,
		},
	)
}
