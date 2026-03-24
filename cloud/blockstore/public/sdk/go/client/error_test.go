package client

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

////////////////////////////////////////////////////////////////////////////////

func TestGetClientError(t *testing.T) {
	code := E_NOT_IMPLEMENTED
	message := "Message"
	clientErr := &ClientError{
		Code:    code,
		Message: message,
	}

	var err error
	err = clientErr

	require.Equal(t, code, GetClientCode(err))

	e := GetClientError(err)
	require.Equal(t, code, e.Code)
	require.Equal(t, message, e.Message)

	err = fmt.Errorf("wrapped error: %w", err)

	require.Equal(t, code, GetClientCode(err))

	e = GetClientError(err)
	require.Equal(t, code, e.Code)
	require.Equal(t, message, e.Message)
}

func TestIsDiskNotFoundError(t *testing.T) {
	clientErr := &ClientError{}
	var err error
	err = clientErr

	clientErr.Code = E_NOT_IMPLEMENTED
	require.False(t, IsDiskNotFoundError(err))

	clientErr.Code = E_NOT_FOUND
	require.True(t, IsDiskNotFoundError(err))
}
