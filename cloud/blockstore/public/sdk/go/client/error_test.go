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

func TestIsRetriable(t *testing.T) {
	retriable := []uint32{
		E_REJECTED, E_TIMEOUT, E_THROTTLED, E_OUT_OF_SPACE,
		E_RDMA_UNAVAILABLE, E_TRANSPORT_ERROR,
	}
	for _, code := range retriable {
		err := &ClientError{Code: code}
		require.True(
			t, err.IsRetriable(),
			"expected retriable: %v", formatErrorCode(code))
	}

	notRetriable := []uint32{E_ARGUMENT, E_FAIL, E_IO, E_NOT_FOUND}
	for _, code := range notRetriable {
		err := &ClientError{Code: code}
		require.False(
			t, err.IsRetriable(),
			"expected not retriable: %v", formatErrorCode(code))
	}
}

func TestIsDiskNotFoundError(t *testing.T) {
	clientErr := &ClientError{}
	err := clientErr

	clientErr.Code = E_NOT_IMPLEMENTED
	require.False(t, IsDiskNotFoundError(err))

	clientErr.Code = E_NOT_FOUND
	require.True(t, IsDiskNotFoundError(err))
}
