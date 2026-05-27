package client

import (
	"testing"

	"github.com/stretchr/testify/require"
)

////////////////////////////////////////////////////////////////////////////////

func TestIsRetriable(t *testing.T) {
	retriable := []uint32{
		E_REJECTED, E_TIMEOUT, E_FS_OUT_OF_SPACE, E_FS_THROTTLED,
		E_TRANSPORT_ERROR,
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
