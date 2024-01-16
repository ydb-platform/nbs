package facade_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/api/yandex/cloud/priv/disk_manager/v1"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"google.golang.org/protobuf/proto"
)

////////////////////////////////////////////////////////////////////////////////

// Purpose of this test is to catch stupid bugs, for example when disk-manager
// crashes in main.go unconditionally.
func TestFacade(t *testing.T) {
}

// ErrorDetails from Public API should be binary compatible with ErrorDetails from Task Processor library.
func TestFacadeErrorDetails(t *testing.T) {
	errorDetails1 := &disk_manager.ErrorDetails{}
	errorDetails2 := &errors.ErrorDetails{}

	check := func() {
		bytes, err := proto.Marshal(errorDetails1)
		require.NoError(t, err)

		err = proto.Unmarshal(bytes, errorDetails2)
		require.NoError(t, err)

		require.Equal(t, errorDetails1.Code, errorDetails2.Code)
		require.Equal(t, errorDetails1.Message, errorDetails2.Message)
		require.Equal(t, errorDetails1.Internal, errorDetails2.Internal)
	}

	check()

	errorDetails1.Code = 123
	check()

	errorDetails1.Message = "XYZ"
	check()

	errorDetails1.Internal = true
	check()

	errorDetails1.Code = 321
	errorDetails1.Message = "ZYX"
	check()
}
