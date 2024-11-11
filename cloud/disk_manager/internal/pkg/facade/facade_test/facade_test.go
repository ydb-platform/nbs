package facade_test

import (
	"testing"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
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

func getRequestErrorCount(t *testing.T, requestName string) float64 {
	metricFamily, err := testcommon.GetMetrics("errors")
	require.NoError(t, err)
	metrics := testcommon.FilterMetrics(
		map[string]string{"component": "grpc_facade", "request": requestName},
		metricFamily,
	)
	require.Equal(t, len(metrics), 1)
	return metrics[0].GetCounter().GetValue()
}

func TestDiskServiceInvalidCreateEmptyDisk(t *testing.T) {
	ctx := testcommon.NewContext()
	require.Equal(t, getRequestErrorCount(t, "DiskService.Create"), float64(0))
	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := t.Name()
	reqCtx := testcommon.GetRequestContext(t, ctx)
	_, err = client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size: 1,
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID,
		},
	})
	require.Error(t, err)
	require.Equal(t, getRequestErrorCount(t, "DiskService.Create"), float64(1))
}
