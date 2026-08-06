package tests

import (
	"testing"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
)

////////////////////////////////////////////////////////////////////////////////

const (
	imageSize = 4 * 1024 * 1024
	zoneID    = "zone-a"
)

////////////////////////////////////////////////////////////////////////////////

func TestImageServiceCreateImageFromDiskWithS3StorageClassQuotaFallback(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := t.Name() + "_disk"

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size: int64(imageSize),
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: zoneID,
			DiskId: diskID,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	nbsClient := testcommon.NewNbsTestingClient(t, ctx, zoneID)
	data := make([]byte, imageSize)
	for i := range data {
		data[i] = 1
	}

	err = nbsClient.Write(diskID, 0, data)
	require.NoError(t, err)

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateImage(reqCtx, &disk_manager.CreateImageRequest{
		Src: &disk_manager.CreateImageRequest_SrcDiskId{
			SrcDiskId: &disk_manager.DiskId{
				ZoneId: zoneID,
				DiskId: diskID,
			},
		},
		DstImageId: t.Name(),
		FolderId:   "folder",
		Pooled:     false,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)

	response := disk_manager.CreateImageResponse{}
	err = internal_client.WaitResponse(ctx, client, operation.Id, &response)
	require.NoError(t, err)
	require.Equal(t, int64(imageSize), response.Size)
	require.NotZero(t, response.StorageSize)

	quotaExceededCounters := testcommon.GetCountersDataplane(
		t,
		"errors_quotaExceeded",
		map[string]string{
			"call":      "PutObject",
			"component": "s3_client",
		},
	)

	var quotaExceededCount float64
	for _, counter := range quotaExceededCounters {
		quotaExceededCount += counter
	}

	require.NotZero(t, quotaExceededCount)
}
