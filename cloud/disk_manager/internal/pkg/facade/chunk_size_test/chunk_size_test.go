package tests

import (
	"context"
	"hash/crc32"
	"strings"
	"testing"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
	sdk_client "github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client"
	grpc_codes "google.golang.org/grpc/codes"
	grpc_status "google.golang.org/grpc/status"
)

const (
	chunkSizeMiB      = 1024 * 1024
	chunkSizeDefault  = 8 * chunkSizeMiB
	chunkSizeOverride = 4 * chunkSizeMiB
	chunkSizeS3Folder = "chunk-size-override-s3"
	chunkSizeZone     = "zone-a"
)

func chunkSizeTestID(t *testing.T) string {
	return strings.ReplaceAll(t.Name(), "/", "-")
}

func chunkSizeCreateDisk(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	diskID string,
	diskSize ...int,
) (nbs.TestingClient, []byte) {

	size := 32 * chunkSizeMiB
	if len(diskSize) != 0 && diskSize[0] != 0 {
		size = diskSize[0]
	}
	operation, err := client.CreateDisk(testcommon.GetRequestContext(t, ctx), &disk_manager.CreateDiskRequest{
		Src:    &disk_manager.CreateDiskRequest_SrcEmpty{SrcEmpty: &empty.Empty{}},
		Size:   int64(size),
		Kind:   disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{ZoneId: chunkSizeZone, DiskId: diskID},
	})
	require.NoError(t, err)
	require.NoError(t, internal_client.WaitOperation(ctx, client, operation.Id))

	nbsClient := testcommon.NewNbsTestingClient(t, ctx, chunkSizeZone)
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i+i/chunkSizeMiB)%251 + 1)
	}
	session, err := nbsClient.MountRW(ctx, diskID, 0, 0, nil)
	require.NoError(t, err)
	defer session.Close(ctx)
	for offset := 0; offset < len(data); offset += 4 * chunkSizeMiB {
		require.NoError(t, session.Write(ctx, uint64(offset/4096), data[offset:offset+4*chunkSizeMiB]))
	}
	return nbsClient, data
}

func chunkSizeCheckSnapshot(
	t *testing.T,
	ctx context.Context,
	snapshotID string,
	chunkSize uint32,
	size int64,
	storageSize int64,
) *storage.SnapshotMeta {

	meta, err := testcommon.GetSnapshotMeta(t, ctx, snapshotID)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.True(t, meta.Ready)
	require.Equal(t, chunkSize, meta.ChunkSize)
	require.Equal(t, uint64(size), meta.Size)
	require.Equal(t, uint64(storageSize), meta.StorageSize)
	require.Equal(t, uint32(size/int64(chunkSize)), meta.ChunkCount)
	require.Zero(t, storageSize%int64(chunkSize))
	return meta
}

func chunkSizeRestoreSnapshot(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	snapshotID string,
	data []byte,
) {

	diskID := snapshotID + "-restored"
	operation, err := client.CreateDisk(testcommon.GetRequestContext(t, ctx), &disk_manager.CreateDiskRequest{
		Src:    &disk_manager.CreateDiskRequest_SrcSnapshotId{SrcSnapshotId: snapshotID},
		Size:   int64(len(data)),
		Kind:   disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{ZoneId: chunkSizeZone, DiskId: diskID},
	})
	require.NoError(t, err)
	require.NoError(t, internal_client.WaitOperation(ctx, client, operation.Id))
	nbsClient := testcommon.NewNbsTestingClient(t, ctx, chunkSizeZone)
	require.NoError(t, nbsClient.ValidateCrc32(ctx, diskID, nbs.DiskContentInfo{
		ContentSize: uint64(len(data)),
		Crc32:       crc32.ChecksumIEEE(data),
	}))
}

func chunkSizeRestoreImage(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	imageID string,
	content nbs.DiskContentInfo,
) {

	diskID := imageID + "-restored"
	operation, err := client.CreateDisk(testcommon.GetRequestContext(t, ctx), &disk_manager.CreateDiskRequest{
		Src:    &disk_manager.CreateDiskRequest_SrcImageId{SrcImageId: imageID},
		Size:   int64(content.ContentSize),
		Kind:   disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{ZoneId: chunkSizeZone, DiskId: diskID},
	})
	require.NoError(t, err)
	require.NoError(t, internal_client.WaitOperation(ctx, client, operation.Id))
	nbsClient := testcommon.NewNbsTestingClient(t, ctx, chunkSizeZone)
	require.NoError(t, nbsClient.ValidateCrc32(ctx, diskID, content))
}

func TestSnapshotChunkSizeAPI(t *testing.T) {
	for _, tc := range []struct {
		name      string
		folder    string
		requested uint32
		expected  uint32
		diskSize  int
	}{
		{name: "ydb_default", folder: "folder", expected: 4 * chunkSizeMiB},
		{name: "s3_controlplane_default", folder: chunkSizeS3Folder, expected: chunkSizeDefault},
		{name: "allowed_s3_override", folder: chunkSizeS3Folder, requested: chunkSizeOverride, expected: chunkSizeOverride},
		{name: "non_power_of_two", folder: chunkSizeS3Folder, requested: 12 * chunkSizeMiB, expected: 12 * chunkSizeMiB, diskSize: 36 * chunkSizeMiB},
		{name: "s3_large_chunk", folder: chunkSizeS3Folder, requested: 32 * chunkSizeMiB, expected: 32 * chunkSizeMiB},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testcommon.NewContext()
			client, err := testcommon.NewClient(ctx)
			require.NoError(t, err)
			defer client.Close()
			diskID := chunkSizeTestID(t) + "-disk"
			_, data := chunkSizeCreateDisk(t, ctx, client, diskID, tc.diskSize)
			snapshotID := chunkSizeTestID(t) + "-snapshot"
			operation, err := client.CreateSnapshot(testcommon.GetRequestContext(t, ctx), &disk_manager.CreateSnapshotRequest{
				Src:        &disk_manager.DiskId{ZoneId: chunkSizeZone, DiskId: diskID},
				SnapshotId: snapshotID,
				FolderId:   tc.folder,
				ChunkSize:  tc.requested,
			})
			require.NoError(t, err)
			response := disk_manager.CreateSnapshotResponse{}
			require.NoError(t, internal_client.WaitResponse(ctx, client, operation.Id, &response))
			require.Equal(t, int64(len(data)), response.Size)
			require.Equal(t, response.Size, response.StorageSize)
			chunkSizeCheckSnapshot(t, ctx, snapshotID, tc.expected, response.Size, response.StorageSize)
			chunkSizeRestoreSnapshot(t, ctx, client, snapshotID, data)
		})
	}
}

func TestImageChunkSizeAPI(t *testing.T) {
	for _, source := range []string{"disk", "url"} {
		for _, tc := range []struct {
			name      string
			folder    string
			requested uint32
			expected  uint32
			diskSize  int
			diskOnly  bool
		}{
			{name: "ydb_default", folder: "folder", expected: 4 * chunkSizeMiB},
			{name: "s3_controlplane_default", folder: chunkSizeS3Folder, expected: chunkSizeDefault},
			{name: "allowed_s3_override", folder: chunkSizeS3Folder, requested: chunkSizeOverride, expected: chunkSizeOverride},
			{name: "non_power_of_two", folder: chunkSizeS3Folder, requested: 12 * chunkSizeMiB, expected: 12 * chunkSizeMiB, diskSize: 36 * chunkSizeMiB, diskOnly: true},
			{name: "s3_large_chunk", folder: chunkSizeS3Folder, requested: 32 * chunkSizeMiB, expected: 32 * chunkSizeMiB},
		} {
			if source == "url" && tc.diskOnly {
				continue
			}
			t.Run(source+"/"+tc.name, func(t *testing.T) {
				ctx := testcommon.NewContext()
				client, err := testcommon.NewClient(ctx)
				require.NoError(t, err)
				defer client.Close()
				imageID := chunkSizeTestID(t) + "-image"
				request := &disk_manager.CreateImageRequest{
					DstImageId: imageID,
					FolderId:   tc.folder,
					ChunkSize:  tc.requested,
				}
				var content nbs.DiskContentInfo
				if source == "disk" {
					diskID := chunkSizeTestID(t) + "-disk"
					_, data := chunkSizeCreateDisk(t, ctx, client, diskID, tc.diskSize)
					request.Src = &disk_manager.CreateImageRequest_SrcDiskId{
						SrcDiskId: &disk_manager.DiskId{ZoneId: chunkSizeZone, DiskId: diskID},
					}
					content = nbs.DiskContentInfo{ContentSize: uint64(len(data)), Crc32: crc32.ChecksumIEEE(data)}
				} else {
					request.Src = &disk_manager.CreateImageRequest_SrcUrl{
						SrcUrl: &disk_manager.ImageUrl{Url: testcommon.GetRawImageFileURL()},
					}
					content = nbs.DiskContentInfo{
						ContentSize: testcommon.GetRawImageSize(t),
						Crc32:       testcommon.GetRawImageCrc32(t),
					}
				}
				operation, err := client.CreateImage(testcommon.GetRequestContext(t, ctx), request)
				require.NoError(t, err)
				response := disk_manager.CreateImageResponse{}
				require.NoError(t, internal_client.WaitResponse(ctx, client, operation.Id, &response))
				require.Equal(t, int64(content.ContentSize), response.Size)
				if source == "disk" {
					require.Equal(t, response.Size, response.StorageSize)
				}
				chunkSizeCheckSnapshot(t, ctx, imageID, tc.expected, response.Size, response.StorageSize)
				chunkSizeRestoreImage(t, ctx, client, imageID, content)
			})
		}
	}
}

func TestChunkSizeAPIValidation(t *testing.T) {
	ctx := testcommon.NewContext()
	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()
	for _, tc := range []struct {
		name   string
		folder string
		size   uint32
	}{
		{name: "folder_not_allowed", folder: "folder", size: chunkSizeOverride},
		{name: "below_minimum", folder: chunkSizeS3Folder, size: 2 * chunkSizeMiB},
		{name: "unaligned", folder: chunkSizeS3Folder, size: 6 * chunkSizeMiB},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshotID := chunkSizeTestID(t) + "-snapshot"
			operation, err := client.CreateSnapshot(testcommon.GetRequestContext(t, ctx), &disk_manager.CreateSnapshotRequest{
				Src:        &disk_manager.DiskId{ZoneId: chunkSizeZone, DiskId: "unused-disk"},
				SnapshotId: snapshotID,
				FolderId:   tc.folder,
				ChunkSize:  tc.size,
			})
			require.Error(t, err)
			require.Nil(t, operation)
			require.Equal(t, grpc_codes.InvalidArgument, grpc_status.Code(err))
			require.Contains(t, err.Error(), "chunk size")
			meta, metaErr := testcommon.GetSnapshotMeta(t, ctx, snapshotID)
			require.NoError(t, metaErr)
			require.Nil(t, meta)

			for _, source := range []string{"disk", "url"} {
				imageID := chunkSizeTestID(t) + "-image-" + source
				request := &disk_manager.CreateImageRequest{
					DstImageId: imageID,
					FolderId:   tc.folder,
					ChunkSize:  tc.size,
				}
				if source == "disk" {
					request.Src = &disk_manager.CreateImageRequest_SrcDiskId{
						SrcDiskId: &disk_manager.DiskId{ZoneId: chunkSizeZone, DiskId: "unused-disk"},
					}
				} else {
					request.Src = &disk_manager.CreateImageRequest_SrcUrl{
						SrcUrl: &disk_manager.ImageUrl{Url: testcommon.GetRawImageFileURL()},
					}
				}
				operation, err := client.CreateImage(testcommon.GetRequestContext(t, ctx), request)
				require.Error(t, err)
				require.Nil(t, operation)
				require.Equal(t, grpc_codes.InvalidArgument, grpc_status.Code(err))
				require.Contains(t, err.Error(), "chunk size")
				meta, metaErr := testcommon.GetSnapshotMeta(t, ctx, imageID)
				require.NoError(t, metaErr)
				require.Nil(t, meta)
			}
		})
	}
}

func TestSnapshotChunkSizeIncremental(t *testing.T) {
	testChunkSizeIncremental(t, false)
}

func TestImageChunkSizeIncremental(t *testing.T) {
	testChunkSizeIncremental(t, true)
}

func testChunkSizeIncremental(t *testing.T, image bool) {
	for _, tc := range []struct {
		name          string
		parentSize    uint32
		requestedSize uint32
		diskSize      int
	}{
		{name: "default_parent_configured_request", parentSize: 4 * chunkSizeMiB},
		{name: "default_parent_explicit_request", parentSize: 4 * chunkSizeMiB, requestedSize: 8 * chunkSizeMiB},
		{name: "custom_parent_explicit_request", parentSize: 8 * chunkSizeMiB, requestedSize: 4 * chunkSizeMiB},
		{name: "custom_parent_configured_request", parentSize: 12 * chunkSizeMiB, diskSize: 36 * chunkSizeMiB},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testChunkSizeIncrementalCase(t, image, tc.parentSize, tc.requestedSize, tc.diskSize)
		})
	}
}

func testChunkSizeIncrementalCase(
	t *testing.T,
	image bool,
	parentSize uint32,
	requestedSize uint32,
	diskSize int,
) {
	ctx := testcommon.NewContext()
	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()
	diskID := chunkSizeTestID(t) + "-disk"
	nbsClient, data := chunkSizeCreateDisk(t, ctx, client, diskID, diskSize)
	baseID := chunkSizeTestID(t) + "-base"
	create := func(snapshotID string, size uint32) *disk_manager.Operation {
		if image {
			operation, err := client.CreateImage(testcommon.GetRequestContext(t, ctx), &disk_manager.CreateImageRequest{
				Src: &disk_manager.CreateImageRequest_SrcDiskId{
					SrcDiskId: &disk_manager.DiskId{ZoneId: chunkSizeZone, DiskId: diskID},
				},
				DstImageId: snapshotID,
				FolderId:   chunkSizeS3Folder,
				ChunkSize:  size,
			})
			require.NoError(t, err)
			return operation
		}
		operation, err := client.CreateSnapshot(testcommon.GetRequestContext(t, ctx), &disk_manager.CreateSnapshotRequest{
			Src:        &disk_manager.DiskId{ZoneId: chunkSizeZone, DiskId: diskID},
			SnapshotId: snapshotID,
			FolderId:   chunkSizeS3Folder,
			ChunkSize:  size,
		})
		require.NoError(t, err)
		return operation
	}
	waitResponse := func(operationID string) (int64, int64) {
		if image {
			response := disk_manager.CreateImageResponse{}
			require.NoError(t, internal_client.WaitResponse(ctx, client, operationID, &response))
			return response.Size, response.StorageSize
		}
		response := disk_manager.CreateSnapshotResponse{}
		require.NoError(t, internal_client.WaitResponse(ctx, client, operationID, &response))
		return response.Size, response.StorageSize
	}
	size, storageSize := waitResponse(create(baseID, parentSize).Id)
	chunkSizeCheckSnapshot(t, ctx, baseID, parentSize, size, storageSize)

	for i := 4 * chunkSizeMiB; i < 8*chunkSizeMiB; i++ {
		data[i] ^= 0x5a
	}
	session, err := nbsClient.MountRW(ctx, diskID, 0, 0, nil)
	require.NoError(t, err)
	require.NoError(t, session.Write(ctx, 4*chunkSizeMiB/4096, data[4*chunkSizeMiB:8*chunkSizeMiB]))
	session.Close(ctx)

	incrementalID := chunkSizeTestID(t) + "-incremental"
	size, storageSize = waitResponse(create(incrementalID, requestedSize).Id)
	require.Equal(t, int64(len(data)), size)
	require.Equal(t, size, storageSize)
	meta := chunkSizeCheckSnapshot(t, ctx, incrementalID, parentSize, size, storageSize)
	require.Equal(t, baseID, meta.BaseSnapshotID)
	chunkSizeCheckSnapshot(t, ctx, baseID, parentSize, size, storageSize)
	if image {
		chunkSizeRestoreImage(t, ctx, client, incrementalID, nbs.DiskContentInfo{
			ContentSize: uint64(len(data)),
			Crc32:       crc32.ChecksumIEEE(data),
		})
	} else {
		chunkSizeRestoreSnapshot(t, ctx, client, incrementalID, data)
	}
}

func TestImageCopyPreservesChunkSize(t *testing.T) {
	ctx := testcommon.NewContext()
	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()
	diskID := chunkSizeTestID(t) + "-disk"
	_, data := chunkSizeCreateDisk(t, ctx, client, diskID)
	snapshotID := chunkSizeTestID(t) + "-snapshot"
	operation, err := client.CreateSnapshot(testcommon.GetRequestContext(t, ctx), &disk_manager.CreateSnapshotRequest{
		Src:        &disk_manager.DiskId{ZoneId: chunkSizeZone, DiskId: diskID},
		SnapshotId: snapshotID,
		FolderId:   chunkSizeS3Folder,
		ChunkSize:  chunkSizeOverride,
	})
	require.NoError(t, err)
	require.NoError(t, internal_client.WaitOperation(ctx, client, operation.Id))
	previousImageID := ""
	for _, source := range []string{"snapshot", "image"} {
		imageID := chunkSizeTestID(t) + "-from-" + source
		request := &disk_manager.CreateImageRequest{DstImageId: imageID, FolderId: chunkSizeS3Folder}
		if source == "snapshot" {
			request.Src = &disk_manager.CreateImageRequest_SrcSnapshotId{SrcSnapshotId: snapshotID}
		} else {
			request.Src = &disk_manager.CreateImageRequest_SrcImageId{SrcImageId: previousImageID}
		}
		request.ChunkSize = chunkSizeOverride
		operation, err := client.CreateImage(testcommon.GetRequestContext(t, ctx), request)
		require.Error(t, err)
		require.Nil(t, operation)
		require.Equal(t, grpc_codes.InvalidArgument, grpc_status.Code(err))
		require.Contains(t, err.Error(), "chunk size")

		// No override: copying retains 4 MiB despite the 8 MiB controlplane default.
		request.ChunkSize = 0
		operation, err = client.CreateImage(testcommon.GetRequestContext(t, ctx), request)
		require.NoError(t, err)
		response := disk_manager.CreateImageResponse{}
		require.NoError(t, internal_client.WaitResponse(ctx, client, operation.Id, &response))
		require.Equal(t, int64(len(data)), response.Size)
		require.Equal(t, response.Size, response.StorageSize)
		chunkSizeCheckSnapshot(t, ctx, imageID, chunkSizeOverride, response.Size, response.StorageSize)
		chunkSizeRestoreImage(t, ctx, client, imageID, nbs.DiskContentInfo{
			ContentSize: uint64(len(data)),
			Crc32:       crc32.ChecksumIEEE(data),
		})
		previousImageID = imageID
	}
}
