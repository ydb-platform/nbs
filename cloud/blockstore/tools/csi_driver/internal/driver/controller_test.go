package driver

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"
	nbs "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	"github.com/ydb-platform/nbs/cloud/blockstore/tools/csi_driver/internal/driver/mocks"
	nfs "github.com/ydb-platform/nbs/cloud/filestore/public/api/protos"
	storage "github.com/ydb-platform/nbs/cloud/storage/core/protos"
)

////////////////////////////////////////////////////////////////////////////////

func doTestCreateDeleteVolume(t *testing.T, parameters map[string]string) {
	nbsClient := mocks.NewNbsClientMock()
	nfsClient := mocks.NewNfsClientMock()

	ctx := context.Background()
	volumeID := "test-volume-id-42"
	var blockSize uint32 = 4096
	var blockCount uint64 = 1024

	controller := newNBSServerControllerService(nbsClient, nfsClient)

	if parameters["backend"] == "nbs" {
		nbsClient.On("CreateVolume", ctx, &nbs.TCreateVolumeRequest{
			DiskId:               volumeID,
			BlockSize:            blockSize,
			BlocksCount:          blockCount,
			StorageMediaKind:     getStorageMediaKind(parameters),
			BaseDiskId:           parameters["base-disk-id"],
			BaseDiskCheckpointId: parameters["base-disk-checkpoint-id"],
		}).Return(&nbs.TCreateVolumeResponse{}, nil)
	}

	if parameters["backend"] == "nfs" {
		nfsClient.On("CreateFileStore", ctx, &nfs.TCreateFileStoreRequest{
			FileSystemId:     volumeID,
			CloudId:          "fakeCloud",
			FolderId:         "fakeFolder",
			BlockSize:        blockSize,
			BlocksCount:      blockCount,
			StorageMediaKind: storage.EStorageMediaKind_STORAGE_MEDIA_SSD,
		}).Return(&nfs.TCreateFileStoreResponse{}, nil)
	}

	_, err := controller.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               volumeID,
		Parameters:         parameters,
		VolumeCapabilities: []*csi.VolumeCapability{},
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: int64(blockCount * uint64(blockSize)),
		},
	})
	require.NoError(t, err)

	nbsClient.On("DestroyVolume", ctx, &nbs.TDestroyVolumeRequest{
		DiskId: volumeID,
	}).Return(&nbs.TDestroyVolumeResponse{}, nil)

	nfsClient.On("DestroyFileStore", ctx, &nfs.TDestroyFileStoreRequest{
		FileSystemId: volumeID,
	}).Return(&nfs.TDestroyFileStoreResponse{}, nil)

	_, err = controller.DeleteVolume(ctx, &csi.DeleteVolumeRequest{
		VolumeId: volumeID,
	})
	require.NoError(t, err)
}

func TestCreateDeleteNbsDisk(t *testing.T) {
	doTestCreateDeleteVolume(t, map[string]string{
		"backend":                 "nbs",
		"base-disk-id":            "testBaseDiskId",
		"base-disk-checkpoint-id": "testBaseCheckpointId",
	})

	doTestCreateDeleteVolume(t, map[string]string{
		"backend":            "nbs",
		"storage-media-kind": "ssd_nonrepl",
	})
}

func TestCreateDeleteNfsFilesystem(t *testing.T) {
	doTestCreateDeleteVolume(t, map[string]string{
		"backend": "nfs",
	})
}
