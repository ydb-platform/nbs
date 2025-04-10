package driver

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	nbs "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	"github.com/ydb-platform/nbs/cloud/blockstore/tools/csi_driver/internal/driver/mocks"
	nfs "github.com/ydb-platform/nbs/cloud/filestore/public/api/protos"
	storage "github.com/ydb-platform/nbs/cloud/storage/core/protos"
)

////////////////////////////////////////////////////////////////////////////////

func doTestCreateDeleteVolume(t *testing.T, parameters map[string]string, isLocalFsOverride bool) {
	nbsClient := mocks.NewNbsClientMock()
	nfsClient := mocks.NewNfsClientMock()
	nfsLocalClient := mocks.NewNfsClientMock()

	ctx := context.Background()
	volumeID := "test-volume-id-42"
	var blockSize uint32 = 4096
	var blockCount uint64 = 1024

	localFsOverrides := make(LocalFilestoreOverrideMap)
	if isLocalFsOverride {
		localFsOverrides[volumeID] = LocalFilestoreOverride{
			FsId:           volumeID,
			LocalMountPath: "/tmp/mnt/local_mount",
		}
	}

	controller := newNBSServerControllerService(localFsOverrides, nbsClient, nfsClient, nfsLocalClient)

	if parameters["backend"] == "nbs" {
		nbsClient.On("CreateVolume", ctx, &nbs.TCreateVolumeRequest{
			DiskId:               volumeID,
			BlockSize:            blockSize,
			BlocksCount:          blockCount,
			CloudId:              "nbs",
			FolderId:             "nbs",
			StorageMediaKind:     getStorageMediaKind(parameters),
			BaseDiskId:           parameters["base-disk-id"],
			BaseDiskCheckpointId: parameters["base-disk-checkpoint-id"],
		}).Return(&nbs.TCreateVolumeResponse{}, nil)
	}

	expectedNfsClient := nfsClient
	if isLocalFsOverride {
		expectedNfsClient = nfsLocalClient
	}

	if parameters["backend"] == "nfs" {
		expectedNfsClient.On("CreateFileStore", ctx, &nfs.TCreateFileStoreRequest{
			FileSystemId:     volumeID,
			CloudId:          "monitoring",
			FolderId:         "monitoring",
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

	expectedNfsClient.On("DestroyFileStore", ctx, &nfs.TDestroyFileStoreRequest{
		FileSystemId: volumeID,
	}).Return(&nfs.TDestroyFileStoreResponse{}, nil)

	_, err = controller.DeleteVolume(ctx, &csi.DeleteVolumeRequest{
		VolumeId: volumeID,
	})
	require.NoError(t, err)
}

func TestCreateDeleteNbsDisk(t *testing.T) {
	doTestCreateDeleteVolume(
		t,
		map[string]string{
			"backend":                          "nbs",
			"base-disk-id":                     "testBaseDiskId",
			"base-disk-checkpoint-id":          "testBaseCheckpointId",
			"csi.storage.k8s.io/pvc/namespace": "nbs",
		},
		false, // don't override local fs
	)

	doTestCreateDeleteVolume(
		t,
		map[string]string{
			"backend":                          "nbs",
			"storage-media-kind":               "ssd_nonrepl",
			"csi.storage.k8s.io/pvc/namespace": "nbs",
		},
		false, // don't override local fs
	)
}

func TestCreateDeleteNfsFilesystem(t *testing.T) {
	doTestCreateDeleteVolume(
		t,
		map[string]string{
			"backend":                          "nfs",
			"csi.storage.k8s.io/pvc/namespace": "monitoring",
		},
		false, // don't override local fs
	)
}

func TestCreateDeleteNfsLocalFilesystem(t *testing.T) {
	doTestCreateDeleteVolume(
		t,
		map[string]string{
			"backend":                          "nfs",
			"csi.storage.k8s.io/pvc/namespace": "monitoring",
		},
		true, // override local fs
	)
}

func TestGetStorageMediaKind(t *testing.T) {

	assert.Equal(
		t,
		getStorageMediaKind(map[string]string{}),
		storage.EStorageMediaKind_STORAGE_MEDIA_SSD,
	)

	assert.Equal(
		t,
		getStorageMediaKind(map[string]string{
			"storage-media-kind": "xxx",
		}),
		storage.EStorageMediaKind_STORAGE_MEDIA_SSD,
	)

	p := map[string]storage.EStorageMediaKind{
		"hdd":         storage.EStorageMediaKind_STORAGE_MEDIA_HDD,
		"hybrid":      storage.EStorageMediaKind_STORAGE_MEDIA_HDD,
		"ssd":         storage.EStorageMediaKind_STORAGE_MEDIA_SSD,
		"ssd_nonrepl": storage.EStorageMediaKind_STORAGE_MEDIA_SSD_NONREPLICATED,
		"ssd_mirror2": storage.EStorageMediaKind_STORAGE_MEDIA_SSD_MIRROR2,
		"ssd_mirror3": storage.EStorageMediaKind_STORAGE_MEDIA_SSD_MIRROR3,
		"ssd_local":   storage.EStorageMediaKind_STORAGE_MEDIA_SSD_LOCAL,
		"hdd_local":   storage.EStorageMediaKind_STORAGE_MEDIA_HDD_LOCAL,
		"hdd_nonrepl": storage.EStorageMediaKind_STORAGE_MEDIA_HDD_NONREPLICATED,
	}

	for s, v := range p {
		assert.Equal(
			t,
			getStorageMediaKind(map[string]string{
				"storage-media-kind": s,
			}),
			v,
		)
	}
}
