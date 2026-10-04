package driver

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	nbs "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	nbsclient "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	"github.com/ydb-platform/nbs/cloud/blockstore/tools/csi_driver/internal/driver/mocks"
	nfs "github.com/ydb-platform/nbs/cloud/filestore/public/api/protos"
	storage "github.com/ydb-platform/nbs/cloud/storage/core/protos"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

////////////////////////////////////////////////////////////////////////////////

func doTestCreateDeleteVolume(t *testing.T, parameters map[string]string) {
	nbsClient := mocks.NewNbsClientMock()
	nfsClient := mocks.NewNfsClientMock()

	ctx := context.Background()
	volumeID := "test-volume-id-42"
	var blockSize uint32 = 4096
	var blockCount uint64 = 1024

	controller := newNBSServerControllerService(nbsClient, nfsClient, false, false)

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

	if parameters["backend"] == "nfs" {
		nfsClient.On("CreateFileStore", ctx, &nfs.TCreateFileStoreRequest{
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

	nfsClient.On("DestroyFileStore", ctx, &nfs.TDestroyFileStoreRequest{
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
	)

	doTestCreateDeleteVolume(
		t,
		map[string]string{
			"backend":                          "nbs",
			"storage-media-kind":               "ssd_nonrepl",
			"csi.storage.k8s.io/pvc/namespace": "nbs",
		},
	)
}

func TestCreateDeleteNfsFilesystem(t *testing.T) {
	doTestCreateDeleteVolume(
		t,
		map[string]string{
			"backend":                          "nfs",
			"csi.storage.k8s.io/pvc/namespace": "monitoring",
		},
	)
}

func TestCreateDeleteNfsLocalFilesystem(t *testing.T) {
	doTestCreateDeleteVolume(
		t,
		map[string]string{
			"backend":                          "nfs",
			"csi.storage.k8s.io/pvc/namespace": "monitoring",
		},
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

func TestControllerExpandVolume(t *testing.T) {
	for _, tc := range []struct {
		name          string
		clients       []*nbs.TVolumeClient
		currentBlocks uint64
		requiredBytes int64
		limitBytes    int64
		statError     error
		resizeError   error
		wantCode      codes.Code
		wantBlocks    uint64
		wantResize    bool
	}{
		{name: "offline", currentBlocks: 1, requiredBytes: 8192, wantBlocks: 2, wantResize: true},
		{name: "clients", currentBlocks: 1, requiredBytes: 8192,
			clients: []*nbs.TVolumeClient{{ClientId: "csi-client"}}, wantCode: codes.FailedPrecondition},
		{name: "already expanded", currentBlocks: 2, requiredBytes: 8192, wantBlocks: 2,
			clients: []*nbs.TVolumeClient{{ClientId: "csi-client"}}},
		{name: "larger than requested", currentBlocks: 3, requiredBytes: 8192, wantBlocks: 3},
		{name: "round up", currentBlocks: 1, requiredBytes: 8193, wantBlocks: 3, wantResize: true},
		{name: "limit after rounding", currentBlocks: 1, requiredBytes: 8193, limitBytes: 9000, wantCode: codes.OutOfRange},
		{name: "limit below current size", currentBlocks: 3, requiredBytes: 8192, limitBytes: 8192, wantCode: codes.OutOfRange},
		{name: "limit only", currentBlocks: 1, limitBytes: 8192, wantBlocks: 1},
		{name: "large capacity", currentBlocks: 1, requiredBytes: (1 << 53) + 1,
			wantBlocks: (1 << 41) + 1, wantResize: true},
		{name: "capacity overflow", currentBlocks: 1, requiredBytes: math.MaxInt64, wantCode: codes.OutOfRange},
		{name: "stat timeout", currentBlocks: 1, requiredBytes: 8192,
			statError: &nbsclient.ClientError{Code: nbsclient.E_GRPC_DEADLINE_EXCEEDED}, wantCode: codes.DeadlineExceeded},
		{name: "stat not found", currentBlocks: 1, requiredBytes: 8192,
			statError: &nbsclient.ClientError{Code: nbsclient.E_NOT_FOUND}, wantCode: codes.NotFound},
		{name: "stat error", currentBlocks: 1, requiredBytes: 8192,
			statError: fmt.Errorf("stat failed"), wantCode: codes.Internal},
		{name: "resize unavailable", currentBlocks: 1, requiredBytes: 8192,
			wantBlocks: 2, wantResize: true,
			resizeError: &nbsclient.ClientError{Code: nbsclient.E_REJECTED}, wantCode: codes.Unavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			client := mocks.NewNbsClientMock()
			controller := newNBSServerControllerService(client, nil, false, true)
			client.On("StatVolume", ctx, &nbs.TStatVolumeRequest{DiskId: defaultDiskId, NoPartition: true}).
				Return(&nbs.TStatVolumeResponse{
					Volume:  &nbs.TVolume{BlockSize: 4096, BlocksCount: tc.currentBlocks, ConfigVersion: 42},
					Clients: tc.clients,
				}, tc.statError).Once()
			if tc.wantResize {
				client.On("ResizeVolume", ctx, &nbs.TResizeVolumeRequest{
					DiskId: defaultDiskId, BlocksCount: tc.wantBlocks, ConfigVersion: 42,
				}).Return(&nbs.TResizeVolumeResponse{}, tc.resizeError).Once()
			}
			resp, err := controller.ControllerExpandVolume(ctx, &csi.ControllerExpandVolumeRequest{
				VolumeId:      defaultDiskId,
				CapacityRange: &csi.CapacityRange{RequiredBytes: tc.requiredBytes, LimitBytes: tc.limitBytes},
			})
			require.Equal(t, tc.wantCode, status.Code(err), "%v", err)
			if tc.wantCode == codes.OK {
				require.NotNil(t, resp)
				assert.Equal(t, int64(tc.wantBlocks*4096), resp.CapacityBytes)
				assert.False(t, resp.NodeExpansionRequired)
			} else {
				assert.Nil(t, resp)
			}
			client.AssertExpectations(t)
			client.AssertNotCalled(t, "RefreshEndpoint", mock.Anything, mock.Anything)
			client.AssertNotCalled(t, "DescribeVolume", mock.Anything, mock.Anything)
		})
	}
}

func TestControllerExpandVolumeInvalidRequest(t *testing.T) {
	for _, tc := range []struct {
		name string
		req  *csi.ControllerExpandVolumeRequest
	}{
		{name: "missing volume", req: &csi.ControllerExpandVolumeRequest{CapacityRange: &csi.CapacityRange{RequiredBytes: 8192}}},
		{name: "missing range", req: &csi.ControllerExpandVolumeRequest{VolumeId: defaultDiskId}},
		{name: "empty range", req: &csi.ControllerExpandVolumeRequest{VolumeId: defaultDiskId, CapacityRange: &csi.CapacityRange{}}},
		{name: "negative required", req: &csi.ControllerExpandVolumeRequest{VolumeId: defaultDiskId, CapacityRange: &csi.CapacityRange{RequiredBytes: -1}}},
		{name: "negative limit", req: &csi.ControllerExpandVolumeRequest{VolumeId: defaultDiskId, CapacityRange: &csi.CapacityRange{RequiredBytes: 8192, LimitBytes: -1}}},
		{name: "inverted range", req: &csi.ControllerExpandVolumeRequest{VolumeId: defaultDiskId, CapacityRange: &csi.CapacityRange{RequiredBytes: 8192, LimitBytes: 4096}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := mocks.NewNbsClientMock()
			controller := newNBSServerControllerService(client, nil, false, true)
			_, err := controller.ControllerExpandVolume(context.Background(), tc.req)
			require.Equal(t, codes.InvalidArgument, status.Code(err))
			assert.Empty(t, client.Calls)
		})
	}
}

func TestExpansionCapabilities(t *testing.T) {
	for _, vmMode := range []bool{false, true} {
		for _, offline := range []bool{false, true} {
			t.Run(fmt.Sprintf("vmMode=%t/offline=%t", vmMode, offline), func(t *testing.T) {
				ctx := context.Background()
				identity := newIdentityService("test", "test", vmMode, offline)
				resp, err := identity.GetPluginCapabilities(ctx, &csi.GetPluginCapabilitiesRequest{})
				require.NoError(t, err)
				expansionType := csi.PluginCapability_VolumeExpansion_UNKNOWN
				for _, capability := range resp.Capabilities {
					if expansion := capability.GetVolumeExpansion(); expansion != nil {
						expansionType = expansion.Type
					}
				}
				wantType := csi.PluginCapability_VolumeExpansion_ONLINE
				if offline {
					wantType = csi.PluginCapability_VolumeExpansion_OFFLINE
				}
				if vmMode {
					wantType = csi.PluginCapability_VolumeExpansion_UNKNOWN
				}
				assert.Equal(t, wantType, expansionType)
				controller := newNBSServerControllerService(nil, nil, vmMode, offline)
				controllerResp, err := controller.ControllerGetCapabilities(ctx, &csi.ControllerGetCapabilitiesRequest{})
				require.NoError(t, err)
				expands := false
				for _, capability := range controllerResp.Capabilities {
					if capability.GetRpc().GetType() == csi.ControllerServiceCapability_RPC_EXPAND_VOLUME {
						expands = true
					}
				}
				assert.Equal(t, !vmMode && offline, expands)
				node := &nodeService{vmMode: vmMode, offlineResize: offline}
				nodeResp, err := node.NodeGetCapabilities(ctx, &csi.NodeGetCapabilitiesRequest{})
				require.NoError(t, err)
				nodeExpands := false
				for _, capability := range nodeResp.Capabilities {
					if capability.GetRpc().GetType() == csi.NodeServiceCapability_RPC_EXPAND_VOLUME {
						nodeExpands = true
					}
				}
				assert.Equal(t, vmMode || !offline, nodeExpands)
				if !vmMode && offline {
					_, err := node.NodeExpandVolume(ctx, &csi.NodeExpandVolumeRequest{})
					assert.Equal(t, codes.Unimplemented, status.Code(err))
				}
				if vmMode || !offline {
					_, err := controller.ControllerExpandVolume(ctx, &csi.ControllerExpandVolumeRequest{})
					assert.Equal(t, codes.Unimplemented, status.Code(err))
				}
			})
		}
	}
}

func TestOfflineExpansionRetryAfterUnstage(t *testing.T) {
	ctx := context.Background()
	client := mocks.NewNbsClientMock()
	controller := newNBSServerControllerService(client, nil, false, true)
	volume := &nbs.TVolume{BlockSize: 4096, BlocksCount: 1, ConfigVersion: 42}
	stat := &nbs.TStatVolumeResponse{Volume: volume, Clients: []*nbs.TVolumeClient{{ClientId: "csi-client"}}}
	request := &csi.ControllerExpandVolumeRequest{
		VolumeId: defaultDiskId, CapacityRange: &csi.CapacityRange{RequiredBytes: 8192},
	}
	client.On("StatVolume", ctx, &nbs.TStatVolumeRequest{DiskId: defaultDiskId, NoPartition: true}).
		Return(stat, nil).Times(3)

	// While staged, the CSI endpoint is itself a client of the disk.
	_, err := controller.ControllerExpandVolume(ctx, request)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	client.AssertNotCalled(t, "ResizeVolume", mock.Anything, mock.Anything)

	// Unstage disconnects the endpoint. A retry must release the operation lock
	// and reach ResizeVolume, even though the previous attempt failed.
	stat.Clients = nil
	client.On("ResizeVolume", ctx, &nbs.TResizeVolumeRequest{
		DiskId: defaultDiskId, BlocksCount: 2, ConfigVersion: 42,
	}).Return(&nbs.TResizeVolumeResponse{}, nil).Run(func(mock.Arguments) {
		volume.BlocksCount = 2
		volume.ConfigVersion++
	}).Once()
	resp, err := controller.ControllerExpandVolume(ctx, request)
	require.NoError(t, err)
	require.Equal(t, int64(8192), resp.CapacityBytes)
	require.False(t, resp.NodeExpansionRequired)

	// A lost response can be retried after staging has connected a new client.
	// The controller must succeed despite clients and must not mutate the disk again.
	stat.Clients = []*nbs.TVolumeClient{{ClientId: "new-csi-client"}}
	retryResp, err := controller.ControllerExpandVolume(ctx, request)
	require.NoError(t, err)
	assert.Equal(t, resp, retryResp)
	client.AssertExpectations(t)
}
