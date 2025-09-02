package driver

//lint:file-ignore ST1003 protobuf generates names that break golang naming convention

import (
	"context"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	nbs "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	nbsclient "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	"github.com/ydb-platform/nbs/cloud/blockstore/tools/csi_driver/internal/driver/mocks"
	csimounter "github.com/ydb-platform/nbs/cloud/blockstore/tools/csi_driver/internal/mounter"
	nfs "github.com/ydb-platform/nbs/cloud/filestore/public/api/protos"
	storagecoreapi "github.com/ydb-platform/nbs/cloud/storage/core/protos"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultStartEndpointRequestTimeout = 10 * time.Second
	defaultNodeId                      = "testNodeId"
	defaultCientId                     = "testClientId"
	defaultPodId                       = "test-pod-id-13"
	defaultDiskId                      = "test-disk-id-42"
	defaultInstanceId                  = "testInstanceId"
)

type LocalFsOverride int

const (
	LocalFsOverrideDisabled LocalFsOverride = iota
	LocalFsOverrideLegacyEnabled
	LocalFsOverrideEnabled
)

////////////////////////////////////////////////////////////////////////////////

func getDefaultStartEndpointRequestHeaders() *nbs.THeaders {
	return &nbs.THeaders{
		RequestTimeout: uint32(defaultStartEndpointRequestTimeout.Milliseconds()),
	}
}

////////////////////////////////////////////////////////////////////////////////

func doTestPublishUnpublishVolumeForKubevirtHelper(
	t *testing.T,
	backend string,
	deviceNameOpt *string,
	requestQueuesCountOpt *uint32,
) {
	t.Helper()
	tempDir := t.TempDir()

	nbsClient := mocks.NewNbsClientMock()
	nfsClient := mocks.NewNfsEndpointClientMock()
	mounter := csimounter.NewMock()

	ctx := context.Background()

	actualClientId := defaultCientId + "-" + defaultPodId
	deviceName := defaultDiskId
	if deviceNameOpt != nil {
		deviceName = *deviceNameOpt
	}
	stagingTargetPath := "testStagingTargetPath"
	socketsDir := filepath.Join(tempDir, "sockets")
	sourcePath := filepath.Join(socketsDir, defaultPodId, defaultDiskId)
	targetPath := filepath.Join(tempDir, "pods", defaultPodId, "volumes", defaultDiskId, "mount")
	targetFsPathPattern := filepath.Join(tempDir, "pods/([a-z0-9-]+)/volumes/([a-z0-9-]+)/mount")
	nbsSocketPath := filepath.Join(sourcePath, "nbs.sock")
	nfsSocketPath := filepath.Join(sourcePath, "nfs.sock")

	nodeService := newNodeService(
		defaultNodeId,
		defaultCientId,
		true, // vmMode
		socketsDir,
		targetFsPathPattern,
		"", // targetBlkPathPattern
		nil,
		nbsClient,
		nfsClient,
		nil,
		nil,
		mounter,
		[]string{},
		true, // enable discard
		defaultStartEndpointRequestTimeout,
	)

	accessMode := csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER
	if backend == "nfs" {
		accessMode = csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER
	}

	volumeCapability := csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{
			Mount: &csi.VolumeCapability_MountVolume{},
		},
		AccessMode: &csi.VolumeCapability_AccessMode{
			Mode: accessMode,
		},
	}

	volumeContext := map[string]string{
		backendVolumeContextKey: backend,
	}
	if deviceNameOpt != nil {
		volumeContext[deviceNameVolumeContextKey] = *deviceNameOpt
	}

	_, err := nodeService.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
		VolumeId:          defaultDiskId,
		StagingTargetPath: stagingTargetPath,
		VolumeCapability:  &volumeCapability,
		VolumeContext:     volumeContext,
	})
	require.NoError(t, err)

	hostType := nbs.EHostType_HOST_TYPE_DEFAULT

	vhostQueuesCount := defaultVhostQueuesCount // explicit default value for unset behavior
	if backend == "nbs" {
		if requestQueuesCountOpt != nil {
			volumeContext[requestQueuesCountVolumeContextKey] = strconv.Itoa(int(*requestQueuesCountOpt))
			vhostQueuesCount = virtioBlkVhostQueuesCount(*requestQueuesCountOpt)
		}
		nbsClient.On("ListEndpoints", ctx, &nbs.TListEndpointsRequest{}).Return(&nbs.TListEndpointsResponse{}, nil)
		nbsClient.On("StartEndpoint", ctx, &nbs.TStartEndpointRequest{
			Headers:          getDefaultStartEndpointRequestHeaders(),
			UnixSocketPath:   nbsSocketPath,
			DiskId:           defaultDiskId,
			InstanceId:       defaultPodId,
			ClientId:         actualClientId,
			DeviceName:       deviceName,
			IpcType:          nbs.EClientIpcType_IPC_VHOST,
			VhostQueuesCount: vhostQueuesCount,
			VolumeAccessMode: nbs.EVolumeAccessMode_VOLUME_ACCESS_READ_WRITE,
			VolumeMountMode:  nbs.EVolumeMountMode_VOLUME_MOUNT_LOCAL,
			Persistent:       true,
			NbdDevice: &nbs.TStartEndpointRequest_UseFreeNbdDeviceFile{
				false,
			},
			ClientProfile: &nbs.TClientProfile{
				HostType: &hostType,
			},
		}).Return(&nbs.TStartEndpointResponse{}, nil)
	}

	if backend == "nfs" {
		if requestQueuesCountOpt != nil {
			volumeContext[requestQueuesCountVolumeContextKey] = strconv.Itoa(int(*requestQueuesCountOpt))
			vhostQueuesCount = virtioFsVhostQueuesCount(*requestQueuesCountOpt)
		}
		nfsClient.On("StartEndpoint", ctx, &nfs.TStartEndpointRequest{
			Endpoint: &nfs.TEndpointConfig{
				SocketPath:       nfsSocketPath,
				FileSystemId:     defaultDiskId,
				ClientId:         actualClientId,
				VhostQueuesCount: vhostQueuesCount,
				Persistent:       true,
			},
		}).Return(&nfs.TStartEndpointResponse{}, nil)
	}

	mounter.On("IsMountPoint", targetPath).Return(false, nil)
	mounter.On("Mount", sourcePath, targetPath, "", []string{"bind"}).Return(nil)

	_, err = nodeService.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:          defaultDiskId,
		StagingTargetPath: stagingTargetPath,
		TargetPath:        targetPath,
		VolumeCapability:  &volumeCapability,
		VolumeContext:     volumeContext,
	})
	require.NoError(t, err)

	fileInfo, err := os.Stat(sourcePath)
	assert.False(t, os.IsNotExist(err))
	assert.True(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0755), fileInfo.Mode().Perm())

	fileInfo, err = os.Stat(filepath.Join(sourcePath, "disk.img"))
	assert.False(t, os.IsNotExist(err))
	assert.False(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0644), fileInfo.Mode().Perm())

	fileInfo, err = os.Stat(targetPath)
	assert.False(t, os.IsNotExist(err))
	assert.True(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0775), fileInfo.Mode().Perm())

	mounter.On("CleanupMountPoint", targetPath).Return(nil)

	nbsClient.On("StopEndpoint", ctx, &nbs.TStopEndpointRequest{
		UnixSocketPath: nbsSocketPath,
	}).Return(&nbs.TStopEndpointResponse{}, nil)

	nfsClient.On("StopEndpoint", ctx, &nfs.TStopEndpointRequest{
		SocketPath: nfsSocketPath,
	}).Return(&nfs.TStopEndpointResponse{}, nil)

	_, err = nodeService.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   defaultDiskId,
		TargetPath: targetPath,
	})
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(socketsDir, defaultPodId))
	assert.True(t, os.IsNotExist(err))

	_, err = nodeService.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{
		VolumeId:          defaultDiskId,
		StagingTargetPath: stagingTargetPath,
	})
	require.NoError(t, err)

	nbsClient.AssertExpectations(t)
	nfsClient.AssertExpectations(t)
	mounter.AssertExpectations(t)
}

func TestPublishUnpublishVolumeForKubevirt(t *testing.T) {
	deviceName1 := "test-disk-name-42"
	rqc32 := uint32(32)

	testCases := []struct {
		name                  string
		backend               string
		deviceNameOpt         *string
		requestQueuesCountOpt *uint32
	}{
		{
			name:                  "DiskForKubevirt",
			backend:               "nbs",
			deviceNameOpt:         nil,
			requestQueuesCountOpt: nil,
		},
		{
			name:                  "DiskForKubevirtSetDeviceName",
			backend:               "nbs",
			deviceNameOpt:         &deviceName1,
			requestQueuesCountOpt: nil,
		},
		{
			name:                  "FilestoreForKubevirt",
			backend:               "nfs",
			deviceNameOpt:         nil,
			requestQueuesCountOpt: nil,
		},
		{
			name:                  "DiskForKubevirtMultiqueue",
			backend:               "nbs",
			deviceNameOpt:         nil,
			requestQueuesCountOpt: &rqc32,
		},
		{
			name:                  "DiskForKubevirtSetDeviceNameMultiqueue",
			backend:               "nbs",
			deviceNameOpt:         &deviceName1,
			requestQueuesCountOpt: &rqc32,
		},
		{
			name:                  "FilestoreForKubevirtMultiqueue",
			backend:               "nfs",
			deviceNameOpt:         nil,
			requestQueuesCountOpt: &rqc32,
		},
	}

	for _, tc := range testCases {
		tc := tc // Capture range variable.
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel() // Mark test as parallelizable.
			doTestPublishUnpublishVolumeForKubevirtHelper(
				t,
				tc.backend,
				tc.deviceNameOpt,
				tc.requestQueuesCountOpt,
			)
		})
	}
}

func doTestStagedPublishUnpublishVolumeForKubevirtHelper(
	t *testing.T,
	backend string,
	deviceNameOpt *string,
	perInstanceVolumes bool,
	localFsOverride LocalFsOverride,
	requestQueuesCountOpt *uint32,
) {
	t.Helper()
	tempDir := t.TempDir()

	nbsClient := mocks.NewNbsClientMock()
	nfsClient := mocks.NewNfsEndpointClientMock()
	nfsLocalClient := mocks.NewNfsEndpointClientMock()
	nfsLocalFilestoreClient := mocks.NewNfsClientMock()
	mounter := csimounter.NewMock()

	ctx := context.Background()
	actualClientId := defaultCientId + "-" + defaultInstanceId
	deviceName := defaultDiskId
	if deviceNameOpt != nil {
		deviceName = *deviceNameOpt
	}
	volumeId := defaultDiskId
	if perInstanceVolumes {
		volumeId = defaultDiskId + "#" + defaultInstanceId
	}
	stagingTargetPath := filepath.Join(tempDir, "testStagingTargetPath")
	socketsDir := filepath.Join(tempDir, "sockets")
	sourcePath := filepath.Join(socketsDir, defaultInstanceId, defaultDiskId)
	targetPath := filepath.Join(tempDir, "pods", defaultPodId, "volumes", defaultDiskId, "mount")
	targetFsPathPattern := filepath.Join(tempDir, "pods/([a-z0-9-]+)/volumes/([a-z0-9-]+)/mount")
	nbsSocketPath := filepath.Join(sourcePath, "nbs.sock")
	nfsSocketPath := filepath.Join(sourcePath, "nfs.sock")

	localFsOverrides := make(ExternalFsOverrideMap)
	if localFsOverride == LocalFsOverrideLegacyEnabled {
		localFsOverrides[defaultDiskId] = ExternalFsConfig{
			Id: defaultDiskId,
		}
	} else if localFsOverride == LocalFsOverrideEnabled {
		localFsOverrides[defaultDiskId] = ExternalFsConfig{
			Id:         defaultDiskId,
			Type:       "local",
			SizeGb:     10,
			CloudId:    "test",
			FolderId:   "test",
			MountCmd:   "echo",
			MountArgs:  []string{"hello"},
			UmountCmd:  "echo",
			UmountArgs: []string{"goodbye"},
		}
	}

	nodeService := newNodeService(
		defaultNodeId,
		defaultCientId,
		true, // vmMode
		socketsDir,
		targetFsPathPattern,
		"", // targetBlkPathPattern
		localFsOverrides,
		nbsClient,
		nfsClient,
		nfsLocalClient,
		nfsLocalFilestoreClient,
		mounter,
		[]string{},
		false, // enableDiscard is false for staged tests
		defaultStartEndpointRequestTimeout,
	)

	accessMode := csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER
	if backend == "nfs" {
		accessMode = csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER
	}

	volumeCapability := csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{
			Mount: &csi.VolumeCapability_MountVolume{},
		},
		AccessMode: &csi.VolumeCapability_AccessMode{
			Mode: accessMode,
		},
	}

	volumeContext := map[string]string{
		backendVolumeContextKey: backend,
		instanceIdKey:           defaultInstanceId,
	}
	if deviceNameOpt != nil {
		volumeContext[deviceNameVolumeContextKey] = *deviceNameOpt
	}

	vhostQueuesCount := defaultVhostQueuesCount // explicit default value for unset behavior
	hostType := nbs.EHostType_HOST_TYPE_DEFAULT
	if backend == "nbs" {
		if requestQueuesCountOpt != nil {
			volumeContext[requestQueuesCountVolumeContextKey] = strconv.Itoa(int(*requestQueuesCountOpt))
			vhostQueuesCount = virtioBlkVhostQueuesCount(*requestQueuesCountOpt)
		}
		nbsClient.On("ListEndpoints", ctx, &nbs.TListEndpointsRequest{}).Return(&nbs.TListEndpointsResponse{}, nil)
		nbsClient.On("StartEndpoint", ctx, &nbs.TStartEndpointRequest{
			Headers:          getDefaultStartEndpointRequestHeaders(),
			UnixSocketPath:   nbsSocketPath,
			DiskId:           defaultDiskId,
			InstanceId:       defaultInstanceId,
			ClientId:         actualClientId,
			DeviceName:       deviceName,
			IpcType:          nbs.EClientIpcType_IPC_VHOST,
			VhostQueuesCount: vhostQueuesCount,
			VolumeAccessMode: nbs.EVolumeAccessMode_VOLUME_ACCESS_READ_WRITE,
			VolumeMountMode:  nbs.EVolumeMountMode_VOLUME_MOUNT_LOCAL,
			Persistent:       true,
			NbdDevice: &nbs.TStartEndpointRequest_UseFreeNbdDeviceFile{
				false,
			},
			ClientProfile: &nbs.TClientProfile{
				HostType: &hostType,
			},
			// VhostDiscardEnabled is false because enableDiscard for newNodeService is false
		}).Return(&nbs.TStartEndpointResponse{}, nil)
	}

	expectedNfsClient := nfsClient
	if localFsOverride == LocalFsOverrideLegacyEnabled || localFsOverride == LocalFsOverrideEnabled {
		expectedNfsClient = nfsLocalClient
	}

	if backend == "nfs" {
		if localFsOverride == LocalFsOverrideEnabled {
			nfsLocalFilestoreClient.On("CreateFileStore", ctx, &nfs.TCreateFileStoreRequest{
				FileSystemId:     fmt.Sprintf("%s-%s", defaultDiskId, defaultInstanceId),
				CloudId:          localFsOverrides[defaultDiskId].CloudId,
				FolderId:         localFsOverrides[defaultDiskId].FolderId,
				BlockSize:        4096,
				BlocksCount:      (localFsOverrides[defaultDiskId].SizeGb << 30) / 4096,
				StorageMediaKind: storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_SSD,
			}).Return(&nfs.TCreateFileStoreResponse{}, nil)

			nfsLocalClient.On("ListEndpoints", ctx, &nfs.TListEndpointsRequest{}).Return(&nfs.TListEndpointsResponse{}, nil)

		}
		if requestQueuesCountOpt != nil {
			volumeContext[requestQueuesCountVolumeContextKey] = strconv.Itoa(int(*requestQueuesCountOpt))
			vhostQueuesCount = virtioFsVhostQueuesCount(*requestQueuesCountOpt)
		}

		expectedFsId := defaultDiskId
		if localFsOverride == LocalFsOverrideEnabled {
			expectedFsId = fmt.Sprintf("%s-%s", defaultDiskId, defaultInstanceId)
		}

		expectedNfsClient.On("StartEndpoint", ctx, &nfs.TStartEndpointRequest{
			Endpoint: &nfs.TEndpointConfig{
				SocketPath:       nfsSocketPath,
				FileSystemId:     expectedFsId,
				ClientId:         actualClientId,
				VhostQueuesCount: vhostQueuesCount,
				Persistent:       true,
			},
		}).Return(&nfs.TStartEndpointResponse{}, nil)
	}

	_, err := nodeService.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
		VolumeId:          volumeId,
		StagingTargetPath: stagingTargetPath,
		VolumeCapability:  &volumeCapability,
		VolumeContext:     volumeContext,
	})
	require.NoError(t, err)

	fileInfo, err := os.Stat(sourcePath)
	assert.False(t, os.IsNotExist(err))
	assert.True(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0755), fileInfo.Mode().Perm())

	fileInfo, err = os.Stat(filepath.Join(sourcePath, "disk.img"))
	assert.False(t, os.IsNotExist(err))
	assert.False(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0644), fileInfo.Mode().Perm())

	mounter.On("IsMountPoint", targetPath).Return(false, nil)
	mounter.On("Mount", sourcePath, targetPath, "", []string{"bind"}).Return(nil)

	_, err = nodeService.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:          volumeId,
		StagingTargetPath: stagingTargetPath,
		TargetPath:        targetPath,
		VolumeCapability:  &volumeCapability,
		VolumeContext:     volumeContext,
	})
	require.NoError(t, err)

	fileInfo, err = os.Stat(targetPath)
	assert.False(t, os.IsNotExist(err))
	assert.True(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0775), fileInfo.Mode().Perm())

	mounter.On("CleanupMountPoint", targetPath).Return(nil)

	if !perInstanceVolumes {
		// Driver attempts to stop legacy endpoints only for legacy volumes.
		nbsClient.On("StopEndpoint", ctx, &nbs.TStopEndpointRequest{
			UnixSocketPath: filepath.Join(socketsDir, defaultPodId, defaultDiskId, nbsSocketName),
		}).Return(&nbs.TStopEndpointResponse{}, nil)

		expectedNfsClient.On("StopEndpoint", ctx, &nfs.TStopEndpointRequest{
			SocketPath: filepath.Join(socketsDir, defaultPodId, defaultDiskId, nfsSocketName),
		}).Return(&nfs.TStopEndpointResponse{}, nil)
	}

	_, err = nodeService.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   volumeId,
		TargetPath: targetPath,
	})
	require.NoError(t, err)

	if backend == "nbs" {
		nbsClient.On("StopEndpoint", ctx, &nbs.TStopEndpointRequest{
			UnixSocketPath: nbsSocketPath,
		}).Return(&nbs.TStopEndpointResponse{}, nil)
	}

	if backend == "nfs" {
		expectedNfsClient.On("StopEndpoint", ctx, &nfs.TStopEndpointRequest{
			SocketPath: nfsSocketPath,
		}).Return(&nfs.TStopEndpointResponse{}, nil)

		if localFsOverride == LocalFsOverrideEnabled {
			nfsLocalFilestoreClient.On("DestroyFileStore", ctx, &nfs.TDestroyFileStoreRequest{
				FileSystemId: fmt.Sprintf("%s-%s", defaultDiskId, defaultInstanceId),
			}).Return(&nfs.TDestroyFileStoreResponse{}, nil)
		}
	}

	_, err = nodeService.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{
		VolumeId:          volumeId,
		StagingTargetPath: stagingTargetPath,
	})
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(socketsDir, defaultInstanceId))
	assert.True(t, os.IsNotExist(err))

	nbsClient.AssertExpectations(t)
	nfsClient.AssertExpectations(t)
	nfsLocalClient.AssertExpectations(t)
	nfsLocalFilestoreClient.AssertExpectations(t)
	mounter.AssertExpectations(t)
}

func TestStagedPublishUnpublishVolumeForKubevirt(t *testing.T) {
	deviceName1 := "test-disk-name-42"
	rqc32 := uint32(32)

	testCases := []struct {
		name                  string
		backend               string
		deviceNameOpt         *string
		perInstanceVolumes    bool
		localFsOverride       LocalFsOverride
		requestQueuesCountOpt *uint32
	}{
		// Legacy tests (perInstanceVolumes = false, requestQueuesCountOpt = nil)
		{
			name:                  "DiskForKubevirtLegacy",
			backend:               "nbs",
			deviceNameOpt:         nil,
			perInstanceVolumes:    false,
			localFsOverride:       LocalFsOverrideDisabled,
			requestQueuesCountOpt: nil,
		},
		{
			name:                  "DiskForKubevirtSetDeviceNameLegacy",
			backend:               "nbs",
			deviceNameOpt:         &deviceName1,
			perInstanceVolumes:    false,
			localFsOverride:       LocalFsOverrideDisabled,
			requestQueuesCountOpt: nil,
		},
		{
			name:                  "FilestoreForKubevirtLegacy",
			backend:               "nfs",
			deviceNameOpt:         nil,
			perInstanceVolumes:    false,
			localFsOverride:       LocalFsOverrideDisabled,
			requestQueuesCountOpt: nil,
		},
		{
			name:                  "LocalFilestoreForKubevirtLegacy",
			backend:               "nfs",
			deviceNameOpt:         nil,
			perInstanceVolumes:    false,
			localFsOverride:       LocalFsOverrideLegacyEnabled,
			requestQueuesCountOpt: nil,
		},
		// Per-instance volume tests (perInstanceVolumes = true, requestQueuesCountOpt = nil)
		{
			name:                  "DiskForKubevirt",
			backend:               "nbs",
			deviceNameOpt:         nil,
			perInstanceVolumes:    true,
			localFsOverride:       LocalFsOverrideDisabled,
			requestQueuesCountOpt: nil,
		},
		{
			name:                  "DiskForKubevirtSetDeviceName",
			backend:               "nbs",
			deviceNameOpt:         &deviceName1,
			perInstanceVolumes:    true,
			localFsOverride:       LocalFsOverrideDisabled,
			requestQueuesCountOpt: nil,
		},
		{
			name:                  "FilestoreForKubevirt",
			backend:               "nfs",
			deviceNameOpt:         nil,
			perInstanceVolumes:    true,
			localFsOverride:       LocalFsOverrideDisabled,
			requestQueuesCountOpt: nil,
		},
		{
			name:                  "LocalFilestoreForKubevirt",
			backend:               "nfs",
			deviceNameOpt:         nil,
			perInstanceVolumes:    true,
			localFsOverride:       LocalFsOverrideEnabled,
			requestQueuesCountOpt: nil,
		},
		// Multiqueue tests (perInstanceVolumes = false, requestQueuesCountOpt = &rqc32)
		// These correspond to the original "Multiqueue" tests which were legacy style.
		{
			name:                  "DiskForKubevirtMultiqueue",
			backend:               "nbs",
			deviceNameOpt:         nil,
			perInstanceVolumes:    false,
			localFsOverride:       LocalFsOverrideDisabled,
			requestQueuesCountOpt: &rqc32,
		},
		{
			name:                  "DiskForKubevirtSetDeviceNameMultiqueue",
			backend:               "nbs",
			deviceNameOpt:         &deviceName1,
			perInstanceVolumes:    false,
			localFsOverride:       LocalFsOverrideDisabled,
			requestQueuesCountOpt: &rqc32,
		},
		{
			name:                  "FilestoreForKubevirtMultiqueue",
			backend:               "nfs",
			deviceNameOpt:         nil,
			perInstanceVolumes:    false,
			localFsOverride:       LocalFsOverrideDisabled,
			requestQueuesCountOpt: &rqc32,
		},
		{
			name:                  "LocalFilestoreForKubevirtMultiqueueLegacy",
			backend:               "nfs",
			deviceNameOpt:         nil,
			perInstanceVolumes:    false,
			localFsOverride:       LocalFsOverrideLegacyEnabled,
			requestQueuesCountOpt: &rqc32,
		},
		{
			name:                  "LocalFilestoreForKubevirtMultiqueue",
			backend:               "nfs",
			deviceNameOpt:         nil,
			perInstanceVolumes:    false,
			localFsOverride:       LocalFsOverrideEnabled,
			requestQueuesCountOpt: &rqc32,
		},
	}

	for _, tc := range testCases {
		tc := tc // Capture range variable.
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel() // Mark test as parallelizable.
			doTestStagedPublishUnpublishVolumeForKubevirtHelper(
				t,
				tc.backend,
				tc.deviceNameOpt,
				tc.perInstanceVolumes,
				tc.localFsOverride,
				tc.requestQueuesCountOpt,
			)
		})
	}
}

func TestReadVhostSettings(t *testing.T) {
	t.Run("Invalid queues count value", func(t *testing.T) {
		volumeContext := map[string]string{
			requestQueuesCountVolumeContextKey: "invalid",
		}
		_, err := readVhostSettings(volumeContext)
		require.ErrorContains(t, err, "failed to parse context attribute")
	})

	t.Run("Negative queues count value", func(t *testing.T) {
		volumeContext := map[string]string{
			requestQueuesCountVolumeContextKey: "-1",
		}
		_, err := readVhostSettings(volumeContext)
		require.ErrorContains(t, err, "failed to parse context attribute")
	})

	t.Run("Zero queues count value", func(t *testing.T) {
		volumeContext := map[string]string{
			requestQueuesCountVolumeContextKey: "0",
		}
		_, err := readVhostSettings(volumeContext)
		require.ErrorContains(t, err, "at least 1")
	})

	t.Run("Valid queues count value", func(t *testing.T) {
		volumeContext := map[string]string{
			requestQueuesCountVolumeContextKey: "128",
		}
		vhostSettings, err := readVhostSettings(volumeContext)
		require.NoError(t, err)
		assert.Equal(t, uint32(128), vhostSettings.queuesCount)
	})
}

func TestPublishUnpublishDiskForInfrakuber(t *testing.T) {
	tempDir := t.TempDir()

	groupId := ""
	currentUser, err := user.Current()
	require.NoError(t, err)
	groups, err := currentUser.GroupIds()
	require.NoError(t, err)
	for _, group := range groups {
		if group != "" && group != "0" {
			groupId = group
		}
	}
	log.Printf("groupId: %s", groupId)

	nbsClient := mocks.NewNbsClientMock()
	mounter := csimounter.NewMock()

	ipcType := nbs.EClientIpcType_IPC_NBD
	nbdDeviceFile := filepath.Join(tempDir, "dev", "nbd3")
	err = os.MkdirAll(nbdDeviceFile, fs.FileMode(0755))
	require.NoError(t, err)

	ctx := context.Background()
	actualClientId := defaultCientId + "-" + defaultNodeId
	targetPath := filepath.Join(tempDir, "pods", defaultPodId, "volumes", defaultDiskId, "mount")
	targetFsPathPattern := filepath.Join(tempDir, "pods/([a-z0-9-]+)/volumes/([a-z0-9-]+)/mount")
	stagingTargetPath := "testStagingTargetPath"
	socketsDir := filepath.Join(tempDir, "sockets")
	sourcePath := filepath.Join(socketsDir, defaultDiskId)
	socketPath := filepath.Join(socketsDir, defaultDiskId, "nbs.sock")
	deprecatedSocketPath := filepath.Join(socketsDir, defaultPodId, defaultDiskId, "nbs.sock")
	localFsOverrides := make(ExternalFsOverrideMap)

	nodeService := newNodeService(
		defaultNodeId,
		defaultCientId,
		false, // vmMode
		socketsDir,
		targetFsPathPattern,
		"", // targetBlkPathPattern
		localFsOverrides,
		nbsClient,
		nil,
		nil, // nfsLocalClient
		nil, // nfsLocalFilestoreClient
		mounter,
		[]string{"grpid"},
		true,
		defaultStartEndpointRequestTimeout,
	)

	volumeCapability := csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{
			Mount: &csi.VolumeCapability_MountVolume{
				VolumeMountGroup: groupId,
			},
		},
		AccessMode: &csi.VolumeCapability_AccessMode{
			Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		},
	}

	volumeContext := map[string]string{}

	hostType := nbs.EHostType_HOST_TYPE_DEFAULT
	nbsClient.On("ListEndpoints", ctx, &nbs.TListEndpointsRequest{}).Return(&nbs.TListEndpointsResponse{}, nil)
	nbsClient.On("StartEndpoint", ctx, &nbs.TStartEndpointRequest{
		Headers:          getDefaultStartEndpointRequestHeaders(),
		UnixSocketPath:   socketPath,
		DiskId:           defaultDiskId,
		InstanceId:       defaultNodeId,
		ClientId:         actualClientId,
		DeviceName:       defaultDiskId,
		IpcType:          ipcType,
		VhostQueuesCount: defaultVhostQueuesCount,
		VolumeAccessMode: nbs.EVolumeAccessMode_VOLUME_ACCESS_READ_WRITE,
		VolumeMountMode:  nbs.EVolumeMountMode_VOLUME_MOUNT_LOCAL,
		Persistent:       true,
		NbdDevice: &nbs.TStartEndpointRequest_UseFreeNbdDeviceFile{
			true,
		},
		ClientProfile: &nbs.TClientProfile{
			HostType: &hostType,
		},
	}).Return(&nbs.TStartEndpointResponse{
		NbdDeviceFile: nbdDeviceFile,
	}, nil)

	mounter.On("HasBlockDevice", nbdDeviceFile).Return(true, nil)

	mockCallIsMountPoint := mounter.On("IsMountPoint", stagingTargetPath).Return(false, nil)

	mounter.On("FormatAndMount", nbdDeviceFile, stagingTargetPath, "ext4",
		[]string{"grpid", "errors=remount-ro", "discard"}).Return(nil)

	_, err = nodeService.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
		VolumeId:          defaultDiskId,
		StagingTargetPath: stagingTargetPath,
		VolumeCapability:  &volumeCapability,
		VolumeContext:     volumeContext,
	})
	require.NoError(t, err)

	mockCallIsMountPoint.Unset()

	mounter.On("IsMountPoint", stagingTargetPath).Return(true, nil)
	mounter.On("IsMountPoint", targetPath).Return(false, nil)
	mounter.On("IsFilesystemRemountedAsReadonly", stagingTargetPath).Return(false, nil)

	mounter.On("Mount", stagingTargetPath, targetPath, "", []string{"bind"}).Return(nil)

	_, err = nodeService.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:          defaultDiskId,
		StagingTargetPath: stagingTargetPath,
		TargetPath:        targetPath,
		VolumeCapability:  &volumeCapability,
		VolumeContext:     volumeContext,
	})
	require.NoError(t, err)

	fileInfo, err := os.Stat(sourcePath)
	assert.False(t, os.IsNotExist(err))
	assert.True(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0755), fileInfo.Mode().Perm())

	fileInfo, err = os.Stat(targetPath)
	assert.False(t, os.IsNotExist(err))
	assert.True(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0775), fileInfo.Mode().Perm())

	output, err := exec.Command("ls", "-ldn", targetPath).CombinedOutput()
	assert.False(t, os.IsNotExist(err))
	log.Printf("Target path: %s", output)
	fields := strings.Fields(string(output))
	assert.Equal(t, groupId, fields[3])

	mockCallCleanupMountPoint := mounter.On("CleanupMountPoint", targetPath).Return(nil)

	mockCallStopEndpoint := nbsClient.On("StopEndpoint", ctx, &nbs.TStopEndpointRequest{
		UnixSocketPath: deprecatedSocketPath,
	}).Return(&nbs.TStopEndpointResponse{}, nil)

	_, err = nodeService.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   defaultDiskId,
		TargetPath: targetPath,
	})
	require.NoError(t, err)

	mockCallStopEndpoint.Unset()
	mockCallCleanupMountPoint.Unset()

	_, err = os.Stat(filepath.Join(socketsDir, defaultPodId))
	assert.True(t, os.IsNotExist(err))

	mounter.On("CleanupMountPoint", stagingTargetPath).Return(nil)

	nbsClient.On("StopEndpoint", ctx, &nbs.TStopEndpointRequest{
		UnixSocketPath: socketPath,
	}).Return(&nbs.TStopEndpointResponse{}, nil)

	_, err = nodeService.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{
		VolumeId:          defaultDiskId,
		StagingTargetPath: stagingTargetPath,
	})
	require.NoError(t, err)

	nbsClient.AssertExpectations(t)
	mounter.AssertExpectations(t)
}

func TestPublishUnpublishDeviceForInfrakuber(t *testing.T) {
	tempDir := t.TempDir()

	nbsClient := mocks.NewNbsClientMock()
	mounter := csimounter.NewMock()

	ipcType := nbs.EClientIpcType_IPC_NBD
	nbdDeviceFile := filepath.Join(tempDir, "dev", "nbd3")
	err := os.MkdirAll(nbdDeviceFile, 0755)
	require.NoError(t, err)

	ctx := context.Background()
	actualClientId := defaultCientId + "-" + defaultNodeId
	targetPath := filepath.Join(tempDir, "volumeDevices", "publish", defaultDiskId, defaultPodId)
	targetBlkPathPattern := filepath.Join(tempDir, "volumeDevices/publish/([a-z0-9-]+)/([a-z0-9-]+)")
	stagingTargetPath := "testStagingTargetPath"
	stagingDevicePath := filepath.Join(stagingTargetPath, defaultDiskId)
	socketsDir := filepath.Join(tempDir, "sockets")
	sourcePath := filepath.Join(socketsDir, defaultDiskId)
	socketPath := filepath.Join(socketsDir, defaultDiskId, "nbs.sock")
	localFsOverrides := make(ExternalFsOverrideMap)

	nodeService := newNodeService(
		defaultNodeId,
		defaultCientId,
		false, // vmMode
		socketsDir,
		"", // targetFsPathPattern
		targetBlkPathPattern,
		localFsOverrides,
		nbsClient,
		nil,
		nil, // nfsLocalClient
		nil, // nfsLocalFilestoreClient
		mounter,
		[]string{},
		false,
		defaultStartEndpointRequestTimeout,
	)

	volumeCapability := csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Block{
			Block: &csi.VolumeCapability_BlockVolume{},
		},
		AccessMode: &csi.VolumeCapability_AccessMode{
			Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		},
	}

	volumeContext := map[string]string{}

	var volumeOperationInProgress = func(args mock.Arguments) {
		_, err = nodeService.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
			VolumeId:          defaultDiskId,
			StagingTargetPath: stagingTargetPath,
			VolumeCapability:  &volumeCapability,
		})
		require.Error(t, err)
		expectedError := "rpc error: code = Aborted desc = [n=testNodeId]: " +
			"Another operation with volume test-disk-id-42 is in progress"
		require.Equal(t, expectedError, err.Error())
	}

	hostType := nbs.EHostType_HOST_TYPE_DEFAULT
	nbsClient.On("ListEndpoints", ctx, &nbs.TListEndpointsRequest{}).Return(&nbs.TListEndpointsResponse{}, nil)
	nbsClient.On("StartEndpoint", ctx, &nbs.TStartEndpointRequest{
		Headers:          getDefaultStartEndpointRequestHeaders(),
		UnixSocketPath:   socketPath,
		DiskId:           defaultDiskId,
		InstanceId:       defaultNodeId,
		ClientId:         actualClientId,
		DeviceName:       defaultDiskId,
		IpcType:          ipcType,
		VhostQueuesCount: defaultVhostQueuesCount,
		VolumeAccessMode: nbs.EVolumeAccessMode_VOLUME_ACCESS_READ_WRITE,
		VolumeMountMode:  nbs.EVolumeMountMode_VOLUME_MOUNT_LOCAL,
		Persistent:       true,
		NbdDevice: &nbs.TStartEndpointRequest_UseFreeNbdDeviceFile{
			true,
		},
		ClientProfile: &nbs.TClientProfile{
			HostType: &hostType,
		},
	}).Return(&nbs.TStartEndpointResponse{
		NbdDeviceFile: nbdDeviceFile,
	}, nil)

	mockCallIsMountPointStagingPath :=
		mounter.On("IsMountPoint", stagingDevicePath).Run(volumeOperationInProgress).Return(true, nil)
	mockCallMount := mounter.On("Mount", nbdDeviceFile, stagingDevicePath, "", []string{"bind"}).Return(nil)

	_, err = nodeService.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
		VolumeId:          defaultDiskId,
		StagingTargetPath: stagingTargetPath,
		VolumeCapability:  &volumeCapability,
		VolumeContext:     volumeContext,
	})
	require.NoError(t, err)
	mockCallMount.Unset()

	mockCallIsMountPointTargetPath :=
		mounter.On("IsMountPoint", targetPath).Run(volumeOperationInProgress).Return(false, nil)
	mounter.On("Mount", stagingDevicePath, targetPath, "", []string{"bind"}).Return(nil)

	_, err = nodeService.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:          defaultDiskId,
		StagingTargetPath: stagingTargetPath,
		TargetPath:        targetPath,
		VolumeCapability:  &volumeCapability,
		VolumeContext:     volumeContext,
	})
	require.NoError(t, err)

	mockCallIsMountPointStagingPath.Unset()
	mockCallIsMountPointTargetPath.Unset()

	fileInfo, err := os.Stat(sourcePath)
	assert.False(t, os.IsNotExist(err))
	assert.True(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0755), fileInfo.Mode().Perm())

	fileInfo, err = os.Stat(filepath.Dir(targetPath))
	assert.False(t, os.IsNotExist(err))
	assert.True(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0750), fileInfo.Mode().Perm())

	fileInfo, err = os.Stat(targetPath)
	assert.False(t, os.IsNotExist(err))
	assert.False(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0660), fileInfo.Mode().Perm())

	mounter.On("CleanupMountPoint", targetPath).Return(nil)

	_, err = nodeService.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   defaultDiskId,
		TargetPath: targetPath,
	})
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(socketsDir, defaultPodId))
	assert.True(t, os.IsNotExist(err))

	mounter.On("IsMountPoint", stagingTargetPath).Return(false, nil)
	mounter.On("IsMountPoint", stagingDevicePath).Return(true, nil)
	mounter.On("CleanupMountPoint", stagingDevicePath).Return(nil)

	nbsClient.On("StopEndpoint", ctx, &nbs.TStopEndpointRequest{
		UnixSocketPath: socketPath,
	}).Run(volumeOperationInProgress).Return(&nbs.TStopEndpointResponse{}, nil)

	_, err = nodeService.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{
		VolumeId:          defaultDiskId,
		StagingTargetPath: stagingTargetPath,
	})
	require.NoError(t, err)

	nbsClient.AssertExpectations(t)
	mounter.AssertExpectations(t)
}

func TestGetVolumeStatCapabilitiesWithoutVmMode(t *testing.T) {
	tempDir := t.TempDir()

	nbsClient := mocks.NewNbsClientMock()
	mounter := csimounter.NewMock()
	socketsDir := filepath.Join(tempDir, "sockets")
	targetPath := filepath.Join(tempDir, "pods", defaultPodId, "volumes", defaultDiskId, "mount")
	targetFsPathPattern := filepath.Join(tempDir,
		"pods/([a-z0-9-]+)/volumes/([a-z0-9-]+)/mount")

	info, err := os.Stat(tempDir)
	require.NoError(t, err)
	err = os.MkdirAll(targetPath, info.Mode())
	require.NoError(t, err)
	localFsOverrides := make(ExternalFsOverrideMap)

	nodeService := newNodeService(
		defaultNodeId,
		defaultCientId,
		false,
		socketsDir,
		targetFsPathPattern,
		"",
		localFsOverrides,
		nbsClient,
		nil,
		nil, // nfsLocalClient
		nil, // nfsLocalFilestoreClient
		mounter,
		[]string{},
		false,
		defaultStartEndpointRequestTimeout,
	)

	ctx := context.Background()
	resp, err := nodeService.NodeGetCapabilities(
		ctx,
		&csi.NodeGetCapabilitiesRequest{})
	require.NoError(t, err)

	capabilityIndex := slices.IndexFunc(resp.GetCapabilities(),
		func(capability *csi.NodeServiceCapability) bool {
			rpc := capability.GetRpc()
			if rpc == nil {
				return false
			}
			return rpc.GetType() == csi.NodeServiceCapability_RPC_GET_VOLUME_STATS
		})
	assert.NotEqual(t, -1, capabilityIndex)

	mounter.On("IsMountPoint", targetPath).Return(true, nil)

	stat, err := nodeService.NodeGetVolumeStats(ctx, &csi.NodeGetVolumeStatsRequest{
		VolumeId:   defaultDiskId,
		VolumePath: targetPath,
	})
	require.NoError(t, err)
	assert.Equal(t, 2, len(stat.GetUsage()))

	bytesUsage := stat.GetUsage()[0]
	assert.Equal(t, bytesUsage.Unit, csi.VolumeUsage_BYTES)
	assert.NotEqual(t, 0, bytesUsage.Total)
	assert.LessOrEqual(t, bytesUsage.Used+bytesUsage.Available, bytesUsage.Total)

	nodesUsage := stat.GetUsage()[1]
	assert.Equal(t, nodesUsage.Unit, csi.VolumeUsage_INODES)
	assert.NotEqual(t, 0, nodesUsage.Total)
	assert.Equal(t, nodesUsage.Used+nodesUsage.Available, nodesUsage.Total)

	nbsClient.AssertExpectations(t)
	mounter.AssertExpectations(t)
}

func TestGetVolumeStatCapabilitiesWithVmMode(t *testing.T) {
	tempDir := t.TempDir()

	nbsClient := mocks.NewNbsClientMock()
	mounter := csimounter.NewMock()
	targetPath := filepath.Join(tempDir, "volumeDevices", "publish", defaultDiskId, defaultPodId)
	targetBlkPathPattern := filepath.Join(tempDir,
		"volumeDevices/publish/([a-z0-9-]+)/([a-z0-9-]+)")
	socketsDir := filepath.Join(tempDir, "sockets")
	localFsOverrides := make(ExternalFsOverrideMap)

	nodeService := newNodeService(
		defaultNodeId,
		defaultCientId,
		true,
		socketsDir,
		"",
		targetBlkPathPattern,
		localFsOverrides,
		nbsClient,
		nil,
		nil, // nfsLocalClient
		nil, // nfsLocalFilestoreClient
		mounter,
		[]string{},
		false,
		defaultStartEndpointRequestTimeout,
	)

	ctx := context.Background()
	resp, err := nodeService.NodeGetCapabilities(
		ctx,
		&csi.NodeGetCapabilitiesRequest{})
	require.NoError(t, err)

	capabilityIndex := slices.IndexFunc(
		resp.GetCapabilities(),
		func(capability *csi.NodeServiceCapability) bool {
			rpc := capability.GetRpc()
			if rpc == nil {
				return false
			}
			return rpc.GetType() == csi.NodeServiceCapability_RPC_GET_VOLUME_STATS
		})
	assert.Equal(t, -1, capabilityIndex)

	_, err = nodeService.NodeGetVolumeStats(ctx, &csi.NodeGetVolumeStatsRequest{
		VolumeId:   defaultDiskId,
		VolumePath: targetPath,
	})
	require.Error(t, err)

	nbsClient.AssertExpectations(t)
	mounter.AssertExpectations(t)
}

func TestPublishDeviceWithReadWriteManyModeIsNotSupportedWithNBS(t *testing.T) {
	tempDir := t.TempDir()

	nbsClient := mocks.NewNbsClientMock()
	mounter := csimounter.NewMock()

	ctx := context.Background()
	targetPath := filepath.Join(tempDir, "volumeDevices", "publish", defaultDiskId, defaultPodId)
	targetBlkPathPattern := filepath.Join(tempDir, "volumeDevices/publish/([a-z0-9-]+)/([a-z0-9-]+)")
	socketsDir := filepath.Join(tempDir, "sockets")
	volumeContext := map[string]string{
		backendVolumeContextKey: "nbs",
	}
	localFsOverrides := make(ExternalFsOverrideMap)

	nodeService := newNodeService(
		defaultNodeId,
		defaultCientId,
		false,
		socketsDir,
		"",
		targetBlkPathPattern,
		localFsOverrides,
		nbsClient,
		nil,
		nil, // nfsLocalClient
		nil, // nfsLocalFilestoreClient
		mounter,
		[]string{},
		false,
		defaultStartEndpointRequestTimeout,
	)

	_, err := nodeService.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
		VolumeId:          defaultDiskId,
		StagingTargetPath: "testStagingTargetPath",
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{
				Block: &csi.VolumeCapability_BlockVolume{},
			},
		},
	})
	require.Error(t, err)

	// NodePublishVolume without access mode should fail
	_, err = nodeService.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:          defaultDiskId,
		StagingTargetPath: "testStagingTargetPath",
		TargetPath:        targetPath,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{
				Block: &csi.VolumeCapability_BlockVolume{},
			},
		},
		VolumeContext: volumeContext,
	})
	require.Error(t, err)

	_, err = nodeService.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:          defaultDiskId,
		StagingTargetPath: "testStagingTargetPath",
		TargetPath:        targetPath,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{
				Block: &csi.VolumeCapability_BlockVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
		VolumeContext: volumeContext,
	})
	require.Error(t, err)

	nbsClient.AssertExpectations(t)
	mounter.AssertExpectations(t)
}

func TestExternaFs(t *testing.T) {
	tempDir := t.TempDir()

	nbsClient := mocks.NewNbsClientMock()
	nfsClient := mocks.NewNfsEndpointClientMock()
	nfsLocalClient := mocks.NewNfsEndpointClientMock()
	nfsLocalFilestoreClient := mocks.NewNfsClientMock()
	mounter := csimounter.NewMock()

	ctx := context.Background()
	actualClientId := defaultCientId + "-" + defaultInstanceId
	nfsId := "test-nfs"
	volumeId := nfsId + "#" + defaultInstanceId
	stagingTargetPath := filepath.Join(tempDir, "testStagingTargetPath")
	socketsDir := filepath.Join(tempDir, "sockets")
	sourcePath := filepath.Join(socketsDir, defaultInstanceId, nfsId)
	targetPath := filepath.Join(tempDir, "pods", defaultPodId, "volumes", nfsId, "mount")
	targetFsPathPattern := filepath.Join(tempDir, "pods/([a-z0-9-]+)/volumes/([a-z0-9-]+)/mount")
	nfsSocketPath := filepath.Join(sourcePath, "nfs.sock")

	mountFilePath := filepath.Join(tempDir, "mountDone")

	fsConfig := ExternalFsConfig{
		Id:       nfsId,
		Type:     "external",
		SizeGb:   100,
		CloudId:  "cloud",
		FolderId: "folder",
		MountCmd: "bash",
		MountArgs: []string{
			"-c",
			fmt.Sprintf("echo LOCAL_FS_ID=$LOCAL_FS_ID; echo EXTERNAL_FS_ID=$EXTERNAL_FS_ID; touch %s", mountFilePath),
		},
		UmountCmd: "bash",
		UmountArgs: []string{
			"-c",
			fmt.Sprintf("rm %s", mountFilePath),
		},
	}

	localFsOverrides := ExternalFsOverrideMap{
		nfsId: fsConfig,
	}

	nodeService := newNodeService(
		defaultNodeId,
		defaultCientId,
		true, // vmMode
		socketsDir,
		targetFsPathPattern,
		"", // targetBlkPathPattern
		localFsOverrides,
		nbsClient,
		nfsClient,
		nfsLocalClient,
		nfsLocalFilestoreClient,
		mounter,
		[]string{},
		false,
		defaultStartEndpointRequestTimeout,
	)

	accessMode := csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER

	volumeCapability := csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{
			Mount: &csi.VolumeCapability_MountVolume{},
		},
		AccessMode: &csi.VolumeCapability_AccessMode{
			Mode: accessMode,
		},
	}

	volumeContext := map[string]string{
		backendVolumeContextKey: "nfs",
		instanceIdKey:           defaultInstanceId,
	}

	nfsLocalFilestoreClient.On("CreateFileStore", ctx, &nfs.TCreateFileStoreRequest{
		FileSystemId:     fmt.Sprintf("%s-%s", nfsId, defaultInstanceId),
		CloudId:          fsConfig.CloudId,
		FolderId:         fsConfig.FolderId,
		BlockSize:        4096,
		BlocksCount:      (fsConfig.SizeGb << 30) / 4096,
		StorageMediaKind: storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_SSD,
	}).Return(&nfs.TCreateFileStoreResponse{}, nil)

	nfsLocalClient.On("ListEndpoints", ctx, &nfs.TListEndpointsRequest{}).Return(&nfs.TListEndpointsResponse{}, nil)

	nfsLocalClient.On("StartEndpoint", ctx, &nfs.TStartEndpointRequest{
		Endpoint: &nfs.TEndpointConfig{
			SocketPath:       nfsSocketPath,
			FileSystemId:     fmt.Sprintf("%s-%s", nfsId, defaultInstanceId),
			ClientId:         actualClientId,
			VhostQueuesCount: defaultVhostQueuesCount,
			Persistent:       true,
		},
	}).Return(&nfs.TStartEndpointResponse{}, nil)

	_, err := nodeService.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
		VolumeId:          volumeId,
		StagingTargetPath: stagingTargetPath,
		VolumeCapability:  &volumeCapability,
		VolumeContext:     volumeContext,
	})
	require.NoError(t, err)

	fileInfo, err := os.Stat(mountFilePath)
	require.NoError(t, err)
	assert.True(t, fileInfo.Mode().IsRegular())

	fileInfo, err = os.Stat(sourcePath)
	assert.False(t, os.IsNotExist(err))
	assert.True(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0755), fileInfo.Mode().Perm())

	fileInfo, err = os.Stat(filepath.Join(sourcePath, "disk.img"))
	assert.False(t, os.IsNotExist(err))
	assert.False(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0644), fileInfo.Mode().Perm())

	mounter.On("IsMountPoint", targetPath).Return(false, nil)
	mounter.On("Mount", sourcePath, targetPath, "", []string{"bind"}).Return(nil)

	_, err = nodeService.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:          volumeId,
		StagingTargetPath: stagingTargetPath,
		TargetPath:        targetPath,
		VolumeCapability:  &volumeCapability,
		VolumeContext:     volumeContext,
	})
	require.NoError(t, err)

	fileInfo, err = os.Stat(targetPath)
	assert.False(t, os.IsNotExist(err))
	assert.True(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0775), fileInfo.Mode().Perm())

	mounter.On("CleanupMountPoint", targetPath).Return(nil)

	_, err = nodeService.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   volumeId,
		TargetPath: targetPath,
	})
	require.NoError(t, err)

	nfsLocalClient.On("StopEndpoint", ctx, &nfs.TStopEndpointRequest{
		SocketPath: nfsSocketPath,
	}).Return(&nfs.TStopEndpointResponse{}, nil)

	nfsLocalFilestoreClient.On("DestroyFileStore", ctx, &nfs.TDestroyFileStoreRequest{
		FileSystemId: fmt.Sprintf("%s-%s", nfsId, defaultInstanceId),
	}).Return(&nfs.TDestroyFileStoreResponse{}, nil)

	_, err = nodeService.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{
		VolumeId:          volumeId,
		StagingTargetPath: stagingTargetPath,
	})
	require.NoError(t, err)

	_, err = os.Stat(mountFilePath)
	assert.True(t, os.IsNotExist(err))

	_, err = os.Stat(filepath.Join(socketsDir, defaultInstanceId))
	assert.True(t, os.IsNotExist(err))

	nbsClient.AssertExpectations(t)
	nfsClient.AssertExpectations(t)
	nfsLocalClient.AssertExpectations(t)
	nfsLocalFilestoreClient.AssertExpectations(t)
	mounter.AssertExpectations(t)
}

func TestStopEndpointAfterNodeStageVolumeFailureForInfrakuber(t *testing.T) {
	tempDir := t.TempDir()

	nbsClient := mocks.NewNbsClientMock()
	mounter := csimounter.NewMock()

	ipcType := nbs.EClientIpcType_IPC_NBD
	nbdDeviceFile := filepath.Join(tempDir, "dev", "nbd3")
	err := os.MkdirAll(nbdDeviceFile, fs.FileMode(0755))
	require.NoError(t, err)

	ctx := context.Background()
	actualClientId := defaultCientId + "-" + defaultNodeId
	targetFsPathPattern := filepath.Join(tempDir, "pods/([a-z0-9-]+)/volumes/([a-z0-9-]+)/mount")
	stagingTargetPath := "testStagingTargetPath"
	socketsDir := filepath.Join(tempDir, "sockets")
	socketPath := filepath.Join(socketsDir, defaultDiskId, "nbs.sock")

	nodeService := newNodeService(
		defaultNodeId,
		defaultCientId,
		false,
		socketsDir,
		targetFsPathPattern,
		"",
		make(ExternalFsOverrideMap),
		nbsClient,
		nil,
		nil, // nfsLocalClient
		nil, // nfsLocalFilestoreClient
		mounter,
		[]string{},
		false,
		defaultStartEndpointRequestTimeout,
	)

	volumeCapability := csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{},
		AccessMode: &csi.VolumeCapability_AccessMode{
			Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		},
	}

	hostType := nbs.EHostType_HOST_TYPE_DEFAULT
	nbsClient.On("ListEndpoints", ctx, &nbs.TListEndpointsRequest{}).Return(
		&nbs.TListEndpointsResponse{}, nil)
	nbsClient.On("StartEndpoint", ctx, &nbs.TStartEndpointRequest{
		Headers:          getDefaultStartEndpointRequestHeaders(),
		UnixSocketPath:   socketPath,
		DiskId:           defaultDiskId,
		InstanceId:       defaultNodeId,
		ClientId:         actualClientId,
		DeviceName:       defaultDiskId,
		IpcType:          ipcType,
		VhostQueuesCount: defaultVhostQueuesCount,
		VolumeAccessMode: nbs.EVolumeAccessMode_VOLUME_ACCESS_READ_WRITE,
		VolumeMountMode:  nbs.EVolumeMountMode_VOLUME_MOUNT_LOCAL,
		Persistent:       true,
		NbdDevice: &nbs.TStartEndpointRequest_UseFreeNbdDeviceFile{
			true,
		},
		ClientProfile: &nbs.TClientProfile{
			HostType: &hostType,
		},
	}).Return(&nbs.TStartEndpointResponse{
		NbdDeviceFile: nbdDeviceFile,
	}, nil)

	nbdError := fmt.Errorf("%w", nbsclient.ClientError{Code: nbsclient.E_FAIL})
	mounter.On("HasBlockDevice", nbdDeviceFile).Return(false, nbdError)

	nbsClient.On("StopEndpoint", ctx, &nbs.TStopEndpointRequest{
		UnixSocketPath: socketPath,
	}).Return(&nbs.TStopEndpointResponse{}, nil)

	_, err = nodeService.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
		VolumeId:          defaultDiskId,
		StagingTargetPath: stagingTargetPath,
		VolumeCapability:  &volumeCapability,
	})
	require.Error(t, err)

	mounter.AssertExpectations(t)
}

func TestNodeStageVolumeErrorForKubevirt(
	t *testing.T,
) {
	t.Helper()
	tempDir := t.TempDir()

	nbsClient := mocks.NewNbsClientMock()
	nfsClient := mocks.NewNfsEndpointClientMock()
	nfsLocalClient := mocks.NewNfsEndpointClientMock()
	nfsLocalFilestoreClient := mocks.NewNfsClientMock()
	mounter := csimounter.NewMock()

	ctx := context.Background()
	backend := "nbs"
	actualClientId := defaultCientId + "-" + defaultInstanceId
	volumeId := defaultDiskId + "#" + defaultInstanceId
	stagingTargetPath := filepath.Join(tempDir, "testStagingTargetPath")
	socketsDir := filepath.Join(tempDir, "sockets")
	sourcePath := filepath.Join(socketsDir, defaultInstanceId, defaultDiskId)
	targetFsPathPattern := filepath.Join(tempDir, "pods/([a-z0-9-]+)/volumes/([a-z0-9-]+)/mount")
	nbsSocketPath := filepath.Join(sourcePath, "nbs.sock")

	nodeService := newNodeService(
		defaultNodeId,
		defaultCientId,
		true, // vmMode
		socketsDir,
		targetFsPathPattern,
		"", // targetBlkPathPattern
		nil,
		nbsClient,
		nfsClient,
		nfsLocalClient,
		nfsLocalFilestoreClient,
		mounter,
		[]string{},
		false,
		defaultStartEndpointRequestTimeout,
	)

	accessMode := csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER
	volumeCapability := csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{
			Mount: &csi.VolumeCapability_MountVolume{},
		},
		AccessMode: &csi.VolumeCapability_AccessMode{
			Mode: accessMode,
		},
	}

	volumeContext := map[string]string{
		backendVolumeContextKey: backend,
		instanceIdKey:           defaultInstanceId,
	}

	hostType := nbs.EHostType_HOST_TYPE_DEFAULT

	if backend == "nbs" {
		nbsClient.On("ListEndpoints", ctx, &nbs.TListEndpointsRequest{}).Return(&nbs.TListEndpointsResponse{}, nil)
		nbsClient.On("StartEndpoint", ctx, &nbs.TStartEndpointRequest{
			Headers:          getDefaultStartEndpointRequestHeaders(),
			UnixSocketPath:   nbsSocketPath,
			DiskId:           defaultDiskId,
			InstanceId:       defaultInstanceId,
			ClientId:         actualClientId,
			DeviceName:       defaultDiskId,
			IpcType:          nbs.EClientIpcType_IPC_VHOST,
			VhostQueuesCount: defaultVhostQueuesCount,
			VolumeAccessMode: nbs.EVolumeAccessMode_VOLUME_ACCESS_READ_WRITE,
			VolumeMountMode:  nbs.EVolumeMountMode_VOLUME_MOUNT_LOCAL,
			Persistent:       true,
			NbdDevice: &nbs.TStartEndpointRequest_UseFreeNbdDeviceFile{
				false,
			},
			ClientProfile: &nbs.TClientProfile{
				HostType: &hostType,
			},
		}).Return(&nbs.TStartEndpointResponse{}, status.Error(codes.DeadlineExceeded, ""))
		nbsClient.On("StopEndpoint", ctx, &nbs.TStopEndpointRequest{
			UnixSocketPath: nbsSocketPath,
		}).Return(&nbs.TStopEndpointResponse{}, nil)
	}

	_, err := nodeService.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
		VolumeId:          volumeId,
		StagingTargetPath: stagingTargetPath,
		VolumeCapability:  &volumeCapability,
		VolumeContext:     volumeContext,
	})
	require.Error(t, err)

	mounter.AssertExpectations(t)
}

func TestNodeUnstageVolumeErrorForKubevirt(
	t *testing.T,
) {
	t.Helper()
	tempDir := t.TempDir()

	nbsClient := mocks.NewNbsClientMock()
	nfsClient := mocks.NewNfsEndpointClientMock()
	nfsLocalClient := mocks.NewNfsEndpointClientMock()
	nfsLocalFilestoreClient := mocks.NewNfsClientMock()
	mounter := csimounter.NewMock()

	ctx := context.Background()
	backend := "nbs"
	actualClientId := defaultCientId + "-" + defaultInstanceId
	stagingTargetPath := filepath.Join(tempDir, "testStagingTargetPath")
	socketsDir := filepath.Join(tempDir, "sockets")
	sourcePath := filepath.Join(socketsDir, defaultInstanceId, defaultDiskId)
	targetFsPathPattern := filepath.Join(tempDir, "pods/([a-z0-9-]+)/volumes/([a-z0-9-]+)/mount")
	nbsSocketPath := filepath.Join(sourcePath, "nbs.sock")

	nodeService := newNodeService(
		defaultNodeId,
		defaultCientId,
		true, // vmMode
		socketsDir,
		targetFsPathPattern,
		"", // targetBlkPathPattern
		nil,
		nbsClient,
		nfsClient,
		nfsLocalClient,
		nfsLocalFilestoreClient,
		mounter,
		[]string{},
		false,
		defaultStartEndpointRequestTimeout,
	)

	accessMode := csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER
	volumeCapability := csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{
			Mount: &csi.VolumeCapability_MountVolume{},
		},
		AccessMode: &csi.VolumeCapability_AccessMode{
			Mode: accessMode,
		},
	}

	volumeContext := map[string]string{
		backendVolumeContextKey: backend,
		instanceIdKey:           defaultInstanceId,
	}

	hostType := nbs.EHostType_HOST_TYPE_DEFAULT

	if backend == "nbs" {
		nbsClient.On("ListEndpoints", ctx, &nbs.TListEndpointsRequest{}).Return(
			&nbs.TListEndpointsResponse{}, nil)
		nbsClient.On("StartEndpoint", ctx, &nbs.TStartEndpointRequest{
			Headers:          getDefaultStartEndpointRequestHeaders(),
			UnixSocketPath:   nbsSocketPath,
			DiskId:           defaultDiskId,
			InstanceId:       defaultInstanceId,
			ClientId:         actualClientId,
			DeviceName:       defaultDiskId,
			IpcType:          nbs.EClientIpcType_IPC_VHOST,
			VhostQueuesCount: defaultVhostQueuesCount,
			VolumeAccessMode: nbs.EVolumeAccessMode_VOLUME_ACCESS_READ_WRITE,
			VolumeMountMode:  nbs.EVolumeMountMode_VOLUME_MOUNT_LOCAL,
			Persistent:       true,
			NbdDevice: &nbs.TStartEndpointRequest_UseFreeNbdDeviceFile{
				false,
			},
			ClientProfile: &nbs.TClientProfile{
				HostType: &hostType,
			},
		}).Return(&nbs.TStartEndpointResponse{}, nil)
	}

	_, err := nodeService.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
		VolumeId:          defaultDiskId,
		StagingTargetPath: stagingTargetPath,
		VolumeCapability:  &volumeCapability,
		VolumeContext:     volumeContext,
	})
	require.NoError(t, err)

	nbsClient.On("StopEndpoint", ctx, &nbs.TStopEndpointRequest{
		UnixSocketPath: nbsSocketPath,
	}).Return(&nbs.TStopEndpointResponse{},
		&nbsclient.ClientError{Code: nbsclient.E_GRPC_DEADLINE_EXCEEDED, Message: ""})

	_, err = nodeService.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{
		VolumeId:          defaultDiskId,
		StagingTargetPath: stagingTargetPath,
	})
	require.Error(t, err)
	status, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.DeadlineExceeded, status.Code())

	mounter.AssertExpectations(t)
}
