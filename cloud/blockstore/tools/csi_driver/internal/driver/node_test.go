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
)

const (
	defaultStartEndpointRequestTimeout = 10 * time.Second
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
	nodeId := "testNodeId"
	clientId := "testClientId"
	podId := "test-pod-id-13"
	actualClientId := "testClientId-test-pod-id-13"
	diskId := "test-disk-id-42"
	deviceName := diskId
	if deviceNameOpt != nil {
		deviceName = *deviceNameOpt
	}
	stagingTargetPath := "testStagingTargetPath"
	socketsDir := filepath.Join(tempDir, "sockets")
	sourcePath := filepath.Join(socketsDir, podId, diskId)
	targetPath := filepath.Join(tempDir, "pods", podId, "volumes", diskId, "mount")
	targetFsPathPattern := filepath.Join(tempDir, "pods/([a-z0-9-]+)/volumes/([a-z0-9-]+)/mount")
	nbsSocketPath := filepath.Join(sourcePath, "nbs.sock")
	nfsSocketPath := filepath.Join(sourcePath, "nfs.sock")

	nodeService := newNodeService(
		nodeId,
		clientId,
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
		VolumeId:          diskId,
		StagingTargetPath: stagingTargetPath,
		VolumeCapability:  &volumeCapability,
		VolumeContext:     volumeContext,
	})
	require.NoError(t, err)

	hostType := nbs.EHostType_HOST_TYPE_DEFAULT

	vhostQueuesCount := uint32(8) // explicit default value for unset behavior
	if backend == "nbs" {
		if requestQueuesCountOpt != nil {
			volumeContext[requestQueuesCountVolumeContextKey] = strconv.Itoa(int(*requestQueuesCountOpt))
			vhostQueuesCount = virtioBlkVhostQueuesCount(*requestQueuesCountOpt)
		}
		nbsClient.On("ListEndpoints", ctx, &nbs.TListEndpointsRequest{}).Return(&nbs.TListEndpointsResponse{}, nil)
		nbsClient.On("StartEndpoint", ctx, &nbs.TStartEndpointRequest{
			Headers:          getDefaultStartEndpointRequestHeaders(),
			UnixSocketPath:   nbsSocketPath,
			DiskId:           diskId,
			InstanceId:       podId,
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
				FileSystemId:     diskId,
				ClientId:         actualClientId,
				VhostQueuesCount: vhostQueuesCount,
				Persistent:       true,
			},
		}).Return(&nfs.TStartEndpointResponse{}, nil)
	}

	mounter.On("IsMountPoint", targetPath).Return(false, nil)
	mounter.On("Mount", sourcePath, targetPath, "", []string{"bind"}).Return(nil)

	_, err = nodeService.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:          diskId,
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
		VolumeId:   diskId,
		TargetPath: targetPath,
	})
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(socketsDir, podId))
	assert.True(t, os.IsNotExist(err))

	_, err = nodeService.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{
		VolumeId:          diskId,
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
	nodeId := "testNodeId"
	clientId := "testClientId"
	podId := "test-pod-id-13"
	instanceId := "testInstanceId"
	actualClientId := "testClientId-" + instanceId
	diskId := "test-disk-id-42"
	deviceName := diskId
	if deviceNameOpt != nil {
		deviceName = *deviceNameOpt
	}
	volumeId := diskId
	if perInstanceVolumes {
		volumeId = diskId + "#" + instanceId
	}
	stagingTargetPath := filepath.Join(tempDir, "testStagingTargetPath")
	socketsDir := filepath.Join(tempDir, "sockets")
	sourcePath := filepath.Join(socketsDir, instanceId, diskId)
	targetPath := filepath.Join(tempDir, "pods", podId, "volumes", diskId, "mount")
	targetFsPathPattern := filepath.Join(tempDir, "pods/([a-z0-9-]+)/volumes/([a-z0-9-]+)/mount")
	nbsSocketPath := filepath.Join(sourcePath, "nbs.sock")
	nfsSocketPath := filepath.Join(sourcePath, "nfs.sock")

	localFsOverrides := make(ExternalFsOverrideMap)
	if localFsOverride == LocalFsOverrideLegacyEnabled {
		localFsOverrides[diskId] = ExternalFsConfig{
			Id: diskId,
		}
	} else if localFsOverride == LocalFsOverrideEnabled {
		localFsOverrides[diskId] = ExternalFsConfig{
			Id:         diskId,
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
		nodeId,
		clientId,
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
		instanceIdKey:           instanceId,
	}
	if deviceNameOpt != nil {
		volumeContext[deviceNameVolumeContextKey] = *deviceNameOpt
	}

	vhostQueuesCount := uint32(8) // explicit default value for unset behavior
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
			DiskId:           diskId,
			InstanceId:       instanceId,
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
				FileSystemId:     fmt.Sprintf("%s-%s", diskId, instanceId),
				CloudId:          localFsOverrides[diskId].CloudId,
				FolderId:         localFsOverrides[diskId].FolderId,
				BlockSize:        4096,
				BlocksCount:      (localFsOverrides[diskId].SizeGb << 30) / 4096,
				StorageMediaKind: storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_SSD,
			}).Return(&nfs.TCreateFileStoreResponse{}, nil)

			nfsLocalClient.On("ListEndpoints", ctx, &nfs.TListEndpointsRequest{}).Return(&nfs.TListEndpointsResponse{}, nil)

		}
		if requestQueuesCountOpt != nil {
			volumeContext[requestQueuesCountVolumeContextKey] = strconv.Itoa(int(*requestQueuesCountOpt))
			vhostQueuesCount = virtioFsVhostQueuesCount(*requestQueuesCountOpt)
		}

		expectedFsId := diskId
		if localFsOverride == LocalFsOverrideEnabled {
			expectedFsId = fmt.Sprintf("%s-%s", diskId, instanceId)
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
			UnixSocketPath: filepath.Join(socketsDir, podId, diskId, nbsSocketName),
		}).Return(&nbs.TStopEndpointResponse{}, nil)

		expectedNfsClient.On("StopEndpoint", ctx, &nfs.TStopEndpointRequest{
			SocketPath: filepath.Join(socketsDir, podId, diskId, nfsSocketName),
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
				FileSystemId: fmt.Sprintf("%s-%s", diskId, instanceId),
			}).Return(&nfs.TDestroyFileStoreResponse{}, nil)
		}
	}

	_, err = nodeService.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{
		VolumeId:          volumeId,
		StagingTargetPath: stagingTargetPath,
	})
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(socketsDir, instanceId))
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
	nodeId := "testNodeId"
	clientId := "testClientId"
	podId := "test-pod-id-13"
	diskId := "test-disk-id-42"
	actualClientId := "testClientId-testNodeId"
	targetPath := filepath.Join(tempDir, "pods", podId, "volumes", diskId, "mount")
	targetFsPathPattern := filepath.Join(tempDir, "pods/([a-z0-9-]+)/volumes/([a-z0-9-]+)/mount")
	stagingTargetPath := "testStagingTargetPath"
	socketsDir := filepath.Join(tempDir, "sockets")
	sourcePath := filepath.Join(socketsDir, diskId)
	socketPath := filepath.Join(socketsDir, diskId, "nbs.sock")
	deprecatedSocketPath := filepath.Join(socketsDir, podId, diskId, "nbs.sock")
	localFsOverrides := make(ExternalFsOverrideMap)

	nodeService := newNodeService(
		nodeId,
		clientId,
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
		DiskId:           diskId,
		InstanceId:       nodeId,
		ClientId:         actualClientId,
		DeviceName:       diskId,
		IpcType:          ipcType,
		VhostQueuesCount: 8,
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
		VolumeId:          diskId,
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
		VolumeId:          diskId,
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
		VolumeId:   diskId,
		TargetPath: targetPath,
	})
	require.NoError(t, err)

	mockCallStopEndpoint.Unset()
	mockCallCleanupMountPoint.Unset()

	_, err = os.Stat(filepath.Join(socketsDir, podId))
	assert.True(t, os.IsNotExist(err))

	mounter.On("CleanupMountPoint", stagingTargetPath).Return(nil)

	nbsClient.On("StopEndpoint", ctx, &nbs.TStopEndpointRequest{
		UnixSocketPath: socketPath,
	}).Return(&nbs.TStopEndpointResponse{}, nil)

	_, err = nodeService.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{
		VolumeId:          diskId,
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
	nodeId := "testNodeId"
	clientId := "testClientId"
	podId := "test-pod-id-13"
	diskId := "test-disk-id-42"
	actualClientId := "testClientId-testNodeId"
	targetPath := filepath.Join(tempDir, "volumeDevices", "publish", diskId, podId)
	targetBlkPathPattern := filepath.Join(tempDir, "volumeDevices/publish/([a-z0-9-]+)/([a-z0-9-]+)")
	stagingTargetPath := "testStagingTargetPath"
	stagingDevicePath := filepath.Join(stagingTargetPath, diskId)
	socketsDir := filepath.Join(tempDir, "sockets")
	sourcePath := filepath.Join(socketsDir, diskId)
	socketPath := filepath.Join(socketsDir, diskId, "nbs.sock")
	localFsOverrides := make(ExternalFsOverrideMap)

	nodeService := newNodeService(
		nodeId,
		clientId,
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
			VolumeId:          diskId,
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
		DiskId:           diskId,
		InstanceId:       nodeId,
		ClientId:         actualClientId,
		DeviceName:       diskId,
		IpcType:          ipcType,
		VhostQueuesCount: 8,
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
		VolumeId:          diskId,
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
		VolumeId:          diskId,
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
		VolumeId:   diskId,
		TargetPath: targetPath,
	})
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(socketsDir, podId))
	assert.True(t, os.IsNotExist(err))

	mounter.On("IsMountPoint", stagingTargetPath).Return(false, nil)
	mounter.On("IsMountPoint", stagingDevicePath).Return(true, nil)
	mounter.On("CleanupMountPoint", stagingDevicePath).Return(nil)

	nbsClient.On("StopEndpoint", ctx, &nbs.TStopEndpointRequest{
		UnixSocketPath: socketPath,
	}).Run(volumeOperationInProgress).Return(&nbs.TStopEndpointResponse{}, nil)

	_, err = nodeService.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{
		VolumeId:          diskId,
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

	podId := "test-pod-id-13"
	diskId := "test-disk-id-42"
	socketsDir := filepath.Join(tempDir, "sockets")
	targetPath := filepath.Join(tempDir, "pods", podId, "volumes", diskId, "mount")
	targetFsPathPattern := filepath.Join(tempDir,
		"pods/([a-z0-9-]+)/volumes/([a-z0-9-]+)/mount")

	info, err := os.Stat(tempDir)
	require.NoError(t, err)
	err = os.MkdirAll(targetPath, info.Mode())
	require.NoError(t, err)
	localFsOverrides := make(ExternalFsOverrideMap)

	nodeService := newNodeService(
		"testNodeId",
		"testClientId",
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
		VolumeId:   diskId,
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

	clientId := "testClientId"
	podId := "test-pod-id-13"
	diskId := "test-disk-id-42"
	targetPath := filepath.Join(tempDir, "volumeDevices", "publish", diskId, podId)
	targetBlkPathPattern := filepath.Join(tempDir,
		"volumeDevices/publish/([a-z0-9-]+)/([a-z0-9-]+)")
	socketsDir := filepath.Join(tempDir, "sockets")
	localFsOverrides := make(ExternalFsOverrideMap)

	nodeService := newNodeService(
		"testNodeId",
		clientId,
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
		VolumeId:   diskId,
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
	clientId := "testClientId"
	podId := "test-pod-id-13"
	diskId := "test-disk-id-42"
	targetPath := filepath.Join(tempDir, "volumeDevices", "publish", diskId, podId)
	targetBlkPathPattern := filepath.Join(tempDir, "volumeDevices/publish/([a-z0-9-]+)/([a-z0-9-]+)")
	socketsDir := filepath.Join(tempDir, "sockets")
	volumeContext := map[string]string{
		backendVolumeContextKey: "nbs",
	}
	localFsOverrides := make(ExternalFsOverrideMap)

	nodeService := newNodeService(
		"testNodeId",
		clientId,
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
		VolumeId:          diskId,
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
		VolumeId:          diskId,
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
		VolumeId:          diskId,
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
	nodeId := "testNodeId"
	clientId := "testClientId"
	podId := "test-pod-id-13"
	instanceId := "testInstanceId"
	actualClientId := "testClientId-" + instanceId
	nfsId := "test-nfs"
	volumeId := nfsId + "#" + instanceId
	stagingTargetPath := filepath.Join(tempDir, "testStagingTargetPath")
	socketsDir := filepath.Join(tempDir, "sockets")
	sourcePath := filepath.Join(socketsDir, instanceId, nfsId)
	targetPath := filepath.Join(tempDir, "pods", podId, "volumes", nfsId, "mount")
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
		nodeId,
		clientId,
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
		instanceIdKey:           instanceId,
	}

	nfsLocalFilestoreClient.On("CreateFileStore", ctx, &nfs.TCreateFileStoreRequest{
		FileSystemId:     fmt.Sprintf("%s-%s", nfsId, instanceId),
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
			FileSystemId:     fmt.Sprintf("%s-%s", nfsId, instanceId),
			ClientId:         actualClientId,
			VhostQueuesCount: 8,
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
		FileSystemId: fmt.Sprintf("%s-%s", nfsId, instanceId),
	}).Return(&nfs.TDestroyFileStoreResponse{}, nil)

	_, err = nodeService.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{
		VolumeId:          volumeId,
		StagingTargetPath: stagingTargetPath,
	})
	require.NoError(t, err)

	_, err = os.Stat(mountFilePath)
	assert.True(t, os.IsNotExist(err))

	_, err = os.Stat(filepath.Join(socketsDir, instanceId))
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
	nodeId := "testNodeId"
	clientId := "testClientId"
	diskId := "test-disk-id-42"
	actualClientId := "testClientId-testNodeId"
	targetFsPathPattern := filepath.Join(tempDir, "pods/([a-z0-9-]+)/volumes/([a-z0-9-]+)/mount")
	stagingTargetPath := "testStagingTargetPath"
	socketsDir := filepath.Join(tempDir, "sockets")
	socketPath := filepath.Join(socketsDir, diskId, "nbs.sock")

	nodeService := newNodeService(
		nodeId,
		clientId,
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
		DiskId:           diskId,
		InstanceId:       nodeId,
		ClientId:         actualClientId,
		DeviceName:       diskId,
		IpcType:          ipcType,
		VhostQueuesCount: 8,
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
		VolumeId:          diskId,
		StagingTargetPath: stagingTargetPath,
		VolumeCapability:  &volumeCapability,
	})
	require.Error(t, err)

	mounter.AssertExpectations(t)
}
