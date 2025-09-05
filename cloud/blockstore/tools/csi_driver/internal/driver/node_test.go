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

type testContext struct {
	nbsClient               *mocks.NbsClientMock
	nfsClient               *mocks.NfsEndpointClientMock
	nfsLocalClient          *mocks.NfsEndpointClientMock
	nfsLocalFilestoreClient *mocks.NfsClientMock
	mounter                 *csimounter.Mock
	actualClientId          string
	volumeId                string
	tempDir                 string
	socketsDir              string
	targetFsPathPattern     string
	targetBlkPathPattern    string
	stagingTargetPath       string
	sourcePath              string
	nbsSocketPath           string
	nfsSocketPath           string
	targetPathMountMode     string
	targetPathBlockMode     string
	localFsOverrides        ExternalFsOverrideMap
}

func getSourcePath(socketsDir string, vmMode bool, legacyMode bool) string {
	if vmMode {
		if legacyMode {
			return filepath.Join(socketsDir, defaultPodId, defaultDiskId)
		}

		return filepath.Join(socketsDir, defaultInstanceId, defaultDiskId)
	}

	return filepath.Join(socketsDir, defaultDiskId)
}

func getActualClientId(vmMode bool, legacyMode bool) string {
	if vmMode {
		if legacyMode {
			return defaultCientId + "-" + defaultPodId
		}

		return defaultCientId + "-" + defaultInstanceId
	}

	return defaultCientId + "-" + defaultNodeId
}

func getVolumeId(perInstanceVolumes bool) string {
	if perInstanceVolumes {
		return defaultDiskId + "#" + defaultInstanceId
	}

	return defaultDiskId
}

func CreateTestContext(t *testing.T, vmMode bool, legacyMode bool, perInstanceVolumes bool) testContext {
	tempDir := t.TempDir()
	socketsDir := filepath.Join(tempDir, "sockets")
	sourcePath := getSourcePath(socketsDir, vmMode, legacyMode)

	return testContext{nbsClient: mocks.NewNbsClientMock(),
		nfsClient:               mocks.NewNfsEndpointClientMock(),
		nfsLocalClient:          mocks.NewNfsEndpointClientMock(),
		nfsLocalFilestoreClient: mocks.NewNfsClientMock(),
		mounter:                 csimounter.NewMock(),
		actualClientId:          getActualClientId(vmMode, legacyMode),
		volumeId:                getVolumeId(perInstanceVolumes),
		tempDir:                 tempDir,
		socketsDir:              socketsDir,
		targetFsPathPattern:     filepath.Join(tempDir, "pods/([a-z0-9-]+)/volumes/([a-z0-9-]+)/mount"),
		targetBlkPathPattern:    filepath.Join(tempDir, "volumeDevices/publish/([a-z0-9-]+)/([a-z0-9-]+)"),
		stagingTargetPath:       filepath.Join(tempDir, "testStagingTargetPath"),
		sourcePath:              sourcePath,
		nbsSocketPath:           filepath.Join(sourcePath, "nbs.sock"),
		nfsSocketPath:           filepath.Join(sourcePath, "nfs.sock"),
		targetPathMountMode:     filepath.Join(tempDir, "pods", defaultPodId, "volumes", defaultDiskId, "mount"),
		targetPathBlockMode:     filepath.Join(tempDir, "volumeDevices", "publish", defaultDiskId, defaultPodId),
		localFsOverrides:        make(ExternalFsOverrideMap),
	}
}

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
	testCtx := CreateTestContext(t, true, true, false)

	ctx := context.Background()

	deviceName := defaultDiskId
	if deviceNameOpt != nil {
		deviceName = *deviceNameOpt
	}

	nodeService := newNodeService(
		defaultNodeId,
		defaultCientId,
		true, // vmMode
		testCtx.socketsDir,
		testCtx.targetFsPathPattern,
		testCtx.targetBlkPathPattern,
		testCtx.localFsOverrides,
		testCtx.nbsClient,
		testCtx.nfsClient,
		testCtx.nfsLocalClient,
		testCtx.nfsLocalFilestoreClient,
		testCtx.mounter,
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
		VolumeId:          testCtx.volumeId,
		StagingTargetPath: testCtx.stagingTargetPath,
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
		testCtx.nbsClient.On("ListEndpoints",
			ctx, &nbs.TListEndpointsRequest{}).Return(&nbs.TListEndpointsResponse{}, nil)
		testCtx.nbsClient.On("StartEndpoint", ctx, &nbs.TStartEndpointRequest{
			Headers:          getDefaultStartEndpointRequestHeaders(),
			UnixSocketPath:   testCtx.nbsSocketPath,
			DiskId:           testCtx.volumeId,
			InstanceId:       defaultPodId,
			ClientId:         testCtx.actualClientId,
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
		testCtx.nfsClient.On("StartEndpoint", ctx, &nfs.TStartEndpointRequest{
			Endpoint: &nfs.TEndpointConfig{
				SocketPath:       testCtx.nfsSocketPath,
				FileSystemId:     testCtx.volumeId,
				ClientId:         testCtx.actualClientId,
				VhostQueuesCount: vhostQueuesCount,
				Persistent:       true,
			},
		}).Return(&nfs.TStartEndpointResponse{}, nil)
	}

	testCtx.mounter.On("IsMountPoint", testCtx.targetPathMountMode).Return(false, nil)
	testCtx.mounter.On("Mount", testCtx.sourcePath, testCtx.targetPathMountMode, "", []string{"bind"}).Return(nil)

	_, err = nodeService.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:          testCtx.volumeId,
		StagingTargetPath: testCtx.stagingTargetPath,
		TargetPath:        testCtx.targetPathMountMode,
		VolumeCapability:  &volumeCapability,
		VolumeContext:     volumeContext,
	})
	require.NoError(t, err)

	fileInfo, err := os.Stat(testCtx.sourcePath)
	assert.False(t, os.IsNotExist(err))
	assert.True(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0755), fileInfo.Mode().Perm())

	fileInfo, err = os.Stat(filepath.Join(testCtx.sourcePath, "disk.img"))
	assert.False(t, os.IsNotExist(err))
	assert.False(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0644), fileInfo.Mode().Perm())

	fileInfo, err = os.Stat(testCtx.targetPathMountMode)
	assert.False(t, os.IsNotExist(err))
	assert.True(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0775), fileInfo.Mode().Perm())

	testCtx.mounter.On("CleanupMountPoint", testCtx.targetPathMountMode).Return(nil)

	testCtx.nbsClient.On("StopEndpoint", ctx, &nbs.TStopEndpointRequest{
		UnixSocketPath: testCtx.nbsSocketPath,
	}).Return(&nbs.TStopEndpointResponse{}, nil)

	testCtx.nfsClient.On("StopEndpoint", ctx, &nfs.TStopEndpointRequest{
		SocketPath: testCtx.nfsSocketPath,
	}).Return(&nfs.TStopEndpointResponse{}, nil)

	_, err = nodeService.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   testCtx.volumeId,
		TargetPath: testCtx.targetPathMountMode,
	})
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(testCtx.socketsDir, defaultPodId))
	assert.True(t, os.IsNotExist(err))

	_, err = nodeService.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{
		VolumeId:          testCtx.volumeId,
		StagingTargetPath: testCtx.stagingTargetPath,
	})
	require.NoError(t, err)

	testCtx.nbsClient.AssertExpectations(t)
	testCtx.nfsClient.AssertExpectations(t)
	testCtx.mounter.AssertExpectations(t)
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
	testCtx := CreateTestContext(t, true, false, perInstanceVolumes)

	ctx := context.Background()
	deviceName := defaultDiskId
	if deviceNameOpt != nil {
		deviceName = *deviceNameOpt
	}

	if localFsOverride == LocalFsOverrideLegacyEnabled {
		testCtx.localFsOverrides[defaultDiskId] = ExternalFsConfig{
			Id: defaultDiskId,
		}
	} else if localFsOverride == LocalFsOverrideEnabled {
		testCtx.localFsOverrides[defaultDiskId] = ExternalFsConfig{
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
		testCtx.socketsDir,
		testCtx.targetFsPathPattern,
		testCtx.targetBlkPathPattern,
		testCtx.localFsOverrides,
		testCtx.nbsClient,
		testCtx.nfsClient,
		testCtx.nfsLocalClient,
		testCtx.nfsLocalFilestoreClient,
		testCtx.mounter,
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
		testCtx.nbsClient.On("ListEndpoints",
			ctx, &nbs.TListEndpointsRequest{}).Return(&nbs.TListEndpointsResponse{}, nil)
		testCtx.nbsClient.On("StartEndpoint", ctx, &nbs.TStartEndpointRequest{
			Headers:          getDefaultStartEndpointRequestHeaders(),
			UnixSocketPath:   testCtx.nbsSocketPath,
			DiskId:           defaultDiskId,
			InstanceId:       defaultInstanceId,
			ClientId:         testCtx.actualClientId,
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

	expectedNfsClient := testCtx.nfsClient
	if localFsOverride == LocalFsOverrideLegacyEnabled || localFsOverride == LocalFsOverrideEnabled {
		expectedNfsClient = testCtx.nfsLocalClient
	}

	if backend == "nfs" {
		if localFsOverride == LocalFsOverrideEnabled {
			testCtx.nfsLocalFilestoreClient.On("CreateFileStore", ctx, &nfs.TCreateFileStoreRequest{
				FileSystemId:     fmt.Sprintf("%s-%s", defaultDiskId, defaultInstanceId),
				CloudId:          testCtx.localFsOverrides[defaultDiskId].CloudId,
				FolderId:         testCtx.localFsOverrides[defaultDiskId].FolderId,
				BlockSize:        4096,
				BlocksCount:      (testCtx.localFsOverrides[defaultDiskId].SizeGb << 30) / 4096,
				StorageMediaKind: storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_SSD,
			}).Return(&nfs.TCreateFileStoreResponse{}, nil)

			testCtx.nfsLocalClient.On("ListEndpoints",
				ctx, &nfs.TListEndpointsRequest{}).Return(&nfs.TListEndpointsResponse{}, nil)

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
				SocketPath:       testCtx.nfsSocketPath,
				FileSystemId:     expectedFsId,
				ClientId:         testCtx.actualClientId,
				VhostQueuesCount: vhostQueuesCount,
				Persistent:       true,
			},
		}).Return(&nfs.TStartEndpointResponse{}, nil)
	}

	_, err := nodeService.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
		VolumeId:          testCtx.volumeId,
		StagingTargetPath: testCtx.stagingTargetPath,
		VolumeCapability:  &volumeCapability,
		VolumeContext:     volumeContext,
	})
	require.NoError(t, err)

	fileInfo, err := os.Stat(testCtx.sourcePath)
	assert.False(t, os.IsNotExist(err))
	assert.True(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0755), fileInfo.Mode().Perm())

	fileInfo, err = os.Stat(filepath.Join(testCtx.sourcePath, "disk.img"))
	assert.False(t, os.IsNotExist(err))
	assert.False(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0644), fileInfo.Mode().Perm())

	testCtx.mounter.On("IsMountPoint", testCtx.targetPathMountMode).Return(false, nil)
	testCtx.mounter.On("Mount", testCtx.sourcePath, testCtx.targetPathMountMode, "", []string{"bind"}).Return(nil)

	_, err = nodeService.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:          testCtx.volumeId,
		StagingTargetPath: testCtx.stagingTargetPath,
		TargetPath:        testCtx.targetPathMountMode,
		VolumeCapability:  &volumeCapability,
		VolumeContext:     volumeContext,
	})
	require.NoError(t, err)

	fileInfo, err = os.Stat(testCtx.targetPathMountMode)
	assert.False(t, os.IsNotExist(err))
	assert.True(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0775), fileInfo.Mode().Perm())

	testCtx.mounter.On("CleanupMountPoint", testCtx.targetPathMountMode).Return(nil)

	if !perInstanceVolumes {
		// Driver attempts to stop legacy endpoints only for legacy volumes.
		testCtx.nbsClient.On("StopEndpoint", ctx, &nbs.TStopEndpointRequest{
			UnixSocketPath: filepath.Join(testCtx.socketsDir, defaultPodId, defaultDiskId, nbsSocketName),
		}).Return(&nbs.TStopEndpointResponse{}, nil)

		expectedNfsClient.On("StopEndpoint", ctx, &nfs.TStopEndpointRequest{
			SocketPath: filepath.Join(testCtx.socketsDir, defaultPodId, defaultDiskId, nfsSocketName),
		}).Return(&nfs.TStopEndpointResponse{}, nil)
	}

	_, err = nodeService.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   testCtx.volumeId,
		TargetPath: testCtx.targetPathMountMode,
	})
	require.NoError(t, err)

	if backend == "nbs" {
		testCtx.nbsClient.On("StopEndpoint", ctx, &nbs.TStopEndpointRequest{
			UnixSocketPath: testCtx.nbsSocketPath,
		}).Return(&nbs.TStopEndpointResponse{}, nil)
	}

	if backend == "nfs" {
		expectedNfsClient.On("StopEndpoint", ctx, &nfs.TStopEndpointRequest{
			SocketPath: testCtx.nfsSocketPath,
		}).Return(&nfs.TStopEndpointResponse{}, nil)

		if localFsOverride == LocalFsOverrideEnabled {
			testCtx.nfsLocalFilestoreClient.On("DestroyFileStore", ctx, &nfs.TDestroyFileStoreRequest{
				FileSystemId: fmt.Sprintf("%s-%s", defaultDiskId, defaultInstanceId),
			}).Return(&nfs.TDestroyFileStoreResponse{}, nil)
		}
	}

	_, err = nodeService.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{
		VolumeId:          testCtx.volumeId,
		StagingTargetPath: testCtx.stagingTargetPath,
	})
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(testCtx.socketsDir, defaultInstanceId))
	assert.True(t, os.IsNotExist(err))

	testCtx.nbsClient.AssertExpectations(t)
	testCtx.nfsClient.AssertExpectations(t)
	testCtx.nfsLocalClient.AssertExpectations(t)
	testCtx.nfsLocalFilestoreClient.AssertExpectations(t)
	testCtx.mounter.AssertExpectations(t)
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
	testCtx := CreateTestContext(t, false, false, false)

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

	ipcType := nbs.EClientIpcType_IPC_NBD
	nbdDeviceFile := filepath.Join(testCtx.tempDir, "dev", "nbd3")
	err = os.MkdirAll(nbdDeviceFile, fs.FileMode(0755))
	require.NoError(t, err)

	ctx := context.Background()
	deprecatedSocketPath := filepath.Join(testCtx.socketsDir, defaultPodId, defaultDiskId, "nbs.sock")

	nodeService := newNodeService(
		defaultNodeId,
		defaultCientId,
		false, // vmMode
		testCtx.socketsDir,
		testCtx.targetFsPathPattern,
		testCtx.targetBlkPathPattern,
		testCtx.localFsOverrides,
		testCtx.nbsClient,
		testCtx.nfsClient,
		testCtx.nfsLocalClient,
		testCtx.nfsLocalFilestoreClient,
		testCtx.mounter,
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
	testCtx.nbsClient.On("ListEndpoints", ctx, &nbs.TListEndpointsRequest{}).Return(&nbs.TListEndpointsResponse{}, nil)
	testCtx.nbsClient.On("StartEndpoint", ctx, &nbs.TStartEndpointRequest{
		Headers:          getDefaultStartEndpointRequestHeaders(),
		UnixSocketPath:   testCtx.nbsSocketPath,
		DiskId:           defaultDiskId,
		InstanceId:       defaultNodeId,
		ClientId:         testCtx.actualClientId,
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

	testCtx.mounter.On("HasBlockDevice", nbdDeviceFile).Return(true, nil)

	mockCallIsMountPoint := testCtx.mounter.On("IsMountPoint", testCtx.stagingTargetPath).Return(false, nil)

	testCtx.mounter.On("FormatAndMount", nbdDeviceFile, testCtx.stagingTargetPath, "ext4",
		[]string{"grpid", "errors=remount-ro", "discard"}).Return(nil)

	_, err = nodeService.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
		VolumeId:          testCtx.volumeId,
		StagingTargetPath: testCtx.stagingTargetPath,
		VolumeCapability:  &volumeCapability,
		VolumeContext:     volumeContext,
	})
	require.NoError(t, err)

	mockCallIsMountPoint.Unset()

	testCtx.mounter.On("IsMountPoint", testCtx.stagingTargetPath).Return(true, nil)
	testCtx.mounter.On("IsMountPoint", testCtx.targetPathMountMode).Return(false, nil)
	testCtx.mounter.On("IsFilesystemRemountedAsReadonly", testCtx.stagingTargetPath).Return(false, nil)

	testCtx.mounter.On("Mount", testCtx.stagingTargetPath, testCtx.targetPathMountMode, "", []string{"bind"}).Return(nil)

	_, err = nodeService.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:          testCtx.volumeId,
		StagingTargetPath: testCtx.stagingTargetPath,
		TargetPath:        testCtx.targetPathMountMode,
		VolumeCapability:  &volumeCapability,
		VolumeContext:     volumeContext,
	})
	require.NoError(t, err)

	fileInfo, err := os.Stat(testCtx.sourcePath)
	assert.False(t, os.IsNotExist(err))
	assert.True(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0755), fileInfo.Mode().Perm())

	fileInfo, err = os.Stat(testCtx.targetPathMountMode)
	assert.False(t, os.IsNotExist(err))
	assert.True(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0775), fileInfo.Mode().Perm())

	output, err := exec.Command("ls", "-ldn", testCtx.targetPathMountMode).CombinedOutput()
	assert.False(t, os.IsNotExist(err))
	log.Printf("Target path: %s", output)
	fields := strings.Fields(string(output))
	assert.Equal(t, groupId, fields[3])

	mockCallCleanupMountPoint := testCtx.mounter.On("CleanupMountPoint", testCtx.targetPathMountMode).Return(nil)

	mockCallStopEndpoint := testCtx.nbsClient.On("StopEndpoint", ctx, &nbs.TStopEndpointRequest{
		UnixSocketPath: deprecatedSocketPath,
	}).Return(&nbs.TStopEndpointResponse{}, nil)

	_, err = nodeService.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   testCtx.volumeId,
		TargetPath: testCtx.targetPathMountMode,
	})
	require.NoError(t, err)

	mockCallStopEndpoint.Unset()
	mockCallCleanupMountPoint.Unset()

	_, err = os.Stat(filepath.Join(testCtx.socketsDir, defaultPodId))
	assert.True(t, os.IsNotExist(err))

	testCtx.mounter.On("CleanupMountPoint", testCtx.stagingTargetPath).Return(nil)

	testCtx.nbsClient.On("StopEndpoint", ctx, &nbs.TStopEndpointRequest{
		UnixSocketPath: testCtx.nbsSocketPath,
	}).Return(&nbs.TStopEndpointResponse{}, nil)

	_, err = nodeService.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{
		VolumeId:          testCtx.volumeId,
		StagingTargetPath: testCtx.stagingTargetPath,
	})
	require.NoError(t, err)

	testCtx.nbsClient.AssertExpectations(t)
	testCtx.mounter.AssertExpectations(t)
}

func TestPublishUnpublishDeviceForInfrakuber(t *testing.T) {
	testCtx := CreateTestContext(t, false, false, false)

	ipcType := nbs.EClientIpcType_IPC_NBD
	nbdDeviceFile := filepath.Join(testCtx.tempDir, "dev", "nbd3")
	err := os.MkdirAll(nbdDeviceFile, 0755)
	require.NoError(t, err)

	ctx := context.Background()
	stagingDevicePath := filepath.Join(testCtx.stagingTargetPath, defaultDiskId)

	nodeService := newNodeService(
		defaultNodeId,
		defaultCientId,
		false, // vmMode
		testCtx.socketsDir,
		testCtx.targetFsPathPattern,
		testCtx.targetBlkPathPattern,
		testCtx.localFsOverrides,
		testCtx.nbsClient,
		testCtx.nfsClient,
		testCtx.nfsLocalClient,
		testCtx.nfsLocalFilestoreClient,
		testCtx.mounter,
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
			VolumeId:          testCtx.volumeId,
			StagingTargetPath: testCtx.stagingTargetPath,
			VolumeCapability:  &volumeCapability,
		})
		require.Error(t, err)
		expectedError := "rpc error: code = Aborted desc = [n=testNodeId]: " +
			"Another operation with volume test-disk-id-42 is in progress"
		require.Equal(t, expectedError, err.Error())
	}

	hostType := nbs.EHostType_HOST_TYPE_DEFAULT
	testCtx.nbsClient.On("ListEndpoints",
		ctx, &nbs.TListEndpointsRequest{}).Return(&nbs.TListEndpointsResponse{}, nil)
	testCtx.nbsClient.On("StartEndpoint", ctx, &nbs.TStartEndpointRequest{
		Headers:          getDefaultStartEndpointRequestHeaders(),
		UnixSocketPath:   testCtx.nbsSocketPath,
		DiskId:           defaultDiskId,
		InstanceId:       defaultNodeId,
		ClientId:         testCtx.actualClientId,
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
		testCtx.mounter.On("IsMountPoint",
			stagingDevicePath).Run(volumeOperationInProgress).Return(true, nil)
	mockCallMount := testCtx.mounter.On("Mount",
		nbdDeviceFile, stagingDevicePath, "", []string{"bind"}).Return(nil)

	_, err = nodeService.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
		VolumeId:          testCtx.volumeId,
		StagingTargetPath: testCtx.stagingTargetPath,
		VolumeCapability:  &volumeCapability,
		VolumeContext:     volumeContext,
	})
	require.NoError(t, err)
	mockCallMount.Unset()

	mockCallIsMountPointTargetPath :=
		testCtx.mounter.On("IsMountPoint", testCtx.targetPathBlockMode).Run(volumeOperationInProgress).Return(false, nil)
	testCtx.mounter.On("Mount", stagingDevicePath, testCtx.targetPathBlockMode, "", []string{"bind"}).Return(nil)

	_, err = nodeService.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:          testCtx.volumeId,
		StagingTargetPath: testCtx.stagingTargetPath,
		TargetPath:        testCtx.targetPathBlockMode,
		VolumeCapability:  &volumeCapability,
		VolumeContext:     volumeContext,
	})
	require.NoError(t, err)

	mockCallIsMountPointStagingPath.Unset()
	mockCallIsMountPointTargetPath.Unset()

	fileInfo, err := os.Stat(testCtx.sourcePath)
	assert.False(t, os.IsNotExist(err))
	assert.True(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0755), fileInfo.Mode().Perm())

	fileInfo, err = os.Stat(filepath.Dir(testCtx.targetPathBlockMode))
	assert.False(t, os.IsNotExist(err))
	assert.True(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0750), fileInfo.Mode().Perm())

	fileInfo, err = os.Stat(testCtx.targetPathBlockMode)
	assert.False(t, os.IsNotExist(err))
	assert.False(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0660), fileInfo.Mode().Perm())

	testCtx.mounter.On("CleanupMountPoint", testCtx.targetPathBlockMode).Return(nil)

	_, err = nodeService.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   testCtx.volumeId,
		TargetPath: testCtx.targetPathBlockMode,
	})
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(testCtx.socketsDir, defaultPodId))
	assert.True(t, os.IsNotExist(err))

	testCtx.mounter.On("IsMountPoint", testCtx.stagingTargetPath).Return(false, nil)
	testCtx.mounter.On("IsMountPoint", stagingDevicePath).Return(true, nil)
	testCtx.mounter.On("CleanupMountPoint", stagingDevicePath).Return(nil)

	testCtx.nbsClient.On("StopEndpoint", ctx, &nbs.TStopEndpointRequest{
		UnixSocketPath: testCtx.nbsSocketPath,
	}).Run(volumeOperationInProgress).Return(&nbs.TStopEndpointResponse{}, nil)

	_, err = nodeService.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{
		VolumeId:          testCtx.volumeId,
		StagingTargetPath: testCtx.stagingTargetPath,
	})
	require.NoError(t, err)

	testCtx.nbsClient.AssertExpectations(t)
	testCtx.mounter.AssertExpectations(t)
}

func TestGetVolumeStatCapabilitiesWithoutVmMode(t *testing.T) {
	testCtx := CreateTestContext(t, false, false, false)

	info, err := os.Stat(testCtx.tempDir)
	require.NoError(t, err)
	err = os.MkdirAll(testCtx.targetPathMountMode, info.Mode())
	require.NoError(t, err)

	nodeService := newNodeService(
		defaultNodeId,
		defaultCientId,
		false,
		testCtx.socketsDir,
		testCtx.targetFsPathPattern,
		testCtx.targetBlkPathPattern,
		testCtx.localFsOverrides,
		testCtx.nbsClient,
		testCtx.nfsClient,
		testCtx.nfsLocalClient,
		testCtx.nfsLocalFilestoreClient,
		testCtx.mounter,
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

	testCtx.mounter.On("IsMountPoint", testCtx.targetPathMountMode).Return(true, nil)

	stat, err := nodeService.NodeGetVolumeStats(ctx, &csi.NodeGetVolumeStatsRequest{
		VolumeId:   testCtx.volumeId,
		VolumePath: testCtx.targetPathMountMode,
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

	testCtx.nbsClient.AssertExpectations(t)
	testCtx.mounter.AssertExpectations(t)
}

func TestGetVolumeStatCapabilitiesWithVmMode(t *testing.T) {
	testCtx := CreateTestContext(t, true, true, false)

	nodeService := newNodeService(
		defaultNodeId,
		defaultCientId,
		true,
		testCtx.socketsDir,
		testCtx.targetFsPathPattern,
		testCtx.targetBlkPathPattern,
		testCtx.localFsOverrides,
		testCtx.nbsClient,
		testCtx.nfsClient,
		testCtx.nfsLocalClient,
		testCtx.nfsLocalFilestoreClient,
		testCtx.mounter,
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
		VolumeId:   testCtx.volumeId,
		VolumePath: testCtx.targetPathBlockMode,
	})
	require.Error(t, err)

	testCtx.nbsClient.AssertExpectations(t)
	testCtx.mounter.AssertExpectations(t)
}

func TestPublishDeviceWithReadWriteManyModeIsNotSupportedWithNBS(t *testing.T) {
	testCtx := CreateTestContext(t, false, false, false)
	ctx := context.Background()

	volumeContext := map[string]string{
		backendVolumeContextKey: "nbs",
	}

	nodeService := newNodeService(
		defaultNodeId,
		defaultCientId,
		false,
		testCtx.socketsDir,
		testCtx.targetFsPathPattern,
		testCtx.targetBlkPathPattern,
		testCtx.localFsOverrides,
		testCtx.nbsClient,
		testCtx.nfsClient,
		testCtx.nfsLocalClient,
		testCtx.nfsLocalFilestoreClient,
		testCtx.mounter,
		[]string{},
		false,
		defaultStartEndpointRequestTimeout,
	)

	_, err := nodeService.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
		VolumeId:          testCtx.volumeId,
		StagingTargetPath: testCtx.stagingTargetPath,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{
				Block: &csi.VolumeCapability_BlockVolume{},
			},
		},
	})
	require.Error(t, err)

	// NodePublishVolume without access mode should fail
	_, err = nodeService.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:          testCtx.volumeId,
		StagingTargetPath: testCtx.stagingTargetPath,
		TargetPath:        testCtx.targetPathBlockMode,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{
				Block: &csi.VolumeCapability_BlockVolume{},
			},
		},
		VolumeContext: volumeContext,
	})
	require.Error(t, err)

	_, err = nodeService.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:          testCtx.volumeId,
		StagingTargetPath: testCtx.stagingTargetPath,
		TargetPath:        testCtx.targetPathBlockMode,
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

	testCtx.nbsClient.AssertExpectations(t)
	testCtx.mounter.AssertExpectations(t)
}

func TestExternaFs(t *testing.T) {
	testCtx := CreateTestContext(t, true, false, true)

	ctx := context.Background()
	mountFilePath := filepath.Join(testCtx.tempDir, "mountDone")

	fsConfig := ExternalFsConfig{
		Id:       defaultDiskId,
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

	testCtx.localFsOverrides[defaultDiskId] = fsConfig

	nodeService := newNodeService(
		defaultNodeId,
		defaultCientId,
		true, // vmMode
		testCtx.socketsDir,
		testCtx.targetFsPathPattern,
		testCtx.targetBlkPathPattern,
		testCtx.localFsOverrides,
		testCtx.nbsClient,
		testCtx.nfsClient,
		testCtx.nfsLocalClient,
		testCtx.nfsLocalFilestoreClient,
		testCtx.mounter,
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

	testCtx.nfsLocalFilestoreClient.On("CreateFileStore", ctx, &nfs.TCreateFileStoreRequest{
		FileSystemId:     fmt.Sprintf("%s-%s", defaultDiskId, defaultInstanceId),
		CloudId:          fsConfig.CloudId,
		FolderId:         fsConfig.FolderId,
		BlockSize:        4096,
		BlocksCount:      (fsConfig.SizeGb << 30) / 4096,
		StorageMediaKind: storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_SSD,
	}).Return(&nfs.TCreateFileStoreResponse{}, nil)

	testCtx.nfsLocalClient.On("ListEndpoints",
		ctx, &nfs.TListEndpointsRequest{}).Return(&nfs.TListEndpointsResponse{}, nil)

	testCtx.nfsLocalClient.On("StartEndpoint", ctx, &nfs.TStartEndpointRequest{
		Endpoint: &nfs.TEndpointConfig{
			SocketPath:       testCtx.nfsSocketPath,
			FileSystemId:     fmt.Sprintf("%s-%s", defaultDiskId, defaultInstanceId),
			ClientId:         testCtx.actualClientId,
			VhostQueuesCount: defaultVhostQueuesCount,
			Persistent:       true,
		},
	}).Return(&nfs.TStartEndpointResponse{}, nil)

	_, err := nodeService.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
		VolumeId:          testCtx.volumeId,
		StagingTargetPath: testCtx.stagingTargetPath,
		VolumeCapability:  &volumeCapability,
		VolumeContext:     volumeContext,
	})
	require.NoError(t, err)

	fileInfo, err := os.Stat(mountFilePath)
	require.NoError(t, err)
	assert.True(t, fileInfo.Mode().IsRegular())

	fileInfo, err = os.Stat(testCtx.sourcePath)
	assert.False(t, os.IsNotExist(err))
	assert.True(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0755), fileInfo.Mode().Perm())

	fileInfo, err = os.Stat(filepath.Join(testCtx.sourcePath, "disk.img"))
	assert.False(t, os.IsNotExist(err))
	assert.False(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0644), fileInfo.Mode().Perm())

	testCtx.mounter.On("IsMountPoint", testCtx.targetPathMountMode).Return(false, nil)
	testCtx.mounter.On("Mount", testCtx.sourcePath, testCtx.targetPathMountMode, "", []string{"bind"}).Return(nil)

	_, err = nodeService.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:          testCtx.volumeId,
		StagingTargetPath: testCtx.stagingTargetPath,
		TargetPath:        testCtx.targetPathMountMode,
		VolumeCapability:  &volumeCapability,
		VolumeContext:     volumeContext,
	})
	require.NoError(t, err)

	fileInfo, err = os.Stat(testCtx.targetPathMountMode)
	assert.False(t, os.IsNotExist(err))
	assert.True(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0775), fileInfo.Mode().Perm())

	testCtx.mounter.On("CleanupMountPoint", testCtx.targetPathMountMode).Return(nil)

	_, err = nodeService.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   testCtx.volumeId,
		TargetPath: testCtx.targetPathMountMode,
	})
	require.NoError(t, err)

	testCtx.nfsLocalClient.On("StopEndpoint", ctx, &nfs.TStopEndpointRequest{
		SocketPath: testCtx.nfsSocketPath,
	}).Return(&nfs.TStopEndpointResponse{}, nil)

	testCtx.nfsLocalFilestoreClient.On("DestroyFileStore", ctx, &nfs.TDestroyFileStoreRequest{
		FileSystemId: fmt.Sprintf("%s-%s", defaultDiskId, defaultInstanceId),
	}).Return(&nfs.TDestroyFileStoreResponse{}, nil)

	_, err = nodeService.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{
		VolumeId:          testCtx.volumeId,
		StagingTargetPath: testCtx.stagingTargetPath,
	})
	require.NoError(t, err)

	_, err = os.Stat(mountFilePath)
	assert.True(t, os.IsNotExist(err))

	_, err = os.Stat(filepath.Join(testCtx.socketsDir, defaultInstanceId))
	assert.True(t, os.IsNotExist(err))

	testCtx.nbsClient.AssertExpectations(t)
	testCtx.nfsClient.AssertExpectations(t)
	testCtx.nfsLocalClient.AssertExpectations(t)
	testCtx.nfsLocalFilestoreClient.AssertExpectations(t)
	testCtx.mounter.AssertExpectations(t)
}

func TestStopEndpointAfterNodeStageVolumeFailureForInfrakuber(t *testing.T) {
	testCtx := CreateTestContext(t, false, false, false)

	ipcType := nbs.EClientIpcType_IPC_NBD
	nbdDeviceFile := filepath.Join(testCtx.tempDir, "dev", "nbd3")
	err := os.MkdirAll(nbdDeviceFile, fs.FileMode(0755))
	require.NoError(t, err)

	ctx := context.Background()

	nodeService := newNodeService(
		defaultNodeId,
		defaultCientId,
		false,
		testCtx.socketsDir,
		testCtx.targetFsPathPattern,
		testCtx.targetBlkPathPattern,
		testCtx.localFsOverrides,
		testCtx.nbsClient,
		testCtx.nfsClient,
		testCtx.nfsLocalClient,
		testCtx.nfsLocalFilestoreClient,
		testCtx.mounter,
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
	testCtx.nbsClient.On("ListEndpoints", ctx, &nbs.TListEndpointsRequest{}).Return(
		&nbs.TListEndpointsResponse{}, nil)
	testCtx.nbsClient.On("StartEndpoint", ctx, &nbs.TStartEndpointRequest{
		Headers:          getDefaultStartEndpointRequestHeaders(),
		UnixSocketPath:   testCtx.nbsSocketPath,
		DiskId:           defaultDiskId,
		InstanceId:       defaultNodeId,
		ClientId:         testCtx.actualClientId,
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
	testCtx.mounter.On("HasBlockDevice", nbdDeviceFile).Return(false, nbdError)

	testCtx.nbsClient.On("StopEndpoint", ctx, &nbs.TStopEndpointRequest{
		UnixSocketPath: testCtx.nbsSocketPath,
	}).Return(&nbs.TStopEndpointResponse{}, nil)

	_, err = nodeService.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
		VolumeId:          testCtx.volumeId,
		StagingTargetPath: testCtx.stagingTargetPath,
		VolumeCapability:  &volumeCapability,
	})
	require.Error(t, err)

	testCtx.mounter.AssertExpectations(t)
}

func TestNodeStageVolumeErrorForKubevirt(
	t *testing.T,
) {
	t.Helper()
	testCtx := CreateTestContext(t, true, false, true)

	ctx := context.Background()
	backend := "nbs"

	nodeService := newNodeService(
		defaultNodeId,
		defaultCientId,
		true, // vmMode
		testCtx.socketsDir,
		testCtx.targetFsPathPattern,
		testCtx.targetBlkPathPattern,
		testCtx.localFsOverrides,
		testCtx.nbsClient,
		testCtx.nfsClient,
		testCtx.nfsLocalClient,
		testCtx.nfsLocalFilestoreClient,
		testCtx.mounter,
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
		testCtx.nbsClient.On("ListEndpoints",
			ctx, &nbs.TListEndpointsRequest{}).Return(&nbs.TListEndpointsResponse{}, nil)
		testCtx.nbsClient.On("StartEndpoint", ctx, &nbs.TStartEndpointRequest{
			Headers:          getDefaultStartEndpointRequestHeaders(),
			UnixSocketPath:   testCtx.nbsSocketPath,
			DiskId:           defaultDiskId,
			InstanceId:       defaultInstanceId,
			ClientId:         testCtx.actualClientId,
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
		testCtx.nbsClient.On("StopEndpoint", ctx, &nbs.TStopEndpointRequest{
			UnixSocketPath: testCtx.nbsSocketPath,
		}).Return(&nbs.TStopEndpointResponse{}, nil)
	}

	_, err := nodeService.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
		VolumeId:          testCtx.volumeId,
		StagingTargetPath: testCtx.stagingTargetPath,
		VolumeCapability:  &volumeCapability,
		VolumeContext:     volumeContext,
	})
	require.Error(t, err)

	testCtx.mounter.AssertExpectations(t)
}

func TestNodeUnstageVolumeErrorForKubevirt(
	t *testing.T,
) {
	t.Helper()
	testCtx := CreateTestContext(t, true, false, false)

	ctx := context.Background()

	nodeService := newNodeService(
		defaultNodeId,
		defaultCientId,
		true, // vmMode
		testCtx.socketsDir,
		testCtx.targetFsPathPattern,
		testCtx.targetBlkPathPattern,
		testCtx.localFsOverrides,
		testCtx.nbsClient,
		testCtx.nfsClient,
		testCtx.nfsLocalClient,
		testCtx.nfsLocalFilestoreClient,
		testCtx.mounter,
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
		backendVolumeContextKey: "nbs",
		instanceIdKey:           defaultInstanceId,
	}

	hostType := nbs.EHostType_HOST_TYPE_DEFAULT
	testCtx.nbsClient.On("ListEndpoints", ctx, &nbs.TListEndpointsRequest{}).Return(
		&nbs.TListEndpointsResponse{}, nil)
	testCtx.nbsClient.On("StartEndpoint", ctx, &nbs.TStartEndpointRequest{
		Headers:          getDefaultStartEndpointRequestHeaders(),
		UnixSocketPath:   testCtx.nbsSocketPath,
		DiskId:           defaultDiskId,
		InstanceId:       defaultInstanceId,
		ClientId:         testCtx.actualClientId,
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

	_, err := nodeService.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
		VolumeId:          testCtx.volumeId,
		StagingTargetPath: testCtx.stagingTargetPath,
		VolumeCapability:  &volumeCapability,
		VolumeContext:     volumeContext,
	})
	require.NoError(t, err)

	testCtx.nbsClient.On("StopEndpoint", ctx, &nbs.TStopEndpointRequest{
		UnixSocketPath: testCtx.nbsSocketPath,
	}).Return(&nbs.TStopEndpointResponse{},
		&nbsclient.ClientError{Code: nbsclient.E_GRPC_DEADLINE_EXCEEDED, Message: ""})

	_, err = nodeService.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{
		VolumeId:          testCtx.volumeId,
		StagingTargetPath: testCtx.stagingTargetPath,
	})
	require.Error(t, err)
	status, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.DeadlineExceeded, status.Code())

	testCtx.mounter.AssertExpectations(t)
}
