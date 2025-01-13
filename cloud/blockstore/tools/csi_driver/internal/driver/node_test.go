package driver

//lint:file-ignore ST1003 protobuf generates names that break golang naming convention

import (
	"context"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	nbs "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	"github.com/ydb-platform/nbs/cloud/blockstore/tools/csi_driver/internal/driver/mocks"
	csimounter "github.com/ydb-platform/nbs/cloud/blockstore/tools/csi_driver/internal/mounter"
	nfs "github.com/ydb-platform/nbs/cloud/filestore/public/api/protos"
)

////////////////////////////////////////////////////////////////////////////////

type TestContextBuilder struct {
	tempDir            string
	backend            string
	vmMode             bool
	podId              string
	diskId             string
	nodeId             string
	clientId           string
	instanceId         string
	deviceName         string
	isLocalFsOverride  bool
	perInstanceVolumes bool
}

type TestContext struct {
	tempDir             string
	backend             string
	vmMode              bool
	nodeId              string
	clientId            string
	podId               string
	instanceId          string
	actualClientId      string
	diskId              string
	volumeId            string
	deviceName          string
	stagingTargetPath   string
	socketsDir          string
	sourcePath          string
	targetPath          string
	targetFsPathPattern string
	nbsSocketPath       string
	nfsSocketPath       string
	localFsOverrides    LocalFilestoreOverrideMap
	accessMode          csi.VolumeCapability_AccessMode_Mode
	volumeCapability    csi.VolumeCapability
	volumeContext       map[string]string
	perInstanceVolumes  bool
	nbsClient           *mocks.NbsClientMock
	nfsClient           *mocks.NfsEndpointClientMock
	nfsLocalClient      *mocks.NfsEndpointClientMock
	mounter             *csimounter.Mock
}

func (ctx *TestContext) SetDeviceName(deviceName string) {
	ctx.deviceName = deviceName
}

func CreateTestContextBuilder(tempDir string, backend string, vmMode bool) TestContextBuilder {
	return TestContextBuilder{
		tempDir:            tempDir,
		backend:            backend,
		vmMode:             vmMode,
		podId:              "test-pod-id-13",
		diskId:             "test-disk-id-42",
		nodeId:             "testNodeId",
		clientId:           "testClientId",
		instanceId:         "",
		deviceName:         "",
		isLocalFsOverride:  false,
		perInstanceVolumes: false,
	}
}

func (c *TestContextBuilder) WithInstanceId() {
	c.instanceId = "testInstanceId"
}

func (c *TestContextBuilder) WithDeviceName() {
	c.deviceName = "test-disk-name-42"
}

func (c *TestContextBuilder) WithLocalFsOverride() {
	c.isLocalFsOverride = true
}

func (c *TestContextBuilder) WithPerInstanceVolumes() {
	c.perInstanceVolumes = true
}

func (c *TestContextBuilder) Build() TestContext {
	socketsDir := filepath.Join(c.tempDir, "sockets")
	sourcePath := filepath.Join(socketsDir, c.podId, c.diskId)
	actualClientId := c.clientId + "-" + c.podId
	if c.instanceId != "" {
		sourcePath = filepath.Join(socketsDir, c.instanceId, c.diskId)
		actualClientId = c.clientId + "-" + c.instanceId
	}

	volumeId := c.diskId
	if c.perInstanceVolumes {
		volumeId = c.diskId + "#" + c.instanceId
	}

	deviceName := c.diskId
	if c.deviceName != "" {
		deviceName = c.deviceName
	}

	accessMode := csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER
	if c.backend == "nfs" {
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

	localFsOverrides := make(LocalFilestoreOverrideMap)
	if c.isLocalFsOverride {
		localFsOverrides[c.diskId] = LocalFilestoreOverride{
			FsId:           c.diskId,
			LocalMountPath: "/tmp/mnt/local_mount",
		}
	}

	volumeContext := map[string]string{
		backendVolumeContextKey: c.backend,
	}
	if c.diskId != deviceName {
		volumeContext[deviceNameVolumeContextKey] = c.deviceName
	}

	if c.instanceId != "" {
		volumeContext[instanceIdKey] = c.instanceId
	}

	return TestContext{
		tempDir:             c.tempDir,
		backend:             c.backend,
		vmMode:              c.vmMode,
		nodeId:              "testNodeId",
		clientId:            c.clientId,
		podId:               c.podId,
		instanceId:          c.instanceId,
		actualClientId:      actualClientId,
		diskId:              c.diskId,
		volumeId:            volumeId,
		deviceName:          deviceName,
		stagingTargetPath:   filepath.Join(c.tempDir, "testStagingTargetPath"),
		socketsDir:          socketsDir,
		sourcePath:          sourcePath,
		targetPath:          filepath.Join(c.tempDir, "pods", c.podId, "volumes", c.diskId, "mount"),
		targetFsPathPattern: filepath.Join(c.tempDir, "pods/([a-z0-9-]+)/volumes/([a-z0-9-]+)/mount"),
		nbsSocketPath:       filepath.Join(sourcePath, "nbs.sock"),
		nfsSocketPath:       filepath.Join(sourcePath, "nfs.sock"),
		localFsOverrides:    localFsOverrides,
		accessMode:          accessMode,
		volumeCapability:    volumeCapability,
		volumeContext:       volumeContext,
		perInstanceVolumes:  c.perInstanceVolumes,
		nbsClient:           mocks.NewNbsClientMock(),
		nfsClient:           mocks.NewNfsEndpointClientMock(),
		nfsLocalClient:      mocks.NewNfsEndpointClientMock(),
		mounter:             csimounter.NewMock(),
	}
}

func expectSuccessNodeStageVolume(ctx context.Context, testContext *TestContext) {
	if testContext.instanceId != "" {
		hostType := nbs.EHostType_HOST_TYPE_DEFAULT
		if testContext.backend == "nbs" {
			testContext.nbsClient.On("StartEndpoint", ctx, &nbs.TStartEndpointRequest{
				UnixSocketPath:   testContext.nbsSocketPath,
				DiskId:           testContext.diskId,
				InstanceId:       testContext.instanceId,
				ClientId:         testContext.actualClientId,
				DeviceName:       testContext.deviceName,
				IpcType:          nbs.EClientIpcType_IPC_VHOST,
				VhostQueuesCount: 8,
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

		expectedNfsClient := testContext.nfsClient
		if len(testContext.localFsOverrides) != 0 {
			expectedNfsClient = testContext.nfsLocalClient
		}

		if testContext.backend == "nfs" {
			expectedNfsClient.On("StartEndpoint", ctx, &nfs.TStartEndpointRequest{
				Endpoint: &nfs.TEndpointConfig{
					SocketPath:       testContext.nfsSocketPath,
					FileSystemId:     testContext.diskId,
					ClientId:         testContext.actualClientId,
					VhostQueuesCount: 8,
					Persistent:       true,
				},
			}).Return(&nfs.TStartEndpointResponse{}, nil)
		}
	}
}

func expectSuccessNodePublishVolume(ctx context.Context, testContext *TestContext) {
	if testContext.instanceId == "" {
		hostType := nbs.EHostType_HOST_TYPE_DEFAULT

		if testContext.backend == "nbs" {
			testContext.nbsClient.On("StartEndpoint", ctx, &nbs.TStartEndpointRequest{
				UnixSocketPath:   testContext.nbsSocketPath,
				DiskId:           testContext.diskId,
				InstanceId:       testContext.podId,
				ClientId:         testContext.actualClientId,
				DeviceName:       testContext.deviceName,
				IpcType:          nbs.EClientIpcType_IPC_VHOST,
				VhostQueuesCount: 8,
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

		expectedNfsClient := testContext.nfsClient
		if len(testContext.localFsOverrides) != 0 {
			expectedNfsClient = testContext.nfsLocalClient
		}

		if testContext.backend == "nfs" {
			expectedNfsClient.On("StartEndpoint", ctx, &nfs.TStartEndpointRequest{
				Endpoint: &nfs.TEndpointConfig{
					SocketPath:       testContext.nfsSocketPath,
					FileSystemId:     testContext.diskId,
					ClientId:         testContext.actualClientId,
					VhostQueuesCount: 8,
					Persistent:       true,
				},
			}).Return(&nfs.TStartEndpointResponse{}, nil)
		}
	}

	testContext.mounter.On("IsMountPoint", testContext.targetPath).Return(false, nil)
	testContext.mounter.On("Mount", testContext.sourcePath, testContext.targetPath, "", []string{"bind"}).Return(nil)
}

func expectNodeUnpublishVolume(ctx context.Context, testContext *TestContext) {
	expectedNfsClient := testContext.nfsClient
	if len(testContext.localFsOverrides) != 0 {
		expectedNfsClient = testContext.nfsLocalClient
	}

	testContext.mounter.On("CleanupMountPoint", testContext.targetPath).Return(nil)

	if testContext.instanceId == "" {
		testContext.nbsClient.On("StopEndpoint", ctx, &nbs.TStopEndpointRequest{
			UnixSocketPath: testContext.nbsSocketPath,
		}).Return(&nbs.TStopEndpointResponse{}, nil)

		expectedNfsClient.On("StopEndpoint", ctx, &nfs.TStopEndpointRequest{
			SocketPath: testContext.nfsSocketPath,
		}).Return(&nfs.TStopEndpointResponse{}, nil)
	}

	if !testContext.perInstanceVolumes {
		// Driver attempts to stop legacy endpoints only for legacy volumes.
		testContext.nbsClient.On("StopEndpoint", ctx, &nbs.TStopEndpointRequest{
			UnixSocketPath: filepath.Join(testContext.socketsDir, testContext.podId, testContext.diskId, nbsSocketName),
		}).Return(&nbs.TStopEndpointResponse{}, nil)

		expectedNfsClient.On("StopEndpoint", ctx, &nfs.TStopEndpointRequest{
			SocketPath: filepath.Join(testContext.socketsDir, testContext.podId, testContext.diskId, nfsSocketName),
		}).Return(&nfs.TStopEndpointResponse{}, nil)
	}
}

func expectNodeUnstageVolume(ctx context.Context, testContext *TestContext) {
	expectedNfsClient := testContext.nfsClient
	if len(testContext.localFsOverrides) != 0 {
		expectedNfsClient = testContext.nfsLocalClient
	}

	if testContext.instanceId != "" {
		if testContext.backend == "nbs" {
			testContext.nbsClient.On("StopEndpoint", ctx, &nbs.TStopEndpointRequest{
				UnixSocketPath: testContext.nbsSocketPath,
			}).Return(&nbs.TStopEndpointResponse{}, nil)
		}

		if testContext.backend == "nfs" {
			expectedNfsClient.On("StopEndpoint", ctx, &nfs.TStopEndpointRequest{
				SocketPath: testContext.nfsSocketPath,
			}).Return(&nfs.TStopEndpointResponse{}, nil)
		}
	}
}

func doTestPublishUnpublishVolumeForKubevirt(t *testing.T, testContext TestContext) {
	ctx := context.Background()

	nodeService := newNodeService(
		testContext.nodeId,
		testContext.clientId,
		testContext.vmMode,
		testContext.socketsDir,
		testContext.targetFsPathPattern,
		"", // targetBlkPathPattern
		testContext.localFsOverrides,
		testContext.nbsClient,
		testContext.nfsClient,
		testContext.nfsLocalClient,
		testContext.mounter,
	)

	expectSuccessNodeStageVolume(ctx, &testContext)
	_, err := nodeService.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
		VolumeId:          testContext.diskId,
		StagingTargetPath: testContext.stagingTargetPath,
		VolumeCapability:  &testContext.volumeCapability,
		VolumeContext:     testContext.volumeContext,
	})
	require.NoError(t, err)

	expectSuccessNodePublishVolume(ctx, &testContext)
	_, err = nodeService.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:          testContext.diskId,
		StagingTargetPath: testContext.stagingTargetPath,
		TargetPath:        testContext.targetPath,
		VolumeCapability:  &testContext.volumeCapability,
		VolumeContext:     testContext.volumeContext,
	})
	require.NoError(t, err)

	fileInfo, err := os.Stat(testContext.sourcePath)
	assert.False(t, os.IsNotExist(err))
	assert.True(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0755), fileInfo.Mode().Perm())

	fileInfo, err = os.Stat(filepath.Join(testContext.sourcePath, "disk.img"))
	assert.False(t, os.IsNotExist(err))
	assert.False(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0644), fileInfo.Mode().Perm())

	fileInfo, err = os.Stat(testContext.targetPath)
	assert.False(t, os.IsNotExist(err))
	assert.True(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0775), fileInfo.Mode().Perm())

	expectNodeUnpublishVolume(ctx, &testContext)
	_, err = nodeService.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   testContext.diskId,
		TargetPath: testContext.targetPath,
	})
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(testContext.socketsDir, testContext.podId))
	assert.True(t, os.IsNotExist(err))

	expectNodeUnstageVolume(ctx, &testContext)
	_, err = nodeService.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{
		VolumeId:          testContext.diskId,
		StagingTargetPath: testContext.stagingTargetPath,
	})
	require.NoError(t, err)
}

func TestPublishUnpublishDiskForKubevirt(t *testing.T) {
	testContextBuilder := CreateTestContextBuilder(t.TempDir(), "nbs", true)
	doTestPublishUnpublishVolumeForKubevirt(t, testContextBuilder.Build())
}

func TestPublishUnpublishDiskForKubevirtSetDeviceName(t *testing.T) {
	testContextBuilder := CreateTestContextBuilder(t.TempDir(), "nbs", true)
	testContextBuilder.WithDeviceName()
	doTestPublishUnpublishVolumeForKubevirt(t, testContextBuilder.Build())
}

func TestPublishUnpublishFilestoreForKubevirt(t *testing.T) {
	testContextBuilder := CreateTestContextBuilder(t.TempDir(), "nfs", true)
	doTestPublishUnpublishVolumeForKubevirt(t, testContextBuilder.Build())
}

func TestPublishUnpublishLocalFilestoreForKubevirt(t *testing.T) {
	testContextBuilder := CreateTestContextBuilder(t.TempDir(), "nfs", true)
	testContextBuilder.WithLocalFsOverride()
	doTestPublishUnpublishVolumeForKubevirt(t, testContextBuilder.Build())
}

func doTestStagedPublishUnpublishVolumeForKubevirt(t *testing.T, testContext TestContext) {
	ctx := context.Background()

	nodeService := newNodeService(
		testContext.nodeId,
		testContext.clientId,
		testContext.vmMode,
		testContext.socketsDir,
		testContext.targetFsPathPattern,
		"", // targetBlkPathPattern
		testContext.localFsOverrides,
		testContext.nbsClient,
		testContext.nfsClient,
		testContext.nfsLocalClient,
		testContext.mounter,
	)

	expectSuccessNodeStageVolume(ctx, &testContext)
	_, err := nodeService.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
		VolumeId:          testContext.volumeId,
		StagingTargetPath: testContext.stagingTargetPath,
		VolumeCapability:  &testContext.volumeCapability,
		VolumeContext:     testContext.volumeContext,
	})
	require.NoError(t, err)

	fileInfo, err := os.Stat(testContext.sourcePath)
	assert.False(t, os.IsNotExist(err))
	assert.True(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0755), fileInfo.Mode().Perm())

	fileInfo, err = os.Stat(filepath.Join(testContext.sourcePath, "disk.img"))
	assert.False(t, os.IsNotExist(err))
	assert.False(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0644), fileInfo.Mode().Perm())

	expectSuccessNodePublishVolume(ctx, &testContext)
	_, err = nodeService.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:          testContext.volumeId,
		StagingTargetPath: testContext.stagingTargetPath,
		TargetPath:        testContext.targetPath,
		VolumeCapability:  &testContext.volumeCapability,
		VolumeContext:     testContext.volumeContext,
	})
	require.NoError(t, err)

	fileInfo, err = os.Stat(testContext.targetPath)
	assert.False(t, os.IsNotExist(err))
	assert.True(t, fileInfo.IsDir())
	assert.Equal(t, fs.FileMode(0775), fileInfo.Mode().Perm())

	expectNodeUnpublishVolume(ctx, &testContext)
	_, err = nodeService.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   testContext.volumeId,
		TargetPath: testContext.targetPath,
	})
	require.NoError(t, err)

	expectNodeUnstageVolume(ctx, &testContext)
	_, err = nodeService.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{
		VolumeId:          testContext.volumeId,
		StagingTargetPath: testContext.stagingTargetPath,
	})
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(testContext.socketsDir, testContext.instanceId))
	assert.True(t, os.IsNotExist(err))
}

func TestStagedPublishUnpublishDiskForKubevirtLegacy(t *testing.T) {
	testContextBuilder := CreateTestContextBuilder(t.TempDir(), "nbs", true)
	testContextBuilder.WithInstanceId()
	doTestStagedPublishUnpublishVolumeForKubevirt(t, testContextBuilder.Build())
}

func TestStagedPublishUnpublishDiskForKubevirtSetDeviceNameLegacy(t *testing.T) {
	testContextBuilder := CreateTestContextBuilder(t.TempDir(), "nbs", true)
	testContextBuilder.WithInstanceId()
	testContextBuilder.WithDeviceName()
	doTestStagedPublishUnpublishVolumeForKubevirt(t, testContextBuilder.Build())
}

func TestStagedPublishUnpublishFilestoreForKubevirtLegacy(t *testing.T) {
	testContextBuilder := CreateTestContextBuilder(t.TempDir(), "nfs", true)
	testContextBuilder.WithInstanceId()
	doTestStagedPublishUnpublishVolumeForKubevirt(t, testContextBuilder.Build())
}

func TestStagedPublishUnpublishLocalFilestoreForKubevirtLegacy(t *testing.T) {
	testContextBuilder := CreateTestContextBuilder(t.TempDir(), "nfs", true)
	testContextBuilder.WithInstanceId()
	doTestStagedPublishUnpublishVolumeForKubevirt(t, testContextBuilder.Build())
}

func TestStagedPublishUnpublishDiskForKubevirt(t *testing.T) {
	testContextBuilder := CreateTestContextBuilder(t.TempDir(), "nbs", true)
	testContextBuilder.WithInstanceId()
	testContextBuilder.WithPerInstanceVolumes()
	doTestStagedPublishUnpublishVolumeForKubevirt(t, testContextBuilder.Build())
}

func TestStagedPublishUnpublishDiskForKubevirtSetDeviceName(t *testing.T) {
	testContextBuilder := CreateTestContextBuilder(t.TempDir(), "nbs", true)
	testContextBuilder.WithInstanceId()
	testContextBuilder.WithDeviceName()
	testContextBuilder.WithPerInstanceVolumes()
	doTestStagedPublishUnpublishVolumeForKubevirt(t, testContextBuilder.Build())
}

func TestStagedPublishUnpublishFilestoreForKubevirt(t *testing.T) {
	testContextBuilder := CreateTestContextBuilder(t.TempDir(), "nfs", true)
	testContextBuilder.WithInstanceId()
	testContextBuilder.WithPerInstanceVolumes()
	doTestStagedPublishUnpublishVolumeForKubevirt(t, testContextBuilder.Build())
}

func TestStagedPublishUnpublishLocalFilestoreForKubevirt(t *testing.T) {
	testContextBuilder := CreateTestContextBuilder(t.TempDir(), "nfs", true)
	testContextBuilder.WithInstanceId()
	testContextBuilder.WithLocalFsOverride()
	testContextBuilder.WithPerInstanceVolumes()
	doTestStagedPublishUnpublishVolumeForKubevirt(t, testContextBuilder.Build())
}

func TestPublishUnpublishDiskForInfrakuber(t *testing.T) {
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
	localFsOverrides := make(LocalFilestoreOverrideMap)

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
		mounter,
	)

	volumeCapability := csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{},
		AccessMode: &csi.VolumeCapability_AccessMode{
			Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		},
	}

	volumeContext := map[string]string{}

	hostType := nbs.EHostType_HOST_TYPE_DEFAULT
	nbsClient.On("StartEndpoint", ctx, &nbs.TStartEndpointRequest{
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
		[]string{"errors=remount-ro"}).Return(nil)

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

	_, err = exec.Command("ls", "-ldn", targetPath).CombinedOutput()
	assert.False(t, os.IsNotExist(err))

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
	deprecatedSocketPath := filepath.Join(socketsDir, podId, diskId, "nbs.sock")
	localFsOverrides := make(LocalFilestoreOverrideMap)

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
		mounter,
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
			VolumeContext:     volumeContext,
		})
		require.Error(t, err)
		expectedError := "rpc error: code = Aborted desc = [n=testNodeId]: " +
			"Another operation with volume test-disk-id-42 is in progress"
		require.Equal(t, expectedError, err.Error())
	}

	hostType := nbs.EHostType_HOST_TYPE_DEFAULT
	nbsClient.On("StartEndpoint", ctx, &nbs.TStartEndpointRequest{
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

	nbsClient.On("StopEndpoint", ctx, &nbs.TStopEndpointRequest{
		UnixSocketPath: deprecatedSocketPath,
	}).Run(volumeOperationInProgress).Return(&nbs.TStopEndpointResponse{}, nil)

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
	localFsOverrides := make(LocalFilestoreOverrideMap)

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
		mounter,
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

	nbsClient.On("DescribeVolume", ctx, &nbs.TDescribeVolumeRequest{DiskId: diskId}).Return(&nbs.TDescribeVolumeResponse{}, nil)
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
	assert.Equal(t, bytesUsage.Used+bytesUsage.Available, bytesUsage.Total)

	nodesUsage := stat.GetUsage()[1]
	assert.Equal(t, nodesUsage.Unit, csi.VolumeUsage_INODES)
	assert.NotEqual(t, 0, nodesUsage.Total)
	assert.Equal(t, nodesUsage.Used+nodesUsage.Available, nodesUsage.Total)
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
	localFsOverrides := make(LocalFilestoreOverrideMap)

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
		mounter,
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
	localFsOverrides := make(LocalFilestoreOverrideMap)

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
		mounter,
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
}
