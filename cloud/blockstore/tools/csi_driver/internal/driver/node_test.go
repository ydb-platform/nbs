package driver

//lint:file-ignore ST1003 protobuf generates names that break golang naming convention

import (
	"context"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	nbs "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	"github.com/ydb-platform/nbs/cloud/blockstore/tools/csi_driver/internal/driver/mocks"
	csimounter "github.com/ydb-platform/nbs/cloud/blockstore/tools/csi_driver/internal/mounter"
	nfs "github.com/ydb-platform/nbs/cloud/filestore/public/api/protos"
)

////////////////////////////////////////////////////////////////////////////////

func doTestPublishUnpublishVolumeForKubevirt(t *testing.T, backend string, deviceNameOpt *string) {
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
		nbsClient,
		nfsClient,
		mounter,
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

	if backend == "nbs" {
		nbsClient.On("StartEndpoint", ctx, &nbs.TStartEndpointRequest{
			UnixSocketPath:   nbsSocketPath,
			DiskId:           diskId,
			InstanceId:       podId,
			ClientId:         actualClientId,
			DeviceName:       deviceName,
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

	if backend == "nfs" {
		nfsClient.On("StartEndpoint", ctx, &nfs.TStartEndpointRequest{
			Endpoint: &nfs.TEndpointConfig{
				SocketPath:       nfsSocketPath,
				FileSystemId:     diskId,
				ClientId:         actualClientId,
				VhostQueuesCount: 8,
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
}

func TestPublishUnpublishDiskForKubevirt(t *testing.T) {
	doTestPublishUnpublishVolumeForKubevirt(t, "nbs", nil)
}

func TestPublishUnpublishDiskForKubevirtSetDeviceName(t *testing.T) {
	deviceName := "test-disk-name-42"
	doTestPublishUnpublishVolumeForKubevirt(t, "nbs", &deviceName)
}

func TestPublishUnpublishFilestoreForKubevirt(t *testing.T) {
	doTestPublishUnpublishVolumeForKubevirt(t, "nfs", nil)
}

func doTestStagedPublishUnpublishVolumeForKubevirt(t *testing.T, backend string, deviceNameOpt *string, perInstanceVolumes bool) {
	tempDir := t.TempDir()

	nbsClient := mocks.NewNbsClientMock()
	nfsClient := mocks.NewNfsEndpointClientMock()
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

	nodeService := newNodeService(
		nodeId,
		clientId,
		true, // vmMode
		socketsDir,
		targetFsPathPattern,
		"", // targetBlkPathPattern
		nbsClient,
		nfsClient,
		mounter,
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

	hostType := nbs.EHostType_HOST_TYPE_DEFAULT
	if backend == "nbs" {
		nbsClient.On("StartEndpoint", ctx, &nbs.TStartEndpointRequest{
			UnixSocketPath:   nbsSocketPath,
			DiskId:           diskId,
			InstanceId:       instanceId,
			ClientId:         actualClientId,
			DeviceName:       deviceName,
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

	if backend == "nfs" {
		nfsClient.On("StartEndpoint", ctx, &nfs.TStartEndpointRequest{
			Endpoint: &nfs.TEndpointConfig{
				SocketPath:       nfsSocketPath,
				FileSystemId:     diskId,
				ClientId:         actualClientId,
				VhostQueuesCount: 8,
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

		nfsClient.On("StopEndpoint", ctx, &nfs.TStopEndpointRequest{
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
		nfsClient.On("StopEndpoint", ctx, &nfs.TStopEndpointRequest{
			SocketPath: nfsSocketPath,
		}).Return(&nfs.TStopEndpointResponse{}, nil)
	}

	_, err = nodeService.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{
		VolumeId:          volumeId,
		StagingTargetPath: stagingTargetPath,
	})
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(socketsDir, instanceId))
	assert.True(t, os.IsNotExist(err))
}

func TestStagedPublishUnpublishDiskForKubevirtLegacy(t *testing.T) {
	doTestStagedPublishUnpublishVolumeForKubevirt(t, "nbs", nil, false)
}

func TestStagedPublishUnpublishDiskForKubevirtSetDeviceNameLegacy(t *testing.T) {
	deviceName := "test-disk-name-42"
	doTestStagedPublishUnpublishVolumeForKubevirt(t, "nbs", &deviceName, false)
}

func TestStagedPublishUnpublishFilestoreForKubevirtLegacy(t *testing.T) {
	doTestStagedPublishUnpublishVolumeForKubevirt(t, "nfs", nil, false)
}

func TestStagedPublishUnpublishDiskForKubevirt(t *testing.T) {
	doTestStagedPublishUnpublishVolumeForKubevirt(t, "nbs", nil, true)
}

func TestStagedPublishUnpublishDiskForKubevirtSetDeviceName(t *testing.T) {
	deviceName := "test-disk-name-42"
	doTestStagedPublishUnpublishVolumeForKubevirt(t, "nbs", &deviceName, true)
}

func TestStagedPublishUnpublishFilestoreForKubevirt(t *testing.T) {
	doTestStagedPublishUnpublishVolumeForKubevirt(t, "nfs", nil, true)
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

	nodeService := newNodeService(
		nodeId,
		clientId,
		false, // vmMode
		socketsDir,
		targetFsPathPattern,
		"", // targetBlkPathPattern
		nbsClient,
		nil,
		mounter,
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

	mounter.On("IsFilesystemExisted", nbdDeviceFile).Return(false, nil)

	mounter.On("MakeFilesystem", nbdDeviceFile, "ext4").Return([]byte{}, nil)

	mockCallIsMountPoint := mounter.On("IsMountPoint", stagingTargetPath).Return(false, nil)

	mounter.On("Mount", nbdDeviceFile, stagingTargetPath, "ext4",
		[]string{"grpid", "errors=remount-ro"}).Return(nil)

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

	nodeService := newNodeService(
		nodeId,
		clientId,
		false, // vmMode
		socketsDir,
		"", // targetFsPathPattern
		targetBlkPathPattern,
		nbsClient,
		nil,
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

	mockCallIsMountPointStagingPath := mounter.On("IsMountPoint", stagingDevicePath).Return(true, nil)
	mockCallMount := mounter.On("Mount", nbdDeviceFile, stagingDevicePath, "", []string{"bind"}).Return(nil)

	_, err = nodeService.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
		VolumeId:          diskId,
		StagingTargetPath: stagingTargetPath,
		VolumeCapability:  &volumeCapability,
		VolumeContext:     volumeContext,
	})
	require.NoError(t, err)
	mockCallMount.Unset()

	mockCallIsMountPointTargetPath := mounter.On("IsMountPoint", targetPath).Return(false, nil)
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
	}).Return(&nbs.TStopEndpointResponse{}, nil)

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
	}).Return(&nbs.TStopEndpointResponse{}, nil)

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

	nodeService := newNodeService(
		"testNodeId",
		"testClientId",
		false,
		socketsDir,
		targetFsPathPattern,
		"",
		nbsClient,
		nil,
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

	nodeService := newNodeService(
		"testNodeId",
		clientId,
		true,
		socketsDir,
		"",
		targetBlkPathPattern,
		nbsClient,
		nil,
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

	nodeService := newNodeService(
		"testNodeId",
		clientId,
		false,
		socketsDir,
		"",
		targetBlkPathPattern,
		nbsClient,
		nil,
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
