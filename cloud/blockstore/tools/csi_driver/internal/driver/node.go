package driver

import (
	"context"
	"fmt"
	"io/fs"
	"log"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"

	"github.com/container-storage-interface/spec/lib/go/csi"
	nbsapi "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	nbsclient "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	"github.com/ydb-platform/nbs/cloud/blockstore/tools/csi_driver/internal/mounter"
	nfsapi "github.com/ydb-platform/nbs/cloud/filestore/public/api/protos"
	nfsclient "github.com/ydb-platform/nbs/cloud/filestore/public/sdk/go/client"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

////////////////////////////////////////////////////////////////////////////////

const NodeFsTargetPathPattern = "/var/lib/kubelet/pods/([a-z0-9-]+)/volumes/kubernetes.io~csi/([a-z0-9-]+)/mount"
const NodeBlkTargetPathPattern = "/var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/publish/([a-z0-9-]+)/([a-z0-9-]+)"

const topologyNodeKey = "topology.nbs.csi/node"

const nbsSocketName = "nbs.sock"
const nfsSocketName = "nfs.sock"

const vhostIpc = nbsapi.EClientIpcType_IPC_VHOST
const nbdIpc = nbsapi.EClientIpcType_IPC_NBD

const backendVolumeContextKey = "backend"
const deviceNameVolumeContextKey = "deviceName"

var vmModeCapabilities = []*csi.NodeServiceCapability{
	{
		Type: &csi.NodeServiceCapability_Rpc{
			Rpc: &csi.NodeServiceCapability_RPC{
				Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
			},
		},
	},
	{
		Type: &csi.NodeServiceCapability_Rpc{
			Rpc: &csi.NodeServiceCapability_RPC{
				Type: csi.NodeServiceCapability_RPC_VOLUME_MOUNT_GROUP,
			},
		},
	},
}

// CSI driver provides RPC_GET_VOLUME_STATS capability only in podMode
// when volume is mounted to the pod as a local directory.
var podModeCapabilities = []*csi.NodeServiceCapability{
	{
		Type: &csi.NodeServiceCapability_Rpc{
			Rpc: &csi.NodeServiceCapability_RPC{
				Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
			},
		},
	},
	{
		Type: &csi.NodeServiceCapability_Rpc{
			Rpc: &csi.NodeServiceCapability_RPC{
				Type: csi.NodeServiceCapability_RPC_VOLUME_MOUNT_GROUP,
			},
		},
	},
	{
		Type: &csi.NodeServiceCapability_Rpc{
			Rpc: &csi.NodeServiceCapability_RPC{
				Type: csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
			},
		},
	},
	{
		Type: &csi.NodeServiceCapability_Rpc{
			Rpc: &csi.NodeServiceCapability_RPC{
				Type: csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
			},
		},
	},
}

////////////////////////////////////////////////////////////////////////////////

type nodeService struct {
	csi.NodeServer

	nodeID              string
	clientID            string
	vmMode              bool
	socketsDir          string
	targetFsPathRegexp  *regexp.Regexp
	targetBlkPathRegexp *regexp.Regexp

	nbsClient nbsclient.ClientIface
	nfsClient nfsclient.EndpointClientIface
	mounter   mounter.Interface
}

func newNodeService(
	nodeID string,
	clientID string,
	vmMode bool,
	socketsDir string,
	targetFsPathPattern string,
	targetBlkPathPattern string,
	nbsClient nbsclient.ClientIface,
	nfsClient nfsclient.EndpointClientIface,
	mounter mounter.Interface) csi.NodeServer {

	return &nodeService{
		nodeID:              nodeID,
		clientID:            clientID,
		vmMode:              vmMode,
		socketsDir:          socketsDir,
		nbsClient:           nbsClient,
		nfsClient:           nfsClient,
		mounter:             mounter,
		targetFsPathRegexp:  regexp.MustCompile(targetFsPathPattern),
		targetBlkPathRegexp: regexp.MustCompile(targetBlkPathPattern),
	}
}

func (s *nodeService) NodeStageVolume(
	ctx context.Context,
	req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {

	log.Printf("csi.NodeStageVolumeRequest: %+v", req)

	if req.VolumeId == "" {
		return nil, s.statusError(
			codes.InvalidArgument,
			"VolumeId is missing in NodeStageVolumeRequest")
	}
	if req.StagingTargetPath == "" {
		return nil, s.statusError(
			codes.InvalidArgument,
			"StagingTargetPath is missing in NodeStageVolumeRequest")
	}
	if req.VolumeCapability == nil {
		return nil, s.statusError(
			codes.InvalidArgument,
			"VolumeCapability is missing in NodeStageVolumeRequest")
	}

	return &csi.NodeStageVolumeResponse{}, nil
}

func (s *nodeService) NodeUnstageVolume(
	ctx context.Context,
	req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {

	log.Printf("csi.NodeUnstageVolumeRequest: %+v", req)

	if req.VolumeId == "" {
		return nil, s.statusError(
			codes.InvalidArgument,
			"VolumeId is missing in NodeUnstageVolumeRequest")
	}
	if req.StagingTargetPath == "" {
		return nil, s.statusError(
			codes.InvalidArgument,
			"StagingTargetPath is missing in NodeUnstageVolumeRequest")
	}

	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (s *nodeService) NodePublishVolume(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {

	log.Printf("csi.NodePublishVolumeRequest: %+v", req)

	if req.VolumeId == "" {
		return nil, s.statusError(
			codes.InvalidArgument,
			"VolumeId missing in NodePublishVolumeRequest")
	}
	if req.StagingTargetPath == "" {
		return nil, s.statusError(
			codes.InvalidArgument,
			"StagingTargetPath is missing in NodePublishVolumeRequest")
	}
	if req.TargetPath == "" {
		return nil, s.statusError(
			codes.InvalidArgument,
			"TargetPath is missing in NodePublishVolumeRequest")
	}
	if req.VolumeCapability == nil {
		return nil, s.statusError(
			codes.InvalidArgument,
			"VolumeCapability is missing in NodePublishVolumeRequest")
	}
	if req.VolumeContext == nil {
		return nil, s.statusError(
			codes.InvalidArgument,
			"VolumeContext is missing in NodePublishVolumeRequest")
	}

	if s.getPodId(req) == "" {
		return nil, s.statusError(codes.Internal,
			"podUID is missing in NodePublishVolumeRequest.VolumeContext")
	}

	var err error
	nfsBackend := (req.VolumeContext[backendVolumeContextKey] == "nfs")

	switch req.VolumeCapability.GetAccessType().(type) {
	case *csi.VolumeCapability_Mount:
		if s.vmMode {
			if nfsBackend {
				err = s.nodePublishFileStoreAsVhostSocket(ctx, req)
			} else {
				err = s.nodePublishDiskAsVhostSocket(ctx, req)
			}
		} else {
			if nfsBackend {
				return nil, s.statusError(codes.InvalidArgument,
					"FileStore can't be mounted to container as a filesystem")
			} else {
				err = s.nodePublishDiskAsFilesystem(ctx, req)
			}
		}
	case *csi.VolumeCapability_Block:
		if nfsBackend {
			return nil, s.statusError(codes.InvalidArgument,
				"'Block' volume mode is not supported for nfs backend")
		} else {
			err = s.nodePublishDiskAsBlockDevice(ctx, req)
		}
	default:
		return nil, s.statusError(codes.InvalidArgument, "Unknown access type")
	}

	if err != nil {
		return nil, s.statusErrorf(codes.Internal,
			"Failed to publish volume: %v", err)
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

func (s *nodeService) NodeUnpublishVolume(
	ctx context.Context,
	req *csi.NodeUnpublishVolumeRequest,
) (*csi.NodeUnpublishVolumeResponse, error) {

	log.Printf("csi.NodeUnpublishVolumeRequest: %+v", req)

	if req.VolumeId == "" {
		return nil, s.statusError(
			codes.InvalidArgument,
			"Volume ID is missing in NodeUnpublishVolumeRequest")
	}
	if req.TargetPath == "" {
		return nil, s.statusError(
			codes.InvalidArgument,
			"Target Path is missing in NodeUnpublishVolumeRequest")
	}

	if err := s.nodeUnpublishVolume(ctx, req); err != nil {
		return nil, s.statusErrorf(
			codes.InvalidArgument,
			"Failed to unpublish volume: %v", err)
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (s *nodeService) NodeGetCapabilities(
	ctx context.Context,
	req *csi.NodeGetCapabilitiesRequest,
) (*csi.NodeGetCapabilitiesResponse, error) {

	if s.vmMode {
		return &csi.NodeGetCapabilitiesResponse{
			Capabilities: vmModeCapabilities,
		}, nil
	}

	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: podModeCapabilities,
	}, nil
}

func (s *nodeService) NodeGetInfo(
	ctx context.Context,
	req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {

	return &csi.NodeGetInfoResponse{
		NodeId: s.nodeID,
		AccessibleTopology: &csi.Topology{
			Segments: map[string]string{topologyNodeKey: s.nodeID},
		},
	}, nil
}

func (s *nodeService) nodePublishDiskAsVhostSocket(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest) error {

	_, err := s.startNbsEndpoint(ctx, s.getPodId(req), req.VolumeId, req.VolumeContext, vhostIpc)
	if err != nil {
		return fmt.Errorf("failed to start NBS endpoint: %w", err)
	}

	return s.mountSocketDir(req)
}

func (s *nodeService) nodePublishDiskAsFilesystem(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest) error {

	resp, err := s.startNbsEndpoint(ctx, s.getPodId(req), req.VolumeId, req.VolumeContext, nbdIpc)
	if err != nil {
		return fmt.Errorf("failed to start NBS endpoint: %w", err)
	}

	if resp.NbdDeviceFile == "" {
		return fmt.Errorf("NbdDeviceFile shouldn't be empty")
	}

	logVolume(req.VolumeId, "endpoint started with device: %q", resp.NbdDeviceFile)

	mnt := req.VolumeCapability.GetMount()

	fsType := req.VolumeContext["fsType"]
	if mnt != nil && mnt.FsType != "" {
		fsType = mnt.FsType
	}
	if fsType == "" {
		fsType = "ext4"
	}

	err = s.makeFilesystemIfNeeded(req.VolumeId, resp.NbdDeviceFile, fsType)
	if err != nil {
		return err
	}

	targetPerm := os.FileMode(0775)
	if err := os.MkdirAll(req.TargetPath, targetPerm); err != nil {
		return fmt.Errorf("failed to create target directory: %w", err)
	}

	mountOptions := []string{}
	if mnt != nil {
		for _, flag := range mnt.MountFlags {
			mountOptions = append(mountOptions, flag)
		}
	}

	err = s.mountIfNeeded(
		req.VolumeId,
		resp.NbdDeviceFile,
		req.TargetPath,
		fsType,
		mountOptions)
	if err != nil {
		return err
	}

	if mnt.VolumeMountGroup != "" {
		cmd := exec.Command("chown", "-R", ":"+mnt.VolumeMountGroup, req.TargetPath)
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("failed to chown %s to %q: %w, output %q",
				mnt.VolumeMountGroup, req.TargetPath, err, out)
		}
	}

	if err := os.Chmod(req.TargetPath, targetPerm); err != nil {
		return fmt.Errorf("failed to chmod target path: %w", err)
	}

	return nil
}

func (s *nodeService) nodePublishDiskAsBlockDevice(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest) error {

	resp, err := s.startNbsEndpoint(ctx, s.getPodId(req), req.VolumeId, req.VolumeContext, nbdIpc)
	if err != nil {
		return fmt.Errorf("failed to start NBS endpoint: %w", err)
	}

	if resp.NbdDeviceFile == "" {
		return fmt.Errorf("NbdDeviceFile shouldn't be empty")
	}

	logVolume(req.VolumeId, "endpoint started with device: %q", resp.NbdDeviceFile)
	return s.mountBlockDevice(req.VolumeId, resp.NbdDeviceFile, req.TargetPath)
}

func (s *nodeService) startNbsEndpoint(
	ctx context.Context,
	podId string,
	volumeId string,
	volumeContext map[string]string,
	ipcType nbsapi.EClientIpcType) (*nbsapi.TStartEndpointResponse, error) {

	endpointDir := filepath.Join(s.socketsDir, podId, volumeId)
	if err := os.MkdirAll(endpointDir, os.FileMode(0755)); err != nil {
		return nil, err
	}

	deviceName, found := volumeContext[deviceNameVolumeContextKey]
	if !found {
		deviceName = volumeId
	}

	hostType := nbsapi.EHostType_HOST_TYPE_DEFAULT
	return s.nbsClient.StartEndpoint(ctx, &nbsapi.TStartEndpointRequest{
		UnixSocketPath:   filepath.Join(endpointDir, nbsSocketName),
		DiskId:           volumeId,
		InstanceId:       podId,
		ClientId:         fmt.Sprintf("%s-%s", s.clientID, podId),
		DeviceName:       deviceName,
		IpcType:          ipcType,
		VhostQueuesCount: 8,
		VolumeAccessMode: nbsapi.EVolumeAccessMode_VOLUME_ACCESS_READ_WRITE,
		VolumeMountMode:  nbsapi.EVolumeMountMode_VOLUME_MOUNT_LOCAL,
		Persistent:       true,
		NbdDevice: &nbsapi.TStartEndpointRequest_UseFreeNbdDeviceFile{
			ipcType == nbdIpc,
		},
		ClientProfile: &nbsapi.TClientProfile{
			HostType: &hostType,
		},
	})
}

func (s *nodeService) nodePublishFileStoreAsVhostSocket(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest) error {

	endpointDir := filepath.Join(s.socketsDir, s.getPodId(req), req.VolumeId)
	if err := os.MkdirAll(endpointDir, os.FileMode(0755)); err != nil {
		return err
	}

	if s.nfsClient == nil {
		return fmt.Errorf("NFS client wasn't created")
	}

	_, err := s.nfsClient.StartEndpoint(ctx, &nfsapi.TStartEndpointRequest{
		Endpoint: &nfsapi.TEndpointConfig{
			SocketPath:       filepath.Join(endpointDir, nfsSocketName),
			FileSystemId:     req.VolumeId,
			ClientId:         fmt.Sprintf("%s-%s", s.clientID, s.getPodId(req)),
			VhostQueuesCount: 8,
			Persistent:       true,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to start NFS endpoint: %w", err)
	}

	return s.mountSocketDir(req)
}

func (s *nodeService) nodeUnpublishVolume(
	ctx context.Context,
	req *csi.NodeUnpublishVolumeRequest) error {

	if err := s.mounter.CleanupMountPoint(req.TargetPath); err != nil {
		return err
	}

	// no other way to get podId from NodeUnpublishVolumeRequest
	podId, err := s.parsePodId(req.TargetPath)
	if err != nil {
		return err
	}

	socketDir := filepath.Join(s.socketsDir, podId, req.VolumeId)

	// Trying to stop both NBS and NFS endpoints,
	// because the endpoint's backend service is unknown here.
	// When we miss we get S_FALSE/S_ALREADY code (err == nil).

	if s.nbsClient != nil {
		_, err := s.nbsClient.StopEndpoint(ctx, &nbsapi.TStopEndpointRequest{
			UnixSocketPath: filepath.Join(socketDir, nbsSocketName),
		})
		if err != nil {
			return fmt.Errorf("failed to stop nbs endpoint: %w", err)
		}
	}

	if s.nfsClient != nil {
		_, err := s.nfsClient.StopEndpoint(ctx, &nfsapi.TStopEndpointRequest{
			SocketPath: filepath.Join(socketDir, nfsSocketName),
		})
		if err != nil {
			return fmt.Errorf("failed to stop nfs endpoint: %w", err)
		}
	}

	if err := os.RemoveAll(socketDir); err != nil {
		return err
	}

	// remove pod's folder if it's empty
	os.Remove(filepath.Join(s.socketsDir, podId))
	return nil
}

func (s *nodeService) mountSocketDir(req *csi.NodePublishVolumeRequest) error {

	endpointDir := filepath.Join(s.socketsDir, s.getPodId(req), req.VolumeId)

	// https://kubevirt.io/user-guide/virtual_machines/disks_and_volumes/#persistentvolumeclaim
	// "If the disk.img image file has not been created manually before starting a VM
	// then it will be created automatically with the PersistentVolumeClaim size."
	// So, let's create an empty disk.img to avoid automatic creation and save disk space.
	diskImgPath := filepath.Join(endpointDir, "disk.img")
	file, err := os.OpenFile(diskImgPath, os.O_CREATE, os.FileMode(0644))
	if err != nil {
		return fmt.Errorf("failed to create disk.img: %w", err)
	}
	file.Close()

	targetPerm := os.FileMode(0775)
	if err := os.MkdirAll(req.TargetPath, targetPerm); err != nil {
		return fmt.Errorf("failed to create target directory: %w", err)
	}

	mountOptions := []string{"bind"}
	mnt := req.VolumeCapability.GetMount()
	if mnt != nil {
		for _, flag := range mnt.MountFlags {
			mountOptions = append(mountOptions, flag)
		}
	}
	err = s.mountIfNeeded(
		req.VolumeId,
		endpointDir,
		req.TargetPath,
		"",
		mountOptions)
	if err != nil {
		return fmt.Errorf("failed to mount: %w", err)
	}

	if err := os.Chmod(req.TargetPath, targetPerm); err != nil {
		return fmt.Errorf("failed to chmod target path: %w", err)
	}

	return nil
}

func (s *nodeService) mountBlockDevice(
	volumeId string,
	source string,
	target string) error {

	if err := os.MkdirAll(filepath.Dir(target), os.FileMode(0750)); err != nil {
		return fmt.Errorf("failed to create target directory: %w", err)
	}

	targetPerm := os.FileMode(0660)
	file, err := os.OpenFile(target, os.O_CREATE, targetPerm)
	if err != nil {
		return fmt.Errorf("failed to create target file: %w", err)
	}
	file.Close()

	mountOptions := []string{"bind"}
	err = s.mountIfNeeded(volumeId, source, target, "", mountOptions)
	if err != nil {
		return fmt.Errorf("failed to mount: %w", err)
	}

	if err := os.Chmod(target, targetPerm); err != nil {
		return fmt.Errorf("failed to chmod target path: %w", err)
	}

	return nil
}

func (s *nodeService) mountIfNeeded(
	volumeId string,
	source string,
	target string,
	fsType string,
	options []string) error {

	mounted, err := s.mounter.IsMountPoint(target)
	if err != nil {
		return err
	}

	if mounted {
		logVolume(volumeId, "target path %q is already mounted", target)
		return nil
	}

	logVolume(volumeId, "mount source %q to target %q, fsType: %q, options: %v",
		source, target, fsType, options)
	return s.mounter.Mount(source, target, fsType, options)
}

func (s *nodeService) makeFilesystemIfNeeded(
	volumeId string,
	deviceName string,
	fsType string) error {

	existed, err := s.mounter.IsFilesystemExisted(deviceName)
	if err != nil {
		return err
	}

	if existed {
		logVolume(volumeId, "filesystem exists on device: %q", deviceName)
		return nil
	}

	logVolume(volumeId, "making filesystem %q on device %q", fsType, deviceName)
	out, err := s.mounter.MakeFilesystem(deviceName, fsType)
	if err != nil {
		return fmt.Errorf("failed to make filesystem: %w, output %q", err, out)
	}

	logVolume(volumeId, "succeeded making filesystem: %q", out)
	return nil
}

func (s *nodeService) getPodId(req *csi.NodePublishVolumeRequest) string {
	// another way to get podId is: return req.VolumeContext["csi.storage.k8s.io/pod.uid"]

	switch req.VolumeCapability.GetAccessType().(type) {
	case *csi.VolumeCapability_Mount:
		podId, _, err := s.parseFsTargetPath(req.TargetPath)
		if err != nil {
			return ""
		}
		return podId
	case *csi.VolumeCapability_Block:
		podId, _, err := s.parseBlkTargetPath(req.TargetPath)
		if err != nil {
			return ""
		}
		return podId
	}

	return ""
}

func (s *nodeService) parseFsTargetPath(targetPath string) (string, string, error) {
	matches := s.targetFsPathRegexp.FindStringSubmatch(targetPath)

	if len(matches) <= 2 {
		return "", "", fmt.Errorf("failed to parse TargetPath: %q", targetPath)
	}

	podID := matches[1]
	pvcID := matches[2]
	return podID, pvcID, nil
}

func (s *nodeService) parseBlkTargetPath(targetPath string) (string, string, error) {
	matches := s.targetBlkPathRegexp.FindStringSubmatch(targetPath)

	if len(matches) <= 2 {
		return "", "", fmt.Errorf("failed to parse TargetPath: %q", targetPath)
	}

	pvcID := matches[1]
	podID := matches[2]
	return podID, pvcID, nil
}

func (s *nodeService) isMountAccessType(targetPath string) bool {
	_, _, err := s.parseFsTargetPath(targetPath)
	return err == nil
}

func (s *nodeService) parsePodId(targetPath string) (string, error) {
	var err error = nil
	podId, _, err := s.parseFsTargetPath(targetPath)
	if err != nil {
		podId, _, err = s.parseBlkTargetPath(targetPath)
	}
	return podId, err
}

func (s *nodeService) statusError(c codes.Code, msg string) error {
	return status.Error(c, fmt.Sprintf("[n=%s]: %s", s.nodeID, msg))
}

func (s *nodeService) statusErrorf(c codes.Code, format string, a ...interface{}) error {
	msg := fmt.Sprintf(format, a...)
	return s.statusError(c, msg)
}

func logVolume(volumeId string, format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	log.Printf("[v=%s]: %s", volumeId, msg)
}

func (s *nodeService) NodeGetVolumeStats(
	ctx context.Context,
	req *csi.NodeGetVolumeStatsRequest) (
	*csi.NodeGetVolumeStatsResponse, error) {

	log.Printf("csi.NodeGetVolumeStats: %+v", req)

	if req.VolumeId == "" {
		return nil, s.statusError(
			codes.InvalidArgument,
			"VolumeId is missing in NodeGetVolumeStatsRequest")
	}

	if req.VolumePath == "" {
		return nil, s.statusError(
			codes.InvalidArgument,
			"VolumePath is missing in NodeGetVolumeStatsRequest")
	}

	if s.vmMode {
		return nil, fmt.Errorf("NodeGetVolumeStats is not supported in vmMode")
	}

	if s.nbsClient == nil {
		return nil, fmt.Errorf("NBS client is not available")
	}

	mounted, err := s.mounter.IsMountPoint(req.VolumePath)
	if err != nil {
		if err == fs.ErrNotExist {
			return nil, s.statusError(
				codes.NotFound,
				"Mount point does not exist")
		}

		return nil, s.statusErrorf(
			codes.Internal,
			"NodeGetVolumeStats failed: %w", err)
	}

	if !mounted {
		return nil, s.statusError(
			codes.NotFound,
			"Volume does not exist on the specified path")
	}

	var stat unix.Statfs_t
	err = unix.Statfs(req.VolumePath, &stat)
	if err != nil {
		return nil, err
	}

	totalBytes := int64(stat.Blocks) * int64(stat.Bsize)
	availableBytes := int64(stat.Bavail) * int64(stat.Bsize)
	usedBytes := totalBytes - availableBytes

	totalNodes := int64(stat.Files)
	availableNodes := int64(stat.Ffree)
	usedNodes := totalNodes - availableNodes

	return &csi.NodeGetVolumeStatsResponse{Usage: []*csi.VolumeUsage{
		{
			Available: availableBytes,
			Used:      usedBytes,
			Total:     totalBytes,
			Unit:      csi.VolumeUsage_BYTES,
		},
		{
			Available: availableNodes,
			Used:      usedNodes,
			Total:     totalNodes,
			Unit:      csi.VolumeUsage_INODES,
		},
	}}, nil
}

func (s *nodeService) NodeExpandVolume(
	ctx context.Context,
	req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {

	log.Printf("csi.NodeExpandVolume: %+v", req)

	if req.VolumeId == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"VolumeId is missing in NodeExpandVolumeRequest")
	}

	if req.VolumePath == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"VolumePath is missing in NodeExpandVolumeRequest")
	}

	if s.nbsClient == nil {
		return nil, fmt.Errorf("NodeExpandVolume is not supported")
	}

	resp, err := s.nbsClient.DescribeVolume(
		ctx, &nbsapi.TDescribeVolumeRequest{
			DiskId: req.VolumeId,
		},
	)

	if err != nil {
		if nbsclient.IsDiskNotFoundError(err) {
			return nil, s.statusError(
				codes.NotFound,
				"Volume is not found")
		}
		return nil, s.statusErrorf(
			codes.Internal,
			"Failed to expand volume: %v", err)
	}

	if req.CapacityRange == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"CapacityRange is missing in NodeExpandVolumeRequest")
	}

	if resp.Volume.BlockSize == 0 {
		return nil, status.Error(
			codes.Internal,
			"Invalid block size")
	}

	newBlocksCount := uint64(math.Ceil(
		float64(req.CapacityRange.RequiredBytes) / float64(resp.Volume.BlockSize)),
	)
	if newBlocksCount <= resp.Volume.BlocksCount {
		return nil, status.Error(
			codes.InvalidArgument,
			"New blocks count is less than or equal to current blocks count value")
	}

	podId, err := s.parsePodId(req.VolumePath)
	if err != nil {
		return nil, err
	}

	endpointDir := filepath.Join(s.socketsDir, podId, req.VolumeId)
	unixSocketPath := filepath.Join(endpointDir, nbsSocketName)

	listEndpointsResp, err := s.nbsClient.ListEndpoints(
		ctx, &nbsapi.TListEndpointsRequest{},
	)
	if err != nil {
		log.Printf("List endpoints failed %v", err)
		return nil, err
	}

	nbdDevicePath := ""
	for _, endpoint := range listEndpointsResp.Endpoints {
		if endpoint.UnixSocketPath == unixSocketPath {
			nbdDevicePath = endpoint.GetNbdDeviceFile()
			break
		}
	}

	if nbdDevicePath == "" {
		return nil, status.Error(
			codes.Internal,
			"Failed to determine NBD Device filename")
	}

	log.Printf("Resize volume id %v blocks count %v", req.VolumeId, newBlocksCount)
	_, err = s.nbsClient.ResizeVolume(ctx, &nbsapi.TResizeVolumeRequest{
		DiskId:             req.VolumeId,
		BlocksCount:        newBlocksCount,
		ConfigVersion:      resp.Volume.ConfigVersion,
		PerformanceProfile: resp.Volume.PerformanceProfile,
	})

	if err != nil {
		log.Printf("Resize volume failed %v", err)
		return nil, err
	}

	_, err = s.nbsClient.ResizeDevice(ctx, &nbsapi.TResizeDeviceRequest{
		UnixSocketPath:    unixSocketPath,
		DeviceSizeInBytes: newBlocksCount * uint64(resp.Volume.BlockSize),
	})

	if err != nil {
		log.Printf("Resize device failed %v", err)
		return nil, s.statusErrorf(
			codes.Internal,
			"Failed to resize device %v", err)
	}

	if s.isMountAccessType(req.VolumePath) {
		cmd := exec.Command("resize2fs", nbdDevicePath)
		if out, err := cmd.CombinedOutput(); err != nil {
			return nil, s.statusErrorf(
				codes.Internal,
				"Failed to resize filesystem %v, output %s",
				err, out)
		}
	}

	return &csi.NodeExpandVolumeResponse{
		CapacityBytes: int64(newBlocksCount * uint64(resp.Volume.BlockSize)),
	}, nil
}
