package driver

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	nbsapi "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	nbsclient "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

////////////////////////////////////////////////////////////////////////////////

const topologyKeyNode = "topology.nbs.csi/node"

const socketName = "nbs-volume.sock"

var capabilities = []*csi.NodeServiceCapability{
	&csi.NodeServiceCapability{
		Type: &csi.NodeServiceCapability_Rpc{
			Rpc: &csi.NodeServiceCapability_RPC{
				Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
			},
		},
	},
}

////////////////////////////////////////////////////////////////////////////////

type nodeService struct {
	csi.NodeServer

	nodeID        string
	clientID      string
	nbsSocketsDir string
	podSocketsDir string
	nbsClient     nbsclient.ClientIface
}

func newNodeService(
	nodeID string,
	clientID string,
	nbsSocketsDir string,
	podSocketsDir string,
	nbsClient nbsclient.ClientIface) csi.NodeServer {

	return &nodeService{
		nodeID:        nodeID,
		clientID:      clientID,
		nbsSocketsDir: nbsSocketsDir,
		podSocketsDir: podSocketsDir,
		nbsClient:     nbsClient,
	}
}

func (s *nodeService) NodeStageVolume(
	ctx context.Context,
	req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {

	log.Printf("csi.NodeStageVolumeRequest: %+v", req)

	if req.VolumeId == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"VolumeId missing in NodeStageVolumeRequest")
	}
	if req.StagingTargetPath == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"StagingTargetPath missing in NodeStageVolumeRequest")
	}
	if req.VolumeCapability == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"VolumeCapability missing im NodeStageVolumeRequest")
	}

	var ipcType nbsapi.EClientIpcType
	switch req.VolumeCapability.GetAccessType().(type) {
	case *csi.VolumeCapability_Block:
		ipcType = nbsapi.EClientIpcType_IPC_NBD
	case *csi.VolumeCapability_Mount:
		ipcType = nbsapi.EClientIpcType_IPC_VHOST
	default:
		return nil, status.Error(codes.InvalidArgument, "Unknown access type")
	}

	endpointDir := filepath.Join(s.podSocketsDir, req.VolumeId)
	if err := os.MkdirAll(endpointDir, 0755); err != nil {
		return nil, err
	}

	if err := os.Chmod(endpointDir, 0777); err != nil {
		return nil, err
	}

	hostType := nbsapi.EHostType_HOST_TYPE_DEFAULT
	socketPath := filepath.Join(s.nbsSocketsDir, req.VolumeId, socketName)
	startEndpointRequest := &nbsapi.TStartEndpointRequest{
		UnixSocketPath:   socketPath,
		DiskId:           req.VolumeId,
		ClientId:         s.clientID,
		DeviceName:       req.VolumeId,
		IpcType:          ipcType,
		VhostQueuesCount: 8,
		VolumeAccessMode: nbsapi.EVolumeAccessMode_VOLUME_ACCESS_READ_WRITE,
		VolumeMountMode:  nbsapi.EVolumeMountMode_VOLUME_MOUNT_REMOTE,
		Persistent:       true,
		ClientProfile: &nbsapi.TClientProfile{
			HostType: &hostType,
		},
	}

	_, err := s.nbsClient.StartEndpoint(ctx, startEndpointRequest)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Failed to start endpoint: %+v", err)
	}

	podSocketPath := filepath.Join(endpointDir, socketName)
	if err := os.Chmod(podSocketPath, 0666); err != nil {
		return nil, err
	}

	// https://kubevirt.io/user-guide/virtual_machines/disks_and_volumes/#persistentvolumeclaim
	// "If the disk.img image file has not been created manually before starting a VM
	// then it will be created automatically with the PersistentVolumeClaim size."
	// So, let's create an empty disk.img to avoid automatic creation and save disk space.
	diskImgPath := filepath.Join(endpointDir, "disk.img")
	file, err := os.OpenFile(diskImgPath, os.O_CREATE, 0660)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to create disk.img: %+v", err)
	}
	file.Close()

	return &csi.NodeStageVolumeResponse{}, nil
}

func (s *nodeService) NodeUnstageVolume(
	ctx context.Context,
	req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {

	log.Printf("csi.NodeUnstageVolumeRequest: %+v", req)

	if req.VolumeId == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"VolumeId missing in NodeUnstageVolumeRequest")
	}
	if req.StagingTargetPath == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"StagingTargetPath missing in NodeUnstageVolumeRequest")
	}

	socketPath := filepath.Join(s.nbsSocketsDir, req.VolumeId, socketName)
	stopEndpointRequest := &nbsapi.TStopEndpointRequest{
		UnixSocketPath: socketPath,
	}

	_, err := s.nbsClient.StopEndpoint(ctx, stopEndpointRequest)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Failed to stop endpoint: %+v", err)
	}

	endpointDir := filepath.Join(s.podSocketsDir, req.VolumeId)
	if err := os.RemoveAll(endpointDir); err != nil {
		return nil, err
	}

	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (s *nodeService) NodePublishVolume(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {

	log.Printf("csi.NodePublishVolumeRequest: %+v", req)

	if req.VolumeCapability == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"NodeStageVolume Volume Capability must be provided")
	}

	options := []string{"bind"}

	var err error
	switch req.VolumeCapability.GetAccessType().(type) {
	case *csi.VolumeCapability_Mount:
		err = s.nodePublishVolumeForFileSystem(req, options)
	case *csi.VolumeCapability_Block:
		err = s.nodePublishVolumeForBlock(req, options)
	default:
		return nil, status.Error(codes.InvalidArgument, "Unknown access type")
	}

	if err != nil {
		return nil, err
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

func (s *nodeService) nodePublishVolumeForFileSystem(
	req *csi.NodePublishVolumeRequest,
	mountOptions []string) error {

	source := filepath.Join(s.podSocketsDir, req.VolumeId)
	target := req.TargetPath

	mnt := req.VolumeCapability.GetMount()
	for _, flag := range mnt.MountFlags {
		mountOptions = append(mountOptions, flag)
	}

	fsType := "ext4"
	if mnt.FsType != "" {
		fsType = mnt.FsType
	}

	return s.mount(source, target, fsType, mountOptions...)
}

func (s *nodeService) nodePublishVolumeForBlock(
	req *csi.NodePublishVolumeRequest,
	mountOptions []string) error {

	source := "/dev/nbd0" // TODO (issues/463): get from endpoint info
	target := req.TargetPath

	return s.mount(source, target, "", mountOptions...)
}

func (s *nodeService) NodeUnpublishVolume(
	ctx context.Context,
	req *csi.NodeUnpublishVolumeRequest,
) (*csi.NodeUnpublishVolumeResponse, error) {

	log.Printf("csi.NodeUnpublishVolumeRequest: %+v", req)

	if req.VolumeId == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"Volume ID missing in NodeUnpublishVolumeRequest")
	}
	if req.TargetPath == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"Target Path missing in NodeUnpublishVolumeRequest")
	}

	err := s.unmount(req.TargetPath)
	if err != nil {
		return nil, err
	}

	// TODO (issues/464): remove req.TargetPath for Block
	err = os.RemoveAll(filepath.Dir(req.TargetPath))
	if err != nil {
		return nil, err
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (s *nodeService) NodeGetCapabilities(
	ctx context.Context,
	req *csi.NodeGetCapabilitiesRequest,
) (*csi.NodeGetCapabilitiesResponse, error) {

	log.Printf("csi.NodeGetCapabilitiesRequest: %+v", req)

	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: capabilities,
	}, nil
}

func (s *nodeService) NodeGetInfo(
	ctx context.Context,
	req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {

	log.Printf("csi.NodeGetInfo: %+v", req)

	return &csi.NodeGetInfoResponse{
		NodeId: s.nodeID,
		AccessibleTopology: &csi.Topology{
			Segments: map[string]string{topologyKeyNode: s.nodeID},
		},
	}, nil
}

func (s *nodeService) mount(
	source string,
	target string,
	fsType string,
	opts ...string) error {

	mountCmd := "mount"
	mountArgs := []string{}

	if source == "" {
		return status.Error(
			codes.Internal,
			"source is not specified for mounting the volume")
	}

	if target == "" {
		return status.Error(
			codes.Internal,
			"target is not specified for mounting the volume")
	}

	// This is a raw block device mount. Create the mount point as a file
	// since bind mount device node requires it to be a file
	if fsType == "" {
		err := os.MkdirAll(filepath.Dir(target), 0750)
		if err != nil {
			return fmt.Errorf("failed to create target directory: %v", err)
		}

		file, err := os.OpenFile(target, os.O_CREATE, 0660)
		if err != nil {
			return fmt.Errorf("failed to create target file: %v", err)
		}
		file.Close()
	} else {
		mountArgs = append(mountArgs, "-t", fsType)

		err := os.MkdirAll(target, 0755)
		if err != nil {
			return err
		}
	}

	if len(opts) > 0 {
		mountArgs = append(mountArgs, "-o", strings.Join(opts, ","))
	}

	mountArgs = append(mountArgs, source)
	mountArgs = append(mountArgs, target)

	out, err := exec.Command(mountCmd, mountArgs...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("mounting failed: %v cmd: '%s %s' output: %q",
			err, mountCmd, strings.Join(mountArgs, " "), string(out))
	}

	return nil
}

func (s *nodeService) unmount(target string) error {
	unmountCmd := "umount"
	unmountArgs := []string{}

	unmountArgs = append(unmountArgs, target)

	out, err := exec.Command(unmountCmd, unmountArgs...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("unmounting failed: %v cmd: '%s %s' output: %q",
			err, unmountCmd, strings.Join(unmountArgs, " "), string(out))
	}

	return nil
}
