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
	nfsapi "github.com/ydb-platform/nbs/cloud/filestore/public/api/protos"
	nfsclient "github.com/ydb-platform/nbs/cloud/filestore/public/sdk/go/client"
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
	nfsClient     nfsclient.EndpointClientIface
}

func newNodeService(
	nodeID string,
	clientID string,
	nbsSocketsDir string,
	podSocketsDir string,
	nbsClient nbsclient.ClientIface,
	nfsClient nfsclient.EndpointClientIface) csi.NodeServer {

	return &nodeService{
		nodeID:        nodeID,
		clientID:      clientID,
		nbsSocketsDir: nbsSocketsDir,
		podSocketsDir: podSocketsDir,
		nbsClient:     nbsClient,
		nfsClient:     nfsClient,
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

	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (s *nodeService) NodePublishVolume(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {

	log.Printf("csi.NodePublishVolumeRequest: %+v", req)

	if req.VolumeId == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"VolumeId missing in NodePublishVolumeRequest")
	}
	if req.VolumeCapability == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"VolumeCapability missing im NodePublishVolumeRequest")
	}

	var err error
	nfsBackend := (req.VolumeContext != nil && req.VolumeContext["backend"] == "nfs")

	switch req.VolumeCapability.GetAccessType().(type) {
	case *csi.VolumeCapability_Mount:
		if nfsBackend {
			err = s.nodePublishFileStoreAsVhostSocket(ctx, req)
		} else {
			err = s.nodePublishDiskAsVhostSocket(ctx, req)
		}
	case *csi.VolumeCapability_Block:
		if nfsBackend {
			err = status.Error(codes.InvalidArgument,
				"'Block' volume mode is not supported for nfs backend")
		} else {
			err = s.nodePublishDiskAsBlockDevice(ctx, req)
		}
	default:
		err = status.Error(codes.InvalidArgument, "Unknown access type")
	}

	if err != nil {
		return nil, err
	}

	return &csi.NodePublishVolumeResponse{}, nil
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

	if err := s.nodeUnpublishVolume(ctx, req); err != nil {
		return nil, err
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (s *nodeService) NodeGetCapabilities(
	ctx context.Context,
	req *csi.NodeGetCapabilitiesRequest,
) (*csi.NodeGetCapabilitiesResponse, error) {

	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: capabilities,
	}, nil
}

func (s *nodeService) NodeGetInfo(
	ctx context.Context,
	req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {

	return &csi.NodeGetInfoResponse{
		NodeId: s.nodeID,
		AccessibleTopology: &csi.Topology{
			Segments: map[string]string{topologyKeyNode: s.nodeID},
		},
	}, nil
}

func (s *nodeService) nodePublishDiskAsVhostSocket(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest) error {

	_, err := s.startNbsEndpoint(ctx, req, nbsapi.EClientIpcType_IPC_VHOST)
	if err != nil {
		return status.Errorf(codes.Internal,
			"Failed to start NBS endpoint: %+v", err)
	}

	return s.mountSocketDir(req)
}

func (s *nodeService) nodePublishDiskAsBlockDevice(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest) error {

	resp, err := s.startNbsEndpoint(ctx, req, nbsapi.EClientIpcType_IPC_NBD)
	if err != nil {
		return status.Errorf(codes.Internal,
			"Failed to start NBS endpoint: %+v", err)
	}

	if resp.NbdDeviceFile != "" {
		log.Printf("Endpoint started with device file: %q", resp.NbdDeviceFile)
	}

	return s.mountBlockDevice(resp.NbdDeviceFile, req.TargetPath)
}

func (s *nodeService) startNbsEndpoint(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest,
	ipcType nbsapi.EClientIpcType) (*nbsapi.TStartEndpointResponse, error) {

	endpointDir := filepath.Join(s.podSocketsDir, req.VolumeId)
	if err := os.MkdirAll(endpointDir, 0777); err != nil {
		return nil, err
	}

	socketPath := filepath.Join(s.nbsSocketsDir, req.VolumeId, socketName)
	hostType := nbsapi.EHostType_HOST_TYPE_DEFAULT
	return s.nbsClient.StartEndpoint(ctx, &nbsapi.TStartEndpointRequest{
		UnixSocketPath:   socketPath,
		DiskId:           req.VolumeId,
		ClientId:         s.clientID,
		DeviceName:       req.VolumeId,
		IpcType:          ipcType,
		VhostQueuesCount: 8,
		VolumeAccessMode: nbsapi.EVolumeAccessMode_VOLUME_ACCESS_READ_WRITE,
		VolumeMountMode:  nbsapi.EVolumeMountMode_VOLUME_MOUNT_REMOTE,
		Persistent:       true,
		NbdDevice: &nbsapi.TStartEndpointRequest_UseFreeNbdDeviceFile{
			ipcType == nbsapi.EClientIpcType_IPC_NBD,
		},
		ClientProfile: &nbsapi.TClientProfile{
			HostType: &hostType,
		},
	})
}

func (s *nodeService) nodePublishFileStoreAsVhostSocket(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest) error {

	endpointDir := filepath.Join(s.podSocketsDir, req.VolumeId)
	if err := os.MkdirAll(endpointDir, 0777); err != nil {
		return err
	}

	if s.nfsClient == nil {
		return status.Errorf(codes.Internal, "NFS client wasn't created")
	}

	socketPath := filepath.Join(s.nbsSocketsDir, req.VolumeId, socketName)
	_, err := s.nfsClient.StartEndpoint(ctx, &nfsapi.TStartEndpointRequest{
		Endpoint: &nfsapi.TEndpointConfig{
			SocketPath:       socketPath,
			FileSystemId:     req.VolumeId,
			ClientId:         s.clientID,
			VhostQueuesCount: 8,
			Persistent:       true,
		},
	})
	if err != nil {
		return status.Errorf(codes.Internal,
			"Failed to start NFS endpoint: %+v", err)
	}

	return s.mountSocketDir(req)
}

func (s *nodeService) nodeUnpublishVolume(
	ctx context.Context,
	req *csi.NodeUnpublishVolumeRequest) error {

	// Trying to stop both NBS and NFS endpoints,
	// because the endpoint's backend service is unknown here.
	// When we miss we get S_FALSE/S_ALREADY code (err == nil).

	socketPath := filepath.Join(s.nbsSocketsDir, req.VolumeId, socketName)
	_, err := s.nbsClient.StopEndpoint(ctx, &nbsapi.TStopEndpointRequest{
		UnixSocketPath: socketPath,
	})
	if err != nil {
		return status.Errorf(codes.Internal,
			"Failed to stop nbs endpoint: %+v", err)
	}

	if s.nfsClient != nil {
		_, err = s.nfsClient.StopEndpoint(ctx, &nfsapi.TStopEndpointRequest{
			SocketPath: socketPath,
		})
		if err != nil {
			return status.Errorf(codes.Internal,
				"Failed to stop nfs endpoint: %+v", err)
		}
	}

	endpointDir := filepath.Join(s.podSocketsDir, req.VolumeId)
	if err := os.RemoveAll(endpointDir); err != nil {
		return err
	}

	if err := s.unmount(req.TargetPath); err != nil {
		return err
	}

	if err := os.RemoveAll(req.TargetPath); err != nil {
		return err
	}

	return nil
}

func (s *nodeService) mountSocketDir(req *csi.NodePublishVolumeRequest) error {

	endpointDir := filepath.Join(s.podSocketsDir, req.VolumeId)

	podSocketPath := filepath.Join(endpointDir, socketName)
	if err := os.Chmod(podSocketPath, 0666); err != nil {
		return err
	}

	// https://kubevirt.io/user-guide/virtual_machines/disks_and_volumes/#persistentvolumeclaim
	// "If the disk.img image file has not been created manually before starting a VM
	// then it will be created automatically with the PersistentVolumeClaim size."
	// So, let's create an empty disk.img to avoid automatic creation and save disk space.
	diskImgPath := filepath.Join(endpointDir, "disk.img")
	file, err := os.OpenFile(diskImgPath, os.O_CREATE, 0660)
	if err != nil {
		return status.Errorf(codes.Internal, "Failed to create disk.img: %+v", err)
	}
	file.Close()

	source := endpointDir
	target := req.TargetPath

	fsType := "ext4"
	mountOptions := []string{"bind"}

	mnt := req.VolumeCapability.GetMount()
	if mnt != nil {
		for _, flag := range mnt.MountFlags {
			mountOptions = append(mountOptions, flag)
		}

		if mnt.FsType != "" {
			fsType = mnt.FsType
		}
	}

	return s.mount(source, target, fsType, mountOptions...)
}

func (s *nodeService) mountBlockDevice(source string, target string) error {
	mountOptions := []string{"bind"}
	return s.mount(source, target, "", mountOptions...)
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
