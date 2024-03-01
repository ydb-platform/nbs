package driver

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	nbsblockstorepublicapi "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	nbsclient "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const topologyKeyNode = "topology.nbs.csi/node"

const nodeEndpointsDir = "/home/nbsd/endpoints/"
const podEndpointsDir = "/nbs-endpoints/"

const volumeSocketName = "nbs-volume.sock"

var capabilities = []*csi.NodeServiceCapability{
	&csi.NodeServiceCapability{
		Type: &csi.NodeServiceCapability_Rpc{
			Rpc: &csi.NodeServiceCapability_RPC{
				Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
			},
		},
	},
}

type nodeService struct {
	csi.NodeServer

	nodeID    string
	clientID  string
	nbsClient nbsclient.ClientIface
}

func newNodeService(nodeID, clientID string, nbsClient nbsclient.ClientIface) csi.NodeServer {
	return &nodeService{nodeID: nodeID, clientID: clientID, nbsClient: nbsClient}
}

func (s *nodeService) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	log.Printf("csi.NodeStageVolumeRequest: %+v", req)

	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "VolumeId missing in NodeStageVolumeRequest")
	}
	if req.StagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "StagingTargetPath missing in NodeStageVolumeRequest")
	}
	if req.VolumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "VolumeCapability missing im NodeStageVolumeRequest")
	}

	var ipcType nbsblockstorepublicapi.EClientIpcType
	switch req.VolumeCapability.GetAccessType().(type) {
	case *csi.VolumeCapability_Block:
		ipcType = nbsblockstorepublicapi.EClientIpcType_IPC_NBD
	case *csi.VolumeCapability_Mount:
		ipcType = nbsblockstorepublicapi.EClientIpcType_IPC_VHOST
	default:
		return nil, status.Error(codes.InvalidArgument, "Unknown access type")
	}

	endpointDir := filepath.Join(podEndpointsDir, req.VolumeId)
	log.Printf("Creating folder for socket: %q", endpointDir)
	if err := os.MkdirAll(endpointDir, 0777); err != nil {
		return nil, err
	}

	hostType := nbsblockstorepublicapi.EHostType_HOST_TYPE_DEFAULT
	socketPath := filepath.Join(nodeEndpointsDir, req.VolumeId, volumeSocketName)
	startEndpointRequest := &nbsblockstorepublicapi.TStartEndpointRequest{
		UnixSocketPath:            socketPath,
		DiskId:                    req.VolumeId,
		ClientId:                  s.clientID,
		DeviceName:                req.VolumeId,
		MountSeqNumber:            0, // TODO: ?
		IpcType:                   ipcType,
		VhostQueuesCount:          8,
		VolumeAccessMode:          nbsblockstorepublicapi.EVolumeAccessMode_VOLUME_ACCESS_READ_WRITE,
		VolumeMountMode:           nbsblockstorepublicapi.EVolumeMountMode_VOLUME_MOUNT_REMOTE,
		UnalignedRequestsDisabled: false,
		Persistent:                true,
		ClientProfile: &nbsblockstorepublicapi.TClientProfile{
			//CpuUnitCount: nil, // TODO: ?
			HostType: &hostType,
		},
		//ClientCGroups:             clientCgroups, // TODO: ?
	}
	log.Printf("Starting endpoint: %+v", startEndpointRequest)
	startEndpointResponse, err := s.nbsClient.StartEndpoint(ctx, startEndpointRequest)
	if err != nil {
		log.Printf("Failed to start endpoint: %+v", err)
		// return nil, err
	}
	log.Printf("Endpoint started: %+v", startEndpointResponse)

	return &csi.NodeStageVolumeResponse{}, nil
}

func (s *nodeService) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	log.Printf("csi.NodeUnstageVolumeRequest: %+v", req)

	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "VolumeId missing in NodeUnstageVolumeRequest")
	}
	if req.StagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "StagingTargetPath missing in NodeUnstageVolumeRequest")
	}

	// TODO: check if endpoint exists
	//s.nbsClient.DescribeEndpoint()

	socketPath := filepath.Join(nodeEndpointsDir, req.VolumeId, volumeSocketName)
	stopEndpointRequest := &nbsblockstorepublicapi.TStopEndpointRequest{
		UnixSocketPath: socketPath,
	}
	log.Printf("Stopping endpoint: %+v", stopEndpointRequest)
	stopEndpointResponse, err := s.nbsClient.StopEndpoint(ctx, stopEndpointRequest)
	if err != nil {
		log.Printf("Failed to stop endpoint: %+v", err)
		// return nil, err
	}
	log.Printf("Endpoint stopped: %+v", stopEndpointResponse)

	endpointDir := filepath.Join(podEndpointsDir, req.VolumeId)
	log.Printf("Deleting folder for socket: %q", endpointDir)
	if err := os.RemoveAll(endpointDir); err != nil {
		return nil, err
	}

	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (s *nodeService) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	log.Printf("csi.NodePublishVolumeRequest: %+v", req)

	if req.VolumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "NodeStageVolume Volume Capability must be provided")
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

func (s *nodeService) nodePublishVolumeForFileSystem(req *csi.NodePublishVolumeRequest, mountOptions []string) error {
	source := filepath.Join(podEndpointsDir, req.VolumeId)
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

func (s *nodeService) nodePublishVolumeForBlock(req *csi.NodePublishVolumeRequest, mountOptions []string) error {
	source := "/dev/nbd0" // TODO: get from endpoint info
	target := req.TargetPath

	return s.mount(source, target, "", mountOptions...)
}

func (s *nodeService) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	log.Printf("csi.NodeUnpublishVolumeRequest: %+v", req)

	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in NodeUnpublishVolumeRequest")
	}
	if req.TargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "Target Path missing in NodeUnpublishVolumeRequest")
	}

	err := s.unmount(req.TargetPath)
	if err != nil {
		return nil, err
	}

	// TODO: remove req.TargetPath for Block
	err = os.RemoveAll(filepath.Dir(req.TargetPath))
	if err != nil {
		return nil, err
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (s *nodeService) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	log.Printf("csi.NodeGetCapabilitiesRequest: %+v", req)

	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: capabilities,
	}, nil
}

func (s *nodeService) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	log.Printf("csi.NodeGetInfo: %+v", req)

	return &csi.NodeGetInfoResponse{
		NodeId: s.nodeID,
		AccessibleTopology: &csi.Topology{
			Segments: map[string]string{topologyKeyNode: s.nodeID},
		},
	}, nil
}

func (s *nodeService) mount(source, target, fsType string, opts ...string) error {
	mountCmd := "mount"
	mountArgs := []string{}

	if source == "" {
		return status.Error(codes.Internal, "source is not specified for mounting the volume")
	}

	if target == "" {
		return status.Error(codes.Internal, "target is not specified for mounting the volume")
	}

	// This is a raw block device mount. Create the mount point as a file
	// since bind mount device node requires it to be a file
	if fsType == "" {
		// create directory for target, os.Mkdirall is noop if directory exists
		err := os.MkdirAll(filepath.Dir(target), 0750)
		if err != nil {
			return fmt.Errorf("failed to create target directory for raw block bind mount: %v", err)
		}

		file, err := os.OpenFile(target, os.O_CREATE, 0660)
		if err != nil {
			return fmt.Errorf("failed to create target file for raw block bind mount: %v", err)
		}
		file.Close()
	} else {
		mountArgs = append(mountArgs, "-t", fsType)

		// create target, os.Mkdirall is noop if directory exists
		err := os.MkdirAll(target, 0750)
		if err != nil {
			return err
		}
	}

	if len(opts) > 0 {
		mountArgs = append(mountArgs, "-o", strings.Join(opts, ","))
	}

	mountArgs = append(mountArgs, source)
	mountArgs = append(mountArgs, target)

	log.Printf("Executing mount command: %s %s", mountCmd, strings.Join(mountArgs, " "))
	// out, err := exec.Command(mountCmd, mountArgs...).CombinedOutput()
	// if err != nil {
	// 	return fmt.Errorf("mounting failed: %v cmd: '%s %s' output: %q",
	// 		err, mountCmd, strings.Join(mountArgs, " "), string(out))
	// }

	return nil
}

func (s *nodeService) unmount(target string) error {
	unmountCmd := "umount"
	unmountArgs := []string{}

	unmountArgs = append(unmountArgs, target)

	log.Printf("Executing unmount command: %s %s", unmountCmd, strings.Join(unmountArgs, " "))
	// out, err := exec.Command(unmountCmd, unmountArgs...).CombinedOutput()
	// if err != nil {
	// 	return fmt.Errorf("mounting failed: %v cmd: '%s %s' output: %q",
	// 		err, unmountCmd, strings.Join(unmountArgs, " "), string(out))
	// }

	return nil
}
