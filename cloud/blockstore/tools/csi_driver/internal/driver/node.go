package driver

//lint:file-ignore ST1003 protobuf generates names that break golang naming convention

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/container-storage-interface/spec/lib/go/csi"
	nbsapi "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	nbsclient "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	"github.com/ydb-platform/nbs/cloud/blockstore/tools/csi_driver/internal/mounter"
	"github.com/ydb-platform/nbs/cloud/blockstore/tools/csi_driver/internal/volume"
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
const instanceIdKey = "instanceId"

const volumeOperationInProgress = "Another operation with volume %s is in progress"

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
	{
		Type: &csi.NodeServiceCapability_Rpc{
			Rpc: &csi.NodeServiceCapability_RPC{
				Type: csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
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

	nodeId              string
	clientId            string
	vmMode              bool
	socketsDir          string
	targetFsPathRegexp  *regexp.Regexp
	targetBlkPathRegexp *regexp.Regexp
	localFsOverrides    LocalFilestoreOverrideMap

	nbsClient      nbsclient.ClientIface
	nfsClient      nfsclient.EndpointClientIface
	nfsLocalClient nfsclient.EndpointClientIface
	mounter        mounter.Interface
	volumeOps      *sync.Map
	mountOptions   []string

	useDiscardForYDBBasedDisks bool
}

func newNodeService(
	nodeId string,
	clientId string,
	vmMode bool,
	socketsDir string,
	targetFsPathPattern string,
	targetBlkPathPattern string,
	localFsOverrides LocalFilestoreOverrideMap,
	nbsClient nbsclient.ClientIface,
	nfsClient nfsclient.EndpointClientIface,
	nfsLocalClient nfsclient.EndpointClientIface,
	mounter mounter.Interface,
	mountOptions []string,
	useDiscardForYDBBasedDisks bool) csi.NodeServer {

	return &nodeService{
		nodeId:                     nodeId,
		clientId:                   clientId,
		vmMode:                     vmMode,
		socketsDir:                 socketsDir,
		nbsClient:                  nbsClient,
		nfsClient:                  nfsClient,
		nfsLocalClient:             nfsLocalClient,
		mounter:                    mounter,
		targetFsPathRegexp:         regexp.MustCompile(targetFsPathPattern),
		targetBlkPathRegexp:        regexp.MustCompile(targetBlkPathPattern),
		localFsOverrides:           localFsOverrides,
		volumeOps:                  new(sync.Map),
		mountOptions:               mountOptions,
		useDiscardForYDBBasedDisks: useDiscardForYDBBasedDisks,
	}
}

// nbsId - diskId for blockstore and filesystemId for filestore
func parseVolumeId(volumeId string) (nbsId string, instanceId string) {
	// if separator is not found then this is just legacy volume without instanceId
	nbsId, instanceId, _ = strings.Cut(volumeId, "#")
	return nbsId, instanceId
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

	accessMode := req.VolumeCapability.AccessMode
	if accessMode == nil {
		return nil, s.statusError(
			codes.InvalidArgument,
			"AccessMode is missing in NodePublishVolumeRequest")
	}

	nfsBackend := (req.VolumeContext[backendVolumeContextKey] == "nfs")
	if !nfsBackend && accessMode.GetMode() ==
		csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER {
		return nil, s.statusError(
			codes.InvalidArgument,
			"ReadWriteMany access mode is supported only with nfs backend")
	}

	if _, opInProgress := s.volumeOps.LoadOrStore(req.VolumeId, nil); opInProgress {
		return nil, s.statusErrorf(codes.Aborted, volumeOperationInProgress, req.VolumeId)
	}
	defer s.volumeOps.Delete(req.VolumeId)

	var err error
	switch req.VolumeCapability.GetAccessType().(type) {
	case *csi.VolumeCapability_Mount:
		if s.vmMode {
			nfsBackend := (req.VolumeContext[backendVolumeContextKey] == "nfs")

			var err error
			if instanceId := req.VolumeContext[instanceIdKey]; instanceId != "" {
				nbsId, _ := parseVolumeId(req.VolumeId)
				stageRecordPath := filepath.Join(req.StagingTargetPath, nbsId+".json")
				// Backend can be empty for old disks, in this case we use NBS
				backend := "nbs"
				if nfsBackend {
					backend = "nfs"
				}
				if err = s.writeStageData(stageRecordPath, &StageData{
					Backend:       backend,
					InstanceId:    instanceId,
					RealStagePath: s.getEndpointDir(instanceId, nbsId),
				}); err != nil {
					return nil, s.statusErrorf(codes.Internal,
						"Failed to write stage record: %v", err)
				}

				if nfsBackend {
					err = s.nodeStageFileStoreAsVhostSocket(ctx, instanceId, nbsId)
				} else {
					err = s.nodeStageDiskAsVhostSocket(ctx, instanceId, nbsId, req.VolumeContext)
				}

				if err != nil {
					ignoreError(os.Remove(stageRecordPath))
					return nil, s.statusErrorf(codes.Internal,
						"Failed to stage volume: %v", err)
				}
			}
		} else {
			if nfsBackend {
				return nil, s.statusError(codes.InvalidArgument,
					"NFS mounts are only supported in VM mode")
			} else {
				err = s.nodeStageDiskAsFilesystem(ctx, req)
			}
		}
	case *csi.VolumeCapability_Block:
		if nfsBackend {
			return nil, s.statusError(codes.InvalidArgument,
				"'Block' volume mode is not supported with nfs backend")
		} else {
			err = s.nodeStageDiskAsBlockDevice(ctx, req)
		}
	default:
		return nil, s.statusError(codes.InvalidArgument, "Unknown access type")
	}

	if err != nil {
		return nil, s.statusErrorf(codes.Internal,
			"Failed to stage volume: %v", err)
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

	if _, opInProgress := s.volumeOps.LoadOrStore(req.VolumeId, nil); opInProgress {
		return nil, s.statusErrorf(codes.Aborted, volumeOperationInProgress, req.VolumeId)
	}
	defer s.volumeOps.Delete(req.VolumeId)

	if s.vmMode {
		nbsId, _ := parseVolumeId(req.VolumeId)

		stageRecordPath := filepath.Join(req.StagingTargetPath, nbsId+".json")
		if stageData, err := s.readStageData(stageRecordPath); err == nil {
			if err := s.nodeUnstageVhostSocket(ctx, nbsId, stageData); err != nil {
				return nil, s.statusErrorf(
					codes.InvalidArgument,
					"Failed to unstage volume: %v", err)
			}
			ignoreError(os.Remove(stageRecordPath))
		}
	} else {
		if err := s.nodeUnstageVolume(ctx, req); err != nil {
			return nil, s.statusErrorf(
				codes.InvalidArgument,
				"Failed to unstage volume: %v", err)
		}
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

	accessMode := req.VolumeCapability.AccessMode
	if accessMode == nil {
		return nil, s.statusError(
			codes.InvalidArgument,
			"AccessMode is missing in NodePublishVolumeRequest")
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

	nfsBackend := (req.VolumeContext[backendVolumeContextKey] == "nfs")
	if !nfsBackend && accessMode.GetMode() ==
		csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER {
		return nil, s.statusError(
			codes.InvalidArgument,
			"ReadWriteMany access mode is supported only with nfs backend")
	}

	if _, opInProgress := s.volumeOps.LoadOrStore(req.VolumeId, nil); opInProgress {
		return nil, s.statusErrorf(codes.Aborted, volumeOperationInProgress, req.VolumeId)
	}
	defer s.volumeOps.Delete(req.VolumeId)

	var err error
	switch req.VolumeCapability.GetAccessType().(type) {
	case *csi.VolumeCapability_Mount:
		if s.vmMode {
			if instanceId := req.VolumeContext[instanceIdKey]; instanceId != "" {
				err = s.nodePublishStagedVhostSocket(req, instanceId)
			} else {
				if nfsBackend {
					err = s.nodePublishFileStoreAsVhostSocket(ctx, req)
				} else {
					err = s.nodePublishDiskAsVhostSocket(ctx, req)
				}
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

	if _, opInProgress := s.volumeOps.LoadOrStore(req.VolumeId, nil); opInProgress {
		return nil, s.statusErrorf(codes.Aborted, volumeOperationInProgress, req.VolumeId)
	}
	defer s.volumeOps.Delete(req.VolumeId)

	if err := s.nodeUnpublishVolume(ctx, req); err != nil {
		return nil, s.statusErrorf(
			codes.InvalidArgument,
			"Failed to unpublish volume: %v", err)
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (s *nodeService) NodeGetCapabilities(
	_ context.Context,
	_ *csi.NodeGetCapabilitiesRequest,
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
	_ context.Context,
	_ *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {

	return &csi.NodeGetInfoResponse{
		NodeId: s.nodeId,
		AccessibleTopology: &csi.Topology{
			Segments: map[string]string{topologyNodeKey: s.nodeId},
		},
	}, nil
}

func (s *nodeService) nodePublishDiskAsVhostSocket(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest) error {

	podId := s.getPodId(req)
	diskId := req.VolumeId
	volumeContext := req.VolumeContext

	endpointDir := s.getEndpointDir(podId, diskId)
	if err := os.MkdirAll(endpointDir, os.FileMode(0755)); err != nil {
		return err
	}

	deviceName, found := volumeContext[deviceNameVolumeContextKey]
	if !found {
		deviceName = diskId
	}

	hostType := nbsapi.EHostType_HOST_TYPE_DEFAULT
	_, err := s.nbsClient.StartEndpoint(ctx, &nbsapi.TStartEndpointRequest{
		UnixSocketPath:   filepath.Join(endpointDir, nbsSocketName),
		DiskId:           diskId,
		InstanceId:       podId,
		ClientId:         fmt.Sprintf("%s-%s", s.clientId, podId),
		DeviceName:       deviceName,
		IpcType:          vhostIpc,
		VhostQueuesCount: 8,
		VolumeAccessMode: nbsapi.EVolumeAccessMode_VOLUME_ACCESS_READ_WRITE,
		VolumeMountMode:  nbsapi.EVolumeMountMode_VOLUME_MOUNT_LOCAL,
		Persistent:       true,
		NbdDevice: &nbsapi.TStartEndpointRequest_UseFreeNbdDeviceFile{
			false,
		},
		ClientProfile: &nbsapi.TClientProfile{
			HostType: &hostType,
		},
	})

	if err != nil {
		if s.IsGrpcTimeoutError(err) {
			s.nbsClient.StopEndpoint(ctx, &nbsapi.TStopEndpointRequest{
				UnixSocketPath: filepath.Join(endpointDir, nbsSocketName),
			})
		}
		return fmt.Errorf("failed to start NBS endpoint: %w", err)
	}

	if err := s.createDummyImgFile(endpointDir); err != nil {
		return err
	}

	return s.mountSocketDir(endpointDir, req)
}

type StageData struct {
	Backend       string `json:"backend"`
	InstanceId    string `json:"instanceId"`
	RealStagePath string `json:"realStagePath"`
}

func (s *nodeService) writeStageData(path string, data *StageData) error {
	bytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	err = os.MkdirAll(filepath.Dir(path), 0750)
	if err != nil {
		return err
	}

	err = os.WriteFile(path, bytes, 0600)
	if err != nil {
		return err
	}

	return nil
}

func (s *nodeService) readStageData(path string) (*StageData, error) {
	data := StageData{}

	bytes, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(bytes, &data)
	if err != nil {
		return nil, err
	}

	return &data, nil
}

func (s *nodeService) nodeStageDiskAsVhostSocket(
	ctx context.Context,
	instanceId string,
	diskId string,
	volumeContext map[string]string) error {

	log.Printf("csi.nodeStageDiskAsVhostSocket: %s %s %+v", instanceId, diskId, volumeContext)

	endpointDir := s.getEndpointDir(instanceId, diskId)
	if err := os.MkdirAll(endpointDir, os.FileMode(0755)); err != nil {
		return err
	}

	deviceName, found := volumeContext[deviceNameVolumeContextKey]
	if !found {
		deviceName = diskId
	}

	hostType := nbsapi.EHostType_HOST_TYPE_DEFAULT
	_, err := s.nbsClient.StartEndpoint(ctx, &nbsapi.TStartEndpointRequest{
		UnixSocketPath:   filepath.Join(endpointDir, nbsSocketName),
		DiskId:           diskId,
		InstanceId:       instanceId,
		ClientId:         fmt.Sprintf("%s-%s", s.clientId, instanceId),
		DeviceName:       deviceName,
		IpcType:          vhostIpc,
		VhostQueuesCount: 8,
		VolumeAccessMode: nbsapi.EVolumeAccessMode_VOLUME_ACCESS_READ_WRITE,
		VolumeMountMode:  nbsapi.EVolumeMountMode_VOLUME_MOUNT_LOCAL,
		Persistent:       true,
		NbdDevice: &nbsapi.TStartEndpointRequest_UseFreeNbdDeviceFile{
			false,
		},
		ClientProfile: &nbsapi.TClientProfile{
			HostType: &hostType,
		},
	})

	if err != nil {
		if s.IsGrpcTimeoutError(err) {
			s.nbsClient.StopEndpoint(ctx, &nbsapi.TStopEndpointRequest{
				UnixSocketPath: filepath.Join(endpointDir, nbsSocketName),
			})
		}
		return fmt.Errorf("failed to start NBS endpoint: %w", err)
	}

	return s.createDummyImgFile(endpointDir)
}

func (s *nodeService) nodePublishDiskAsFilesystem(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest) error {

	mounted, _ := s.mounter.IsMountPoint(req.StagingTargetPath)
	if !mounted {
		return s.statusErrorf(codes.FailedPrecondition,
			"Staging target path is not mounted: %w", req.VolumeId)
	}

	readOnly, _ := s.mounter.IsFilesystemRemountedAsReadonly(req.StagingTargetPath)
	if readOnly {
		return s.statusErrorf(
			codes.Internal,
			"Filesystem was remounted as readonly")
	}

	mounted, _ = s.mounter.IsMountPoint(req.TargetPath)
	if !mounted {
		targetPerm := os.FileMode(0775)
		if err := os.MkdirAll(req.TargetPath, targetPerm); err != nil {
			return fmt.Errorf("failed to create target directory: %w", err)
		}

		if err := os.Chmod(req.TargetPath, targetPerm); err != nil {
			return fmt.Errorf("failed to chmod target path: %w", err)
		}
	}

	diskId := req.VolumeId
	mountOptions := []string{"bind"}
	mnt := req.VolumeCapability.GetMount()
	if mnt != nil {
		for _, flag := range mnt.MountFlags {
			mountOptions = append(mountOptions, flag)
		}
	}

	if req.Readonly {
		mountOptions = append(mountOptions, "ro")
	}

	err := s.mountIfNeeded(
		diskId,
		req.StagingTargetPath,
		req.TargetPath,
		"",
		mountOptions)
	if err != nil {
		return err
	}

	if mnt != nil && mnt.VolumeMountGroup != "" {
		fsGroup, err := strconv.ParseInt(mnt.VolumeMountGroup, 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse volume mount group: %w", err)
		}

		err = volume.SetVolumeOwnership(req.TargetPath, &fsGroup, req.Readonly)
		if err != nil {
			return fmt.Errorf("failed to set volume ownership: %w", err)
		}
	}

	return nil
}

func (s *nodeService) IsGrpcTimeoutError(err error) bool {
	if err != nil {
		var clientErr *nbsclient.ClientError
		if errors.As(err, &clientErr) {
			if clientErr.Code == nbsclient.E_GRPC_DEADLINE_EXCEEDED {
				return true
			}
		}
	}

	return false
}

func (s *nodeService) nodeStageDiskAsFilesystem(
	ctx context.Context,
	req *csi.NodeStageVolumeRequest) error {

	diskId := req.VolumeId
	resp, err := s.startNbsEndpointForNBD(ctx, "", diskId, req.VolumeContext)
	if err != nil {
		return fmt.Errorf("failed to start NBS endpoint: %w", err)
	}

	if resp.NbdDeviceFile == "" {
		return fmt.Errorf("NbdDeviceFile shouldn't be empty")
	}

	logVolume(req.VolumeId, "endpoint started with device: %q", resp.NbdDeviceFile)

	// startNbsEndpointForNBD is async function. Kubelet will retry
	// NodeStageVolume request if nbd device is not available yet.
	hasBlockDevice, err := s.mounter.HasBlockDevice(resp.NbdDeviceFile)
	if !hasBlockDevice {
		return fmt.Errorf("Nbd device is not available: %w", err)
	}

	mnt := req.VolumeCapability.GetMount()

	fsType := req.VolumeContext["fsType"]
	if mnt != nil && mnt.FsType != "" {
		fsType = mnt.FsType
	}
	if fsType == "" {
		fsType = "ext4"
	}

	targetPerm := os.FileMode(0775)
	if err := os.MkdirAll(req.StagingTargetPath, targetPerm); err != nil {
		return fmt.Errorf("failed to create staging directory: %w", err)
	}

	mountOptions := s.mountOptions
	if fsType == "ext4" {
		mountOptions = append(mountOptions, "errors=remount-ro")
	}

	if s.useDiscardForYDBBasedDisks && !isDiskRegistryMediaKind(getStorageMediaKind(req.VolumeContext)) {
		mountOptions = append(mountOptions, "discard")
	}

	if mnt != nil {
		for _, flag := range mnt.MountFlags {
			mountOptions = append(mountOptions, flag)
		}
	}

	err = s.formatAndMount(
		diskId,
		resp.NbdDeviceFile,
		req.StagingTargetPath,
		fsType,
		mountOptions)
	if err != nil {
		return fmt.Errorf("failed to format or mount filesystem: %w", err)
	}

	if err := os.Chmod(req.StagingTargetPath, targetPerm); err != nil {
		return fmt.Errorf("failed to chmod target path: %w", err)
	}

	return nil
}

func (s *nodeService) nodeStageDiskAsBlockDevice(
	ctx context.Context,
	req *csi.NodeStageVolumeRequest) error {

	diskId := req.VolumeId
	resp, err := s.startNbsEndpointForNBD(ctx, "", diskId, req.VolumeContext)
	if err != nil {
		return fmt.Errorf("failed to start NBS endpoint: %w", err)
	}

	if resp.NbdDeviceFile == "" {
		return fmt.Errorf("NbdDeviceFile shouldn't be empty")
	}

	logVolume(req.VolumeId, "endpoint started with device: %q", resp.NbdDeviceFile)

	devicePath := filepath.Join(req.StagingTargetPath, diskId)
	return s.mountBlockDevice(diskId, resp.NbdDeviceFile, devicePath, false)
}

func (s *nodeService) nodePublishDiskAsBlockDevice(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest) error {

	diskId := req.VolumeId
	devicePath := filepath.Join(req.StagingTargetPath, diskId)
	mounted, _ := s.mounter.IsMountPoint(devicePath)
	if !mounted {
		return s.statusErrorf(codes.FailedPrecondition,
			"Staging target path is not mounted: %w", req.VolumeId)
	}

	return s.mountBlockDevice(diskId, devicePath, req.TargetPath, req.Readonly)
}

func (s *nodeService) startNbsEndpointForNBD(
	ctx context.Context,
	instanceId string,
	diskId string,
	volumeContext map[string]string) (*nbsapi.TStartEndpointResponse, error) {

	endpointDir := s.getEndpointDir(instanceId, diskId)
	if err := os.MkdirAll(endpointDir, os.FileMode(0755)); err != nil {
		return nil, err
	}

	deviceName, found := volumeContext[deviceNameVolumeContextKey]
	if !found {
		deviceName = diskId
	}

	nbsInstanceId := instanceId
	if nbsInstanceId == "" {
		nbsInstanceId = s.nodeId
	}

	hostType := nbsapi.EHostType_HOST_TYPE_DEFAULT
	resp, err := s.nbsClient.StartEndpoint(ctx, &nbsapi.TStartEndpointRequest{
		UnixSocketPath:   filepath.Join(endpointDir, nbsSocketName),
		DiskId:           diskId,
		InstanceId:       nbsInstanceId,
		ClientId:         fmt.Sprintf("%s-%s", s.clientId, nbsInstanceId),
		DeviceName:       deviceName,
		IpcType:          nbdIpc,
		VhostQueuesCount: 8,
		VolumeAccessMode: nbsapi.EVolumeAccessMode_VOLUME_ACCESS_READ_WRITE,
		VolumeMountMode:  nbsapi.EVolumeMountMode_VOLUME_MOUNT_LOCAL,
		Persistent:       true,
		NbdDevice: &nbsapi.TStartEndpointRequest_UseFreeNbdDeviceFile{
			true,
		},
		ClientProfile: &nbsapi.TClientProfile{
			HostType: &hostType,
		},
	})

	if s.IsGrpcTimeoutError(err) {
		s.nbsClient.StopEndpoint(ctx, &nbsapi.TStopEndpointRequest{
			UnixSocketPath: filepath.Join(endpointDir, nbsSocketName),
		})
	}

	return resp, err
}

func (s *nodeService) getNfsClient(fileSystemId string) nfsclient.EndpointClientIface {
	_, ok := s.localFsOverrides[fileSystemId]
	if !ok {
		return s.nfsClient
	}

	return s.nfsLocalClient
}

func (s *nodeService) nodePublishFileStoreAsVhostSocket(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest) error {

	filesystemId := req.VolumeId
	podId := s.getPodId(req)
	endpointDir := s.getEndpointDir(podId, filesystemId)
	if err := os.MkdirAll(endpointDir, os.FileMode(0755)); err != nil {
		return err
	}

	nfsClient := s.getNfsClient(filesystemId)

	if nfsClient == nil {
		return fmt.Errorf("NFS client wasn't created")
	}

	_, err := nfsClient.StartEndpoint(ctx, &nfsapi.TStartEndpointRequest{
		Endpoint: &nfsapi.TEndpointConfig{
			SocketPath:       filepath.Join(endpointDir, nfsSocketName),
			FileSystemId:     filesystemId,
			ClientId:         fmt.Sprintf("%s-%s", s.clientId, podId),
			VhostQueuesCount: 8,
			Persistent:       true,
		},
	})
	if err != nil {
		if s.IsGrpcTimeoutError(err) {
			s.nbsClient.StopEndpoint(ctx, &nbsapi.TStopEndpointRequest{
				UnixSocketPath: filepath.Join(endpointDir, nbsSocketName),
			})
		}

		return fmt.Errorf("failed to start NFS endpoint: %w", err)
	}

	if err := s.createDummyImgFile(endpointDir); err != nil {
		return err
	}

	return s.mountSocketDir(endpointDir, req)
}

func (s *nodeService) nodeStageFileStoreAsVhostSocket(
	ctx context.Context,
	instanceId string,
	filesystemId string) error {

	log.Printf("csi.nodeStageFileStoreAsVhostSocket: %s %s", instanceId, filesystemId)

	endpointDir := s.getEndpointDir(instanceId, filesystemId)
	if err := os.MkdirAll(endpointDir, os.FileMode(0755)); err != nil {
		return err
	}

	nfsClient := s.getNfsClient(filesystemId)

	if nfsClient == nil {
		return fmt.Errorf("NFS client wasn't created")
	}

	_, err := nfsClient.StartEndpoint(ctx, &nfsapi.TStartEndpointRequest{
		Endpoint: &nfsapi.TEndpointConfig{
			SocketPath:       filepath.Join(endpointDir, nfsSocketName),
			FileSystemId:     filesystemId,
			ClientId:         fmt.Sprintf("%s-%s", s.clientId, instanceId),
			VhostQueuesCount: 8,
			Persistent:       true,
		},
	})
	if err != nil {
		if s.IsGrpcTimeoutError(err) {
			s.nbsClient.StopEndpoint(ctx, &nbsapi.TStopEndpointRequest{
				UnixSocketPath: filepath.Join(endpointDir, nbsSocketName),
			})
		}

		return fmt.Errorf("failed to start NFS endpoint: %w", err)
	}

	return s.createDummyImgFile(endpointDir)
}

func (s *nodeService) nodePublishStagedVhostSocket(req *csi.NodePublishVolumeRequest, instanceId string) error {
	nbsId, _ := parseVolumeId(req.VolumeId)
	endpointDir := s.getEndpointDir(instanceId, nbsId)
	return s.mountSocketDir(endpointDir, req)
}

func (s *nodeService) nodeUnstageVolume(
	ctx context.Context,
	req *csi.NodeUnstageVolumeRequest) error {

	diskId := req.VolumeId
	// Check mount points for StagingTargetPath and StagingTargetPath/diskId
	// as it's not possible to distinguish mount and block mode from request
	// parameters
	mountPoint := req.StagingTargetPath
	mounted, _ := s.mounter.IsMountPoint(mountPoint)
	if !mounted {
		mountPoint = filepath.Join(req.StagingTargetPath, diskId)
		mounted, _ = s.mounter.IsMountPoint(mountPoint)
	}

	if !mounted {
		// Fallback to previous implementation for already mounted volumes to
		// stop endpoint in nodeUnpublishVolume
		// Must be removed after migration of all endpoints to the new format
		return nil
	}

	if err := s.mounter.CleanupMountPoint(mountPoint); err != nil {
		return err
	}

	endpointDir := s.getEndpointDir("", diskId)
	if s.nbsClient != nil {
		_, err := s.nbsClient.StopEndpoint(ctx, &nbsapi.TStopEndpointRequest{
			UnixSocketPath: filepath.Join(endpointDir, nbsSocketName),
		})
		if err != nil {
			return fmt.Errorf("failed to stop nbs endpoint: %w", err)
		}
	}

	if err := os.RemoveAll(endpointDir); err != nil {
		return err
	}

	return nil
}

func (s *nodeService) nodeUnpublishVolume(
	ctx context.Context,
	req *csi.NodeUnpublishVolumeRequest) error {

	if err := s.mounter.CleanupMountPoint(req.TargetPath); err != nil {
		return err
	}

	// In VM-mode for volumes that were staged we need just the unmount, so
	// we could return here. Unfortunately we don't have enough information
	// to know if this is a staged volume or a legacy one.
	// Next StopEndpoint calls have no effect for staged volumes because their
	// endpoints were started in socketsDir/instanceId/nbsId instead of
	// socketsDir/podId/nbsId.
	//
	// When all VM disks are migrated to staged volumes we can enforce non-empty
	// instanceId for new VM disks and add early return here.
	if s.vmMode {
		// this is guaranteed to be a new volume that was properly staged, so rest
		// of unpublish can be skipped.
		if _, instanceId := parseVolumeId(req.VolumeId); instanceId != "" {
			return nil
		}
	}

	// no other way to get podId from NodeUnpublishVolumeRequest
	podId, err := s.parsePodId(req.TargetPath)
	if err != nil {
		return err
	}

	nbsId := req.VolumeId
	endpointDir := s.getEndpointDir(podId, nbsId)

	// Trying to stop both NBS and NFS endpoints,
	// because the endpoint's backend service is unknown here.
	// When we miss we get S_FALSE/S_ALREADY code (err == nil).

	// Fallback to previous implementation for already mounted volumes to
	// stop endpoint in nodeUnpublishVolume.
	// Must be removed after migration of all endpoints to the new format
	if s.nbsClient != nil {
		_, err := s.nbsClient.StopEndpoint(ctx, &nbsapi.TStopEndpointRequest{
			UnixSocketPath: filepath.Join(endpointDir, nbsSocketName),
		})
		if err != nil {
			return fmt.Errorf("failed to stop nbs endpoint: %w", err)
		}
	}

	nfsClient := s.getNfsClient(nbsId)
	if nfsClient != nil {
		_, err := nfsClient.StopEndpoint(ctx, &nfsapi.TStopEndpointRequest{
			SocketPath: filepath.Join(endpointDir, nfsSocketName),
		})
		if err != nil {
			return fmt.Errorf("failed to stop nfs endpoint (%T): %w", nfsClient, err)
		}
	}

	if err := os.RemoveAll(endpointDir); err != nil {
		return err
	}

	// remove pod's folder if it's empty
	ignoreError(os.Remove(s.getEndpointDir(podId, "")))
	return nil
}

func (s *nodeService) getEndpointDir(instanceId string, nbsId string) string {
	return filepath.Join(s.socketsDir, instanceId, nbsId)
}

func (s *nodeService) mountSocketDir(sourcePath string, req *csi.NodePublishVolumeRequest) error {

	mounted, _ := s.mounter.IsMountPoint(req.TargetPath)
	if !mounted {
		targetPerm := os.FileMode(0775)
		if err := os.MkdirAll(req.TargetPath, targetPerm); err != nil {
			return fmt.Errorf("failed to create target directory: %w", err)
		}

		if err := os.Chmod(req.TargetPath, targetPerm); err != nil {
			return fmt.Errorf("failed to chmod target path: %w", err)
		}
	}

	mountOptions := []string{"bind"}
	mnt := req.VolumeCapability.GetMount()
	if mnt != nil {
		for _, flag := range mnt.MountFlags {
			mountOptions = append(mountOptions, flag)
		}
	}
	if req.Readonly {
		mountOptions = append(mountOptions, "ro")
	}

	nbsId := req.VolumeId
	err := s.mountIfNeeded(
		nbsId,
		sourcePath,
		req.TargetPath,
		"",
		mountOptions)
	if err != nil {
		return fmt.Errorf("failed to mount: %w", err)
	}

	return nil
}

func (s *nodeService) nodeUnstageVhostSocket(
	ctx context.Context,
	nbsId string,
	stageData *StageData) error {

	log.Printf("csi.nodeUnstageVhostSocket[%s]: %s %s %s", stageData.Backend, stageData.InstanceId,
		nbsId, stageData.RealStagePath)

	if stageData.Backend == "nbs" {
		_, err := s.nbsClient.StopEndpoint(ctx, &nbsapi.TStopEndpointRequest{
			UnixSocketPath: filepath.Join(stageData.RealStagePath, nbsSocketName),
		})
		if err != nil {
			return fmt.Errorf("failed to stop nbs endpoint: %w", err)
		}
	} else if stageData.Backend == "nfs" {
		nfsClient := s.getNfsClient(nbsId)

		if nfsClient == nil {
			return fmt.Errorf("NFS client wasn't created")
		}

		_, err := nfsClient.StopEndpoint(ctx, &nfsapi.TStopEndpointRequest{
			SocketPath: filepath.Join(stageData.RealStagePath, nfsSocketName),
		})
		if err != nil {
			return fmt.Errorf("failed to stop nfs endpoint (%T): %w", nfsClient, err)
		}
	}

	if err := os.RemoveAll(stageData.RealStagePath); err != nil {
		return err
	}

	// remove staging folder if it's empty
	ignoreError(os.Remove(s.getEndpointDir(stageData.InstanceId, "")))
	return nil
}

func (s *nodeService) createDummyImgFile(dirPath string) error {
	// https://kubevirt.io/user-guide/virtual_machines/disks_and_volumes/#persistentvolumeclaim
	// "If the disk.img image file has not been created manually before starting a VM
	// then it will be created automatically with the PersistentVolumeClaim size."
	// So, let's create an empty disk.img to avoid automatic creation and save disk space.
	diskImgPath := filepath.Join(dirPath, "disk.img")
	file, err := os.OpenFile(diskImgPath, os.O_CREATE, os.FileMode(0644))
	if err != nil {
		return fmt.Errorf("failed to create disk.img: %w", err)
	}
	ignoreError(file.Close())

	return nil
}

func (s *nodeService) mountBlockDevice(
	diskId string,
	source string,
	target string,
	readOnly bool) error {

	mounted, _ := s.mounter.IsMountPoint(target)
	if !mounted {
		if err := os.MkdirAll(filepath.Dir(target), os.FileMode(0750)); err != nil {
			return fmt.Errorf("failed to create target directory: %w", err)
		}

		targetPerm := os.FileMode(0660)
		file, err := os.OpenFile(target, os.O_CREATE, targetPerm)
		if err != nil {
			return fmt.Errorf("failed to create target file: %w", err)
		}
		ignoreError(file.Close())

		if err := os.Chmod(target, targetPerm); err != nil {
			return fmt.Errorf("failed to chmod target path: %w", err)
		}
	}

	mountOptions := []string{"bind"}
	if readOnly {
		mountOptions = append(mountOptions, "ro")
	}
	err := s.mountIfNeeded(diskId, source, target, "", mountOptions)
	if err != nil {
		return fmt.Errorf("failed to mount: %w", err)
	}

	return nil
}

func (s *nodeService) mountIfNeeded(
	nbsId string,
	source string,
	target string,
	fsType string,
	options []string) error {

	mounted, err := s.mounter.IsMountPoint(target)
	if err != nil {
		return err
	}

	if mounted {
		logVolume(nbsId, "target path %q is already mounted", target)
		return nil
	}

	logVolume(nbsId, "mount source %q to target %q, fsType: %q, options: %v",
		source, target, fsType, options)
	return s.mounter.Mount(source, target, fsType, options)
}

func (s *nodeService) formatAndMount(
	nbsId string,
	source string,
	target string,
	fsType string,
	options []string) error {

	mounted, err := s.mounter.IsMountPoint(target)
	if err != nil {
		return err
	}

	if mounted {
		logVolume(nbsId, "target path %q is already mounted", target)
		return nil
	}

	logVolume(nbsId, "mount source %q to target %q, fsType: %q, options: %v",
		source, target, fsType, options)
	return s.mounter.FormatAndMount(source, target, fsType, options)
}

func (s *nodeService) makeFilesystemIfNeeded(
	diskId string,
	deviceName string,
	fsType string) error {

	existed, err := s.mounter.IsFilesystemExisted(deviceName)
	if err != nil {
		return err
	}

	if existed {
		logVolume(diskId, "filesystem exists on device: %q", deviceName)
		return nil
	}

	logVolume(diskId, "making filesystem %q on device %q", fsType, deviceName)
	out, err := s.mounter.MakeFilesystem(deviceName, fsType)
	if err != nil {
		return fmt.Errorf("failed to make filesystem: %w, output %q", err, out)
	}

	logVolume(diskId, "succeeded making filesystem: %q", out)
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

	podId := matches[1]
	pvcId := matches[2]
	return podId, pvcId, nil
}

func (s *nodeService) parseBlkTargetPath(targetPath string) (string, string, error) {
	matches := s.targetBlkPathRegexp.FindStringSubmatch(targetPath)

	if len(matches) <= 2 {
		return "", "", fmt.Errorf("failed to parse TargetPath: %q", targetPath)
	}

	pvcId := matches[1]
	podId := matches[2]
	return podId, pvcId, nil
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
	return status.Error(c, fmt.Sprintf("[n=%s]: %s", s.nodeId, msg))
}

func (s *nodeService) statusErrorf(c codes.Code, format string, a ...interface{}) error {
	msg := fmt.Sprintf(format, a...)
	return s.statusError(c, msg)
}

func logVolume(nbsId string, format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	log.Printf("[v=%s]: %s", nbsId, msg)
}

func (s *nodeService) NodeGetVolumeStats(
	_ context.Context,
	req *csi.NodeGetVolumeStatsRequest) (
	*csi.NodeGetVolumeStatsResponse, error) {

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

	if _, opInProgress := s.volumeOps.LoadOrStore(req.VolumeId, nil); opInProgress {
		return nil, s.statusErrorf(codes.Aborted, volumeOperationInProgress, req.VolumeId)
	}
	defer s.volumeOps.Delete(req.VolumeId)

	if s.vmMode {
		return nil, fmt.Errorf("NodeGetVolumeStats is not supported in vmMode")
	}

	if s.nbsClient == nil {
		return nil, fmt.Errorf("NBS client is not available")
	}

	mounted, err := s.mounter.IsMountPoint(req.VolumePath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
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

func (s *nodeService) nodeExpandVolumeVmMode(
	ctx context.Context,
	req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {

	// support expand volume only for volumes with instance id
	diskId, _ := parseVolumeId(req.VolumeId)

	stageRecordPath := filepath.Join(req.StagingTargetPath, diskId+".json")
	stageData, err := s.readStageData(stageRecordPath)
	if err != nil || stageData.InstanceId == "" {
		return nil, s.statusErrorf(
			codes.NotFound,
			"NodeExpandVolume is not supported for volumes without instance id")
	}

	if stageData.Backend == "nfs" {
		// expanding volumes with nfs backend works without refresh endpoint
		return &csi.NodeExpandVolumeResponse{
			CapacityBytes: req.CapacityRange.RequiredBytes,
		}, nil
	}

	if s.nbsClient == nil {
		return nil, fmt.Errorf("NodeExpandVolume is not supported")
	}

	resp, err := s.nbsClient.DescribeVolume(
		ctx, &nbsapi.TDescribeVolumeRequest{
			DiskId: diskId,
		},
	)

	if err != nil {
		if nbsclient.IsDiskNotFoundError(err) {
			return nil, s.statusError(
				codes.NotFound,
				"Volume is not found")
		}

		log.Printf("Failed to describe volume %v", err)
		return nil, s.statusErrorf(
			codes.Internal,
			"Failed to describe volume %v", err)
	}

	volumeSize := int64(resp.Volume.BlocksCount * uint64(resp.Volume.BlockSize))
	if req.CapacityRange.RequiredBytes > volumeSize {
		return nil, s.statusError(
			codes.OutOfRange,
			"Requested size is more than volume size")
	}

	endpointDir := s.getEndpointDir(stageData.InstanceId, diskId)
	_, err = s.nbsClient.RefreshEndpoint(ctx, &nbsapi.TRefreshEndpointRequest{
		UnixSocketPath: filepath.Join(endpointDir, nbsSocketName),
	})

	if err != nil {
		log.Printf("Failed to resize device %v", err)
		return nil, s.statusErrorf(
			codes.Internal,
			"Failed to resize device %v", err)
	}

	return &csi.NodeExpandVolumeResponse{
		CapacityBytes: int64(resp.Volume.BlocksCount * uint64(resp.Volume.BlockSize)),
	}, nil
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

	if _, opInProgress := s.volumeOps.LoadOrStore(req.VolumeId, nil); opInProgress {
		return nil, s.statusErrorf(codes.Aborted, volumeOperationInProgress, req.VolumeId)
	}
	defer s.volumeOps.Delete(req.VolumeId)

	if s.vmMode {
		return s.nodeExpandVolumeVmMode(ctx, req)
	}

	if s.nbsClient == nil {
		return nil, fmt.Errorf("NodeExpandVolume is not supported")
	}

	diskId := req.VolumeId
	resp, err := s.nbsClient.DescribeVolume(
		ctx, &nbsapi.TDescribeVolumeRequest{
			DiskId: diskId,
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
	if newBlocksCount < resp.Volume.BlocksCount {
		return nil, status.Error(
			codes.InvalidArgument,
			"New blocks count is less than current blocks count value")
	}

	podId, err := s.parsePodId(req.VolumePath)
	if err != nil {
		return nil, err
	}

	endpointDirOld := s.getEndpointDir(podId, diskId)
	unixSocketPathOld := filepath.Join(endpointDirOld, nbsSocketName)

	endpointDirNew := s.getEndpointDir("", diskId)
	unixSocketPathNew := filepath.Join(endpointDirNew, nbsSocketName)

	listEndpointsResp, err := s.nbsClient.ListEndpoints(
		ctx, &nbsapi.TListEndpointsRequest{},
	)
	if err != nil {
		log.Printf("List endpoints failed %v", err)
		return nil, err
	}

	nbdDevicePath := ""
	unixSocketPath := ""
	for _, endpoint := range listEndpointsResp.Endpoints {
		// Fallback to previous implementation for already mounted volumes
		// Must be removed after migration of all endpoints to the new format
		if endpoint.UnixSocketPath == unixSocketPathOld {
			nbdDevicePath = endpoint.GetNbdDeviceFile()
			unixSocketPath = unixSocketPathOld
			break
		}

		if endpoint.UnixSocketPath == unixSocketPathNew {
			nbdDevicePath = endpoint.GetNbdDeviceFile()
			unixSocketPath = unixSocketPathNew
			break
		}
	}

	if nbdDevicePath == "" {
		return nil, status.Error(
			codes.Internal,
			"Failed to determine NBD Device filename")
	}

	log.Printf("Resize volume id %v blocks count %v", diskId, newBlocksCount)
	_, err = s.nbsClient.ResizeVolume(ctx, &nbsapi.TResizeVolumeRequest{
		DiskId:        diskId,
		BlocksCount:   newBlocksCount,
		ConfigVersion: resp.Volume.ConfigVersion,
	})

	if err != nil {
		log.Printf("Resize volume failed %v", err)
		return nil, err
	}

	_, err = s.nbsClient.RefreshEndpoint(ctx, &nbsapi.TRefreshEndpointRequest{
		UnixSocketPath: unixSocketPath,
	})

	if err != nil {
		log.Printf("Resize device failed %v", err)
		return nil, s.statusErrorf(
			codes.Internal,
			"Failed to resize device %v", err)
	}

	if s.isMountAccessType(req.VolumePath) {
		needResize, err := s.mounter.NeedResize(nbdDevicePath, req.VolumePath)
		if err != nil {
			return nil, s.statusErrorf(
				codes.Internal,
				"NeedResize failed %v", err)
		}

		if needResize {
			_, err := s.mounter.Resize(nbdDevicePath, req.VolumePath)
			if err != nil {
				return nil, s.statusErrorf(
					codes.FailedPrecondition,
					"Failed to resize filesystem %v", err)
			}
		}
	}

	return &csi.NodeExpandVolumeResponse{
		CapacityBytes: int64(newBlocksCount * uint64(resp.Volume.BlockSize)),
	}, nil
}

func ignoreError(_ error) {}
