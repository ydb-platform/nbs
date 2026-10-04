package driver

import (
	"context"
	"log"
	"math"
	"sync"

	"github.com/container-storage-interface/spec/lib/go/csi"
	nbsapi "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	nbsclient "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	nfsapi "github.com/ydb-platform/nbs/cloud/filestore/public/api/protos"
	nfsclient "github.com/ydb-platform/nbs/cloud/filestore/public/sdk/go/client"
	storagecoreapi "github.com/ydb-platform/nbs/cloud/storage/core/protos"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

////////////////////////////////////////////////////////////////////////////////

const diskBlockSize uint32 = 4 * 1024

var nbsServerControllerServiceCapabilities = []*csi.ControllerServiceCapability{
	{
		Type: &csi.ControllerServiceCapability_Rpc{
			Rpc: &csi.ControllerServiceCapability_RPC{
				Type: csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
			},
		},
	},
}

////////////////////////////////////////////////////////////////////////////////

type nbsServerControllerService struct {
	csi.ControllerServer

	nbsClient     nbsclient.ClientIface
	nfsClient     nfsclient.ClientIface
	vmMode        bool
	offlineResize bool
	volumeOps     sync.Map
}

func newNBSServerControllerService(
	nbsClient nbsclient.ClientIface,
	nfsClient nfsclient.ClientIface,
	vmMode bool,
	offlineResize bool) csi.ControllerServer {

	return &nbsServerControllerService{
		nbsClient:     nbsClient,
		nfsClient:     nfsClient,
		vmMode:        vmMode,
		offlineResize: offlineResize,
	}
}

func (c *nbsServerControllerService) CreateVolume(
	ctx context.Context,
	req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {

	log.Printf("csi.CreateVolumeRequest: %+v", req)

	if req.Name == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"Name missing in CreateVolumeRequest")
	}

	if req.VolumeCapabilities == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"VolumeCapabilities missing in CreateVolumeRequest")
	}

	var requiredBytes int64 = int64(diskBlockSize)
	if req.CapacityRange != nil {
		if req.CapacityRange.RequiredBytes < 0 {
			return nil, status.Error(
				codes.InvalidArgument,
				"RequiredBytes must not be negative in CreateVolumeRequest")
		}
		requiredBytes = req.CapacityRange.RequiredBytes
	}

	if uint64(requiredBytes)%uint64(diskBlockSize) != 0 {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"incorrect value: required bytes %d, block size: %d",
			requiredBytes,
			diskBlockSize,
		)
	}

	parameters := req.Parameters
	if parameters == nil {
		parameters = make(map[string]string)
	}

	var err error
	if parameters["backend"] == "nfs" {
		err = c.createFileStore(ctx, req.Name, requiredBytes, parameters)
		// TODO (issues/464): return codes.AlreadyExists if volume exists
	} else {
		err = c.createDisk(ctx, req.Name, requiredBytes, parameters)
		if err != nil {
			describeVolumeRequest := &nbsapi.TDescribeVolumeRequest{
				DiskId: req.Name,
			}
			_, describeVolumeErr := c.nbsClient.DescribeVolume(
				ctx,
				describeVolumeRequest)
			if describeVolumeErr == nil {
				return nil, status.Errorf(
					codes.AlreadyExists,
					"Failed to create volume: %v", describeVolumeErr)
			}
		}
	}

	if err != nil {
		return nil, status.Errorf(
			codes.Internal, "Failed to create volume: %v", err)
	}

	return &csi.CreateVolumeResponse{Volume: &csi.Volume{
		CapacityBytes: requiredBytes,
		VolumeId:      req.Name,
		VolumeContext: parameters,
	}}, nil
}

func (c *nbsServerControllerService) createDisk(
	ctx context.Context,
	diskId string,
	requiredBytes int64,
	parameters map[string]string) error {

	_, err := c.nbsClient.CreateVolume(ctx, &nbsapi.TCreateVolumeRequest{
		DiskId:               diskId,
		BlockSize:            diskBlockSize,
		BlocksCount:          uint64(requiredBytes) / uint64(diskBlockSize),
		StorageMediaKind:     getStorageMediaKind(parameters),
		FolderId:             parameters["csi.storage.k8s.io/pvc/namespace"],
		CloudId:              parameters["csi.storage.k8s.io/pvc/namespace"],
		BaseDiskId:           parameters["base-disk-id"],
		BaseDiskCheckpointId: parameters["base-disk-checkpoint-id"],
	})
	return err
}

func (c *nbsServerControllerService) createFileStore(
	ctx context.Context,
	fileSystemId string,
	requiredBytes int64,
	parameters map[string]string) error {

	if c.nfsClient == nil {
		return status.Errorf(codes.Internal, "NFS client wasn't created")
	}

	_, err := c.nfsClient.CreateFileStore(ctx, &nfsapi.TCreateFileStoreRequest{
		FileSystemId:     fileSystemId,
		CloudId:          parameters["csi.storage.k8s.io/pvc/namespace"],
		FolderId:         parameters["csi.storage.k8s.io/pvc/namespace"],
		BlockSize:        diskBlockSize,
		BlocksCount:      uint64(requiredBytes) / uint64(diskBlockSize),
		StorageMediaKind: storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_SSD,
	})
	return err
}

func (c *nbsServerControllerService) DeleteVolume(
	ctx context.Context,
	req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {

	log.Printf("csi.DeleteVolumeRequest: %+v", req)

	if req.VolumeId == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"VolumeId missing in DeleteVolumeRequest")
	}

	// Trying to destroy both disk and filestore,
	// because the resource's type is unknown here.
	// When we miss we get S_FALSE/S_ALREADY code (err == nil).

	if c.nbsClient != nil {
		_, err := c.nbsClient.DestroyVolume(ctx, &nbsapi.TDestroyVolumeRequest{
			DiskId: req.VolumeId,
		})
		if err != nil {
			return nil, status.Errorf(
				codes.Internal,
				"Failed to destroy disk: %v", err)
		}
	}

	if c.nfsClient != nil {
		_, err := c.nfsClient.DestroyFileStore(ctx, &nfsapi.TDestroyFileStoreRequest{
			FileSystemId: req.VolumeId,
		})
		if err != nil {
			return nil, status.Errorf(
				codes.Internal,
				"Failed to destroy filestore: %v", err)
		}
	}

	return &csi.DeleteVolumeResponse{}, nil
}

func (c *nbsServerControllerService) ValidateVolumeCapabilities(
	ctx context.Context,
	req *csi.ValidateVolumeCapabilitiesRequest,
) (*csi.ValidateVolumeCapabilitiesResponse, error) {

	log.Printf("csi.ValidateVolumeCapabilities: %+v", req)

	if req.VolumeId == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"VolumeId missing in ValidateVolumeCapabilitiesRequest")
	}
	if req.VolumeCapabilities == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"VolumeCapabilities missing in ValidateVolumeCapabilitiesRequest")
	}

	describeVolumeRequest := &nbsapi.TDescribeVolumeRequest{
		DiskId: req.VolumeId,
	}
	_, err := c.nbsClient.DescribeVolume(ctx, describeVolumeRequest)
	if err != nil {
		if nbsclient.IsDiskNotFoundError(err) {
			return nil, status.Errorf(
				codes.NotFound, "Volume %q does not exist", req.VolumeId)
		}

		return nil, status.Errorf(
			codes.Internal, "Failed to validate volume capabilities: %v", err)
	}

	return &csi.ValidateVolumeCapabilitiesResponse{}, nil
}

func (c *nbsServerControllerService) ControllerGetCapabilities(
	ctx context.Context,
	req *csi.ControllerGetCapabilitiesRequest,
) (*csi.ControllerGetCapabilitiesResponse, error) {

	capabilities := append([]*csi.ControllerServiceCapability{}, nbsServerControllerServiceCapabilities...)
	if !c.vmMode && c.offlineResize {
		capabilities = append(capabilities, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
				},
			},
		})
	}
	return &csi.ControllerGetCapabilitiesResponse{Capabilities: capabilities}, nil
}

func (c *nbsServerControllerService) ControllerExpandVolume(
	ctx context.Context,
	req *csi.ControllerExpandVolumeRequest,
) (*csi.ControllerExpandVolumeResponse, error) {
	log.Printf("csi.ControllerExpandVolume: %+v", req)

	if c.vmMode || !c.offlineResize {
		return nil, status.Error(codes.Unimplemented, "Controller expansion is only supported in offline pod mode")
	}
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "VolumeId is missing in ControllerExpandVolumeRequest")
	}
	capacityRange := req.GetCapacityRange()
	if capacityRange == nil || capacityRange.RequiredBytes < 0 || capacityRange.LimitBytes < 0 ||
		(capacityRange.RequiredBytes == 0 && capacityRange.LimitBytes == 0) ||
		(capacityRange.LimitBytes != 0 && capacityRange.RequiredBytes > capacityRange.LimitBytes) {
		return nil, status.Error(codes.InvalidArgument, "Invalid CapacityRange in ControllerExpandVolumeRequest")
	}
	if c.nbsClient == nil {
		return nil, status.Error(codes.Unimplemented, "NBS controller expansion is not configured")
	}

	if _, opInProgress := c.volumeOps.LoadOrStore(req.VolumeId, nil); opInProgress {
		return nil, status.Errorf(codes.Aborted, volumeOperationInProgress, req.VolumeId)
	}
	defer c.volumeOps.Delete(req.VolumeId)

	statResp, err := c.nbsClient.StatVolume(ctx, &nbsapi.TStatVolumeRequest{
		DiskId: req.VolumeId, NoPartition: true,
	})
	if err != nil {
		return nil, volumeExpansionError("Stat volume before resize", err)
	}
	volume := statResp.GetVolume()
	blockSize := uint64(volume.GetBlockSize())
	if blockSize == 0 || volume.GetBlocksCount() > uint64(math.MaxInt64)/blockSize {
		return nil, status.Error(codes.Internal, "Invalid volume capacity or block size")
	}

	// Round up using integer arithmetic, including capacities above 2^53.
	blocksCount := uint64(capacityRange.RequiredBytes) / blockSize
	if uint64(capacityRange.RequiredBytes)%blockSize != 0 {
		blocksCount++
	}
	if blocksCount < volume.BlocksCount {
		blocksCount = volume.BlocksCount
	}
	if blocksCount > uint64(math.MaxInt64)/blockSize {
		return nil, status.Error(codes.OutOfRange, "Requested capacity exceeds the supported range")
	}
	capacityBytes := int64(blocksCount * blockSize)
	if capacityRange.LimitBytes != 0 && capacityBytes > capacityRange.LimitBytes {
		return nil, status.Error(codes.OutOfRange, "Volume capacity exceeds LimitBytes")
	}
	response := &csi.ControllerExpandVolumeResponse{
		CapacityBytes: capacityBytes,
		// Staging creates an endpoint with the new capacity and expands the filesystem.
		NodeExpansionRequired: false,
	}
	// A retry may arrive after the volume has been staged again. No backend
	// mutation is needed, so clients must not prevent an idempotent success.
	if blocksCount == volume.BlocksCount {
		return response, nil
	}

	if len(statResp.GetClients()) != 0 {
		return nil, status.Errorf(codes.FailedPrecondition,
			"Cannot resize volume %s with clients; unstage the volume first", req.VolumeId)
	}

	_, err = c.nbsClient.ResizeVolume(ctx, &nbsapi.TResizeVolumeRequest{
		DiskId: req.VolumeId, BlocksCount: blocksCount, ConfigVersion: volume.ConfigVersion,
	})
	if err != nil {
		return nil, volumeExpansionError("Resize volume", err)
	}
	return response, nil
}

func volumeExpansionError(operation string, err error) error {
	code := getGrpcErrorCode(err)
	if nbsclient.IsDiskNotFoundError(err) {
		code = codes.NotFound
	}
	return status.Errorf(code, "%s failed: %v", operation, err)
}
