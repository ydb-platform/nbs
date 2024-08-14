package driver

import (
	"context"
	"log"

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
	{
		Type: &csi.ControllerServiceCapability_Rpc{
			Rpc: &csi.ControllerServiceCapability_RPC{
				Type: csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
			},
		},
	},
}

func getStorageMediaKind(parameters map[string]string) storagecoreapi.EStorageMediaKind {
	kind, ok := parameters["storage-media-kind"]
	if ok {
		switch kind {
		case "hdd":
			return storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_HDD
		case "hybrid":
			return storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_HDD
		case "ssd":
			return storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_SSD
		case "ssd_nonrepl":
			return storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_SSD_NONREPLICATED
		case "ssd_mirror2":
			return storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_SSD_MIRROR2
		case "ssd_mirror3":
			return storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_SSD_MIRROR3
		case "ssd_local":
			return storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_SSD_LOCAL
		case "hdd_nonrepl":
			return storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_HDD_NONREPLICATED
		}
	}

	return storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_SSD
}

////////////////////////////////////////////////////////////////////////////////

type nbsServerControllerService struct {
	csi.ControllerServer

	nbsClient nbsclient.ClientIface
	nfsClient nfsclient.ClientIface
}

func newNBSServerControllerService(
	nbsClient nbsclient.ClientIface,
	nfsClient nfsclient.ClientIface) csi.ControllerServer {

	return &nbsServerControllerService{
		nbsClient: nbsClient,
		nfsClient: nfsClient,
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
		err = c.createFileStore(ctx, req.Name, requiredBytes)
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
					"Failed to create volume: %w", describeVolumeErr)
			}
		}
	}

	if err != nil {
		return nil, status.Errorf(
			codes.Internal, "Failed to create volume: %w", err)
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
		BaseDiskId:           parameters["base-disk-id"],
		BaseDiskCheckpointId: parameters["base-disk-checkpoint-id"],
	})
	return err
}

func (c *nbsServerControllerService) createFileStore(
	ctx context.Context,
	fileSystemId string,
	requiredBytes int64) error {

	if c.nfsClient == nil {
		return status.Errorf(codes.Internal, "NFS client wasn't created")
	}

	_, err := c.nfsClient.CreateFileStore(ctx, &nfsapi.TCreateFileStoreRequest{
		FileSystemId:     fileSystemId,
		CloudId:          "fakeCloud",
		FolderId:         "fakeFolder",
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
				"Failed to destroy disk: %w", err)
		}
	}

	if c.nfsClient != nil {
		_, err := c.nfsClient.DestroyFileStore(ctx, &nfsapi.TDestroyFileStoreRequest{
			FileSystemId: req.VolumeId,
		})
		if err != nil {
			return nil, status.Errorf(
				codes.Internal,
				"Failed to destroy filestore: %w", err)
		}
	}

	return &csi.DeleteVolumeResponse{}, nil
}

func (c *nbsServerControllerService) ControllerPublishVolume(
	ctx context.Context,
	req *csi.ControllerPublishVolumeRequest,
) (*csi.ControllerPublishVolumeResponse, error) {

	log.Printf("csi.ControllerPublishVolumeRequest: %+v", req)

	if req.VolumeId == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"VolumeId missing in ControllerPublishVolumeRequest")
	}
	if req.NodeId == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"NodeId missing in ControllerPublishVolumeRequest")
	}
	if req.VolumeCapability == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"VolumeCapability missing in ControllerPublishVolumeRequest")
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
			codes.Internal, "Failed to publish volume: %w", err)
	}

	// TODO (issues/464): check if req.NodeId exists in the cluster

	return &csi.ControllerPublishVolumeResponse{}, nil
}

func (c *nbsServerControllerService) ControllerUnpublishVolume(
	ctx context.Context,
	req *csi.ControllerUnpublishVolumeRequest,
) (*csi.ControllerUnpublishVolumeResponse, error) {

	log.Printf("csi.ControllerUnpublishVolumeRequest: %+v", req)

	if req.VolumeId == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"VolumeId missing in ControllerUnpublishVolumeRequest")
	}

	return &csi.ControllerUnpublishVolumeResponse{}, nil
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
			codes.Internal, "Failed to validate volume capabilities: %w", err)
	}

	return &csi.ValidateVolumeCapabilitiesResponse{}, nil
}

func (c *nbsServerControllerService) ControllerGetCapabilities(
	ctx context.Context,
	req *csi.ControllerGetCapabilitiesRequest,
) (*csi.ControllerGetCapabilitiesResponse, error) {

	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: nbsServerControllerServiceCapabilities,
	}, nil
}
