package driver

import (
	"context"
	"log"

	"github.com/container-storage-interface/spec/lib/go/csi"
	nbsblockstorepublicapi "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	nbsclient "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	nbsstoragecoreapi "github.com/ydb-platform/nbs/cloud/storage/core/protos"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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

type nbsServerControllerService struct {
	csi.ControllerServer

	nbsClient nbsclient.ClientIface
}

func newNBSServerControllerService(nbsClient nbsclient.ClientIface) csi.ControllerServer {
	return &nbsServerControllerService{nbsClient: nbsClient}
}

func (c *nbsServerControllerService) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	log.Printf("csi.CreateVolumeRequest: %+v", req)

	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "Name missing in CreateVolumeRequest)")
	}

	caps := req.GetVolumeCapabilities()
	if caps == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capabilities missing in request")
	}

	var requiredBytes int64 = 16 * (1 << (10 * 3))
	if req.CapacityRange != nil {
		requiredBytes = req.CapacityRange.RequiredBytes
	}

	if uint64(requiredBytes)%uint64(diskBlockSize) != 0 {
		return nil, status.Errorf(codes.InvalidArgument,
			"incorrect value: required bytes %d, block size: %d", requiredBytes, diskBlockSize,
		)
	}

	volumeId := "nbs-" + req.Name

	createVolumeRequest := &nbsblockstorepublicapi.TCreateVolumeRequest{
		DiskId:           volumeId,
		BlockSize:        diskBlockSize,
		BlocksCount:      uint64(requiredBytes) / uint64(diskBlockSize),
		StorageMediaKind: nbsstoragecoreapi.EStorageMediaKind_STORAGE_MEDIA_SSD,
	}
	log.Printf("Creating volume: %+v", createVolumeRequest)
	createVolumeResponse, err := c.nbsClient.CreateVolume(ctx, createVolumeRequest)
	if err != nil {
		log.Printf("Failed to create volume: %+v", err)
		// TODO: choose error code
		return nil, status.Errorf(codes.AlreadyExists, "Failed to create volume: %+v", err)
	}
	log.Printf("Volume created: %+v", createVolumeResponse)

	return &csi.CreateVolumeResponse{Volume: &csi.Volume{
		CapacityBytes: requiredBytes,
		VolumeId:      volumeId,
	}}, nil
}

func (c *nbsServerControllerService) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	log.Printf("csi.DeleteVolumeRequest: %+v", req)

	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "VolumeId missing in DeleteVolumeRequest)")
	}

	// TODO: check if volume exists
	//c.nbsClient.DescribeVolume()

	destroyVolumeRequest := &nbsblockstorepublicapi.TDestroyVolumeRequest{
		DiskId: req.VolumeId,
	}
	log.Printf("Destroying Volume: %+v", destroyVolumeRequest)
	destroyVolumeResponse, err := c.nbsClient.DestroyVolume(ctx, destroyVolumeRequest)
	if err != nil {
		log.Printf("Failed to destroy volume: %+v", err)
		// return nil, err
	}
	log.Printf("Volume destroyed: %+v", destroyVolumeResponse)

	return &csi.DeleteVolumeResponse{}, nil
}

func (c *nbsServerControllerService) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	log.Printf("csi.ControllerPublishVolumeRequest: %+v", req)

	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "VolumeId missing in ControllerPublishVolumeRequest)")
	}
	if req.NodeId == "" {
		return nil, status.Error(codes.InvalidArgument, "NodeId missing in ControllerPublishVolumeRequest)")
	}
	if req.VolumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "VolumeCapability missing in ControllerPublishVolumeRequest)")
	}

	if !c.doesVolumeExist(ctx, req.VolumeId) {
		return nil, status.Errorf(codes.NotFound, "Volume %q does not exist", req.VolumeId)
	}

	// TODO: check if node exists
	if req.NodeId != "my-node" {
		return nil, status.Errorf(codes.NotFound, "Node %d does not exist", req.NodeId)
	}

	return &csi.ControllerPublishVolumeResponse{}, nil
}

func (c *nbsServerControllerService) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	log.Printf("csi.ControllerUnpublishVolumeRequest: %+v", req)

	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "VolumeId missing in ControllerUnpublishVolumeRequest)")
	}

	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (c *nbsServerControllerService) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	log.Printf("csi.ValidateVolumeCapabilities: %+v", req)

	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "VolumeId missing in ValidateVolumeCapabilitiesRequest)")
	}
	if req.VolumeCapabilities == nil {
		return nil, status.Error(codes.InvalidArgument, "VolumeCapabilities missing in ValidateVolumeCapabilitiesRequest")
	}

	if !c.doesVolumeExist(ctx, req.VolumeId) {
		return nil, status.Errorf(codes.NotFound, "Volume %q does not exist", req.VolumeId)
	}

	return &csi.ValidateVolumeCapabilitiesResponse{}, nil
}

func (c *nbsServerControllerService) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	log.Printf("csi.ControllerGetCapabilitiesRequest: %+v", req)

	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: nbsServerControllerServiceCapabilities,
	}, nil
}

func (c *nbsServerControllerService) doesVolumeExist(ctx context.Context, volumeId string) bool {
	describeVolumeRequest := &nbsblockstorepublicapi.TDescribeVolumeRequest{
		DiskId: volumeId,
	}
	log.Printf("Describing volume: %+v", describeVolumeRequest)
	_, err := c.nbsClient.DescribeVolume(ctx, describeVolumeRequest)
	if err != nil {
		log.Printf("Failed to describe volume: %+v", err)
		// TODO: check error code
		return false
	}
	return true
}
