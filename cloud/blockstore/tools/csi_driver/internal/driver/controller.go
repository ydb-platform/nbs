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

	if len(req.GetName()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Name missing in request")
	}

	caps := req.GetVolumeCapabilities()
	if caps == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capabilities missing in request")
	}

	requiredBytes := req.CapacityRange.RequiredBytes
	if uint64(requiredBytes)%uint64(diskBlockSize) != 0 {
		return nil, status.Errorf(codes.InvalidArgument,
			"incorrect value: required bytes %d, block size: %d", requiredBytes, diskBlockSize,
		)
	}

	diskID := "nbs-" + req.Name
	// diskID, err := generateDiskID(req.Name)
	// if err != nil {
	// 	return nil, err
	// }

	// TODO: check if volume exists
	//c.nbsClient.DescribeVolume()

	createVolumeRequest := &nbsblockstorepublicapi.TCreateVolumeRequest{
		DiskId:           diskID,
		BlockSize:        diskBlockSize,
		BlocksCount:      uint64(requiredBytes) / uint64(diskBlockSize),
		StorageMediaKind: nbsstoragecoreapi.EStorageMediaKind_STORAGE_MEDIA_SSD,
	}
	log.Printf("Createing volume: %+v", createVolumeRequest)
	createVolumeResponse, err := c.nbsClient.CreateVolume(ctx, createVolumeRequest)
	if err != nil {
		log.Printf("Failed to create volume: %+v", err)
		// return nil, err
	}
	log.Printf("Volume created: %+v", createVolumeResponse)

	return &csi.CreateVolumeResponse{Volume: &csi.Volume{
		CapacityBytes: requiredBytes,
		VolumeId:      diskID,
	}}, nil
}

func (c *nbsServerControllerService) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	log.Printf("csi.DeleteVolumeRequest: %+v", req)

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

func (c *nbsServerControllerService) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	log.Printf("csi.ControllerGetCapabilitiesRequest: %+v", req)

	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: nbsServerControllerServiceCapabilities,
	}, nil
}
