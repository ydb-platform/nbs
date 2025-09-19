package client

import (
	protos "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	"golang.org/x/net/context"
)

////////////////////////////////////////////////////////////////////////////////

type ClientIface interface {
	//
	// Destroy client to free any resources allocated.
	//

	Close() error

	//
	// Service requests.
	//

	Ping(
		ctx context.Context,
		req *protos.TPingRequest,
	) (*protos.TPingResponse, error)

	//
	// High level volume management operations.
	//

	CreateVolume(
		ctx context.Context,
		req *protos.TCreateVolumeRequest,
	) (*protos.TCreateVolumeResponse, error)

	DestroyVolume(
		ctx context.Context,
		req *protos.TDestroyVolumeRequest,
	) (*protos.TDestroyVolumeResponse, error)

	ResizeVolume(
		ctx context.Context,
		req *protos.TResizeVolumeRequest,
	) (*protos.TResizeVolumeResponse, error)

	AlterVolume(
		ctx context.Context,
		req *protos.TAlterVolumeRequest,
	) (*protos.TAlterVolumeResponse, error)

	AssignVolume(
		ctx context.Context,
		req *protos.TAssignVolumeRequest,
	) (*protos.TAssignVolumeResponse, error)

	//
	// Volume statistics.
	//

	StatVolume(
		ctx context.Context,
		req *protos.TStatVolumeRequest,
	) (*protos.TStatVolumeResponse, error)

	ListVolumes(
		ctx context.Context,
		req *protos.TListVolumesRequest,
	) (*protos.TListVolumesResponse, error)

	ListDiskStates(
		ctx context.Context,
		req *protos.TListDiskStatesRequest,
	) (*protos.TListDiskStatesResponse, error)

	DescribeVolume(
		ctx context.Context,
		req *protos.TDescribeVolumeRequest,
	) (*protos.TDescribeVolumeResponse, error)

	DescribeVolumeModel(
		ctx context.Context,
		req *protos.TDescribeVolumeModelRequest,
	) (*protos.TDescribeVolumeModelResponse, error)

	//
	// Mount operations.
	//

	MountVolume(
		ctx context.Context,
		req *protos.TMountVolumeRequest,
	) (*protos.TMountVolumeResponse, error)

	UnmountVolume(
		ctx context.Context,
		req *protos.TUnmountVolumeRequest,
	) (*protos.TUnmountVolumeResponse, error)

	//
	// Block I/O.
	//

	ReadBlocks(
		ctx context.Context,
		req *protos.TReadBlocksRequest,
	) (*protos.TReadBlocksResponse, error)

	WriteBlocks(
		ctx context.Context,
		req *protos.TWriteBlocksRequest,
	) (*protos.TWriteBlocksResponse, error)

	ZeroBlocks(
		ctx context.Context,
		req *protos.TZeroBlocksRequest,
	) (*protos.TZeroBlocksResponse, error)

	//
	// Endpoint operations.
	//

	StartEndpoint(
		ctx context.Context,
		req *protos.TStartEndpointRequest,
	) (*protos.TStartEndpointResponse, error)

	StopEndpoint(
		ctx context.Context,
		req *protos.TStopEndpointRequest,
	) (*protos.TStopEndpointResponse, error)

	ListEndpoints(
		ctx context.Context,
		req *protos.TListEndpointsRequest,
	) (*protos.TListEndpointsResponse, error)

	KickEndpoint(
		ctx context.Context,
		req *protos.TKickEndpointRequest,
	) (*protos.TKickEndpointResponse, error)

	ListKeyrings(
		ctx context.Context,
		req *protos.TListKeyringsRequest,
	) (*protos.TListKeyringsResponse, error)

	DescribeEndpoint(
		ctx context.Context,
		req *protos.TDescribeEndpointRequest,
	) (*protos.TDescribeEndpointResponse, error)

	RefreshEndpoint(
		ctx context.Context,
		req *protos.TRefreshEndpointRequest,
	) (*protos.TRefreshEndpointResponse, error)

	//
	// Checkpoint operations.
	//

	CreateCheckpoint(
		ctx context.Context,
		req *protos.TCreateCheckpointRequest,
	) (*protos.TCreateCheckpointResponse, error)

	GetCheckpointStatus(
		ctx context.Context,
		req *protos.TGetCheckpointStatusRequest,
	) (*protos.TGetCheckpointStatusResponse, error)

	DeleteCheckpoint(
		ctx context.Context,
		req *protos.TDeleteCheckpointRequest,
	) (*protos.TDeleteCheckpointResponse, error)

	//
	// Support for differential backup.
	//

	GetChangedBlocks(
		ctx context.Context,
		req *protos.TGetChangedBlocksRequest,
	) (*protos.TGetChangedBlocksResponse, error)

	//
	// Instance discovery.
	//

	DiscoverInstances(
		ctx context.Context,
		req *protos.TDiscoverInstancesRequest,
	) (*protos.TDiscoverInstancesResponse, error)

	//
	// Local SSD
	//

	QueryAvailableStorage(
		ctx context.Context,
		req *protos.TQueryAvailableStorageRequest,
	) (*protos.TQueryAvailableStorageResponse, error)

	ResumeDevice(
		ctx context.Context,
		req *protos.TResumeDeviceRequest,
	) (*protos.TResumeDeviceResponse, error)

	CreateVolumeFromDevice(
		ctx context.Context,
		req *protos.TCreateVolumeFromDeviceRequest,
	) (*protos.TCreateVolumeFromDeviceResponse, error)

	//
	// Private API.
	//

	ExecuteAction(
		ctx context.Context,
		req *protos.TExecuteActionRequest,
	) (*protos.TExecuteActionResponse, error)

	//
	// Placement Group API.
	//

	CreatePlacementGroup(
		ctx context.Context,
		req *protos.TCreatePlacementGroupRequest,
	) (*protos.TCreatePlacementGroupResponse, error)

	DestroyPlacementGroup(
		ctx context.Context,
		req *protos.TDestroyPlacementGroupRequest,
	) (*protos.TDestroyPlacementGroupResponse, error)

	DescribePlacementGroup(
		ctx context.Context,
		req *protos.TDescribePlacementGroupRequest,
	) (*protos.TDescribePlacementGroupResponse, error)

	AlterPlacementGroupMembership(
		ctx context.Context,
		req *protos.TAlterPlacementGroupMembershipRequest,
	) (*protos.TAlterPlacementGroupMembershipResponse, error)

	ListPlacementGroups(
		ctx context.Context,
		req *protos.TListPlacementGroupsRequest,
	) (*protos.TListPlacementGroupsResponse, error)

	//
	// CMS API.
	//

	CmsAction(
		ctx context.Context,
		req *protos.TCmsActionRequest,
	) (*protos.TCmsActionResponse, error)

	//
	// Agents Info.
	//

	QueryAgentsInfo(
		ctx context.Context,
		req *protos.TQueryAgentsInfoRequest,
	) (*protos.TQueryAgentsInfoResponse, error)
}
