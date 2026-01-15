package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
	protos "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	nbs "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
)

////////////////////////////////////////////////////////////////////////////////

type NbsClientMock struct {
	mock.Mock
}

func (c *NbsClientMock) Close() error {
	args := c.Called()
	return args.Error(0)
}

func (c *NbsClientMock) Ping(
	ctx context.Context,
	req *protos.TPingRequest,
) (*protos.TPingResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TPingResponse), args.Error(1)
}

func (c *NbsClientMock) CreateVolume(
	ctx context.Context,
	req *protos.TCreateVolumeRequest,
) (*protos.TCreateVolumeResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TCreateVolumeResponse), args.Error(1)
}

func (c *NbsClientMock) DestroyVolume(
	ctx context.Context,
	req *protos.TDestroyVolumeRequest,
) (*protos.TDestroyVolumeResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TDestroyVolumeResponse), args.Error(1)
}

func (c *NbsClientMock) ResizeVolume(
	ctx context.Context,
	req *protos.TResizeVolumeRequest,
) (*protos.TResizeVolumeResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TResizeVolumeResponse), args.Error(1)
}

func (c *NbsClientMock) AlterVolume(
	ctx context.Context,
	req *protos.TAlterVolumeRequest,
) (*protos.TAlterVolumeResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TAlterVolumeResponse), args.Error(1)
}

func (c *NbsClientMock) AssignVolume(
	ctx context.Context,
	req *protos.TAssignVolumeRequest,
) (*protos.TAssignVolumeResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TAssignVolumeResponse), args.Error(1)
}

func (c *NbsClientMock) StatVolume(
	ctx context.Context,
	req *protos.TStatVolumeRequest,
) (*protos.TStatVolumeResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TStatVolumeResponse), args.Error(1)
}

func (c *NbsClientMock) ListVolumes(
	ctx context.Context,
	req *protos.TListVolumesRequest,
) (*protos.TListVolumesResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TListVolumesResponse), args.Error(1)
}

func (c *NbsClientMock) ListDiskStates(
	ctx context.Context,
	req *protos.TListDiskStatesRequest,
) (*protos.TListDiskStatesResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TListDiskStatesResponse), args.Error(1)
}

func (c *NbsClientMock) DescribeVolume(
	ctx context.Context,
	req *protos.TDescribeVolumeRequest,
) (*protos.TDescribeVolumeResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TDescribeVolumeResponse), args.Error(1)
}

func (c *NbsClientMock) DescribeVolumeModel(
	ctx context.Context,
	req *protos.TDescribeVolumeModelRequest,
) (*protos.TDescribeVolumeModelResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TDescribeVolumeModelResponse), args.Error(1)
}

func (c *NbsClientMock) MountVolume(
	ctx context.Context,
	req *protos.TMountVolumeRequest,
) (*protos.TMountVolumeResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TMountVolumeResponse), args.Error(1)
}

func (c *NbsClientMock) UnmountVolume(
	ctx context.Context,
	req *protos.TUnmountVolumeRequest,
) (*protos.TUnmountVolumeResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TUnmountVolumeResponse), args.Error(1)
}

func (c *NbsClientMock) ReadBlocks(
	ctx context.Context,
	req *protos.TReadBlocksRequest,
) (*protos.TReadBlocksResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TReadBlocksResponse), args.Error(1)
}

func (c *NbsClientMock) WriteBlocks(
	ctx context.Context,
	req *protos.TWriteBlocksRequest,
) (*protos.TWriteBlocksResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TWriteBlocksResponse), args.Error(1)
}

func (c *NbsClientMock) ZeroBlocks(
	ctx context.Context,
	req *protos.TZeroBlocksRequest,
) (*protos.TZeroBlocksResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TZeroBlocksResponse), args.Error(1)
}

func (c *NbsClientMock) StartEndpoint(
	ctx context.Context,
	req *protos.TStartEndpointRequest,
) (*protos.TStartEndpointResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TStartEndpointResponse), args.Error(1)
}

func (c *NbsClientMock) StopEndpoint(
	ctx context.Context,
	req *protos.TStopEndpointRequest,
) (*protos.TStopEndpointResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TStopEndpointResponse), args.Error(1)
}

func (c *NbsClientMock) ListEndpoints(
	ctx context.Context,
	req *protos.TListEndpointsRequest,
) (*protos.TListEndpointsResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TListEndpointsResponse), args.Error(1)
}

func (c *NbsClientMock) KickEndpoint(
	ctx context.Context,
	req *protos.TKickEndpointRequest,
) (*protos.TKickEndpointResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TKickEndpointResponse), args.Error(1)
}

func (c *NbsClientMock) ListKeyrings(
	ctx context.Context,
	req *protos.TListKeyringsRequest,
) (*protos.TListKeyringsResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TListKeyringsResponse), args.Error(1)
}

func (c *NbsClientMock) DescribeEndpoint(
	ctx context.Context,
	req *protos.TDescribeEndpointRequest,
) (*protos.TDescribeEndpointResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TDescribeEndpointResponse), args.Error(1)
}

func (c *NbsClientMock) RefreshEndpoint(
	ctx context.Context,
	req *protos.TRefreshEndpointRequest,
) (*protos.TRefreshEndpointResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TRefreshEndpointResponse), args.Error(1)
}

func (c *NbsClientMock) CreateCheckpoint(
	ctx context.Context,
	req *protos.TCreateCheckpointRequest,
) (*protos.TCreateCheckpointResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TCreateCheckpointResponse), args.Error(1)
}

func (c *NbsClientMock) GetCheckpointStatus(
	ctx context.Context,
	req *protos.TGetCheckpointStatusRequest,
) (*protos.TGetCheckpointStatusResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TGetCheckpointStatusResponse), args.Error(1)
}

func (c *NbsClientMock) DeleteCheckpoint(
	ctx context.Context,
	req *protos.TDeleteCheckpointRequest,
) (*protos.TDeleteCheckpointResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TDeleteCheckpointResponse), args.Error(1)
}

func (c *NbsClientMock) GetChangedBlocks(
	ctx context.Context,
	req *protos.TGetChangedBlocksRequest,
) (*protos.TGetChangedBlocksResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TGetChangedBlocksResponse), args.Error(1)
}

func (c *NbsClientMock) DiscoverInstances(
	ctx context.Context,
	req *protos.TDiscoverInstancesRequest,
) (*protos.TDiscoverInstancesResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TDiscoverInstancesResponse), args.Error(1)
}

func (c *NbsClientMock) QueryAvailableStorage(
	ctx context.Context,
	req *protos.TQueryAvailableStorageRequest,
) (*protos.TQueryAvailableStorageResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TQueryAvailableStorageResponse), args.Error(1)
}

func (c *NbsClientMock) ResumeDevice(
	ctx context.Context,
	req *protos.TResumeDeviceRequest,
) (*protos.TResumeDeviceResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TResumeDeviceResponse), args.Error(1)
}

func (c *NbsClientMock) CreateVolumeFromDevice(
	ctx context.Context,
	req *protos.TCreateVolumeFromDeviceRequest,
) (*protos.TCreateVolumeFromDeviceResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TCreateVolumeFromDeviceResponse), args.Error(1)
}

func (c *NbsClientMock) ExecuteAction(
	ctx context.Context,
	req *protos.TExecuteActionRequest,
) (*protos.TExecuteActionResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TExecuteActionResponse), args.Error(1)
}

func (c *NbsClientMock) CreatePlacementGroup(
	ctx context.Context,
	req *protos.TCreatePlacementGroupRequest,
) (*protos.TCreatePlacementGroupResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TCreatePlacementGroupResponse), args.Error(1)
}

func (c *NbsClientMock) DestroyPlacementGroup(
	ctx context.Context,
	req *protos.TDestroyPlacementGroupRequest,
) (*protos.TDestroyPlacementGroupResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TDestroyPlacementGroupResponse), args.Error(1)
}

func (c *NbsClientMock) DescribePlacementGroup(
	ctx context.Context,
	req *protos.TDescribePlacementGroupRequest,
) (*protos.TDescribePlacementGroupResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TDescribePlacementGroupResponse), args.Error(1)
}

func (c *NbsClientMock) AlterPlacementGroupMembership(
	ctx context.Context,
	req *protos.TAlterPlacementGroupMembershipRequest,
) (*protos.TAlterPlacementGroupMembershipResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TAlterPlacementGroupMembershipResponse), args.Error(1)
}

func (c *NbsClientMock) ListPlacementGroups(
	ctx context.Context,
	req *protos.TListPlacementGroupsRequest,
) (*protos.TListPlacementGroupsResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TListPlacementGroupsResponse), args.Error(1)
}

func (c *NbsClientMock) CmsAction(
	ctx context.Context,
	req *protos.TCmsActionRequest,
) (*protos.TCmsActionResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TCmsActionResponse), args.Error(1)
}

func (c *NbsClientMock) QueryAgentsInfo(
	ctx context.Context,
	req *protos.TQueryAgentsInfoRequest,
) (*protos.TQueryAgentsInfoResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TQueryAgentsInfoResponse), args.Error(1)
}

////////////////////////////////////////////////////////////////////////////////

func NewNbsClientMock() *NbsClientMock {
	return &NbsClientMock{}
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that NbsClientMock implements Client.
func assertNbsClientMockIsClient(arg *NbsClientMock) nbs.ClientIface {
	return arg
}
