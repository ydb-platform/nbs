package client

import (
	protos "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	"golang.org/x/net/context"
)

////////////////////////////////////////////////////////////////////////////////

type pingHandlerFunc func(ctx context.Context, req *protos.TPingRequest) (*protos.TPingResponse, error)
type createVolumeHandlerFunc func(ctx context.Context, req *protos.TCreateVolumeRequest) (*protos.TCreateVolumeResponse, error)
type destroyVolumeHandlerFunc func(ctx context.Context, req *protos.TDestroyVolumeRequest) (*protos.TDestroyVolumeResponse, error)
type resizeVolumeHandlerFunc func(ctx context.Context, req *protos.TResizeVolumeRequest) (*protos.TResizeVolumeResponse, error)
type alterVolumeHandlerFunc func(ctx context.Context, req *protos.TAlterVolumeRequest) (*protos.TAlterVolumeResponse, error)
type assignVolumeHandlerFunc func(ctx context.Context, req *protos.TAssignVolumeRequest) (*protos.TAssignVolumeResponse, error)
type statVolumeHandlerFunc func(ctx context.Context, req *protos.TStatVolumeRequest) (*protos.TStatVolumeResponse, error)
type mountVolumeHandlerFunc func(ctx context.Context, req *protos.TMountVolumeRequest) (*protos.TMountVolumeResponse, error)
type unmountVolumeHandlerFunc func(ctx context.Context, req *protos.TUnmountVolumeRequest) (*protos.TUnmountVolumeResponse, error)
type readBlocksHandlerFunc func(ctx context.Context, req *protos.TReadBlocksRequest) (*protos.TReadBlocksResponse, error)
type writeBlocksHandlerFunc func(ctx context.Context, req *protos.TWriteBlocksRequest) (*protos.TWriteBlocksResponse, error)
type zeroBlocksHandlerFunc func(ctx context.Context, req *protos.TZeroBlocksRequest) (*protos.TZeroBlocksResponse, error)
type startEndpointHandlerFunc func(ctx context.Context, req *protos.TStartEndpointRequest) (*protos.TStartEndpointResponse, error)
type stopEndpointHandlerFunc func(ctx context.Context, req *protos.TStopEndpointRequest) (*protos.TStopEndpointResponse, error)
type listEndpointsHandlerFunc func(ctx context.Context, req *protos.TListEndpointsRequest) (*protos.TListEndpointsResponse, error)
type kickEndpointHandlerFunc func(ctx context.Context, req *protos.TKickEndpointRequest) (*protos.TKickEndpointResponse, error)
type listKeyringsHandlerFunc func(ctx context.Context, req *protos.TListKeyringsRequest) (*protos.TListKeyringsResponse, error)
type describeEndpointHandlerFunc func(ctx context.Context, req *protos.TDescribeEndpointRequest) (*protos.TDescribeEndpointResponse, error)
type refreshEndpointHandlerFunc func(ctx context.Context, req *protos.TRefreshEndpointRequest) (*protos.TRefreshEndpointResponse, error)
type createCheckpointHandlerFunc func(ctx context.Context, req *protos.TCreateCheckpointRequest) (*protos.TCreateCheckpointResponse, error)
type getCheckpointStatusHandlerFunc func(ctx context.Context, req *protos.TGetCheckpointStatusRequest) (*protos.TGetCheckpointStatusResponse, error)
type deleteCheckpointHandlerFunc func(ctx context.Context, req *protos.TDeleteCheckpointRequest) (*protos.TDeleteCheckpointResponse, error)
type getChangedBlocksHandlerFunc func(ctx context.Context, req *protos.TGetChangedBlocksRequest) (*protos.TGetChangedBlocksResponse, error)
type describeVolumeHandlerFunc func(ctx context.Context, req *protos.TDescribeVolumeRequest) (*protos.TDescribeVolumeResponse, error)
type describeVolumeModelHandlerFunc func(ctx context.Context, req *protos.TDescribeVolumeModelRequest) (*protos.TDescribeVolumeModelResponse, error)
type listVolumesHandlerFunc func(ctx context.Context, req *protos.TListVolumesRequest) (*protos.TListVolumesResponse, error)
type listDiskStatesHandlerFunc func(ctx context.Context, req *protos.TListDiskStatesRequest) (*protos.TListDiskStatesResponse, error)
type discoverInstancesHandlerFunc func(ctx context.Context, req *protos.TDiscoverInstancesRequest) (*protos.TDiscoverInstancesResponse, error)
type queryAvailableStorageHandler func(ctx context.Context, req *protos.TQueryAvailableStorageRequest) (*protos.TQueryAvailableStorageResponse, error)
type resumeDeviceHandler func(ctx context.Context, req *protos.TResumeDeviceRequest) (*protos.TResumeDeviceResponse, error)
type createVolumeFromDeviceHandler func(ctx context.Context, req *protos.TCreateVolumeFromDeviceRequest) (*protos.TCreateVolumeFromDeviceResponse, error)
type executeActionFunc func(ctx context.Context, req *protos.TExecuteActionRequest) (*protos.TExecuteActionResponse, error)
type createPlacementGroupHandlerFunc func(ctx context.Context, req *protos.TCreatePlacementGroupRequest) (*protos.TCreatePlacementGroupResponse, error)
type destroyPlacementGroupHandlerFunc func(ctx context.Context, req *protos.TDestroyPlacementGroupRequest) (*protos.TDestroyPlacementGroupResponse, error)
type describePlacementGroupHandlerFunc func(ctx context.Context, req *protos.TDescribePlacementGroupRequest) (*protos.TDescribePlacementGroupResponse, error)
type alterPlacementGroupMembershipHandlerFunc func(ctx context.Context, req *protos.TAlterPlacementGroupMembershipRequest) (*protos.TAlterPlacementGroupMembershipResponse, error)
type listPlacementGroupsHandlerFunc func(ctx context.Context, req *protos.TListPlacementGroupsRequest) (*protos.TListPlacementGroupsResponse, error)
type cmsActionHandlerFunc func(ctx context.Context, req *protos.TCmsActionRequest) (*protos.TCmsActionResponse, error)
type queryAgentsInfoHandler func(ctx context.Context, req *protos.TQueryAgentsInfoRequest) (*protos.TQueryAgentsInfoResponse, error)
type closeHandlerFunc func() error

////////////////////////////////////////////////////////////////////////////////

type testClient struct {
	PingHandler                          pingHandlerFunc
	CreateVolumeHandler                  createVolumeHandlerFunc
	DestroyVolumeHandler                 destroyVolumeHandlerFunc
	ResizeVolumeHandler                  resizeVolumeHandlerFunc
	AlterVolumeHandler                   alterVolumeHandlerFunc
	AssignVolumeHandler                  assignVolumeHandlerFunc
	StatVolumeHandler                    statVolumeHandlerFunc
	MountVolumeHandler                   mountVolumeHandlerFunc
	UnmountVolumeHandler                 unmountVolumeHandlerFunc
	ReadBlocksHandler                    readBlocksHandlerFunc
	WriteBlocksHandler                   writeBlocksHandlerFunc
	ZeroBlocksHandler                    zeroBlocksHandlerFunc
	StartEndpointHandler                 startEndpointHandlerFunc
	StopEndpointHandler                  stopEndpointHandlerFunc
	ListEndpointsHandler                 listEndpointsHandlerFunc
	KickEndpointHandler                  kickEndpointHandlerFunc
	ListKeyringsHandler                  listKeyringsHandlerFunc
	DescribeEndpointHandler              describeEndpointHandlerFunc
	RefreshEndpointHandler               refreshEndpointHandlerFunc
	CreateCheckpointHandler              createCheckpointHandlerFunc
	GetCheckpointStatusHandler           getCheckpointStatusHandlerFunc
	DeleteCheckpointHandler              deleteCheckpointHandlerFunc
	GetChangedBlocksHandler              getChangedBlocksHandlerFunc
	DescribeVolumeHandler                describeVolumeHandlerFunc
	DescribeVolumeModelHandler           describeVolumeModelHandlerFunc
	ListVolumesHandler                   listVolumesHandlerFunc
	ListDiskStatesHandler                listDiskStatesHandlerFunc
	DiscoverInstancesHandler             discoverInstancesHandlerFunc
	QueryAvailableStorageHandler         queryAvailableStorageHandler
	ResumeDeviceHandler                  resumeDeviceHandler
	CreateVolumeFromDeviceHandler        createVolumeFromDeviceHandler
	ExecuteActionHandler                 executeActionFunc
	CreatePlacementGroupHandler          createPlacementGroupHandlerFunc
	DestroyPlacementGroupHandler         destroyPlacementGroupHandlerFunc
	DescribePlacementGroupHandler        describePlacementGroupHandlerFunc
	AlterPlacementGroupMembershipHandler alterPlacementGroupMembershipHandlerFunc
	ListPlacementGroupsHandler           listPlacementGroupsHandlerFunc
	CmsActionHandler                     cmsActionHandlerFunc
	QueryAgentsInfoHandler               queryAgentsInfoHandler
	CloseHandlerFunc                     closeHandlerFunc
}

func (client *testClient) Close() error {
	if client.CloseHandlerFunc != nil {
		return client.CloseHandlerFunc()
	}

	return nil
}

func (client *testClient) Ping(
	ctx context.Context,
	req *protos.TPingRequest,
) (*protos.TPingResponse, error) {

	if client.PingHandler != nil {
		return client.PingHandler(ctx, req)
	}

	return &protos.TPingResponse{}, nil
}

func (client *testClient) CreateVolume(
	ctx context.Context,
	req *protos.TCreateVolumeRequest,
) (*protos.TCreateVolumeResponse, error) {

	if client.CreateVolumeHandler != nil {
		return client.CreateVolumeHandler(ctx, req)
	}

	return &protos.TCreateVolumeResponse{}, nil
}

func (client *testClient) DestroyVolume(
	ctx context.Context,
	req *protos.TDestroyVolumeRequest,
) (*protos.TDestroyVolumeResponse, error) {

	if client.DestroyVolumeHandler != nil {
		return client.DestroyVolumeHandler(ctx, req)
	}

	return &protos.TDestroyVolumeResponse{}, nil
}

func (client *testClient) ResizeVolume(
	ctx context.Context,
	req *protos.TResizeVolumeRequest,
) (*protos.TResizeVolumeResponse, error) {

	if client.ResizeVolumeHandler != nil {
		return client.ResizeVolumeHandler(ctx, req)
	}

	return &protos.TResizeVolumeResponse{}, nil
}

func (client *testClient) AlterVolume(
	ctx context.Context,
	req *protos.TAlterVolumeRequest,
) (*protos.TAlterVolumeResponse, error) {

	if client.AlterVolumeHandler != nil {
		return client.AlterVolumeHandler(ctx, req)
	}

	return &protos.TAlterVolumeResponse{}, nil
}

func (client *testClient) AssignVolume(
	ctx context.Context,
	req *protos.TAssignVolumeRequest,
) (*protos.TAssignVolumeResponse, error) {

	if client.AssignVolumeHandler != nil {
		return client.AssignVolumeHandler(ctx, req)
	}

	return &protos.TAssignVolumeResponse{}, nil
}

func (client *testClient) StatVolume(
	ctx context.Context,
	req *protos.TStatVolumeRequest,
) (*protos.TStatVolumeResponse, error) {

	if client.StatVolumeHandler != nil {
		return client.StatVolumeHandler(ctx, req)
	}

	return &protos.TStatVolumeResponse{}, nil
}

func (client *testClient) QueryAvailableStorage(
	ctx context.Context,
	req *protos.TQueryAvailableStorageRequest,
) (*protos.TQueryAvailableStorageResponse, error) {

	if client.QueryAvailableStorageHandler != nil {
		return client.QueryAvailableStorageHandler(ctx, req)
	}

	return &protos.TQueryAvailableStorageResponse{}, nil
}

func (client *testClient) ResumeDevice(
	ctx context.Context,
	req *protos.TResumeDeviceRequest,
) (*protos.TResumeDeviceResponse, error) {

	if client.ResumeDeviceHandler != nil {
		return client.ResumeDeviceHandler(ctx, req)
	}

	return &protos.TResumeDeviceResponse{}, nil
}

func (client *testClient) CreateVolumeFromDevice(
	ctx context.Context,
	req *protos.TCreateVolumeFromDeviceRequest,
) (*protos.TCreateVolumeFromDeviceResponse, error) {

	if client.CreateVolumeFromDeviceHandler != nil {
		return client.CreateVolumeFromDeviceHandler(ctx, req)
	}

	return &protos.TCreateVolumeFromDeviceResponse{}, nil
}

func (client *testClient) MountVolume(
	ctx context.Context,
	req *protos.TMountVolumeRequest,
) (*protos.TMountVolumeResponse, error) {

	if client.MountVolumeHandler != nil {
		return client.MountVolumeHandler(ctx, req)
	}

	return &protos.TMountVolumeResponse{}, nil
}

func (client *testClient) UnmountVolume(
	ctx context.Context,
	req *protos.TUnmountVolumeRequest,
) (*protos.TUnmountVolumeResponse, error) {

	if client.UnmountVolumeHandler != nil {
		return client.UnmountVolumeHandler(ctx, req)
	}

	return &protos.TUnmountVolumeResponse{}, nil
}

func (client *testClient) ReadBlocks(
	ctx context.Context,
	req *protos.TReadBlocksRequest,
) (*protos.TReadBlocksResponse, error) {

	if client.ReadBlocksHandler != nil {
		return client.ReadBlocksHandler(ctx, req)
	}

	return &protos.TReadBlocksResponse{}, nil
}

func (client *testClient) WriteBlocks(
	ctx context.Context,
	req *protos.TWriteBlocksRequest,
) (*protos.TWriteBlocksResponse, error) {

	if client.WriteBlocksHandler != nil {
		return client.WriteBlocksHandler(ctx, req)
	}

	return &protos.TWriteBlocksResponse{}, nil
}

func (client *testClient) ZeroBlocks(
	ctx context.Context,
	req *protos.TZeroBlocksRequest,
) (*protos.TZeroBlocksResponse, error) {

	if client.ZeroBlocksHandler != nil {
		return client.ZeroBlocksHandler(ctx, req)
	}

	return &protos.TZeroBlocksResponse{}, nil
}

func (client *testClient) StartEndpoint(
	ctx context.Context,
	req *protos.TStartEndpointRequest,
) (*protos.TStartEndpointResponse, error) {

	if client.StartEndpointHandler != nil {
		return client.StartEndpointHandler(ctx, req)
	}

	return &protos.TStartEndpointResponse{}, nil
}

func (client *testClient) StopEndpoint(
	ctx context.Context,
	req *protos.TStopEndpointRequest,
) (*protos.TStopEndpointResponse, error) {

	if client.StopEndpointHandler != nil {
		return client.StopEndpointHandler(ctx, req)
	}

	return &protos.TStopEndpointResponse{}, nil
}

func (client *testClient) ListEndpoints(
	ctx context.Context,
	req *protos.TListEndpointsRequest,
) (*protos.TListEndpointsResponse, error) {

	if client.ListEndpointsHandler != nil {
		return client.ListEndpointsHandler(ctx, req)
	}

	return &protos.TListEndpointsResponse{}, nil
}

func (client *testClient) KickEndpoint(
	ctx context.Context,
	req *protos.TKickEndpointRequest,
) (*protos.TKickEndpointResponse, error) {

	if client.KickEndpointHandler != nil {
		return client.KickEndpointHandler(ctx, req)
	}

	return &protos.TKickEndpointResponse{}, nil
}

func (client *testClient) ListKeyrings(
	ctx context.Context,
	req *protos.TListKeyringsRequest,
) (*protos.TListKeyringsResponse, error) {

	if client.ListKeyringsHandler != nil {
		return client.ListKeyringsHandler(ctx, req)
	}

	return &protos.TListKeyringsResponse{}, nil
}

func (client *testClient) DescribeEndpoint(
	ctx context.Context,
	req *protos.TDescribeEndpointRequest,
) (*protos.TDescribeEndpointResponse, error) {

	if client.DescribeEndpointHandler != nil {
		return client.DescribeEndpointHandler(ctx, req)
	}

	return &protos.TDescribeEndpointResponse{}, nil
}

func (client *testClient) RefreshEndpoint(
	ctx context.Context,
	req *protos.TRefreshEndpointRequest,
) (*protos.TRefreshEndpointResponse, error) {

	if client.RefreshEndpointHandler != nil {
		return client.RefreshEndpointHandler(ctx, req)
	}

	return &protos.TRefreshEndpointResponse{}, nil
}

func (client *testClient) CreateCheckpoint(
	ctx context.Context,
	req *protos.TCreateCheckpointRequest,
) (*protos.TCreateCheckpointResponse, error) {

	if client.CreateCheckpointHandler != nil {
		return client.CreateCheckpointHandler(ctx, req)
	}

	return &protos.TCreateCheckpointResponse{}, nil
}

func (client *testClient) GetCheckpointStatus(
	ctx context.Context,
	req *protos.TGetCheckpointStatusRequest,
) (*protos.TGetCheckpointStatusResponse, error) {

	if client.GetCheckpointStatusHandler != nil {
		return client.GetCheckpointStatusHandler(ctx, req)
	}

	return &protos.TGetCheckpointStatusResponse{}, nil
}

func (client *testClient) DeleteCheckpoint(
	ctx context.Context,
	req *protos.TDeleteCheckpointRequest,
) (*protos.TDeleteCheckpointResponse, error) {

	if client.DeleteCheckpointHandler != nil {
		return client.DeleteCheckpointHandler(ctx, req)
	}

	return &protos.TDeleteCheckpointResponse{}, nil
}

func (client *testClient) GetChangedBlocks(
	ctx context.Context,
	req *protos.TGetChangedBlocksRequest,
) (*protos.TGetChangedBlocksResponse, error) {

	if client.GetChangedBlocksHandler != nil {
		return client.GetChangedBlocksHandler(ctx, req)
	}

	return &protos.TGetChangedBlocksResponse{}, nil
}

func (client *testClient) DescribeVolume(
	ctx context.Context,
	req *protos.TDescribeVolumeRequest,
) (*protos.TDescribeVolumeResponse, error) {

	if client.DescribeVolumeHandler != nil {
		return client.DescribeVolumeHandler(ctx, req)
	}

	return &protos.TDescribeVolumeResponse{}, nil
}

func (client *testClient) DescribeVolumeModel(
	ctx context.Context,
	req *protos.TDescribeVolumeModelRequest,
) (*protos.TDescribeVolumeModelResponse, error) {

	if client.DescribeVolumeModelHandler != nil {
		return client.DescribeVolumeModelHandler(ctx, req)
	}

	return &protos.TDescribeVolumeModelResponse{}, nil
}

func (client *testClient) ListVolumes(
	ctx context.Context,
	req *protos.TListVolumesRequest,
) (*protos.TListVolumesResponse, error) {

	if client.ListVolumesHandler != nil {
		return client.ListVolumesHandler(ctx, req)
	}

	return &protos.TListVolumesResponse{}, nil
}

func (client *testClient) ListDiskStates(
	ctx context.Context,
	req *protos.TListDiskStatesRequest,
) (*protos.TListDiskStatesResponse, error) {

	if client.ListDiskStatesHandler != nil {
		return client.ListDiskStatesHandler(ctx, req)
	}

	return &protos.TListDiskStatesResponse{}, nil
}

func (client *testClient) DiscoverInstances(
	ctx context.Context,
	req *protos.TDiscoverInstancesRequest,
) (*protos.TDiscoverInstancesResponse, error) {

	if client.DiscoverInstancesHandler != nil {
		return client.DiscoverInstancesHandler(ctx, req)
	}

	return &protos.TDiscoverInstancesResponse{}, nil
}

func (client *testClient) ExecuteAction(
	ctx context.Context,
	req *protos.TExecuteActionRequest,
) (*protos.TExecuteActionResponse, error) {

	if client.ExecuteActionHandler != nil {
		return client.ExecuteActionHandler(ctx, req)
	}

	return &protos.TExecuteActionResponse{}, nil
}

func (client *testClient) CreatePlacementGroup(
	ctx context.Context,
	req *protos.TCreatePlacementGroupRequest,
) (*protos.TCreatePlacementGroupResponse, error) {

	if client.CreatePlacementGroupHandler != nil {
		return client.CreatePlacementGroupHandler(ctx, req)
	}

	return &protos.TCreatePlacementGroupResponse{}, nil
}

func (client *testClient) DestroyPlacementGroup(
	ctx context.Context,
	req *protos.TDestroyPlacementGroupRequest,
) (*protos.TDestroyPlacementGroupResponse, error) {

	if client.DestroyPlacementGroupHandler != nil {
		return client.DestroyPlacementGroupHandler(ctx, req)
	}

	return &protos.TDestroyPlacementGroupResponse{}, nil
}

func (client *testClient) DescribePlacementGroup(
	ctx context.Context,
	req *protos.TDescribePlacementGroupRequest,
) (*protos.TDescribePlacementGroupResponse, error) {

	if client.DescribePlacementGroupHandler != nil {
		return client.DescribePlacementGroupHandler(ctx, req)
	}

	return &protos.TDescribePlacementGroupResponse{}, nil
}

func (client *testClient) AlterPlacementGroupMembership(
	ctx context.Context,
	req *protos.TAlterPlacementGroupMembershipRequest,
) (*protos.TAlterPlacementGroupMembershipResponse, error) {

	if client.AlterPlacementGroupMembershipHandler != nil {
		return client.AlterPlacementGroupMembershipHandler(ctx, req)
	}

	return &protos.TAlterPlacementGroupMembershipResponse{}, nil
}

func (client *testClient) ListPlacementGroups(
	ctx context.Context,
	req *protos.TListPlacementGroupsRequest,
) (*protos.TListPlacementGroupsResponse, error) {

	if client.ListPlacementGroupsHandler != nil {
		return client.ListPlacementGroupsHandler(ctx, req)
	}

	return &protos.TListPlacementGroupsResponse{}, nil
}

func (client *testClient) CmsAction(
	ctx context.Context,
	req *protos.TCmsActionRequest,
) (*protos.TCmsActionResponse, error) {

	if client.CmsActionHandler != nil {
		return client.CmsActionHandler(ctx, req)
	}

	return &protos.TCmsActionResponse{}, nil
}

func (client *testClient) QueryAgentsInfo(
	ctx context.Context,
	req *protos.TQueryAgentsInfoRequest,
) (*protos.TQueryAgentsInfoResponse, error) {

	if client.QueryAgentsInfoHandler != nil {
		return client.QueryAgentsInfoHandler(ctx, req)
	}

	return &protos.TQueryAgentsInfoResponse{}, nil
}
