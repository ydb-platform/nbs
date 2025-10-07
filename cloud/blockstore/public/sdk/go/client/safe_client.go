package client

import (
	"time"

	protos "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	coreprotos "github.com/ydb-platform/nbs/cloud/storage/core/protos"
	"golang.org/x/net/context"
)

////////////////////////////////////////////////////////////////////////////////

type CreateVolumeOpts struct {
	ProjectId               string
	BlockSize               uint32
	ChannelsCount           uint32
	FolderId                string
	CloudId                 string
	StorageMediaKind        coreprotos.EStorageMediaKind
	TabletVersion           uint32
	BaseDiskId              string
	BaseDiskCheckpointId    string
	PlacementGroupId        string
	PlacementPartitionIndex uint32
	PartitionsCount         uint32
	IsSystem                bool
	EncryptionSpec          *protos.TEncryptionSpec
	StoragePoolName         string
	AgentIds                []string
	FillGeneration          uint64
}

type MountVolumeOpts struct {
	Token          string
	AccessMode     protos.EVolumeAccessMode
	MountMode      protos.EVolumeMountMode
	MountFlags     uint32
	MountSeqNumber uint64
	EncryptionSpec *protos.TEncryptionSpec
	FillGeneration uint64
	FillSeqNumber  uint64
}

type SessionInfo struct {
	SessionId              string
	InactiveClientsTimeout time.Duration
}

////////////////////////////////////////////////////////////////////////////////

type safeClient struct {
	Impl ClientIface
}

func (client *safeClient) Close() error {
	return client.Impl.Close()
}

func (client *safeClient) Ping(ctx context.Context) error {
	req := &protos.TPingRequest{}
	_, err := client.Impl.Ping(ctx, req)
	return err
}

func (client *safeClient) CreateVolume(
	ctx context.Context,
	diskId string,
	blocksCount uint64,
	opts *CreateVolumeOpts,
) error {
	req := &protos.TCreateVolumeRequest{
		DiskId:      diskId,
		BlocksCount: blocksCount,
	}

	if opts != nil {
		req.ProjectId = opts.ProjectId
		req.BlockSize = opts.BlockSize
		req.ChannelsCount = opts.ChannelsCount
		req.FolderId = opts.FolderId
		req.CloudId = opts.CloudId
		req.StorageMediaKind = opts.StorageMediaKind
		req.TabletVersion = opts.TabletVersion
		req.BaseDiskId = opts.BaseDiskId
		req.BaseDiskCheckpointId = opts.BaseDiskCheckpointId
		req.PlacementGroupId = opts.PlacementGroupId
		req.PlacementPartitionIndex = opts.PlacementPartitionIndex
		req.PartitionsCount = opts.PartitionsCount
		req.IsSystem = opts.IsSystem
		req.EncryptionSpec = opts.EncryptionSpec
		req.StoragePoolName = opts.StoragePoolName
		req.AgentIds = opts.AgentIds
		req.FillGeneration = opts.FillGeneration
	}

	_, err := client.Impl.CreateVolume(ctx, req)
	return err
}

func (client *safeClient) DestroyVolume(
	ctx context.Context,
	diskId string,
	sync bool,
	fillGeneration uint64,
) error {
	req := &protos.TDestroyVolumeRequest{
		DiskId:         diskId,
		Sync:           sync,
		FillGeneration: fillGeneration,
	}

	_, err := client.Impl.DestroyVolume(ctx, req)
	return err
}

func (client *safeClient) ResizeVolume(
	ctx context.Context,
	diskId string,
	blocksCount uint64,
	channelsCount uint32,
	configVersion uint32,
) error {
	req := &protos.TResizeVolumeRequest{
		DiskId:        diskId,
		BlocksCount:   blocksCount,
		ChannelsCount: channelsCount,
		ConfigVersion: configVersion,
	}

	_, err := client.Impl.ResizeVolume(ctx, req)
	return err
}

func (client *safeClient) AlterVolume(
	ctx context.Context,
	diskId string,
	projectId string,
	folderId string,
	cloudId string,
	configVersion uint32,
) error {
	req := &protos.TAlterVolumeRequest{
		DiskId:        diskId,
		ProjectId:     projectId,
		FolderId:      folderId,
		CloudId:       cloudId,
		ConfigVersion: configVersion,
	}

	_, err := client.Impl.AlterVolume(ctx, req)
	return err
}

func (client *safeClient) AssignVolume(
	ctx context.Context,
	diskId string,
	instanceId string,
	token string,
	host string,
) (*protos.TVolume, error) {

	req := &protos.TAssignVolumeRequest{
		DiskId:     diskId,
		InstanceId: instanceId,
		Token:      token,
		Host:       host,
	}

	resp, err := client.Impl.AssignVolume(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.GetVolume(), nil
}

func (client *safeClient) StatVolume(
	ctx context.Context,
	diskId string,
	flags uint32,
) (*protos.TVolume, *protos.TVolumeStats, error) {

	req := &protos.TStatVolumeRequest{
		DiskId: diskId,
		Flags:  flags,
	}

	resp, err := client.Impl.StatVolume(ctx, req)
	if err != nil {
		return nil, nil, err
	}

	return resp.GetVolume(), resp.GetStats(), nil
}

func (client *safeClient) GetCheckpoints(
	ctx context.Context,
	diskId string,
) ([]string, error) {

	req := &protos.TStatVolumeRequest{
		DiskId: diskId,
		Flags:  uint32(0),
	}

	resp, err := client.Impl.StatVolume(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.Checkpoints, nil
}

func (client *safeClient) QueryAvailableStorage(
	ctx context.Context,
	agentIds []string,
) ([]*protos.TAvailableStorageInfo, error) {

	req := &protos.TQueryAvailableStorageRequest{
		AgentIds: agentIds,
	}

	resp, err := client.Impl.QueryAvailableStorage(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.GetAvailableStorage(), nil
}

func (client *safeClient) ResumeDevice(
	ctx context.Context,
	agentId string,
	path string,
	dryRun bool,
) error {

	req := &protos.TResumeDeviceRequest{
		AgentId: agentId,
		Path:    path,
		DryRun:  dryRun,
	}

	_, err := client.Impl.ResumeDevice(ctx, req)
	return err
}

func (client *safeClient) CreateVolumeFromDevice(
	ctx context.Context,
	diskId string,
	agentId string,
	devicePath string,
	opts *CreateVolumeOpts,
) error {

	req := &protos.TCreateVolumeFromDeviceRequest{
		DiskId:  diskId,
		AgentId: agentId,
		Path:    devicePath,
	}

	if opts != nil {
		req.ProjectId = opts.ProjectId
		req.FolderId = opts.FolderId
		req.CloudId = opts.CloudId
	}

	_, err := client.Impl.CreateVolumeFromDevice(ctx, req)
	return err
}

func (client *safeClient) CreateCheckpoint(
	ctx context.Context,
	diskId string,
	checkpointId string,
	checkpointType protos.ECheckpointType,
) error {
	// TODO(NBS-4531): rm IsLight param after nbs release
	req := &protos.TCreateCheckpointRequest{
		DiskId:         diskId,
		CheckpointId:   checkpointId,
		IsLight:        checkpointType == protos.ECheckpointType_LIGHT,
		CheckpointType: checkpointType,
	}

	_, err := client.Impl.CreateCheckpoint(ctx, req)
	return err
}

func (client *safeClient) GetCheckpointStatus(
	ctx context.Context,
	diskId string,
	checkpointId string,
) (protos.ECheckpointStatus, error) {

	req := &protos.TGetCheckpointStatusRequest{
		DiskId:       diskId,
		CheckpointId: checkpointId,
	}

	resp, err := client.Impl.GetCheckpointStatus(ctx, req)
	return resp.GetCheckpointStatus(), err
}

func (client *safeClient) DeleteCheckpoint(
	ctx context.Context,
	diskId string,
	checkpointId string,
) error {
	req := &protos.TDeleteCheckpointRequest{
		DiskId:       diskId,
		CheckpointId: checkpointId,
	}

	_, err := client.Impl.DeleteCheckpoint(ctx, req)
	return err
}

func (client *safeClient) GetChangedBlocks(
	ctx context.Context,
	diskId string,
	startIndex uint64,
	blocksCount uint32,
	lowCheckpointId string,
	highCheckpointId string,
	ignoreBaseDisk bool,
) ([]byte, error) {

	req := &protos.TGetChangedBlocksRequest{
		DiskId:           diskId,
		StartIndex:       startIndex,
		BlocksCount:      blocksCount,
		LowCheckpointId:  lowCheckpointId,
		HighCheckpointId: highCheckpointId,
		IgnoreBaseDisk:   ignoreBaseDisk,
	}

	resp, err := client.Impl.GetChangedBlocks(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.GetMask(), nil
}

func (client *safeClient) DescribeVolume(
	ctx context.Context,
	diskId string,
) (*protos.TVolume, error) {

	request := &protos.TDescribeVolumeRequest{
		DiskId: diskId,
	}

	resp, err := client.Impl.DescribeVolume(ctx, request)
	if err != nil {
		return nil, err
	}

	return resp.GetVolume(), nil
}

func (client *safeClient) DescribeVolumeModel(
	ctx context.Context,
	blocksCount uint64,
	blockSize uint32,
	storageMediaKind coreprotos.EStorageMediaKind,
	tabletVersion uint32,
) (*protos.TVolumeModel, error) {

	request := &protos.TDescribeVolumeModelRequest{
		BlocksCount:      blocksCount,
		BlockSize:        blockSize,
		StorageMediaKind: storageMediaKind,
		TabletVersion:    tabletVersion,
	}

	resp, err := client.Impl.DescribeVolumeModel(ctx, request)
	if err != nil {
		return nil, err
	}

	return resp.GetVolumeModel(), nil
}

func (client *safeClient) ListVolumes(ctx context.Context) ([]string, error) {
	request := &protos.TListVolumesRequest{}

	resp, err := client.Impl.ListVolumes(ctx, request)
	if err != nil {
		return nil, err
	}

	return resp.GetVolumes(), nil
}

func (client *safeClient) ListDiskStates(
	ctx context.Context,
) ([]*protos.TDiskState, error) {

	request := &protos.TListDiskStatesRequest{}

	resp, err := client.Impl.ListDiskStates(ctx, request)
	if err != nil {
		return nil, err
	}

	return resp.GetDiskStates(), nil
}

func (client *safeClient) DiscoverInstances(
	ctx context.Context,
	limit uint32,
	filter protos.EDiscoveryPortFilter,
) ([]*protos.TDiscoveredInstance, error) {

	if limit == 0 {
		// a reasonable default
		limit = 3
	}

	req := &protos.TDiscoverInstancesRequest{
		Limit:          limit,
		InstanceFilter: filter,
	}

	resp, err := client.Impl.DiscoverInstances(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.GetInstances(), nil
}

func (client *safeClient) ExecuteAction(
	ctx context.Context,
	action string,
	input []byte,
) ([]byte, error) {

	req := &protos.TExecuteActionRequest{
		Action: action,
		Input:  input,
	}

	resp, err := client.Impl.ExecuteAction(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.Output, nil
}

func (client *safeClient) CreatePlacementGroup(
	ctx context.Context,
	groupId string,
	placementStrategy protos.EPlacementStrategy,
	placementPartitionCount uint32,
) error {
	req := &protos.TCreatePlacementGroupRequest{
		GroupId:                 groupId,
		PlacementStrategy:       placementStrategy,
		PlacementPartitionCount: placementPartitionCount,
	}

	_, err := client.Impl.CreatePlacementGroup(ctx, req)
	return err
}

func (client *safeClient) DestroyPlacementGroup(
	ctx context.Context,
	groupId string,
) error {
	req := &protos.TDestroyPlacementGroupRequest{
		GroupId: groupId,
	}

	_, err := client.Impl.DestroyPlacementGroup(ctx, req)
	return err
}

func (client *safeClient) DescribePlacementGroup(
	ctx context.Context,
	groupId string,
) (*protos.TPlacementGroup, error) {
	req := &protos.TDescribePlacementGroupRequest{
		GroupId: groupId,
	}

	resp, err := client.Impl.DescribePlacementGroup(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.GetGroup(), nil
}

func (client *safeClient) AlterPlacementGroupMembership(
	ctx context.Context,
	groupId string,
	placementPartitionIndex uint32,
	disksToAdd []string,
	disksToRemove []string,
	configVersion uint32,
) error {
	req := &protos.TAlterPlacementGroupMembershipRequest{
		GroupId:                 groupId,
		PlacementPartitionIndex: placementPartitionIndex,
		DisksToAdd:              disksToAdd,
		DisksToRemove:           disksToRemove,
		ConfigVersion:           configVersion,
	}

	_, err := client.Impl.AlterPlacementGroupMembership(ctx, req)
	return err
}

func (client *safeClient) ListPlacementGroups(
	ctx context.Context,
) ([]string, error) {
	req := &protos.TListPlacementGroupsRequest{}

	resp, err := client.Impl.ListPlacementGroups(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.GetGroupIds(), nil
}

func (client *safeClient) CmsRemoveDevices(
	ctx context.Context,
	host string,
	devices []string,
	dryRun bool,
) (*protos.TCmsActionResponse, error) {

	actions := make([]*protos.TAction, len(devices))
	t := protos.TAction_REMOVE_DEVICE
	for i, device := range devices {
		action := protos.TAction{
			Type:   &t,
			Host:   &host,
			Device: &device,
			DryRun: &dryRun,
		}
		actions[i] = &action
	}

	return client.Impl.CmsAction(
		ctx,
		&protos.TCmsActionRequest{
			Actions: actions,
		},
	)
}

func (client *safeClient) CmsGetDependentDisks(
	ctx context.Context,
	host string,
) (*protos.TCmsActionResponse, error) {

	t := protos.TAction_GET_DEPENDENT_DISKS
	req := &protos.TCmsActionRequest{
		Actions: []*protos.TAction{{
			Type: &t,
			Host: &host,
		}},
	}

	return client.Impl.CmsAction(ctx, req)
}

func (client *safeClient) CmsAddHost(
	ctx context.Context,
	host string,
	dryRun bool,
) (*protos.TCmsActionResponse, error) {

	t := protos.TAction_ADD_HOST
	req := &protos.TCmsActionRequest{
		Actions: []*protos.TAction{{
			Type:   &t,
			Host:   &host,
			DryRun: &dryRun,
		}},
	}

	resp, err := client.Impl.CmsAction(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (client *safeClient) CmsRemoveHost(
	ctx context.Context,
	host string,
	dryRun bool,
) (*protos.TCmsActionResponse, error) {

	t := protos.TAction_REMOVE_HOST
	req := &protos.TCmsActionRequest{
		Actions: []*protos.TAction{{
			Type:   &t,
			Host:   &host,
			DryRun: &dryRun,
		}},
	}

	resp, err := client.Impl.CmsAction(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (client *safeClient) CmsPurgeHost(
	ctx context.Context,
	host string,
	dryRun bool,
) (*protos.TCmsActionResponse, error) {

	t := protos.TAction_PURGE_HOST
	req := &protos.TCmsActionRequest{
		Actions: []*protos.TAction{{
			Type:   &t,
			Host:   &host,
			DryRun: &dryRun,
		}},
	}

	resp, err := client.Impl.CmsAction(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (client *safeClient) CmsAddDevices(
	ctx context.Context,
	host string,
	devices []string,
	dryRun bool,
) (*protos.TCmsActionResponse, error) {

	actions := make([]*protos.TAction, len(devices))
	t := protos.TAction_ADD_DEVICE
	for i, device := range devices {
		action := protos.TAction{
			Type:   &t,
			Host:   &host,
			Device: &device,
			DryRun: &dryRun,
		}
		actions[i] = &action
	}

	return client.Impl.CmsAction(
		ctx,
		&protos.TCmsActionRequest{
			Actions: actions,
		},
	)
}

func (client *safeClient) QueryAgentsInfo(
	ctx context.Context,
	agentId string,
	path string,
	dryRun bool,
) (*protos.TQueryAgentsInfoResponse, error) {

	req := &protos.TQueryAgentsInfoRequest{}
	resp, err := client.Impl.QueryAgentsInfo(ctx, req)
	return resp, err
}
