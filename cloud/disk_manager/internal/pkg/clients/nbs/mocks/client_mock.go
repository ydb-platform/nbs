package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////

type ClientMock struct {
	mock.Mock
}

func (c *ClientMock) Ping(ctx context.Context) error {
	args := c.Called(ctx)
	return args.Error(0)
}

func (c *ClientMock) Create(
	ctx context.Context,
	params nbs.CreateDiskParams,
) error {

	args := c.Called(ctx, params)
	return args.Error(0)
}

func (c *ClientMock) CreateProxyOverlayDisk(
	ctx context.Context,
	diskID string,
	baseDiskID string,
	baseDiskCheckpointID string,
) (bool, error) {

	args := c.Called(ctx, diskID, baseDiskID, baseDiskCheckpointID)
	return args.Bool(0), args.Error(1)
}

func (c *ClientMock) Delete(
	ctx context.Context,
	diskID string,
) error {

	args := c.Called(ctx, diskID)
	return args.Error(0)
}

func (c *ClientMock) DeleteSync(
	ctx context.Context,
	diskID string,
) error {

	args := c.Called(ctx, diskID)
	return args.Error(0)
}

func (c *ClientMock) DeleteWithFillGeneration(
	ctx context.Context,
	diskID string,
	fillGeneration uint64,
) error {

	args := c.Called(ctx, diskID)
	return args.Error(0)
}

func (c *ClientMock) CreateCheckpoint(
	ctx context.Context,
	params nbs.CheckpointParams,
) error {

	args := c.Called(ctx, params)
	return args.Error(0)
}

func (c *ClientMock) GetCheckpointStatus(
	ctx context.Context,
	diskID string,
	checkpointID string,
) (nbs.CheckpointStatus, error) {

	args := c.Called(ctx, diskID, checkpointID)
	return args.Get(0).(nbs.CheckpointStatus), args.Error(1)
}

func (c *ClientMock) DeleteCheckpoint(
	ctx context.Context,
	diskID string,
	checkpointID string,
) error {

	args := c.Called(ctx, diskID, checkpointID)
	return args.Error(0)
}

func (c *ClientMock) DeleteCheckpointData(
	ctx context.Context,
	diskID string,
	checkpointID string,
) error {

	args := c.Called(ctx, diskID, checkpointID)
	return args.Error(0)
}

func (c *ClientMock) EnsureCheckpointReady(
	ctx context.Context,
	diskID string,
	checkpointID string,
) error {

	args := c.Called(ctx, diskID, checkpointID)
	return args.Error(0)
}

func (c *ClientMock) Resize(
	ctx context.Context,
	checkpoint func() error,
	diskID string,
	size uint64,
) error {

	args := c.Called(ctx, checkpoint, diskID, size)
	return args.Error(0)
}

func (c *ClientMock) Alter(
	ctx context.Context,
	checkpoint func() error,
	diskID string,
	cloudID string,
	folderID string,
) error {

	args := c.Called(ctx, checkpoint, diskID, cloudID, folderID)
	return args.Error(0)
}

func (c *ClientMock) Rebase(
	ctx context.Context,
	checkpoint func() error,
	diskID string,
	baseDiskID string,
	targetBaseDiskID string,
) error {

	args := c.Called(ctx, checkpoint, diskID, baseDiskID, targetBaseDiskID)
	return args.Error(0)
}

func (c *ClientMock) Assign(
	ctx context.Context,
	params nbs.AssignDiskParams,
) error {

	args := c.Called(ctx, params)
	return args.Error(0)
}

func (c *ClientMock) Unassign(
	ctx context.Context,
	diskID string,
) error {

	args := c.Called(ctx, diskID)
	return args.Error(0)
}

func (c *ClientMock) DescribeModel(
	ctx context.Context,
	blocksCount uint64,
	blockSize uint32,
	kind types.DiskKind,
	tabletVersion uint32,
) (nbs.DiskModel, error) {

	args := c.Called(ctx, blocksCount, blockSize, kind, tabletVersion)
	return args.Get(0).(nbs.DiskModel), args.Error(1)
}

func (c *ClientMock) Describe(
	ctx context.Context,
	diskID string,
) (nbs.DiskParams, error) {

	args := c.Called(ctx, diskID)
	return args.Get(0).(nbs.DiskParams), args.Error(1)
}

func (c *ClientMock) CreatePlacementGroup(
	ctx context.Context,
	groupID string,
	placementStrategy types.PlacementStrategy,
	placementPartitionCount uint32,
) error {

	args := c.Called(ctx, groupID, placementStrategy, placementPartitionCount)
	return args.Error(0)
}

func (c *ClientMock) DeletePlacementGroup(
	ctx context.Context,
	groupID string,
) error {

	args := c.Called(ctx, groupID)
	return args.Error(0)
}

func (c *ClientMock) AlterPlacementGroupMembership(
	ctx context.Context,
	checkpoint func() error,
	groupID string,
	placementPartitionIndex uint32,
	disksToAdd []string,
	disksToRemove []string,
) error {

	args := c.Called(ctx, checkpoint, groupID, placementPartitionIndex, disksToAdd, disksToRemove)
	return args.Error(0)
}

func (c *ClientMock) ListPlacementGroups(
	ctx context.Context,
) ([]string, error) {

	args := c.Called(ctx)
	return args.Get(0).([]string), args.Error(1)
}

func (c *ClientMock) DescribePlacementGroup(
	ctx context.Context,
	groupID string,
) (nbs.PlacementGroup, error) {

	args := c.Called(ctx, groupID)
	return args.Get(0).(nbs.PlacementGroup), args.Error(1)
}

func (c *ClientMock) MountRO(
	ctx context.Context,
	diskID string,
	encryption *types.EncryptionDesc,
) (*nbs.Session, error) {

	args := c.Called(ctx, diskID, encryption)
	return args.Get(0).(*nbs.Session), args.Error(1)
}

func (c *ClientMock) MountLocalRO(
	ctx context.Context,
	diskID string,
	encryption *types.EncryptionDesc,
) (*nbs.Session, error) {

	args := c.Called(ctx, diskID, encryption)
	return args.Get(0).(*nbs.Session), args.Error(1)
}

func (c *ClientMock) MountRW(
	ctx context.Context,
	diskID string,
	fillGeneration uint64,
	fillSeqNumber uint64,
	encryption *types.EncryptionDesc,
) (*nbs.Session, error) {

	args := c.Called(ctx, diskID, fillGeneration, fillSeqNumber, encryption)
	return args.Get(0).(*nbs.Session), args.Error(1)
}

func (c *ClientMock) GetChangedBlocks(
	ctx context.Context,
	diskID string,
	startIndex uint64,
	blockCount uint32,
	baseCheckpointID string,
	checkpointID string,
	ignoreBaseDisk bool,
) ([]byte, error) {

	args := c.Called(
		ctx,
		diskID,
		startIndex,
		blockCount,
		baseCheckpointID,
		checkpointID,
	)
	return args.Get(0).([]byte), args.Error(1)
}

func (c *ClientMock) GetCheckpointSize(
	ctx context.Context,
	saveState func(uint64, uint64) error,
	diskID string,
	checkpointID string,
	milestoneBlockIndex uint64,
	milestoneCheckpointSize uint64,
) error {

	args := c.Called(
		ctx,
		saveState,
		diskID,
		checkpointID,
		milestoneBlockIndex,
		milestoneCheckpointSize,
	)
	return args.Error(0)
}

func (c *ClientMock) GetChangedBytes(
	ctx context.Context,
	diskID string,
	baseCheckpointID string,
	checkpointID string,
	ignoreBaseDisk bool,
) (uint64, error) {

	args := c.Called(
		ctx,
		diskID,
		baseCheckpointID,
		checkpointID,
		ignoreBaseDisk,
	)
	return args.Get(0).(uint64), args.Error(1)
}

func (c *ClientMock) Stat(
	ctx context.Context,
	diskID string,
) (nbs.DiskStats, error) {

	args := c.Called(ctx, diskID)
	return args.Get(0).(nbs.DiskStats), args.Error(1)
}

////////////////////////////////////////////////////////////////////////////////

func (c *ClientMock) Freeze(
	ctx context.Context,
	saveState func() error,
	diskID string,
) error {

	args := c.Called(ctx, saveState, diskID)
	return args.Error(0)
}

func (c *ClientMock) Unfreeze(
	ctx context.Context,
	saveState func() error,
	diskID string,
) error {

	args := c.Called(ctx, saveState, diskID)
	return args.Error(0)
}

func (c *ClientMock) ScanDisk(
	ctx context.Context,
	diskID string,
	batchSize uint32,
) error {

	args := c.Called(ctx, diskID, batchSize)
	return args.Error(0)
}

func (c *ClientMock) GetScanDiskStatus(
	ctx context.Context,
	diskID string,
) (nbs.ScanDiskStatus, error) {

	args := c.Called(ctx, diskID)
	return args.Get(0).(nbs.ScanDiskStatus), args.Error(1)
}

func (c *ClientMock) FinishFillDisk(
	ctx context.Context,
	saveState func() error,
	diskID string,
	fillGeneration uint64,
) error {

	args := c.Called(ctx, saveState, diskID, fillGeneration)
	return args.Error(0)
}

func (c *ClientMock) ZoneID() string {
	args := c.Called()
	return args.String(0)
}

func (c *ClientMock) GetClusterCapacity(
	ctx context.Context,
) ([]nbs.ClusterCapacityInfo, error) {

	args := c.Called(ctx)
	return args.Get(0).([]nbs.ClusterCapacityInfo), args.Error(1)
}

////////////////////////////////////////////////////////////////////////////////

func NewClientMock() *ClientMock {
	return &ClientMock{}
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that ClientMock implements Client.
func assertClientMockIsClient(arg *ClientMock) nbs.Client {
	return arg
}
