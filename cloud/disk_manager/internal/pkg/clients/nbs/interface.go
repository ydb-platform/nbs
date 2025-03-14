package nbs

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////

type CreateDiskParams struct {
	ID                   string
	BaseDiskID           string
	BaseDiskCheckpointID string
	BlocksCount          uint64
	// When 0, uses DefaultBlockSize from config.
	BlockSize               uint32
	Kind                    types.DiskKind
	CloudID                 string
	FolderID                string
	TabletVersion           uint32
	PlacementGroupID        string
	PlacementPartitionIndex uint32
	PartitionsCount         uint32
	IsSystem                bool
	StoragePoolName         string
	AgentIds                []string
	EncryptionDesc          *types.EncryptionDesc
}

type AssignDiskParams struct {
	ID         string
	InstanceID string
	Token      string
	Host       string
}

type DiskPerformanceProfile struct {
	MaxReadBandwidth   uint64
	MaxPostponedWeight uint64
	ThrottlingEnabled  bool
	MaxReadIops        uint32
	BoostTime          uint32
	BoostRefillTime    uint32
	BoostPercentage    uint32
	MaxWriteBandwidth  uint64
	MaxWriteIops       uint32
	BurstPercentage    uint32
}

type DiskModel struct {
	BlockSize           uint32
	BlocksCount         uint64
	ChannelsCount       uint32
	Kind                types.DiskKind
	PerformanceProfile  DiskPerformanceProfile
	MergedChannelsCount uint32
	MixedChannelsCount  uint32
}

type DiskParams struct {
	BlockSize      uint32
	BlocksCount    uint64
	Kind           types.DiskKind
	EncryptionDesc *types.EncryptionDesc
	CloudID        string
	FolderID       string
	BaseDiskID     string
	IsFillFinished bool

	IsDiskRegistryBasedDisk bool
}

type PlacementGroup struct {
	GroupID                 string
	PlacementStrategy       types.PlacementStrategy
	PlacementPartitionCount uint32
	DiskIDs                 []string
	Racks                   []string
}

type DiskStats struct {
	// In bytes.
	StorageSize uint64
}

type ScanDiskStatus struct {
	Processed   uint64
	Total       uint64
	IsCompleted bool
	BrokenBlobs []string
}

type CheckpointType uint32

const (
	CheckpointTypeNormal CheckpointType = iota // Must be default value of CheckpointType.
	CheckpointTypeLight
	CheckpointTypeWithoutData
)

type CheckpointStatus uint32

const (
	CheckpointStatusNotReady CheckpointStatus = iota
	CheckpointStatusReady
	CheckpointStatusError
)

type CheckpointParams struct {
	DiskID         string
	CheckpointID   string
	CheckpointType CheckpointType
}

// Used in tests.
type DiskContentInfo struct {
	ContentSize uint64 // The coordinate of the last non-zero byte.
	StorageSize uint64
	Crc32       uint32
	BlockCrc32s []uint32
}

////////////////////////////////////////////////////////////////////////////////

type Client interface {
	Ping(ctx context.Context) error

	Create(ctx context.Context, params CreateDiskParams) error

	CreateProxyOverlayDisk(
		ctx context.Context,
		diskID string,
		baseDiskID string,
		baseDiskCheckpointID string,
	) (created bool, err error)

	Delete(ctx context.Context, diskID string) error

	DeleteSync(ctx context.Context, diskID string) error

	DeleteWithFillGeneration(ctx context.Context, diskID string, fillGeneration uint64) error

	CreateCheckpoint(ctx context.Context, params CheckpointParams) error

	GetCheckpointStatus(
		ctx context.Context,
		diskID string,
		checkpointID string,
	) (CheckpointStatus, error)

	DeleteCheckpoint(
		ctx context.Context,
		diskID string,
		checkpointID string,
	) error

	DeleteCheckpointData(
		ctx context.Context,
		diskID string,
		checkpointID string,
	) error

	EnsureCheckpointReady(
		ctx context.Context,
		diskID string,
		checkpointID string,
	) error

	Resize(
		ctx context.Context,
		saveState func() error,
		diskID string,
		size uint64,
	) error

	Alter(
		ctx context.Context,
		saveState func() error,
		diskID string, cloudID string,
		folderID string,
	) error

	Rebase(
		ctx context.Context,
		saveState func() error,
		diskID string,
		baseDiskID string,
		targetBaseDiskID string,
	) error

	Assign(ctx context.Context, params AssignDiskParams) error

	Unassign(ctx context.Context, diskID string) error

	DescribeModel(
		ctx context.Context,
		blocksCount uint64,
		blockSize uint32,
		kind types.DiskKind,
		tabletVersion uint32,
	) (DiskModel, error)

	Describe(
		ctx context.Context,
		diskID string,
	) (DiskParams, error)

	CreatePlacementGroup(
		ctx context.Context,
		groupID string,
		placementStrategy types.PlacementStrategy,
		placementPartitionCount uint32,
	) error

	DeletePlacementGroup(ctx context.Context, groupID string) error

	AlterPlacementGroupMembership(
		ctx context.Context,
		saveState func() error,
		groupID string,
		placementPartitionIndex uint32,
		disksToAdd []string,
		disksToRemove []string,
	) error

	ListPlacementGroups(ctx context.Context) ([]string, error)

	DescribePlacementGroup(
		ctx context.Context,
		groupID string,
	) (PlacementGroup, error)

	MountRO(
		ctx context.Context,
		diskID string,
		encryption *types.EncryptionDesc,
	) (*Session, error)

	MountLocalRO(
		ctx context.Context,
		diskID string,
		encryption *types.EncryptionDesc,
	) (*Session, error)

	MountRW(
		ctx context.Context,
		diskID string,
		fillGeneration uint64,
		fillSeqNumber uint64,
		encryption *types.EncryptionDesc,
	) (*Session, error)

	GetChangedBlocks(
		ctx context.Context,
		diskID string,
		startIndex uint64,
		blockCount uint32,
		baseCheckpointID string,
		checkpointID string,
		ignoreBaseDisk bool,
	) ([]byte, error)

	GetCheckpointSize(
		ctx context.Context,
		saveState func(blockIndex uint64, checkpointSize uint64) error,
		diskID string,
		checkpointID string,
		milestoneBlockIndex uint64,
		milestoneCheckpointSize uint64,
	) error

	// Returns changed bytes count between two checkpoints of a disk.
	GetChangedBytes(
		ctx context.Context,
		diskID string,
		baseCheckpointID string,
		checkpointID string,
		ignoreBaseDisk bool,
	) (uint64, error)

	Stat(ctx context.Context, diskID string) (DiskStats, error)

	Freeze(
		ctx context.Context,
		saveState func() error,
		diskID string,
	) error

	Unfreeze(
		ctx context.Context,
		saveState func() error,
		diskID string,
	) error

	// Scan disk in batches per batchSize blobs each.
	ScanDisk(
		ctx context.Context,
		diskID string,
		batchSize uint32,
	) error

	GetScanDiskStatus(
		ctx context.Context,
		diskID string,
	) (ScanDiskStatus, error)

	// Sets IsFillFinished flag in VolumeConfig.
	FinishFillDisk(
		ctx context.Context,
		saveState func() error,
		diskID string,
		fillGeneration uint64,
	) error
}

////////////////////////////////////////////////////////////////////////////////

type MultiZoneClient interface {
	// Clones volume and deletes its old version with outdated FillGeneration
	// (if it exists).
	Clone(
		ctx context.Context,
		diskID string,
		dstPlacementGroupID string,
		dstPlacementPartitionIndex uint32,
		fillGeneration uint64,
		baseDiskID string,
	) error
}

////////////////////////////////////////////////////////////////////////////////

type Factory interface {
	GetZones() []string

	GetShards(zoneID string) []string

	HasClient(zoneID string) bool

	GetClient(ctx context.Context, zoneID string) (Client, error)

	// Returns client from default zone. Use it carefully.
	GetClientFromDefaultZone(ctx context.Context) (Client, error)

	GetMultiZoneClient(
		srcZoneID string,
		dstZoneID string,
	) (MultiZoneClient, error)
}

////////////////////////////////////////////////////////////////////////////////

// Used in tests.
type TestingClient interface {
	Client

	FillDisk(
		ctx context.Context,
		diskID string,
		contentSize uint64,
	) (DiskContentInfo, error)

	FillEncryptedDisk(
		ctx context.Context,
		diskID string,
		contentSize uint64,
		encryption *types.EncryptionDesc,
	) (DiskContentInfo, error)

	GoWriteRandomBlocksToNbsDisk(
		ctx context.Context,
		diskID string,
	) (func() error, error)

	ValidateCrc32(
		ctx context.Context,
		diskID string,
		expectedDiskContentInfo DiskContentInfo,
	) error

	ValidateCrc32WithEncryption(
		ctx context.Context,
		diskID string,
		expectedDiskContentInfo DiskContentInfo,
		encryption *types.EncryptionDesc,
	) error

	CalculateCrc32(diskID string, contentSize uint64) (DiskContentInfo, error)

	CalculateCrc32WithEncryption(
		diskID string,
		contentSize uint64,
		encryption *types.EncryptionDesc,
	) (DiskContentInfo, error)

	MountForReadWrite(diskID string) (func(), error)

	Write(diskID string, startIndex int, bytes []byte) error

	GetCheckpoints(ctx context.Context, diskID string) ([]string, error)

	List(ctx context.Context) ([]string, error)

	BackupDiskRegistryState(
		ctx context.Context,
	) (*DiskRegistryStateBackup, error)

	DisableDevices(
		ctx context.Context,
		agentID string,
		deviceUUIDs []string,
		message string,
	) error

	ChangeDeviceStateToOnline(
		ctx context.Context,
		deviceUUID string,
		message string,
	) error
}
