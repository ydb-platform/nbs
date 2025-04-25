package resources

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type DiskMeta struct {
	ID                      string `json:"id"`
	ZoneID                  string `json:"zone_id"`
	SrcImageID              string `json:"src_image_id"`
	SrcSnapshotID           string `json:"src_snapshot_id"`
	BlocksCount             uint64 `json:"blocks_count"`
	BlockSize               uint32 `json:"block_size"`
	Kind                    string `json:"kind"`
	CloudID                 string `json:"cloud_id"`
	FolderID                string `json:"folder_id"`
	PlacementGroupID        string `json:"placement_group_id"`
	PlacementPartitionIndex string `json:"placement_partition_index"`

	CreateRequest proto.Message `json:"create_request"`
	CreateTaskID  string        `json:"create_task_id"`
	CreatingAt    time.Time     `json:"creating_at"`
	CreatedAt     time.Time     `json:"created_at"`
	CreatedBy     string        `json:"created_by"`
	DeleteTaskID  string        `json:"delete_task_id"`

	ScannedAt            time.Time `json:"scanned_at"`
	ScanFoundBrokenBlobs bool      `json:"scan_found_broken_blobs"`

	FillGeneration uint64 `json:"fill_generation"`
}

type ImageMeta struct {
	ID                string                `json:"id"`
	FolderID          string                `json:"folder_id"`
	SrcDiskID         string                `json:"src_disk_id"`
	CheckpointID      string                `json:"checkpoint_id"`
	SrcImageID        string                `json:"src_image_id"`
	SrcSnapshotID     string                `json:"src_snapshot_id"`
	CreateRequest     proto.Message         `json:"create_request"`
	CreateTaskID      string                `json:"create_task_id"`
	CreatingAt        time.Time             `json:"creating_at"`
	CreatedBy         string                `json:"created_by"`
	DeleteTaskID      string                `json:"delete_task_id"`
	UseDataplaneTasks bool                  `json:"use_dataplane_tasks"`
	Size              uint64                `json:"size"`
	StorageSize       uint64                `json:"storage_size"`
	Encryption        *types.EncryptionDesc `json:"encryption"`
	Ready             bool                  `json:"ready"`
}

type SnapshotMeta struct {
	ID                string                `json:"id"`
	FolderID          string                `json:"folder_id"`
	Disk              *types.Disk           `json:"disk"`
	CheckpointID      string                `json:"checkpoint_id"`
	CreateRequest     proto.Message         `json:"create_request"`
	CreateTaskID      string                `json:"create_task_id"`
	CreatingAt        time.Time             `json:"creating_at"`
	CreatedBy         string                `json:"created_by"`
	DeleteTaskID      string                `json:"delete_task_id"`
	UseDataplaneTasks bool                  `json:"use_dataplane_tasks"`
	Size              uint64                `json:"size"`
	StorageSize       uint64                `json:"storage_size"`
	Encryption        *types.EncryptionDesc `json:"encryption"`
	Ready             bool                  `json:"ready"`
}

type FilesystemMeta struct {
	ID            string        `json:"id"`
	ZoneID        string        `json:"zone_id"`
	BlocksCount   uint64        `json:"blocks_count"`
	BlockSize     uint32        `json:"block_size"`
	Kind          string        `json:"kind"`
	CloudID       string        `json:"cloud_id"`
	FolderID      string        `json:"folder_id"`
	CreateRequest proto.Message `json:"create_request"`
	CreateTaskID  string        `json:"create_task_id"`
	CreatingAt    time.Time     `json:"creating_at"`
	CreatedAt     time.Time     `json:"created_at"`
	CreatedBy     string        `json:"created_by"`
	DeleteTaskID  string        `json:"delete_task_id"`
}

type PlacementGroupMeta struct {
	ID                      string                  `json:"id"`
	ZoneID                  string                  `json:"zone_id"`
	PlacementStrategy       types.PlacementStrategy `json:"placement_strategy"`
	PlacementPartitionCount uint32                  `json:"placement_partition_count"`
	CreateRequest           proto.Message           `json:"create_request"`
	CreateTaskID            string                  `json:"create_task_id"`
	CreatingAt              time.Time               `json:"creating_at"`
	CreatedAt               time.Time               `json:"created_at"`
	CreatedBy               string                  `json:"created_by"`
	DeleteTaskID            string                  `json:"delete_task_id"`
}

type Storage interface {
	// Returns disk if action has been accepted by storage and nil otherwise.
	CreateDisk(ctx context.Context, disk DiskMeta) (*DiskMeta, error)

	DiskCreated(ctx context.Context, disk DiskMeta) error

	GetDiskMeta(ctx context.Context, diskID string) (*DiskMeta, error)

	// Returns disk if action has been accepted by storage and nil otherwise.
	DeleteDisk(
		ctx context.Context,
		diskID string,
		taskID string,
		deletingAt time.Time,
	) (*DiskMeta, error)

	DiskDeleted(ctx context.Context, diskID string, deletedAt time.Time) error

	ClearDeletedDisks(ctx context.Context, deletedBefore time.Time, limit int) error

	// Lists all existing disk ids in specified |folderID|.
	// Lists all existing disk ids if |folderID| is not set.
	ListDisks(
		ctx context.Context,
		folderID string,
		creatingBefore time.Time,
	) ([]string, error)

	// Returns image if action has been accepted by storage and nil otherwise.
	CreateImage(ctx context.Context, image ImageMeta) (ImageMeta, error)

	ImageCreated(
		ctx context.Context,
		imageID string,
		checkpointID string,
		createdAt time.Time,
		imageSize uint64,
		imageStorageSize uint64,
	) error

	GetImageMeta(ctx context.Context, imageID string) (*ImageMeta, error)

	// Returns image if action has been accepted by storage and nil otherwise.
	DeleteImage(
		ctx context.Context,
		imageID string,
		taskID string,
		deletingAt time.Time,
	) (*ImageMeta, error)

	ImageDeleted(ctx context.Context, imageID string, deletedAt time.Time) error

	ClearDeletedImages(ctx context.Context, deletedBefore time.Time, limit int) error

	// Lists all existing image ids in specified |folderID|.
	// Lists all existing image ids if |folderID| is not set.
	ListImages(
		ctx context.Context,
		folderID string,
		creatingBefore time.Time,
	) ([]string, error)

	// Returns snapshot if action has been accepted by storage and nil otherwise.
	CreateSnapshot(ctx context.Context, snapshot SnapshotMeta) (SnapshotMeta, error)

	SnapshotCreated(
		ctx context.Context,
		snapshotID string,
		checkpointID string,
		createdAt time.Time,
		snapshotSize uint64,
		snapshotStorageSize uint64,
	) error

	GetSnapshotMeta(ctx context.Context, snapshotID string) (*SnapshotMeta, error)

	// Returns snapshot if action has been accepted by storage and nil otherwise.
	DeleteSnapshot(
		ctx context.Context,
		snapshotID string,
		taskID string,
		deletingAt time.Time,
	) (*SnapshotMeta, error)

	SnapshotDeleted(ctx context.Context, snapshotID string, deletedAt time.Time) error

	ClearDeletedSnapshots(ctx context.Context, deletedBefore time.Time, limit int) error

	// Lists all existing snapshot ids in specified |folderID|.
	// Lists all existing snapshot ids if |folderID| is not set.
	ListSnapshots(
		ctx context.Context,
		folderID string,
		creatingBefore time.Time,
	) ([]string, error)

	// Returns filesystem if action has been accepted by storage and nil otherwise.
	CreateFilesystem(ctx context.Context, filesystem FilesystemMeta) (*FilesystemMeta, error)

	FilesystemCreated(ctx context.Context, filesystem FilesystemMeta) error

	GetFilesystemMeta(ctx context.Context, filesystemID string) (*FilesystemMeta, error)

	// Returns filesystem if action has been accepted by storage and nil otherwise.
	DeleteFilesystem(
		ctx context.Context,
		filesystemID string,
		taskID string,
		deletingAt time.Time,
	) (*FilesystemMeta, error)

	FilesystemDeleted(ctx context.Context, filesystemID string, deletedAt time.Time) error

	ClearDeletedFilesystems(ctx context.Context, deletedBefore time.Time, limit int) error

	// Lists all existing filesystem ids in specified |folderID|.
	// Lists all existing filesystem ids if |folderID| is not set.
	ListFilesystems(
		ctx context.Context,
		folderID string,
		creatingBefore time.Time,
	) ([]string, error)

	// Returns placementGroup if action has been accepted by storage and nil otherwise.
	CreatePlacementGroup(ctx context.Context, placementGroup PlacementGroupMeta) (*PlacementGroupMeta, error)

	PlacementGroupCreated(ctx context.Context, placementGroup PlacementGroupMeta) error

	GetPlacementGroupMeta(ctx context.Context, placementGroupID string) (*PlacementGroupMeta, error)

	CheckPlacementGroupReady(ctx context.Context, placementGroupID string) (*PlacementGroupMeta, error)

	// Returns placementGroup if action has been accepted by storage and nil otherwise.
	DeletePlacementGroup(
		ctx context.Context,
		placementGroupID string,
		taskID string,
		deletingAt time.Time,
	) (*PlacementGroupMeta, error)

	PlacementGroupDeleted(ctx context.Context, placementGroupID string, deletedAt time.Time) error

	ClearDeletedPlacementGroups(ctx context.Context, deletedBefore time.Time, limit int) error

	// Lists all existing placement group ids in specified |folderID|.
	// Lists all existing placement group ids if |folderID| is not set.
	ListPlacementGroups(
		ctx context.Context,
		folderID string,
		creatingBefore time.Time,
	) ([]string, error)

	IncrementFillGeneration(
		ctx context.Context,
		diskID string,
	) (uint64, error)

	DiskScanned(
		ctx context.Context,
		diskID string,
		scannedAt time.Time,
		foundBrokenBlobs bool,
	) error

	DiskRelocated(
		ctx context.Context,
		tx *persistence.Transaction,
		diskID string,
		srcZoneID string,
		dstZoneID string,
	) error
}
