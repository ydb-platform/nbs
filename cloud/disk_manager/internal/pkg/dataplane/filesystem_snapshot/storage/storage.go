package storage

import (
	"context"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem_snapshot/storage/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	tasks_common "github.com/ydb-platform/nbs/cloud/tasks/common"
)

////////////////////////////////////////////////////////////////////////////////

type FilesystemSnapshotMeta struct {
	ID           string
	Filesystem   *types.Filesystem
	CreateTaskID string
	// Snapshot virtual size, i.e. the minimum amount of disk space needed to restore.
	Size uint64
	// Snapshot real size, i.e. the amount of disk space occupied in storage.
	StorageSize uint64
	LockTaskID  string
	ChunkCount  uint32
	Ready       bool
}

type NodeQueueEntry struct {
	NodeID uint64
	Cookie string
	Depth  uint64
}

////////////////////////////////////////////////////////////////////////////////

type Storage interface {
	CreateFilesystemSnapshot(
		ctx context.Context,
		snapshotMeta FilesystemSnapshotMeta,
	) (*FilesystemSnapshotMeta, error)

	FilesystemSnapshotCreated(
		ctx context.Context,
		snapshotID string,
		size uint64,
		storageSize uint64,
		chunkCount uint32,
	) error

	DeletingFilesystemSnapshot(
		ctx context.Context,
		snapshotID string,
		taskID string,
	) (*FilesystemSnapshotMeta, error)

	GetFilesystemSnapshotsToDelete(
		ctx context.Context,
		deletingBefore time.Time,
		limit int,
	) ([]*protos.DeletingFilesystemSnapshotKey, error)

	ClearDeletingFilesystemSnapshots(
		ctx context.Context,
		keys []*protos.DeletingFilesystemSnapshotKey,
	) error

	CheckFilesystemSnapshotAlive(
		ctx context.Context,
		snapshotID string,
	) error

	GetFilesystemSnapshotCount(ctx context.Context) (count uint64, err error)

	GetTotalFilesystemSnapshotSize(
		ctx context.Context,
	) (size uint64, err error)

	GetTotalFilesystemSnapshotStorageSize(
		ctx context.Context,
	) (storageSize uint64, err error)

	LockFilesystemSnapshot(
		ctx context.Context,
		snapshotID string,
		lockTaskID string,
	) (locked bool, err error)

	UnlockFilesystemSnapshot(
		ctx context.Context,
		snapshotID string,
		lockTaskID string,
	) error

	GetFilesystemSnapshotMeta(
		ctx context.Context,
		snapshotID string,
	) (*FilesystemSnapshotMeta, error)

	ListFilesystemSnapshots(
		ctx context.Context,
	) (tasks_common.StringSet, error)

	ScheduleRootNodeForListing(
		ctx context.Context,
		snapshotID string,
	) (bool, error)

	SelectNodesToList(
		ctx context.Context,
		snapshotID string,
		nodesToExclude map[uint64]struct{},
		limit uint64,
	) ([]NodeQueueEntry, error)

	ScheduleNodesForListing(
		ctx context.Context,
		snapshotID string,
		nodeID uint64,
		nextCookie string,
		depth uint64,
		children []nfs.Node,
	) error
}
