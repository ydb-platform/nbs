package storage

import (
	"context"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/storage/protos"
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

	CheckFilesystemSnapshotReady(
		ctx context.Context,
		snapshotID string,
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

	// LockFilesystemSnapshot prevents deletion while any lock remains. Multiple
	// callers can lock the same snapshot successfully without waiting for existing
	// locks to be released; repeated calls with the same taskID are idempotent.
	LockFilesystemSnapshot(
		ctx context.Context,
		snapshotID string,
		taskID string,
	) error

	// UnlockFilesystemSnapshot removes the lock owned by taskID. It is
	// idempotent, including for deleting or missing snapshots; deletion is
	// unblocked after the last lock is removed.
	UnlockFilesystemSnapshot(
		ctx context.Context,
		snapshotID string,
		taskID string,
	) error

	GetFilesystemSnapshotMeta(
		ctx context.Context,
		snapshotID string,
	) (*FilesystemSnapshotMeta, error)

	ListFilesystemSnapshots(
		ctx context.Context,
	) (tasks_common.StringSet, error)

	// Used by tests to verify all the data is correctly deleted by collect snapshots task.
	TablesEmpty(ctx context.Context) (bool, error)
}
