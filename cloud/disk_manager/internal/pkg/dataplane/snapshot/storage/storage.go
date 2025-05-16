package storage

import (
	"context"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	task_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

type SnapshotMeta struct {
	ID               string
	Disk             *types.Disk
	CheckpointID     string
	CreateTaskID     string
	BaseSnapshotID   string
	BaseCheckpointID string
	// Snapshot virtual size, i.e. the minimum amount of disk space needed to restore.
	Size uint64
	// Snapshot real size, i.e. the amount of disk space occupied in storage.
	StorageSize uint64
	LockTaskID  string
	ChunkCount  uint32
	Encryption  *types.EncryptionDesc
	Ready       bool
}

////////////////////////////////////////////////////////////////////////////////

type ChunkMapEntry struct {
	ChunkIndex uint32
	ChunkID    string
	StoredInS3 bool
}

////////////////////////////////////////////////////////////////////////////////

type Storage interface {
	CreateSnapshot(
		ctx context.Context,
		snapshotMeta SnapshotMeta,
	) (*SnapshotMeta, error)

	SnapshotCreated(
		ctx context.Context,
		snapshotID string,
		size uint64,
		storageSize uint64,
		chunkCount uint32,
		encryption *types.EncryptionDesc,
	) error

	DeletingSnapshot(
		ctx context.Context,
		snapshotID string,
		taskID string,
	) (*SnapshotMeta, error)

	GetSnapshotsToDelete(
		ctx context.Context,
		deletingBefore time.Time,
		limit int,
	) ([]*protos.DeletingSnapshotKey, error)

	DeleteSnapshotData(ctx context.Context, snapshotID string) error

	ClearDeletingSnapshots(
		ctx context.Context,
		keys []*protos.DeletingSnapshotKey,
	) error

	ShallowCopySnapshot(
		ctx context.Context,
		srcSnapshotID string,
		dstSnapshotID string,
		milestoneChunkIndex uint32,
		saveProgress func(context.Context, uint32) error,
	) error

	ShallowCopyChunk(
		ctx context.Context,
		srcEntry ChunkMapEntry,
		dstSnapshotID string,
	) error

	WriteChunk(
		ctx context.Context,
		uniqueID string,
		snapshotID string,
		chunk common.Chunk,
		useS3 bool,
	) (string, error)

	ReadChunkMap(
		ctx context.Context,
		snapshotID string,
		milestoneChunkIndex uint32,
	) (<-chan ChunkMapEntry, <-chan error)

	ReadChunk(ctx context.Context, chunk *common.Chunk) error

	CheckSnapshotReady(
		ctx context.Context,
		snapshotID string,
	) (SnapshotMeta, error)

	CheckSnapshotAlive(ctx context.Context, snapshotID string) error

	// Returns number of non-zero chunks.
	GetDataChunkCount(ctx context.Context, snapshotID string) (uint64, error)

	GetDeletingSnapshotCount(ctx context.Context) (uint64, error)

	GetSnapshotCount(ctx context.Context) (count uint64, err error)

	GetTotalSnapshotSize(ctx context.Context) (size uint64, err error)

	GetTotalSnapshotStorageSize(ctx context.Context) (storageSize uint64, err error)

	DeleteDiskFromIncremental(
		ctx context.Context,
		zoneID string,
		diskID string,
	) error

	LockSnapshot(
		ctx context.Context,
		snapshotID string,
		lockTaskID string,
	) (locked bool, err error)

	UnlockSnapshot(ctx context.Context, snapshotID string, lockTaskID string) error

	GetSnapshotMeta(
		ctx context.Context,
		snapshotID string,
	) (*SnapshotMeta, error)

	GetIncremental(
		ctx context.Context,
		disk *types.Disk,
	) (snapshotID string, checkpointID string, err error)

	ListSnapshots(ctx context.Context) (task_storage.StringSet, error)
}
