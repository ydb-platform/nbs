package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type filesystemSnapshotStatus uint32

func (s *filesystemSnapshotStatus) UnmarshalYDB(res persistence.RawValue) error {
	*s = filesystemSnapshotStatus(res.Int64())
	return nil
}

// NOTE: These values are stored in DB, do not shuffle them around.
const (
	filesystemSnapshotStatusCreating filesystemSnapshotStatus = iota
	filesystemSnapshotStatusReady    filesystemSnapshotStatus = iota
	filesystemSnapshotStatusDeleting filesystemSnapshotStatus = iota
)

func filesystemSnapshotStatusToString(status filesystemSnapshotStatus) string {
	switch status {
	case filesystemSnapshotStatusCreating:
		return "creating"
	case filesystemSnapshotStatusReady:
		return "ready"
	case filesystemSnapshotStatusDeleting:
		return "deleting"
	}

	return fmt.Sprintf("unknown_%v", status)
}

////////////////////////////////////////////////////////////////////////////////

// This is mapped into a DB row. If you change this struct, make sure to update
// the mapping code.
type filesystemSnapshotState struct {
	id           string
	zoneID       string
	filesystemID string
	createTaskID string
	creatingAt   time.Time
	createdAt    time.Time
	deletingAt   time.Time
	size         uint64
	storageSize  uint64
	chunkCount   uint32
	lockTaskID   string
	status       filesystemSnapshotStatus
}

func (s *filesystemSnapshotState) toFilesystemSnapshotMeta() *FilesystemSnapshotMeta {
	var filesystem *types.Filesystem
	if len(s.filesystemID) != 0 {
		filesystem = &types.Filesystem{
			ZoneId:       s.zoneID,
			FilesystemId: s.filesystemID,
		}
	}

	return &FilesystemSnapshotMeta{
		ID:           s.id,
		Filesystem:   filesystem,
		CreateTaskID: s.createTaskID,
		Size:         s.size,
		StorageSize:  s.storageSize,
		LockTaskID:   s.lockTaskID,
		ChunkCount:   s.chunkCount,
		Ready:        s.status == filesystemSnapshotStatusReady,
	}
}

func (s *filesystemSnapshotState) structValue() persistence.Value {
	return persistence.StructValue(
		persistence.StructFieldValue("id", persistence.UTF8Value(s.id)),
		persistence.StructFieldValue("zone_id", persistence.UTF8Value(s.zoneID)),
		persistence.StructFieldValue("filesystem_id", persistence.UTF8Value(s.filesystemID)),
		persistence.StructFieldValue("create_task_id", persistence.UTF8Value(s.createTaskID)),
		persistence.StructFieldValue("creating_at", persistence.TimestampValue(s.creatingAt)),
		persistence.StructFieldValue("created_at", persistence.TimestampValue(s.createdAt)),
		persistence.StructFieldValue("deleting_at", persistence.TimestampValue(s.deletingAt)),
		persistence.StructFieldValue("size", persistence.Uint64Value(s.size)),
		persistence.StructFieldValue("storage_size", persistence.Uint64Value(s.storageSize)),
		persistence.StructFieldValue("chunk_count", persistence.Uint32Value(s.chunkCount)),
		persistence.StructFieldValue("lock_task_id", persistence.UTF8Value(s.lockTaskID)),
		persistence.StructFieldValue("status", persistence.Int64Value(int64(s.status))),
	)
}

func scanFilesystemSnapshotState(res persistence.Result) (state filesystemSnapshotState, err error) {
	err = res.ScanNamed(
		persistence.OptionalWithDefault("id", &state.id),
		persistence.OptionalWithDefault("zone_id", &state.zoneID),
		persistence.OptionalWithDefault("filesystem_id", &state.filesystemID),
		persistence.OptionalWithDefault("create_task_id", &state.createTaskID),
		persistence.OptionalWithDefault("creating_at", &state.creatingAt),
		persistence.OptionalWithDefault("created_at", &state.createdAt),
		persistence.OptionalWithDefault("deleting_at", &state.deletingAt),
		persistence.OptionalWithDefault("size", &state.size),
		persistence.OptionalWithDefault("storage_size", &state.storageSize),
		persistence.OptionalWithDefault("chunk_count", &state.chunkCount),
		persistence.OptionalWithDefault("lock_task_id", &state.lockTaskID),
		persistence.OptionalWithDefault("status", &state.status),
	)
	if err != nil {
		return state, errors.NewNonRetriableErrorf(
			"scanFilesystemSnapshotStates: failed to parse row: %w",
			err,
		)
	}

	return state, nil
}

func scanFilesystemSnapshotStates(ctx context.Context, res persistence.Result) ([]filesystemSnapshotState, error) {
	var states []filesystemSnapshotState
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			state, err := scanFilesystemSnapshotState(res)
			if err != nil {
				return nil, err
			}

			states = append(states, state)
		}
	}

	return states, nil
}

func filesystemSnapshotStateStructTypeString() string {
	return `Struct<
		id: Utf8,
		zone_id: Utf8,
		filesystem_id: Utf8,
		create_task_id: Utf8,
		creating_at: Timestamp,
		created_at: Timestamp,
		deleting_at: Timestamp,
		size: Uint64,
		storage_size: Uint64,
		chunk_count: Uint32,
		lock_task_id: Utf8,
		status: Int64>`
}
