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

type snapshotStatus uint32

func (s *snapshotStatus) UnmarshalYDB(res persistence.RawValue) error {
	*s = snapshotStatus(res.Int64())
	return nil
}

// NOTE: These values are stored in DB, do not shuffle them around.
const (
	snapshotStatusCreating snapshotStatus = iota
	snapshotStatusReady    snapshotStatus = iota
	snapshotStatusDeleting snapshotStatus = iota
)

func snapshotStatusToString(status snapshotStatus) string {
	switch status {
	case snapshotStatusCreating:
		return "creating"
	case snapshotStatusReady:
		return "ready"
	case snapshotStatusDeleting:
		return "deleting"
	}

	return fmt.Sprintf("unknown_%v", status)
}

////////////////////////////////////////////////////////////////////////////////

// This is mapped into a DB row. If you change this struct, make sure to update
// the mapping code.
type snapshotState struct {
	id                string
	creatingAt        time.Time
	createdAt         time.Time
	deletingAt        time.Time
	size              uint64
	storageSize       uint64
	chunkCount        uint32
	encryptionMode    uint32
	encryptionKeyHash []byte
	status            snapshotStatus
}

func (s *snapshotState) toSnapshotMeta() *SnapshotMeta {
	return &SnapshotMeta{
		Size:        s.size,
		StorageSize: s.storageSize,
		ChunkCount:  s.chunkCount,
		Encryption: &types.EncryptionDesc{
			Mode: types.EncryptionMode(s.encryptionMode),
			Key: &types.EncryptionDesc_KeyHash{
				KeyHash: s.encryptionKeyHash,
			},
		},
		Ready: s.status == snapshotStatusReady,
	}
}

func (s *snapshotState) structValue() persistence.Value {
	return persistence.StructValue(
		persistence.StructFieldValue("id", persistence.UTF8Value(s.id)),
		persistence.StructFieldValue("creating_at", persistence.TimestampValue(s.creatingAt)),
		persistence.StructFieldValue("created_at", persistence.TimestampValue(s.createdAt)),
		persistence.StructFieldValue("deleting_at", persistence.TimestampValue(s.deletingAt)),
		persistence.StructFieldValue("size", persistence.Uint64Value(s.size)),
		persistence.StructFieldValue("storage_size", persistence.Uint64Value(s.storageSize)),
		persistence.StructFieldValue("chunk_count", persistence.Uint32Value(s.chunkCount)),
		persistence.StructFieldValue("encryption_mode", persistence.Uint32Value(s.encryptionMode)),
		persistence.StructFieldValue("encryption_keyhash", persistence.StringValue(s.encryptionKeyHash)),
		persistence.StructFieldValue("status", persistence.Int64Value(int64(s.status))),
	)
}

func scanSnapshotState(res persistence.Result) (state snapshotState, err error) {
	err = res.ScanNamed(
		persistence.OptionalWithDefault("id", &state.id),
		persistence.OptionalWithDefault("creating_at", &state.creatingAt),
		persistence.OptionalWithDefault("created_at", &state.createdAt),
		persistence.OptionalWithDefault("deleting_at", &state.deletingAt),
		persistence.OptionalWithDefault("size", &state.size),
		persistence.OptionalWithDefault("storage_size", &state.storageSize),
		persistence.OptionalWithDefault("chunk_count", &state.chunkCount),
		persistence.OptionalWithDefault("encryption_mode", &state.encryptionMode),
		persistence.OptionalWithDefault("encryption_keyhash", &state.encryptionKeyHash),
		persistence.OptionalWithDefault("status", &state.status),
	)
	if err != nil {
		return state, errors.NewNonRetriableErrorf(
			"scanSnapshotStates: failed to parse row: %w",
			err,
		)
	}

	return state, nil
}

func scanSnapshotStates(ctx context.Context, res persistence.Result) ([]snapshotState, error) {
	var states []snapshotState
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			state, err := scanSnapshotState(res)
			if err != nil {
				return nil, err
			}

			states = append(states, state)
		}
	}

	return states, nil
}

func snapshotStateStructTypeString() string {
	return `Struct<
		id: Utf8,
		creating_at: Timestamp,
		created_at: Timestamp,
		deleting_at: Timestamp,
		size: Uint64,
		storage_size: Uint64,
		chunk_count: Uint32,
		encryption_mode: Uint32,
		encryption_keyhash: String,
		status: Int64>`
}

////////////////////////////////////////////////////////////////////////////////

type IncrementalInfo struct {
	ZoneID         string
	DiskID         string
	CheckpointID   string
	BaseSnapshotID string
}
