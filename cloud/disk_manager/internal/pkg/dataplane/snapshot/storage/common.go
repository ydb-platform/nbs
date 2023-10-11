package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/persistence"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	ydb_result "github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	ydb_named "github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	ydb_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

////////////////////////////////////////////////////////////////////////////////

type snapshotStatus uint32

func (s *snapshotStatus) UnmarshalYDB(res ydb_types.RawValue) error {
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

func (s *snapshotState) structValue() ydb_types.Value {
	return ydb_types.StructValue(
		ydb_types.StructFieldValue("id", ydb_types.UTF8Value(s.id)),
		ydb_types.StructFieldValue("creating_at", persistence.TimestampValue(s.creatingAt)),
		ydb_types.StructFieldValue("created_at", persistence.TimestampValue(s.createdAt)),
		ydb_types.StructFieldValue("deleting_at", persistence.TimestampValue(s.deletingAt)),
		ydb_types.StructFieldValue("size", ydb_types.Uint64Value(s.size)),
		ydb_types.StructFieldValue("storage_size", ydb_types.Uint64Value(s.storageSize)),
		ydb_types.StructFieldValue("chunk_count", ydb_types.Uint32Value(s.chunkCount)),
		ydb_types.StructFieldValue("encryption_mode", ydb_types.Uint32Value(s.encryptionMode)),
		ydb_types.StructFieldValue("encryption_keyhash", ydb_types.StringValue(s.encryptionKeyHash)),
		ydb_types.StructFieldValue("status", ydb_types.Int64Value(int64(s.status))),
	)
}

func scanSnapshotState(res ydb_result.Result) (state snapshotState, err error) {
	err = res.ScanNamed(
		ydb_named.OptionalWithDefault("id", &state.id),
		ydb_named.OptionalWithDefault("creating_at", &state.creatingAt),
		ydb_named.OptionalWithDefault("created_at", &state.createdAt),
		ydb_named.OptionalWithDefault("deleting_at", &state.deletingAt),
		ydb_named.OptionalWithDefault("size", &state.size),
		ydb_named.OptionalWithDefault("storage_size", &state.storageSize),
		ydb_named.OptionalWithDefault("chunk_count", &state.chunkCount),
		ydb_named.OptionalWithDefault("encryption_mode", &state.encryptionMode),
		ydb_named.OptionalWithDefault("encryption_keyhash", &state.encryptionKeyHash),
		ydb_named.OptionalWithDefault("status", &state.status),
	)
	if err != nil {
		return state, errors.NewNonRetriableErrorf(
			"scanSnapshotStates: failed to parse row: %w",
			err,
		)
	}

	return state, nil
}

func scanSnapshotStates(ctx context.Context, res ydb_result.Result) ([]snapshotState, error) {
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
