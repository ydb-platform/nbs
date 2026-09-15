package backup

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////

func TestSnapshotMetaJSON(t *testing.T) {
	createdAt := time.Date(2026, 8, 31, 10, 0, 0, 0, time.UTC)

	meta, err := NewSnapshotMeta(
		storage.SnapshotMeta{
			ID:               "snap-1",
			Disk:             &types.Disk{ZoneId: "zone-a", DiskId: "disk-1"},
			CheckpointID:     "cp-1",
			CreateTaskID:     "task-42",
			BaseSnapshotID:   "snap-0",
			BaseCheckpointID: "cp-0",
			Size:             8192,
			StorageSize:      4096,
			ChunkCount:       2,
			CreatedAt:        createdAt,
			Encryption: &types.EncryptionDesc{
				Mode: types.EncryptionMode_ENCRYPTION_AES_XTS,
				Key:  &types.EncryptionDesc_KeyHash{KeyHash: []byte("hash")},
			},
		},
		4096,
		"lz4",
	)
	require.NoError(t, err)

	data, err := meta.Marshal()
	require.NoError(t, err)

	var fields map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &fields))
	require.EqualValues(t, 1, fields["version"])
	require.Equal(t, "snapshot", fields["kind"])
	require.Equal(t, "snap-1", fields["id"])
	require.Equal(t, "zone-a", fields["zone_id"])
	require.Equal(t, "disk-1", fields["disk_id"])
	require.Equal(t, "snap-0", fields["base_snapshot_id"])
	require.Equal(t, "2026-08-31T10:00:00Z", fields["created_at"])
	require.EqualValues(t, 2, fields["chunk_count"])
	require.EqualValues(t, 4096, fields["chunk_size"])
	require.Equal(t, "task-42", fields["task_id"])
	require.EqualValues(t, types.EncryptionMode_ENCRYPTION_AES_XTS, fields["encryption_mode"])
	require.Equal(t, "lz4", fields["compression"])

	var parsed SnapshotMeta
	require.NoError(t, json.Unmarshal(data, &parsed))
	require.Equal(t, meta, parsed)
}

func TestSnapshotMetaWithoutDisk(t *testing.T) {
	meta, err := NewSnapshotMeta(storage.SnapshotMeta{ID: "snap-1"}, 4096, "")
	require.NoError(t, err)
	require.Empty(t, meta.DiskID)
	require.Empty(t, meta.ZoneID)
	require.EqualValues(t, types.EncryptionMode_NO_ENCRYPTION, meta.EncryptionMode)
	require.Nil(t, meta.EncryptionKeyHash)
}
