package backup

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////

func TestSnapshotMetaJSON(t *testing.T) {
	creatingAt := time.Date(2026, 8, 31, 10, 0, 0, 0, time.UTC)

	meta, err := NewSnapshotMeta(resources.SnapshotMeta{
		ID:           "snap-1",
		FolderID:     "folder-1",
		Disk:         &types.Disk{ZoneId: "zone-a", DiskId: "disk-1"},
		CheckpointID: "cp-1",
		CreateTaskID: "task-42",
		CreatingAt:   creatingAt,
		CreatedBy:    "user-1",
		Size:         8192,
		StorageSize:  4096,
		Encryption: &types.EncryptionDesc{
			Mode: types.EncryptionMode_ENCRYPTION_AES_XTS,
			Key:  &types.EncryptionDesc_KeyHash{KeyHash: []byte("hash")},
		},
	})
	require.NoError(t, err)

	data, err := meta.Marshal()
	require.NoError(t, err)

	var fields map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &fields))
	require.Equal(t, "snap-1", fields["id"])
	require.Equal(t, "folder-1", fields["folder_id"])
	require.Equal(t, "zone-a", fields["zone_id"])
	require.Equal(t, "disk-1", fields["disk_id"])
	require.Equal(t, "cp-1", fields["checkpoint_id"])
	require.Equal(t, "task-42", fields["create_task_id"])
	require.Equal(t, "2026-08-31T10:00:00Z", fields["creating_at"])
	require.Equal(t, "user-1", fields["created_by"])
	require.EqualValues(t, 8192, fields["size"])
	require.EqualValues(t, 4096, fields["storage_size"])
	require.EqualValues(t, types.EncryptionMode_ENCRYPTION_AES_XTS, fields["encryption_mode"])

	var parsed SnapshotMeta
	require.NoError(t, json.Unmarshal(data, &parsed))
	require.Equal(t, meta, parsed)
}

func TestSnapshotMetaWithoutDisk(t *testing.T) {
	meta, err := NewSnapshotMeta(resources.SnapshotMeta{ID: "snap-1"})
	require.NoError(t, err)
	require.Empty(t, meta.DiskID)
	require.Empty(t, meta.ZoneID)
	require.EqualValues(t, types.EncryptionMode_NO_ENCRYPTION, meta.EncryptionMode)
	require.Nil(t, meta.EncryptionKeyHash)
}
