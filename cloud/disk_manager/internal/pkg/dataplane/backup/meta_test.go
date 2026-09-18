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

	data, err := json.Marshal(meta)
	require.NoError(t, err)

	require.JSONEq(t, `{
		"id": "snap-1",
		"folder_id": "folder-1",
		"zone_id": "zone-a",
		"disk_id": "disk-1",
		"checkpoint_id": "cp-1",
		"create_task_id": "task-42",
		"creating_at": "2026-08-31T10:00:00Z",
		"created_by": "user-1",
		"size": 8192,
		"storage_size": 4096,
		"encryption_mode": 1,
		"encryption_keyhash": "aGFzaA=="
	}`, string(data))

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

func TestImageMetaJSON(t *testing.T) {
	creatingAt := time.Date(2026, 8, 31, 10, 0, 0, 0, time.UTC)

	meta, err := NewImageMeta(resources.ImageMeta{
		ID:            "image-1",
		FolderID:      "folder-1",
		SrcDiskID:     "disk-1",
		CheckpointID:  "cp-1",
		SrcImageID:    "image-0",
		SrcSnapshotID: "snap-1",
		CreateTaskID:  "task-42",
		CreatingAt:    creatingAt,
		CreatedBy:     "user-1",
		Size:          8192,
		StorageSize:   4096,
	})
	require.NoError(t, err)

	data, err := json.Marshal(meta)
	require.NoError(t, err)

	require.JSONEq(t, `{
		"id": "image-1",
		"folder_id": "folder-1",
		"src_disk_id": "disk-1",
		"checkpoint_id": "cp-1",
		"src_image_id": "image-0",
		"src_snapshot_id": "snap-1",
		"create_task_id": "task-42",
		"creating_at": "2026-08-31T10:00:00Z",
		"created_by": "user-1",
		"size": 8192,
		"storage_size": 4096,
		"encryption_mode": 0,
		"encryption_keyhash": null
	}`, string(data))

	var parsed ImageMeta
	require.NoError(t, json.Unmarshal(data, &parsed))
	require.Equal(t, meta, parsed)
}
