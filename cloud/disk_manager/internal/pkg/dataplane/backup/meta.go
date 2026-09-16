package backup

import (
	"encoding/json"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
)

////////////////////////////////////////////////////////////////////////////////

type SnapshotMeta struct {
	ID                string    `json:"id"`
	FolderID          string    `json:"folder_id"`
	ZoneID            string    `json:"zone_id"`
	DiskID            string    `json:"disk_id"`
	CheckpointID      string    `json:"checkpoint_id"`
	CreateTaskID      string    `json:"create_task_id"`
	CreatingAt        time.Time `json:"creating_at"`
	CreatedBy         string    `json:"created_by"`
	Size              uint64    `json:"size"`
	StorageSize       uint64    `json:"storage_size"`
	EncryptionMode    uint32    `json:"encryption_mode"`
	EncryptionKeyHash []byte    `json:"encryption_keyhash"`
}

func NewSnapshotMeta(meta resources.SnapshotMeta) (SnapshotMeta, error) {
	encryptionMode, encryptionKeyHash, err := resources.GetEncryptionModeAndKeyHash(
		meta.Encryption,
	)
	if err != nil {
		return SnapshotMeta{}, err
	}

	var zoneID, diskID string
	if meta.Disk != nil {
		zoneID = meta.Disk.ZoneId
		diskID = meta.Disk.DiskId
	}

	return SnapshotMeta{
		ID:                meta.ID,
		FolderID:          meta.FolderID,
		ZoneID:            zoneID,
		DiskID:            diskID,
		CheckpointID:      meta.CheckpointID,
		CreateTaskID:      meta.CreateTaskID,
		CreatingAt:        meta.CreatingAt.UTC(),
		CreatedBy:         meta.CreatedBy,
		Size:              meta.Size,
		StorageSize:       meta.StorageSize,
		EncryptionMode:    uint32(encryptionMode),
		EncryptionKeyHash: encryptionKeyHash,
	}, nil
}

func (m SnapshotMeta) Marshal() ([]byte, error) {
	return json.MarshalIndent(m, "", "  ")
}
