package backup

import (
	"encoding/json"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
)

////////////////////////////////////////////////////////////////////////////////

const snapshotMetaVersion = 1

// Content of meta.json. Everything that is needed to restore the
// snapshot row of data plane, nothing from the working state of the
// installation (task ids, locks, queues).
type SnapshotMeta struct {
	Version           uint32    `json:"version"`
	Kind              string    `json:"kind"`
	ID                string    `json:"id"`
	ZoneID            string    `json:"zone_id"`
	DiskID            string    `json:"disk_id"`
	CheckpointID      string    `json:"checkpoint_id"`
	BaseSnapshotID    string    `json:"base_snapshot_id"`
	BaseCheckpointID  string    `json:"base_checkpoint_id"`
	CreatedAt         time.Time `json:"created_at"`
	Size              uint64    `json:"size"`
	StorageSize       uint64    `json:"storage_size"`
	ChunkCount        uint32    `json:"chunk_count"`
	ChunkSize         uint32    `json:"chunk_size"`
	TaskID            string    `json:"task_id"`
	EncryptionMode    uint32    `json:"encryption_mode"`
	EncryptionKeyHash []byte    `json:"encryption_keyhash"`
	Compression       string    `json:"compression"`
}

func NewSnapshotMeta(
	meta storage.SnapshotMeta,
	chunkSize uint32,
	compression string,
) (SnapshotMeta, error) {

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
		Version:           snapshotMetaVersion,
		Kind:              "snapshot",
		ID:                meta.ID,
		ZoneID:            zoneID,
		DiskID:            diskID,
		CheckpointID:      meta.CheckpointID,
		BaseSnapshotID:    meta.BaseSnapshotID,
		BaseCheckpointID:  meta.BaseCheckpointID,
		CreatedAt:         meta.CreatedAt.UTC(),
		Size:              meta.Size,
		StorageSize:       meta.StorageSize,
		ChunkCount:        meta.ChunkCount,
		ChunkSize:         chunkSize,
		TaskID:            meta.CreateTaskID,
		EncryptionMode:    uint32(encryptionMode),
		EncryptionKeyHash: encryptionKeyHash,
		Compression:       compression,
	}, nil
}

func (m SnapshotMeta) Marshal() ([]byte, error) {
	return json.MarshalIndent(m, "", "  ")
}
