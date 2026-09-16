package backup

import (
	"context"
	"encoding/json"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
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

////////////////////////////////////////////////////////////////////////////////

type ImageMeta struct {
	ID                string    `json:"id"`
	FolderID          string    `json:"folder_id"`
	SrcDiskID         string    `json:"src_disk_id"`
	CheckpointID      string    `json:"checkpoint_id"`
	SrcImageID        string    `json:"src_image_id"`
	SrcSnapshotID     string    `json:"src_snapshot_id"`
	CreateTaskID      string    `json:"create_task_id"`
	CreatingAt        time.Time `json:"creating_at"`
	CreatedBy         string    `json:"created_by"`
	Size              uint64    `json:"size"`
	StorageSize       uint64    `json:"storage_size"`
	EncryptionMode    uint32    `json:"encryption_mode"`
	EncryptionKeyHash []byte    `json:"encryption_keyhash"`
}

func NewImageMeta(meta resources.ImageMeta) (ImageMeta, error) {
	encryptionMode, encryptionKeyHash, err := resources.GetEncryptionModeAndKeyHash(
		meta.Encryption,
	)
	if err != nil {
		return ImageMeta{}, err
	}

	return ImageMeta{
		ID:                meta.ID,
		FolderID:          meta.FolderID,
		SrcDiskID:         meta.SrcDiskID,
		CheckpointID:      meta.CheckpointID,
		SrcImageID:        meta.SrcImageID,
		SrcSnapshotID:     meta.SrcSnapshotID,
		CreateTaskID:      meta.CreateTaskID,
		CreatingAt:        meta.CreatingAt.UTC(),
		CreatedBy:         meta.CreatedBy,
		Size:              meta.Size,
		StorageSize:       meta.StorageSize,
		EncryptionMode:    uint32(encryptionMode),
		EncryptionKeyHash: encryptionKeyHash,
	}, nil
}

////////////////////////////////////////////////////////////////////////////////

func WriteMeta(
	ctx context.Context,
	s3 *persistence.S3Client,
	bucket string,
	key string,
	meta interface{},
) error {

	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return errors.NewNonRetriableError(err)
	}

	return s3.PutObject(ctx, bucket, key, persistence.S3Object{Data: data})
}

func ScheduleBackupSnapshot(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	scheduler tasks.Scheduler,
	snapshotID string,
) (string, error) {

	return scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(ctx, execCtx.GetTaskID()+"_run"),
		"dataplane.BackupSnapshot",
		"",
		&protos.BackupSnapshotRequest{
			SnapshotId: snapshotID,
		},
	)
}
