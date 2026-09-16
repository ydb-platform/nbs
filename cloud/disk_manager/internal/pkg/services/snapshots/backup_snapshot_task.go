package snapshots

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	backup_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type backupSnapshotTask struct {
	scheduler tasks.Scheduler
	storage   resources.Storage
	s3        *persistence.S3Client
	bucket    string
	keyPrefix string
	request   *protos.BackupSnapshotRequest
	state     *protos.BackupSnapshotTaskState
}

func (t *backupSnapshotTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *backupSnapshotTask) Load(request, state []byte) error {
	t.request = &protos.BackupSnapshotRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.BackupSnapshotTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *backupSnapshotTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	snapshotID := t.request.SnapshotId

	meta, err := t.storage.GetSnapshotMeta(ctx, snapshotID)
	if err != nil {
		return err
	}

	if meta == nil {
		return errors.NewNonRetriableErrorf(
			"snapshot %v is not found",
			snapshotID,
		)
	}

	var diskID string
	if meta.Disk != nil {
		diskID = meta.Disk.DiskId
	}

	err = t.writeMeta(ctx, diskID, meta)
	if err != nil {
		return err
	}

	taskID, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(ctx, execCtx.GetTaskID()+"_run"),
		"dataplane.BackupSnapshot",
		"",
		&backup_protos.BackupSnapshotRequest{
			SnapshotId: snapshotID,
			DiskId:     diskID,
		},
	)
	if err != nil {
		return err
	}

	t.state.DataplaneTaskID = taskID

	err = execCtx.SaveState(ctx)
	if err != nil {
		return err
	}

	_, err = t.scheduler.WaitTask(ctx, execCtx, taskID)
	return err
}

func (t *backupSnapshotTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *backupSnapshotTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *backupSnapshotTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func (t *backupSnapshotTask) writeMeta(
	ctx context.Context,
	diskID string,
	meta *resources.SnapshotMeta,
) error {

	snapshotMeta, err := backup.NewSnapshotMeta(*meta)
	if err != nil {
		return err
	}

	data, err := snapshotMeta.Marshal()
	if err != nil {
		return errors.NewNonRetriableError(err)
	}

	return t.s3.PutObject(
		ctx,
		t.bucket,
		backup.MetaKey(t.keyPrefix, diskID, meta.ID),
		persistence.S3Object{Data: data},
	)
}
