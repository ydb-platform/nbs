package snapshots

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	backup_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup/config"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

// The single entry of an export: SnapshotService.Export, ImageService.Export
// and the admin commands all schedule it. Checks the deny list before
// anything is read, resolves the disk of a snapshot for the follower key and
// waits for dataplane.ExportBackupToS3 on the export nodes.
type exportToS3Task struct {
	scheduler tasks.Scheduler
	storage   resources.Storage
	config    *backup_config.SnapshotExportConfig
	request   *protos.ExportToS3Request
	state     *protos.ExportToS3TaskState
}

func (t *exportToS3Task) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *exportToS3Task) Load(request, state []byte) error {
	t.request = &protos.ExportToS3Request{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.ExportToS3TaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *exportToS3Task) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.checkDenyList()
	if err != nil {
		return err
	}

	srcDiskID, err := t.resolveSource(ctx)
	if err != nil {
		return err
	}

	if len(t.state.DataplaneTaskID) == 0 {
		idempotencyKey := fmt.Sprintf(
			"%v_%v_export",
			execCtx.GetTaskID(),
			t.request.SrcId,
		)

		taskID, err := t.scheduler.ScheduleTask(
			headers.SetIncomingIdempotencyKey(ctx, idempotencyKey),
			"dataplane.ExportBackupToS3",
			"",
			&dataplane_protos.ExportBackupToS3Request{
				SrcKind:              t.request.SrcKind,
				SrcId:                t.request.SrcId,
				SrcDiskId:            srcDiskID,
				ExpectedFolderId:     t.request.FolderId,
				Endpoint:             t.request.Endpoint,
				Region:               t.request.Region,
				Bucket:               t.request.Bucket,
				Prefix:               t.request.Prefix,
				PartSize:             t.request.PartSize,
				EncryptedDek:         t.request.EncryptedDek,
				EncryptedCredentials: t.request.EncryptedCredentials,
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
	}

	response, err := t.scheduler.WaitTask(
		ctx,
		execCtx,
		t.state.DataplaneTaskID,
	)
	if err != nil {
		return err
	}

	typedResponse, ok := response.(*dataplane_protos.ExportBackupToS3Response)
	if !ok {
		return errors.NewNonRetriableErrorf(
			"invalid dataplane.ExportBackupToS3 response type %T",
			response,
		)
	}

	t.state.Bucket = typedResponse.Bucket
	t.state.Prefix = typedResponse.Prefix
	t.state.ManifestKey = typedResponse.ManifestKey
	t.state.PartCount = typedResponse.PartCount
	t.state.Size = typedResponse.Size
	return nil
}

func (t *exportToS3Task) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if len(t.state.DataplaneTaskID) != 0 {
		_, err := t.scheduler.CancelTask(ctx, t.state.DataplaneTaskID)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *exportToS3Task) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	metadata := &disk_manager.ExportMetadata{}

	if len(t.state.DataplaneTaskID) != 0 {
		message, err := t.scheduler.GetTaskMetadata(
			ctx,
			t.state.DataplaneTaskID,
		)
		if err != nil {
			return nil, err
		}

		exportMetadata, ok := message.(*dataplane_protos.ExportBackupToS3Metadata)
		if ok {
			metadata.Progress = exportMetadata.Progress
		}
	}

	return metadata, nil
}

func (t *exportToS3Task) GetResponse() proto.Message {
	return &disk_manager.ExportResponse{
		Bucket:      t.state.Bucket,
		Prefix:      t.state.Prefix,
		ManifestKey: t.state.ManifestKey,
		PartCount:   t.state.PartCount,
		Size:        int64(t.state.Size),
	}
}

////////////////////////////////////////////////////////////////////////////////

func (t *exportToS3Task) checkDenyList() error {
	if common.Find(t.config.GetDenyListCloudIds(), t.request.CloudId) {
		return errors.NewNonRetriableErrorf(
			"export is denied for cloud %v",
			t.request.CloudId,
		)
	}

	if common.Find(t.config.GetDenyListFolderIds(), t.request.FolderId) {
		return errors.NewNonRetriableErrorf(
			"export is denied for folder %v",
			t.request.FolderId,
		)
	}

	return nil
}

// Returns the disk id of a snapshot: the follower keeps its meta.json under
// the disk. A missing resource means its follower copy is gone as well.
func (t *exportToS3Task) resolveSource(ctx context.Context) (string, error) {
	switch t.request.SrcKind {
	case dataplane_protos.BackupSourceKind_BACKUP_SOURCE_KIND_SNAPSHOT:
		meta, err := t.storage.GetSnapshotMeta(ctx, t.request.SrcId)
		if err != nil {
			return "", err
		}

		if meta == nil {
			return "", errors.NewNonRetriableErrorf(
				"snapshot %v is not found",
				t.request.SrcId,
			)
		}

		if meta.FolderID != t.request.FolderId {
			return "", errors.NewNonRetriableErrorf(
				"snapshot %v does not belong to folder %v",
				t.request.SrcId,
				t.request.FolderId,
			)
		}

		if meta.Disk == nil || len(meta.Disk.DiskId) == 0 {
			return "", errors.NewNonRetriableErrorf(
				"snapshot %v has no source disk",
				t.request.SrcId,
			)
		}

		return meta.Disk.DiskId, nil

	case dataplane_protos.BackupSourceKind_BACKUP_SOURCE_KIND_IMAGE:
		meta, err := t.storage.GetImageMeta(ctx, t.request.SrcId)
		if err != nil {
			return "", err
		}

		if meta == nil {
			return "", errors.NewNonRetriableErrorf(
				"image %v is not found",
				t.request.SrcId,
			)
		}

		if meta.FolderID != t.request.FolderId {
			return "", errors.NewNonRetriableErrorf(
				"image %v does not belong to folder %v",
				t.request.SrcId,
				t.request.FolderId,
			)
		}

		return "", nil

	default:
		return "", errors.NewNonRetriableErrorf(
			"unknown backup source kind %v",
			t.request.SrcKind,
		)
	}
}
