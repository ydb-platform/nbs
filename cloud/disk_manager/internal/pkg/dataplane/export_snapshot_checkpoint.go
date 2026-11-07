package dataplane

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/exporter"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

type snapshotExportCheckpoint struct {
	task          *exportSnapshotTask
	execCtx       tasks.ExecutionContext
	sourceRetries *snapshotExportSourceRetries
	uploads       *snapshotExportUploadLimiter
}

func (c *snapshotExportCheckpoint) ReadParts(
	ctx context.Context,
	after uint32,
	limit uint64,
) ([]exporter.S3ManifestPart, error) {

	return c.task.parts.ReadParts(ctx, c.execCtx.GetTaskID(), after, limit)
}

func (c *snapshotExportCheckpoint) Save(
	ctx context.Context,
	state *exporter.S3ExportState,
	parts []exporter.S3ManifestPart,
) error {

	t := c.task
	taskID := c.execCtx.GetTaskID()
	c.sourceRetries.checkpoint(t.state)
	c.uploads.checkpoint(t.state)
	if state.Manifest.Size != 0 {
		t.state.Progress = 0.99 * float64(state.UploadedBytes) /
			float64(state.Manifest.Size)
	}
	if state.Completed {
		t.state.Progress = 1
	}
	if !t.state.PartsInYDB {
		return t.saveState(ctx, c.execCtx)
	}
	prepare := func(
		ctx context.Context,
		tx *persistence.Transaction,
	) error {

		return t.parts.UpsertParts(ctx, tx, taskID, parts)
	}

	err := c.execCtx.SaveStateWithPreparation(ctx, prepare)
	if err != nil {
		logging.Error(
			ctx, "export checkpoint transaction failed, task_id %v: %v",
			c.execCtx.GetTaskID(), err,
		)
	}
	return err
}

// Pin the checkpoint format before the first export initialization. Existing
// JSON checkpoints stay in JSON, even when the table is enabled later.
func (t *exportSnapshotTask) prepareExportParts(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if t.state.PartsInYDB || t.resume.Initialized ||
		!t.config.GetUseExportPartsTable() {

		return nil
	}
	if t.parts == nil {
		return errors.NewNonRetriableErrorf(
			"export part storage is not configured",
		)
	}
	t.state.PartsInYDB = true
	return t.saveState(ctx, execCtx)
}

// Completed is saved before cleanup starts. A retry of this phase does not
// contact Compute, read the snapshot, or publish the manifest again.
func (t *exportSnapshotTask) clearExportParts(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if !t.state.PartsInYDB {
		return nil
	}
	if t.parts == nil {
		return errors.NewNonRetriableErrorf(
			"export part storage is not configured",
		)
	}
	batchSize := uint64(t.config.GetPartsCleanupBatchSize())
	if batchSize == 0 {
		return errors.NewNonRetriableErrorf(
			"export cleanup batch size must be positive",
		)
	}
	taskID := execCtx.GetTaskID()
	for {
		var deleted uint64
		prepare := func(
			ctx context.Context,
			tx *persistence.Transaction,
		) error {

			var err error
			deleted, err = t.parts.DeleteParts(
				ctx, tx, taskID, batchSize,
			)
			return err
		}

		err := execCtx.SaveStateWithPreparation(ctx, prepare)
		if err != nil {
			logging.Error(
				ctx, "export parts cleanup failed, task_id %v: %v",
				execCtx.GetTaskID(), err,
			)
			if ctx.Err() != nil ||
				errors.Is(err, errors.NewWrongGenerationError()) {

				return err
			}
			return errors.NewRetriableErrorWithIgnoreRetryLimit(err)
		}
		if deleted < batchSize {
			return nil
		}
	}
}
