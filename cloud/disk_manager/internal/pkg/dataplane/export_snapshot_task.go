package dataplane

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"

	api "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/disk_service"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/exporter"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	storage_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

// The source and manifest plan are pinned before any part is written.
// Completed PUTs are checkpointed and skipped after retries or node changes.
type exportSnapshotTask struct {
	config       *server_config.ExportConfig
	storage      storage.Storage
	backupReader backup.ObjectReader
	diskService  disk_service.Client
	registry     metrics.Registry
	request      *protos.ExportSnapshotRequest
	state        *protos.ExportSnapshotTaskState
	resume       exporter.S3ExportState
}

func (t *exportSnapshotTask) resourceKind() string {
	if t.request.ImageId != "" {
		return "image"
	}

	return "snapshot"
}

func (t *exportSnapshotTask) metricsRegistry() metrics.Registry {
	registry := t.registry
	if registry == nil {
		registry = metrics.NewEmptyRegistry()
	}

	return registry.WithTags(map[string]string{
		"resource_kind": t.resourceKind(),
	})
}

func (t *exportSnapshotTask) Save() ([]byte, error) {
	data, err := json.Marshal(t.resume)
	if err != nil {
		return nil, err
	}

	t.state.ExportState = data
	return proto.Marshal(t.state)
}

func (t *exportSnapshotTask) Load(request, state []byte) error {
	t.request = &protos.ExportSnapshotRequest{}
	if err := proto.Unmarshal(request, t.request); err != nil {
		return err
	}

	t.state = &protos.ExportSnapshotTaskState{}
	if err := proto.Unmarshal(state, t.state); err != nil {
		return err
	}

	if len(t.state.ExportState) != 0 {
		if err := json.Unmarshal(t.state.ExportState, &t.resume); err != nil {
			return errors.NewNonRetriableError(err)
		}
	}

	return nil
}

func (t *exportSnapshotTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) (err error) {

	ctx = logging.WithFields(
		logging.WithTaskID(ctx, execCtx.GetTaskID()),
		logging.String("SNAPSHOT_ID", t.request.SnapshotId),
		logging.String("IMAGE_ID", t.request.ImageId),
	)
	defer func() {
		err = publicSnapshotExportError(ctx, t.resourceKind(), err)
	}()

	if t.request.SnapshotId == "" ||
		(t.request.ImageId != "" &&
			t.request.ImageId != t.request.SnapshotId) {

		return errors.NewNonRetriableErrorf("invalid export resource IDs")
	}

	registry := t.metricsRegistry()

	attempts := registry.Counter("export/runAttempts")
	succeeded := registry.Counter("export/runSucceeded")
	runErrors := registry.Counter("export/runErrors")
	retryableErrors := registry.Counter("export/retryableErrors")
	interrupted := registry.Counter("export/runInterrupted")
	resumed := registry.Counter("export/resumedAttempts")
	registry.Counter("export/cancelSucceeded")
	active := registry.IntGauge("export/active")
	attemptTime := registry.Timer("export/runTime")
	attempts.Inc()
	active.Add(1)
	if t.resume.Initialized && !t.resume.Completed {
		resumed.Inc()
	}

	started := time.Now()
	defer func() {
		active.Add(-1)
		attemptTime.RecordDuration(time.Since(started))
		switch {
		case err == nil:
			succeeded.Inc()
		case ctx.Err() != nil ||
			errors.Is(err, errors.NewWrongGenerationError()) ||
			errors.Is(err, errors.NewInterruptExecutionError()):
			interrupted.Inc()
		default:
			runErrors.Inc()
			if errors.CanRetry(err) || exporter.IsRetryableS3Error(err) {
				retryableErrors.Inc()
			}
		}
	}()

	if t.resume.Completed {
		return t.release(ctx, execCtx)
	}

	sourceRetryTimeout, err := time.ParseDuration(
		t.config.GetSourceRetryTimeout(),
	)
	if err != nil || sourceRetryTimeout <= 0 {
		return errors.NewNonRetriableErrorf(
			"source retry timeout must be positive",
		)
	}

	sourceRetries := newSnapshotExportSourceRetries(t.state, sourceRetryTimeout)

	uploads := newSnapshotExportUploadLimiter(
		int(t.config.GetUploadWorkerCount()), t.state, registry,
	)
	defer uploads.close()
	err = t.run(ctx, execCtx, sourceRetries, uploads)
	uploads.checkpoint(t.state)
	if err == nil || ctx.Err() != nil {
		return err
	}

	var sourceError *snapshotExportSourceError
	if errors.As(err, &sourceError) && sourceError.retryable {
		sourceRetries.checkpoint(t.state)
		if !sourceError.timedOut && sourceRetries.expired() {
			return wrapSnapshotExportStageError(
				errors.NewNonRetriableErrorf(
					"source S3 object %q remained unavailable for %v: %v",
					t.state.SourceRetryObject, sourceRetryTimeout, sourceError.err,
				),
				snapshotExportReadStage,
			)
		}

		if saveErr := t.saveState(ctx, execCtx); saveErr != nil {
			return saveErr
		}

		if sourceError.timedOut {
			logging.Warn(
				ctx, "source S3 read timed out; postponing export task "+
					"without a retry limit: %v", sourceError.err,
			)
			return errors.NewInterruptExecutionError()
		}

		logging.Warn(
			ctx, "retrying source S3 read within %v: %v",
			sourceRetryTimeout, sourceError.err,
		)
		// Drop the adapter's non-retriable marker before scheduling a retry.
		// That marker only prevents the exporter from retrying source reads.
		return wrapSnapshotExportStageError(
			errors.NewRetriableErrorWithIgnoreRetryLimit(fmt.Errorf(
				"source S3 object %q unavailable: %w",
				t.state.SourceRetryObject, sourceError.err,
			)),
			snapshotExportReadStage,
		)
	}

	var targetError *snapshotExportTargetError
	if errors.As(err, &targetError) {
		// Destination calls have already exhausted MaxRetries in the exporter.
		return errors.NewNonRetriableError(err)
	}

	return err
}

func (t *exportSnapshotTask) run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	sourceRetries *snapshotExportSourceRetries,
	uploads *snapshotExportUploadLimiter,
) error {

	destination := t.request.Destination

	snapshotID := t.request.SnapshotId
	if t.request.ImageId != "" {
		snapshotID = ""
	}
	response, err := t.diskService.GetExportCredentials(
		ctx,
		t.request.ComputeTaskId,
		snapshotID,
		t.request.ImageId,
	)
	if err != nil {
		return wrapSnapshotExportStageError(err, snapshotExportCredentialsStage)
	}

	source, err := t.selectSource(ctx, execCtx, sourceRetries)
	if err != nil {
		return wrapSnapshotExportStageError(err, snapshotExportReadStage)
	}

	meta, err := source.CheckSnapshotReady(ctx, t.request.SnapshotId)
	if err != nil {
		t.logSourceCallError(ctx, "CheckSnapshotReady", err)
		return wrapSnapshotExportStageError(err, snapshotExportReadStage)
	}

	if meta.BlockSize != 0 && t.request.BlockSize != 0 &&
		meta.BlockSize != t.request.BlockSize {

		return errors.NewNonRetriableErrorf(
			"export block_size differs between metadata sources",
		)
	}

	blockSize := meta.BlockSize
	if blockSize == 0 {
		blockSize = t.request.BlockSize
	}

	if t.request.DecryptionInfo != nil && blockSize == 0 {
		logging.Warn(
			ctx,
			"%v %v has no block_size in metadata; continuing encrypted "+
				"export without block_size in manifest; decryption will "+
				"require the user to supply the block size manually",
			t.resourceKind(), t.request.SnapshotId,
		)
	}

	timeout, err := time.ParseDuration(t.config.GetS3CallTimeout())
	if err != nil {
		return errors.NewNonRetriableError(err)
	}

	clientOptions := []persistence.S3ClientOption{
		persistence.WithS3EmptySigningRegion(),
	}
	if response.GetCredentials() == nil {
		clientOptions = append(
			clientOptions, persistence.WithS3AnonymousAccess(),
		)
		logging.Info(
			ctx,
			"Compute returned no credentials; using anonymous target S3 "+
				"access, endpoint %v, bucket %v, prefix %v",
			destination.Endpoint, destination.Bucket, destination.Prefix,
		)
	}

	client, err := persistence.NewS3Client(
		destination.Endpoint,
		destination.Region,
		persistence.S3Credentials{
			ID:     response.GetCredentials().GetAccessKeyId(),
			Secret: response.GetCredentials().GetSecretAccessKey(),
		},
		timeout,
		t.metricsRegistry(),
		0, // The exporter owns the destination retry budget.
		nil,
		nil,
		t.config.GetMaxIdleConnsPerHost(),
		clientOptions...,
	)
	if err != nil {
		logging.Error(
			ctx,
			"creating target S3 client failed, endpoint %v, bucket %v: %v",
			destination.Endpoint, destination.Bucket, err,
		)
		return err
	}

	options := exporter.S3ExportOptions{
		Bucket:            destination.Bucket,
		Prefix:            destination.Prefix,
		Compression:       t.config.GetCompression(),
		PartSize:          t.config.GetPartSize(),
		ReadWorkerCount:   int(t.config.GetReadWorkerCount()),
		UploadWorkerCount: int(t.config.GetUploadWorkerCount()),
		BlockSize:         blockSize,
		MaxRetries:        int(t.config.GetMaxRetries()),
		UnpackArchivePath: t.config.GetUnpackArchivePath(),
		UnpackReadmePath:  t.config.GetUnpackReadmePath(),
	}
	if t.resume.Initialized {
		// Live configuration changes must not change an existing part layout.
		options.PartSize = t.resume.Manifest.MaxPartSize
		options.Compression = t.resume.Manifest.Compression
	}

	if info := t.request.DecryptionInfo; info != nil {
		options.KekID = info.KekId
		options.DecryptionInfo = &exporter.S3DecryptionInfo{
			KekID:      info.KekId,
			KekVersion: info.KekVersion,
			// The API bytes field carries Compute's Base64 text.
			EncryptedDEK: string(info.EncryptedDek),
		}
	}

	if t.registry != nil {
		options.MetricsRegistry = t.metricsRegistry().WithTags(map[string]string{
			"source":      t.state.Source,
			"compression": options.Compression,
		})
	}

	_, err = exporter.ExportToS3Resumable(
		ctx,
		source,
		t.request.SnapshotId,
		snapshotExportTarget{S3Destination: client, uploads: uploads},
		options,
		&t.resume,
		func(ctx context.Context) error {
			sourceRetries.checkpoint(t.state)
			uploads.checkpoint(t.state)
			var completed uint64
			for _, part := range t.resume.Manifest.Parts {
				completed += part.Length
			}

			if t.resume.Manifest.Size != 0 {
				t.state.Progress = 0.99 * float64(completed) /
					float64(t.resume.Manifest.Size)
			}

			if t.resume.Completed {
				t.state.Progress = 1
			}

			return t.saveState(ctx, execCtx)
		},
	)
	if err != nil {
		return err
	}

	return t.release(ctx, execCtx)
}

func (t *exportSnapshotTask) selectSource(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	sourceRetries *snapshotExportSourceRetries,
) (exporter.Source, error) {

	if t.state.Source == "" {
		switch t.config.GetSourceStorage() {
		case server_config.ExportConfig_BACKUP:
			t.state.Source = "backup"
		case server_config.ExportConfig_PRIMARY:
			t.state.Source = "primary"
		case server_config.ExportConfig_AUTO:
			meta, err := t.storage.GetSnapshotMeta(ctx, t.request.SnapshotId)
			if err != nil {
				t.logSourceCallError(ctx, "GetSnapshotMeta", err)
				return nil, err
			}

			t.state.Source = "primary"
			if meta == nil {
				t.state.Source = "backup"
			}
		default:
			return nil, errors.NewNonRetriableErrorf(
				"unknown snapshot export source",
			)
		}
	}

	if t.state.Source == "primary" {
		if t.storage == nil {
			return nil, errors.NewNonRetriableErrorf(
				"primary export storage is not configured",
			)
		}

		locked, err := t.storage.LockSnapshot(
			ctx,
			t.request.SnapshotId,
			execCtx.GetTaskID(),
		)
		if err != nil {
			t.logSourceCallError(ctx, "LockSnapshot", err)
			return nil, err
		}

		if !locked {
			return nil, errors.NewNonRetriableErrorf(
				"source snapshot is being deleted or is absent",
			)
		}

		meta, err := t.storage.CheckSnapshotReady(ctx, t.request.SnapshotId)
		if err != nil {
			t.logSourceCallError(ctx, "CheckSnapshotReady", err)
			return nil, err
		}

		if err := t.validateEncryption(meta); err != nil {
			return nil, err
		}

		return newSnapshotExportSource(t.storage, sourceRetries),
			t.saveState(ctx, execCtx)
	}

	if t.state.Source != "backup" || t.backupReader == nil {
		return nil, errors.NewNonRetriableErrorf(
			"backup export storage is not configured",
		)
	}

	reader := snapshotExportBackupReader{
		ObjectReader: t.backupReader,
		retries:      sourceRetries,
	}
	kind := protos.BackupSourceKind_BACKUP_SOURCE_KIND_SNAPSHOT
	if t.request.ImageId != "" {
		kind = protos.BackupSourceKind_BACKUP_SOURCE_KIND_IMAGE
	}
	loaded, err := backup.Load(
		ctx,
		reader,
		backup.LoadOptions{
			Kind:             kind,
			ID:               t.request.SnapshotId,
			DiskID:           t.request.SourceDiskId,
			ExpectedFolderID: t.request.FolderId,
		},
	)
	if err != nil {
		t.logSourceCallError(ctx, "backup.Load", err)
		return nil, err
	}

	metaHash := sha256.Sum256(loaded.MetaData)
	mapHash := sha256.Sum256(loaded.ChunkMapData)
	if len(t.state.BackupMetaSHA256) != 0 &&
		(!bytes.Equal(metaHash[:], t.state.BackupMetaSHA256) ||
			!bytes.Equal(mapHash[:], t.state.BackupChunkMapSHA256)) {

		return nil, errors.NewNonRetriableErrorf(
			"source backup changed since export checkpoint",
		)
	}

	t.state.BackupMetaSHA256 = metaHash[:]
	t.state.BackupChunkMapSHA256 = mapHash[:]
	source := exporter.NewLoadedBackupSource(
		reader,
		loaded,
		t.request.SourceDiskId,
		storage_metrics.New(t.registry, "backup"),
	)
	meta, err := source.CheckSnapshotReady(ctx, t.request.SnapshotId)
	if err != nil {
		t.logSourceCallError(ctx, "CheckSnapshotReady", err)
		return nil, err
	}

	if err := t.validateEncryption(meta); err != nil {
		return nil, err
	}

	sourceRetries.checkpoint(t.state)
	return source, t.saveState(ctx, execCtx)
}

func (t *exportSnapshotTask) saveState(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := execCtx.SaveState(ctx)
	if err != nil {
		logging.Error(
			ctx,
			"export Tasks.SaveState failed, task_id %v, snapshot_id %v: %v",
			execCtx.GetTaskID(), t.request.SnapshotId, err,
		)
	}

	return err
}

func (t *exportSnapshotTask) logSourceCallError(
	ctx context.Context,
	operation string,
	err error,
) {

	if t.state.Source != "backup" {
		config := t.config.GetSnapshotConfig().GetPersistenceConfig()
		ctx = logging.WithFields(
			ctx,
			logging.String("YDB_ENDPOINT", config.GetEndpoint()),
			logging.String("YDB_DATABASE", config.GetDatabase()),
		)
	}

	logging.Error(
		ctx,
		"export source call %v failed, source %v, snapshot_id %v: %v",
		operation, t.state.Source, t.request.SnapshotId, err,
	)
}

func (t *exportSnapshotTask) validateEncryption(
	meta storage.SnapshotMeta,
) error {

	if meta.Encryption.GetMode() != types.EncryptionMode_NO_ENCRYPTION &&
		t.request.DecryptionInfo == nil {
		return errors.NewNonRetriableErrorf(
			"encrypted export requires decryption_info",
		)
	}

	return nil
}

func (t *exportSnapshotTask) release(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	// Unlock only this task's reference, including a lock acquired before an
	// unsuccessful state save. Another task's reference cannot be removed.
	if t.storage != nil {
		err := t.storage.UnlockSnapshot(
			ctx,
			t.request.SnapshotId,
			execCtx.GetTaskID(),
		)
		if err != nil {
			t.logSourceCallError(ctx, "UnlockSnapshot", err)
			return err
		}
	}

	return nil
}

func (t *exportSnapshotTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.release(ctx, execCtx)
	if err == nil && t.registry != nil {
		t.metricsRegistry().Counter("export/cancelSucceeded").Inc()
	}

	return publicSnapshotExportError(ctx, t.resourceKind(), err)
}

func (t *exportSnapshotTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &api.ExportMetadata{Progress: t.state.Progress}, nil
}

func (t *exportSnapshotTask) GetResponse() proto.Message {
	prefix := strings.Trim(t.request.Destination.GetPrefix(), "/")
	if prefix != "" {
		prefix += "/"
	}

	return &api.ExportResponse{
		Bucket:      t.request.Destination.GetBucket(),
		ManifestKey: prefix + "manifest.json",
	}
}

// RegisterExport keeps execution on explicitly configured export nodes.
func RegisterExport(
	registry *tasks.Registry,
	config *server_config.ExportConfig,
	storage storage.Storage,
	backupReader backup.ObjectReader,
	diskServiceClient disk_service.Client,
	metrics metrics.Registry,
) error {

	register := registry.Register
	if config != nil {
		register = registry.RegisterForExecution
	}
	for _, taskType := range []string{
		"dataplane.ExportSnapshotToS3", "dataplane.ExportImageToS3",
	} {
		err := register(taskType, func() tasks.Task {
			return &exportSnapshotTask{
				config:       config,
				storage:      storage,
				backupReader: backupReader,
				diskService:  diskServiceClient,
				registry:     metrics,
			}
		})
		if err != nil {
			return err
		}
	}
	return nil
}
