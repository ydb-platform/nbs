package dataplane

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"github.com/golang/protobuf/proto"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/exporter"
	storage_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type exportSecrets interface {
	DecryptSecret(
		kind string,
		encryptedDEK []byte,
		ciphertext []byte,
	) ([]byte, error)
}

// Exports the follower copy of a snapshot or image to an S3 bucket of the
// caller as raw parts and a manifest. Runs only on the nodes that register it
// for execution (SnapshotExportConfig.ExecuteTasks); needs the follower, the
// kek and network access to the destination, nothing else.
//
// Progress is saved after every part. Object keys derive from BackupUuid set
// by the first attempt, so a part interrupted by a restart is overwritten
// under the same key, and the parts in the state are not read again.
type exportBackupToS3Task struct {
	backupReader             backup.ObjectReader
	secrets                  exportSecrets
	metrics                  storage_metrics.Metrics
	s3Registry               metrics.Registry
	readWorkerCount          int
	s3CallTimeout            time.Duration
	s3MaxRetriableErrorCount uint64
	request                  *protos.ExportBackupToS3Request
	state                    *protos.ExportBackupToS3TaskState
}

func (t *exportBackupToS3Task) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *exportBackupToS3Task) Load(request, state []byte) error {
	t.request = &protos.ExportBackupToS3Request{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.ExportBackupToS3TaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *exportBackupToS3Task) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.validateRequest()
	if err != nil {
		return err
	}

	destination, err := t.newDestination(ctx)
	if err != nil {
		return err
	}

	loaded, err := backup.Load(
		ctx,
		t.backupReader,
		backup.LoadOptions{
			Kind:             t.request.SrcKind,
			ID:               t.request.SrcId,
			DiskID:           t.request.SrcDiskId,
			ExpectedFolderID: t.request.ExpectedFolderId,
		},
	)
	if err != nil {
		return err
	}

	err = t.pinBackup(ctx, execCtx, loaded)
	if err != nil {
		return err
	}

	source := exporter.NewLoadedBackupSnapshotSource(
		t.backupReader,
		loaded,
		t.metrics,
	)

	export, err := exporter.NewS3Export(
		ctx,
		source,
		t.request.SrcId,
		destination,
		exporter.S3ExportOptions{
			Bucket:          t.request.Bucket,
			Prefix:          t.request.Prefix,
			PartSize:        t.request.PartSize,
			ReadWorkerCount: t.readWorkerCount,
		},
		t.state.BackupUuid,
	)
	if err != nil {
		return err
	}

	err = t.pinPlan(ctx, execCtx, export)
	if err != nil {
		return err
	}

	for number := uint32(len(t.state.Parts)) + 1; number <= t.state.PartCount; number++ {
		part, err := export.ExportPart(ctx, number)
		if err != nil {
			return err
		}

		t.state.Parts = append(t.state.Parts, &protos.ExportBackupToS3Part{
			Number: part.Number,
			Key:    part.Key,
			Offset: part.Offset,
			Length: part.Length,
			Sha256: part.SHA256,
		})
		t.state.Progress =
			float64(len(t.state.Parts)) / float64(t.state.PartCount)

		err = execCtx.SaveState(ctx)
		if err != nil {
			return err
		}
	}

	parts := make([]exporter.S3ManifestPart, 0, len(t.state.Parts))
	for _, part := range t.state.Parts {
		parts = append(parts, exporter.S3ManifestPart{
			Number: part.Number,
			Key:    part.Key,
			Offset: part.Offset,
			Length: part.Length,
			SHA256: part.Sha256,
		})
	}

	_, err = export.PublishManifest(ctx, parts)
	if err != nil {
		return err
	}

	t.state.ManifestKey = export.ManifestKey()
	t.state.Progress = 1
	return nil
}

// Parts without a manifest are left for the caller, as the admin command
// does; the keys are in the state if a cleanup is added later.
func (t *exportBackupToS3Task) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *exportBackupToS3Task) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &protos.ExportBackupToS3Metadata{
		Progress: t.state.Progress,
	}, nil
}

func (t *exportBackupToS3Task) GetResponse() proto.Message {
	return &protos.ExportBackupToS3Response{
		Bucket:      t.request.Bucket,
		Prefix:      strings.TrimSuffix(t.state.ManifestKey, "manifest.json"),
		ManifestKey: t.state.ManifestKey,
		PartCount:   t.state.PartCount,
		Size:        t.state.Size,
	}
}

////////////////////////////////////////////////////////////////////////////////

func (t *exportBackupToS3Task) validateRequest() error {
	if len(t.request.SrcId) == 0 {
		return errors.NewNonRetriableErrorf("backup source id is empty")
	}

	isSnapshot := t.request.SrcKind ==
		protos.BackupSourceKind_BACKUP_SOURCE_KIND_SNAPSHOT
	if isSnapshot && len(t.request.SrcDiskId) == 0 {
		return errors.NewNonRetriableErrorf(
			"source disk id is required for backup of snapshot %v",
			t.request.SrcId,
		)
	}

	if len(t.request.Endpoint) == 0 || len(t.request.Bucket) == 0 {
		return errors.NewNonRetriableErrorf(
			"destination endpoint and bucket are required for export of %v",
			t.request.SrcId,
		)
	}

	if len(t.request.EncryptedCredentials) == 0 {
		return errors.NewNonRetriableErrorf(
			"destination credentials are missing for export of %v",
			t.request.SrcId,
		)
	}

	return exporter.ValidateS3PartSize(t.request.PartSize)
}

// The keys live in memory for the duration of the task only.
func (t *exportBackupToS3Task) newDestination(
	ctx context.Context,
) (exporter.S3Destination, error) {

	plaintext, err := t.secrets.DecryptSecret(
		backup.ExportCredentialsSecretKind,
		t.request.EncryptedDek,
		t.request.EncryptedCredentials,
	)
	if err != nil {
		return nil, err
	}

	var credentials persistence.S3Credentials
	err = json.Unmarshal(plaintext, &credentials)
	if err != nil {
		return nil, errors.NewNonRetriableErrorf(
			"failed to parse destination credentials: %w",
			err,
		)
	}

	if len(credentials.ID) == 0 || len(credentials.Secret) == 0 {
		return nil, errors.NewNonRetriableErrorf(
			"destination credentials are empty for export of %v",
			t.request.SrcId,
		)
	}

	return persistence.NewS3Client(
		t.request.Endpoint,
		t.request.Region,
		credentials,
		t.s3CallTimeout,
		t.s3Registry,
		t.s3MaxRetriableErrorCount,
		nil, // availabilityMonitoring
		nil, // tokenProvider
		0,   // maxIdleConnsPerHost
		persistence.WithS3EmptySigningRegion(),
	)
}

// Parts already in the state are valid only for the meta and the chunk map
// seen by the first attempt, so the following attempts must see the same.
func (t *exportBackupToS3Task) pinBackup(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	loaded *backup.LoadedBackup,
) error {

	metaHash := sha256.Sum256(loaded.MetaData)
	chunkMapHash := sha256.Sum256(loaded.ChunkMapData)

	if len(t.state.BackupUuid) == 0 {
		backupUUID, err := uuid.NewV4()
		if err != nil {
			return errors.NewRetriableError(err)
		}

		t.state.BackupUuid = backupUUID.String()
		t.state.MetaSha256 = metaHash[:]
		t.state.ChunkMapSha256 = chunkMapHash[:]
		t.state.ChunkCount = uint32(len(loaded.ChunkIDs))
		return execCtx.SaveState(ctx)
	}

	if !bytes.Equal(t.state.MetaSha256, metaHash[:]) {
		return errors.NewNonRetriableErrorf(
			"meta of backup %v has changed",
			t.request.SrcId,
		)
	}

	if !bytes.Equal(t.state.ChunkMapSha256, chunkMapHash[:]) {
		return errors.NewNonRetriableErrorf(
			"chunk map of backup %v has changed",
			t.request.SrcId,
		)
	}

	return nil
}

func (t *exportBackupToS3Task) pinPlan(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	export *exporter.S3Export,
) error {

	if t.state.PartCount == 0 {
		t.state.PartCount = export.PartCount()
		t.state.Size = export.Size()
		logging.Info(
			ctx,
			"exporting backup %v to %v: %v bytes in %v parts",
			t.request.SrcId,
			export.Prefix(),
			t.state.Size,
			t.state.PartCount,
		)
		return execCtx.SaveState(ctx)
	}

	if t.state.PartCount != export.PartCount() || t.state.Size != export.Size() {
		return errors.NewNonRetriableErrorf(
			"export plan of backup %v has changed: %v parts of %v bytes, "+
				"expected %v parts of %v bytes",
			t.request.SrcId,
			export.PartCount(),
			export.Size(),
			t.state.PartCount,
			t.state.Size,
		)
	}

	return nil
}
