package snapshots

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	backup_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup/config"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/exporter"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

// ExportScheduler validates an export request, seals the destination keys
// under the kek and schedules snapshots.ExportToS3. Shared by the snapshot and
// the image services.
type ExportScheduler struct {
	taskScheduler tasks.Scheduler
	backupS3      *backup.S3
	config        *backup_config.SnapshotExportConfig
}

func NewExportScheduler(
	taskScheduler tasks.Scheduler,
	backupS3 *backup.S3,
	config *backup_config.SnapshotExportConfig,
) *ExportScheduler {

	return &ExportScheduler{
		taskScheduler: taskScheduler,
		backupS3:      backupS3,
		config:        config,
	}
}

func (s *ExportScheduler) Schedule(
	ctx context.Context,
	srcKind dataplane_protos.BackupSourceKind,
	srcID string,
	cloudID string,
	folderID string,
	dst *disk_manager.ExportDestination,
) (string, error) {

	if s.backupS3 == nil || s.config == nil {
		return "", errors.NewNonRetriableErrorf(
			"snapshot export is not configured",
		)
	}

	if len(srcID) == 0 || len(cloudID) == 0 || len(folderID) == 0 {
		return "", common.NewInvalidArgumentError(
			"source id, cloud id and folder id should not be empty",
		)
	}

	if dst == nil {
		return "", common.NewInvalidArgumentError("dst is required")
	}

	endpoint, err := url.Parse(dst.Endpoint)
	if err != nil || endpoint.Hostname() == "" ||
		(endpoint.Scheme != "http" && endpoint.Scheme != "https") ||
		endpoint.User != nil || endpoint.RawQuery != "" ||
		endpoint.Fragment != "" {

		return "", common.NewInvalidArgumentError(
			"dst.endpoint must be an HTTP(S) URL without credentials, " +
				"query or fragment",
		)
	}

	if len(dst.Bucket) == 0 || strings.ContainsAny(dst.Bucket, "/\\\r\n") {
		return "", common.NewInvalidArgumentError("invalid dst.bucket")
	}

	if len(dst.AccessKeyId) == 0 || len(dst.SecretAccessKey) == 0 {
		return "", common.NewInvalidArgumentError(
			"dst.access_key_id and dst.secret_access_key are required",
		)
	}

	partSize := dst.PartSize
	if partSize == 0 {
		partSize = s.config.GetDefaultPartSize()
	}

	err = exporter.ValidateS3PartSize(partSize)
	if err != nil {
		return "", common.NewInvalidArgumentError("invalid dst.part_size: %v", err)
	}

	credentials, err := json.Marshal(persistence.S3Credentials{
		ID:     dst.AccessKeyId,
		Secret: dst.SecretAccessKey,
	})
	if err != nil {
		return "", err
	}

	encryptedDEK, encryptedCredentials, err := s.backupS3.EncryptSecret(
		backup.ExportCredentialsSecretKind,
		credentials,
	)
	if err != nil {
		return "", err
	}

	return s.taskScheduler.ScheduleTask(
		ctx,
		"snapshots.ExportToS3",
		"",
		&protos.ExportToS3Request{
			SrcKind:              srcKind,
			SrcId:                srcID,
			CloudId:              cloudID,
			FolderId:             folderID,
			Endpoint:             dst.Endpoint,
			Region:               dst.Region,
			Bucket:               dst.Bucket,
			Prefix:               dst.Prefix,
			PartSize:             partSize,
			EncryptedDek:         encryptedDEK,
			EncryptedCredentials: encryptedCredentials,
		},
	)
}
