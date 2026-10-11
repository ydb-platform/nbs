package admin

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/auth"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/exporter"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	storage_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

func (c *exportSnapshot) validateSourceOptions(cmd *cobra.Command) error {
	switch c.source {
	case "snapshot-storage":
		if cmd.Flags().Changed("src-disk-id") {
			return fmt.Errorf("--src-disk-id requires --source=backup")
		}

	case "backup":
		if c.srcDiskID == "" {
			return fmt.Errorf("--source=backup requires --src-disk-id")
		}

	default:
		return fmt.Errorf(
			"--source must be snapshot-storage or backup, got %q",
			c.source,
		)
	}

	return nil
}

func (c *exportSnapshot) newSource(
	ctx context.Context,
) (exporter.Source, func(), error) {

	if c.source == "backup" {
		source, err := c.newBackupSource(ctx)
		return source, func() {}, err
	}

	return c.newSnapshotStorageSource(ctx)
}

func (c *exportSnapshot) newBackupSource(
	ctx context.Context,
) (exporter.Source, error) {

	config := c.serverConfig.GetSnapshotStorageBackupConfig()
	if config == nil || config.GetS3Config() == nil {
		return nil, fmt.Errorf("snapshot backup S3 config is missing")
	}

	if config.GetS3Bucket() == "" {
		return nil, fmt.Errorf("snapshot backup S3 bucket is missing")
	}

	kekFile, kekID := config.GetKekFile(), config.GetKekId()
	var kek []byte
	if kekFile != "" || kekID != "" {
		if kekFile == "" || kekID == "" {
			return nil, fmt.Errorf("set backup kek file and kek id together")
		}

		var err error
		kek, err = os.ReadFile(kekFile)
		if err != nil {
			return nil, err
		}
	}

	var creds auth.Credentials
	if config.GetS3Config().GetUseIamToken() {
		var err error
		creds, err = auth.NewCredentials(ctx, c.serverConfig.GetAuthConfig())
		if err != nil {
			return nil, err
		}
	}

	s3, err := persistence.NewS3ClientFromConfig(
		config.GetS3Config(),
		metrics.NewEmptyRegistry(),
		nil, // Availability monitoring.
		creds,
	)
	if err != nil {
		return nil, err
	}

	reader, err := backup.NewS3(
		s3,
		config.GetS3Bucket(),
		config.GetS3KeyPrefix(),
		kekID,
		kek,
		0, // uploadBytesPerSecond: this source only reads from the backup.
		metrics.NewEmptyRegistry(),
	)
	if err != nil {
		return nil, err
	}

	return exporter.NewBackupSnapshotSource(
		ctx,
		reader,
		c.snapshotID,
		c.srcDiskID,
		storage_metrics.New(metrics.NewEmptyRegistry(), "backup_export"),
	)
}

func (c *exportSnapshot) newSnapshotStorageSource(
	ctx context.Context,
) (exporter.Source, func(), error) {

	config := c.serverConfig.GetDataplaneConfig().GetSnapshotConfig()
	if config == nil {
		return nil, nil, fmt.Errorf(
			"dataplane snapshot config is missing in the server config file",
		)
	}

	if config.GetPersistenceConfig() == nil {
		return nil, nil, fmt.Errorf(
			"snapshot persistence config is missing in the server config file",
		)
	}

	creds, err := auth.NewCredentials(ctx, c.serverConfig.GetAuthConfig())
	if err != nil {
		return nil, nil, err
	}

	db, err := persistence.NewYDBClient(
		ctx,
		config.GetPersistenceConfig(),
		metrics.NewEmptyRegistry(),
		persistence.WithCredentials(creds),
	)
	if err != nil {
		return nil, nil, err
	}

	closeSource := func() {
		closeCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx),
			30*time.Second,
		)
		defer cancel()
		_ = db.Close(closeCtx)
	}

	s3Config := config.GetPersistenceConfig().GetS3Config()
	var s3 *persistence.S3Client
	if s3Config != nil {
		s3, err = persistence.NewS3ClientFromConfig(
			s3Config,
			metrics.NewEmptyRegistry(),
			nil, // Availability monitoring.
			creds,
		)
		if err != nil {
			closeSource()
			return nil, nil, err
		}
	}

	source, err := snapshot_storage.NewStorage(
		config,
		metrics.NewEmptyRegistry(),
		db,
		s3,
	)
	if err != nil {
		closeSource()
		return nil, nil, err
	}

	return source, closeSource, nil
}
