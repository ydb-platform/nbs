package admin

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/auth"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/exporter"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	storage_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	disk_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type exportResourceStorage interface {
	GetDiskMeta(context.Context, string) (*resources.DiskMeta, error)
	GetImageMeta(context.Context, string) (*resources.ImageMeta, error)
}

type exportDiskMetadata struct {
	BlockSize    uint32
	KekID        string
	EncryptedDEK []byte
}

func diskCreateEncryptionDesc(disk *resources.DiskMeta) (*types.EncryptionDesc, error) {
	if params, ok := disk.CreateRequest.(*disk_protos.CreateDiskParams); ok {
		return params.GetEncryptionDesc(), nil
	}
	if request, ok := disk.CreateRequest.(interface {
		GetParams() *disk_protos.CreateDiskParams
	}); ok {
		return request.GetParams().GetEncryptionDesc(), nil
	}
	if len(disk.CreateRequestData) == 0 {
		return nil, nil
	}

	// Requests for disks created from an image or snapshot wrap Params in
	// field 2. Empty and overlay disks store CreateDiskParams directly.
	if disk.SrcSnapshotID != "" || disk.SrcImageID != "" {
		var request interface {
			proto.Message
			GetParams() *disk_protos.CreateDiskParams
		}
		if disk.SrcSnapshotID != "" {
			request = &disk_protos.CreateDiskFromSnapshotRequest{}
		} else {
			request = &disk_protos.CreateDiskFromImageRequest{}
		}
		if err := proto.Unmarshal(disk.CreateRequestData, request); err == nil && request.GetParams() != nil {
			return request.GetParams().GetEncryptionDesc(), nil
		}
	}
	params := &disk_protos.CreateDiskParams{}
	if err := proto.Unmarshal(disk.CreateRequestData, params); err != nil {
		return nil, fmt.Errorf("decoding disk %q create request: %w", disk.ID, err)
	}
	return params.GetEncryptionDesc(), nil
}

func readExportDiskMetadata(
	ctx context.Context,
	resourceStorage exportResourceStorage,
	snapshotID string,
	diskID string,
) (exportDiskMetadata, error) {

	if diskID == "" {
		image, err := resourceStorage.GetImageMeta(ctx, snapshotID)
		if err != nil {
			return exportDiskMetadata{}, fmt.Errorf("reading image metadata for source disk: %w", err)
		}
		if image != nil {
			diskID = image.SrcDiskID
		}
	}

	if diskID == "" {
		logging.Warn(ctx, "source disk is unknown; omitting disk metadata")
		return exportDiskMetadata{}, nil
	}

	disk, err := resourceStorage.GetDiskMeta(ctx, diskID)
	if err != nil {
		return exportDiskMetadata{}, fmt.Errorf("reading disk %q metadata: %w", diskID, err)
	}
	if disk == nil {
		logging.Warn(ctx, "disk %v metadata is unavailable; omitting disk metadata", diskID)
		return exportDiskMetadata{}, nil
	}
	metadata := exportDiskMetadata{BlockSize: disk.BlockSize}
	if disk.BlockSize == 0 {
		logging.Warn(ctx, "disk %v block size is unknown; omitting block_size", diskID)
	}

	encryption, err := diskCreateEncryptionDesc(disk)
	if err != nil {
		logging.Warn(ctx, "failed to obtain disk encryption metadata: %v", err)
		return metadata, nil
	}
	key := encryption.GetKmsKey()
	metadata.KekID = key.GetKekId()
	metadata.EncryptedDEK = key.GetEncryptedDEK()
	return metadata, nil
}

func (c *exportResource) getExportDiskMetadata(
	ctx context.Context,
	source exporter.Source,
) (exportDiskMetadata, error) {

	meta, err := source.CheckSnapshotReady(ctx, c.resourceID)
	if err != nil {
		return exportDiskMetadata{}, err
	}
	if meta.Encryption.GetMode() == types.EncryptionMode_NO_ENCRYPTION {
		return exportDiskMetadata{}, nil
	}
	if c.serverConfig.GetPersistenceConfig() == nil {
		logging.Warn(ctx, "resource YDB config is missing; omitting disk metadata")
		return exportDiskMetadata{}, nil
	}

	resourceStorage, db, err := newResourceStorage(ctx, c.serverConfig)
	if err != nil {
		return exportDiskMetadata{}, err
	}
	defer db.Close(ctx)

	return readExportDiskMetadata(ctx, resourceStorage, c.resourceID, meta.Disk.GetDiskId())
}

////////////////////////////////////////////////////////////////////////////////

func (c *exportResource) validateSourceOptions(cmd *cobra.Command) error {
	switch c.source {
	case "snapshot-storage":
		if cmd.Flags().Changed("src-disk-id") {
			return fmt.Errorf("--src-disk-id requires --source=backup")
		}

	case "backup":
		if c.kind == exportSnapshotKind && c.srcDiskID == "" {
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

func (c *exportResource) newSource(
	ctx context.Context,
) (exporter.Source, func(), error) {

	if c.source == "backup" {
		source, err := c.newBackupSource(ctx)
		return source, func() {}, err
	}

	return c.newSnapshotStorageSource(ctx)
}

func (c *exportResource) newBackupSource(
	ctx context.Context,
) (exporter.Source, error) {

	config := c.serverConfig.GetSnapshotStorageBackupConfig()
	if config == nil || config.GetS3Config() == nil {
		return nil, fmt.Errorf("%v backup S3 config is missing", c.kind)
	}

	if config.GetS3Bucket() == "" {
		return nil, fmt.Errorf("%v backup S3 bucket is missing", c.kind)
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
		0, // No upload limit; this source only reads from the backup.
		metrics.NewEmptyRegistry(),
	)
	if err != nil {
		return nil, err
	}

	backupMetrics := storage_metrics.New(
		metrics.NewEmptyRegistry(),
		"backup_export",
	)
	if c.kind == exportImageKind {
		return exporter.NewBackupImageSource(
			ctx,
			reader,
			c.resourceID,
			backupMetrics,
		)
	}

	return exporter.NewBackupSnapshotSource(
		ctx,
		reader,
		c.resourceID,
		c.srcDiskID,
		backupMetrics,
	)
}

func (c *exportResource) newSnapshotStorageSource(
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
