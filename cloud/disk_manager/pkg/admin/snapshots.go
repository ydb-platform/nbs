package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/spf13/cobra"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	client_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/client/config"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/exporter"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	logzap "github.com/ydb-platform/nbs/library/go/core/log/zap"
)

////////////////////////////////////////////////////////////////////////////////

type commandWithScheduler struct {
	clientConfig *client_config.ClientConfig
	serverConfig *server_config.ServerConfig
	scheduler    tasks.Scheduler
	ctx          context.Context
	db           *persistence.YDBClient
}

func (t *commandWithScheduler) init() error {
	t.ctx = newContext(t.clientConfig)
	taskStorage, db, err := newTaskStorage(t.ctx, t.serverConfig)
	if err != nil {
		return err
	}

	t.db = db
	taskRegistry := tasks.NewRegistry()

	regularTasksEnabled := false
	t.serverConfig.TasksConfig.RegularSystemTasksEnabled = &regularTasksEnabled
	t.scheduler, err = tasks.NewScheduler(
		t.ctx,
		taskRegistry,
		taskStorage,
		t.serverConfig.TasksConfig,
		metrics.NewEmptyRegistry(),
	)
	if err != nil {
		t.close()
		logging.Error(t.ctx, "Failed to create task scheduler: %v", err)
		return err
	}

	return nil
}

func (t *commandWithScheduler) close() {
	_ = t.db.Close(t.ctx)
}

func newCommandWithScheduler(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) commandWithScheduler {

	return commandWithScheduler{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
	}
}

////////////////////////////////////////////////////////////////////////////////

type getSnapshot struct {
	clientConfig *client_config.ClientConfig
	serverConfig *server_config.ServerConfig
	snapshotID   string
}

func (c *getSnapshot) run() error {
	ctx := newContext(c.clientConfig)

	resourceStorage, db, err := newResourceStorage(ctx, c.serverConfig)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	image, err := resourceStorage.GetSnapshotMeta(ctx, c.snapshotID)
	if err != nil {
		return err
	}

	j, err := json.Marshal(image)
	if err != nil {
		return err
	}

	fmt.Println(string(j))

	return nil
}

func newGetSnapshotCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	c := &getSnapshot{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
	}

	cmd := &cobra.Command{
		Use: "get",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(
		&c.snapshotID,
		"id",
		"",
		"ID of snapshot to get; required",
	)
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type listSnapshots struct {
	clientConfig *client_config.ClientConfig
	serverConfig *server_config.ServerConfig
	folderID     string
}

func (c *listSnapshots) run() error {
	ctx := newContext(c.clientConfig)

	resourceStorage, db, err := newResourceStorage(ctx, c.serverConfig)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	ids, err := resourceStorage.ListSnapshots(ctx, c.folderID, time.Now())
	if err != nil {
		return err
	}

	fmt.Println(strings.Join(ids, "\n"))

	return nil
}

func newListSnapshotsCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	c := &listSnapshots{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
	}

	cmd := &cobra.Command{
		Use: "list",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(
		&c.folderID,
		"folder-id",
		"",
		"ID of folder where snapshots are located; optional",
	)
	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type backupSnapshot struct {
	clientConfig *client_config.ClientConfig
	serverConfig *server_config.ServerConfig
	snapshotID   string
}

func (c *backupSnapshot) run() error {
	if len(c.snapshotID) == 0 {
		return fmt.Errorf("snapshot ID is required")
	}
	if c.serverConfig.GetSnapshotStorageBackupConfig() == nil {
		return fmt.Errorf("snapshot backup is not configured")
	}

	ctx := newContext(c.clientConfig)
	resourceStorage, db, err := newResourceStorage(ctx, c.serverConfig)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	err = resourceStorage.EnqueueSnapshotBackup(ctx, c.snapshotID, generateID())
	if err != nil {
		return err
	}

	fmt.Println("OK")
	return nil
}

func newBackupSnapshotCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	c := &backupSnapshot{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
	}

	cmd := &cobra.Command{
		Use:   "backup",
		Short: "Enqueue a snapshot backup; repeated requests are successful",
		Long: "Enqueue a snapshot backup and return OK without waiting for completion. " +
			"Use snapshots get --id <id> to check backup_completed: true means " +
			"the backup task completed successfully; false means no recorded success " +
			"and does not distinguish pending, failed, or never requested backups.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(
		&c.snapshotID,
		"id",
		"",
		"ID of snapshot to back up; required",
	)
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}
	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type createSnapshot struct {
	clientConfig  *client_config.ClientConfig
	zoneID        string
	srcDiskID     string
	dstSnapshotID string
	folderID      string
}

func (c *createSnapshot) run() error {
	ctx := newContext(c.clientConfig)

	client, err := internal_client.NewClient(ctx, c.clientConfig)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	req := &disk_manager.CreateSnapshotRequest{
		Src: &disk_manager.DiskId{
			ZoneId: c.zoneID,
			DiskId: c.srcDiskID,
		},
		SnapshotId: c.dstSnapshotID,
		FolderId:   c.folderID,
	}

	resp, err := client.CreateSnapshot(getRequestContext(ctx), req)
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %v\n", resp.Id)

	return internal_client.WaitOperation(ctx, client, resp.Id)
}

func newCreateSnapshotCmd(
	clientConfig *client_config.ClientConfig,
) *cobra.Command {

	c := &createSnapshot{
		clientConfig: clientConfig,
	}

	cmd := &cobra.Command{
		Use: "create",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(
		&c.zoneID,
		"zone-id",
		"",
		"zone ID in which disk is located; required",
	)
	if err := cmd.MarkFlagRequired("zone-id"); err != nil {
		log.Fatalf("Error setting flag zone-id as required: %v", err)
	}

	cmd.Flags().StringVar(
		&c.srcDiskID,
		"src-disk-id",
		"",
		"ID of disk to create snapshot from; required",
	)
	if err := cmd.MarkFlagRequired("src-disk-id"); err != nil {
		log.Fatalf("Error setting flag src-disk-id as required: %v", err)
	}

	cmd.Flags().StringVar(
		&c.dstSnapshotID,
		"id",
		"",
		"ID of snapshot to create; required",
	)
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	cmd.Flags().StringVar(
		&c.folderID,
		"folder-id",
		"",
		"folder ID of the snapshot owner; required",
	)
	if err := cmd.MarkFlagRequired("folder-id"); err != nil {
		log.Fatalf("Error setting flag folder-id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type deleteSnapshot struct {
	clientConfig *client_config.ClientConfig
	snapshotID   string
}

func (c *deleteSnapshot) run() error {
	ctx := newContext(c.clientConfig)

	err := requestConfirmation("snapshot", c.snapshotID)
	if err != nil {
		return err
	}

	client, err := internal_client.NewClient(ctx, c.clientConfig)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	req := &disk_manager.DeleteSnapshotRequest{
		SnapshotId: c.snapshotID,
	}

	resp, err := client.DeleteSnapshot(getRequestContext(ctx), req)
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %v\n", resp.Id)

	return internal_client.WaitOperation(ctx, client, resp.Id)
}

func newDeleteSnapshotCmd(
	clientConfig *client_config.ClientConfig,
) *cobra.Command {

	c := &deleteSnapshot{
		clientConfig: clientConfig,
	}

	cmd := &cobra.Command{
		Use: "delete",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(
		&c.snapshotID,
		"id",
		"",
		"ID of snapshot to delete; required",
	)
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type exportSnapshot struct {
	serverConfig      *server_config.ServerConfig
	source            string
	srcDiskID         string
	snapshotID        string
	readWorkerCount   int
	partition         uint32
	partitionCount    uint32
	verbose           bool
	s3Mode            bool
	s3Endpoint        string
	s3Bucket          string
	s3CredentialsFile string
	s3Region          string
	s3Prefix          string
	s3PartSize        string
	s3RequestTimeout  time.Duration
	s3MaxRetries      int
	parsedS3PartSize  uint64
}

const exportSnapshotLong = `Export a ready snapshot or image from dataplane
snapshot storage, or a snapshot from backup S3, to stdout as a raw disk image
stream or to a user-specified S3 destination.

This command does not call the Disk Manager service and does not require a valid
client config. The default --source=snapshot-storage uses --server-config and
reads DataplaneConfig.SnapshotConfig plus AuthConfig from that file. The host
must have network access to dataplane YDB and, when configured, S3. Read-only
access is enough: the command reads snapshot metadata, chunk maps, chunk blobs
and S3 objects.

--source=backup requires --src-disk-id and supports snapshot backups only.
It reads SnapshotStorageBackupConfig.S3Config (Endpoint, CredentialsFilePath
or UseIamToken, Region), S3Bucket, S3KeyPrefix, and optional KekFile and KekId
from --server-config. Set KekFile and KekId together for encrypted backup
objects. AuthConfig is used only when the source S3 config enables UseIamToken.
Other required server-config fields must still parse; unused service endpoints
are not contacted.
This mode needs access only to backup S3; no snapshot persistence config,
YDB, NBS, resource database or task scheduler is required. Missing meta.json,
missing or invalid chunk maps, and missing or corrupt chunks fail the export.
The backup object envelope is removed using the configured backup key; this
does not decrypt AES-XTS disk data.

Incremental snapshots are exported the same way as full snapshots: their chunk
map is complete, unchanged chunks are shallow copies of base snapshot chunks.
The exporter reads chunk bytes as stored and does not decrypt them. AES-XTS
snapshots with key-hash metadata remain ciphertext. Snapshots encrypted at rest
with a root-KMS-provided key are exported as plaintext logical disk data. Legacy
snapshot storage configured by LegacyStorageFolder is not supported.

The output stream is not sparse: zero chunks are written explicitly because
stdout is sequential. Up to --read-workers * 4 chunk reads/results can be
pending, each using a 4 MiB buffer, plus the selected partition chunk map. With
the default --partition-count=1 this is the whole snapshot map; use
--partition-count and concatenate partitions in ascending order to bound map
memory for large snapshots in snapshot-storage mode. Backup mode always loads
the entire protobuf chunk map, including when exporting a partition, in addition
to the bounded chunk read-ahead and destination part buffer.

S3 mode requires --s3-endpoint, --s3-bucket and --s3-credentials-file. Source
storage still comes from --server-config; the destination is independent. The
credentials JSON contains id, secret and an optional session_token. HTTPS
validates certificates; HTTP is supported for local endpoints. The optional
--s3-region defaults to empty and controls signing only; set the region required
by your provider. The bucket must already exist, with permission to check it,
write objects and read back an uncertain manifest upload.

S3 mode exports the whole snapshot and rejects explicit --partition and
--partition-count flags. --s3-part-size defaults to 256MiB, must be a positive
multiple of 4MiB, and cannot exceed 1GiB. Memory also includes one part buffer.
Objects are placed under [--s3-prefix/]<snapshot-id>-backup-<uuid>/ as
part-00000001.raw, part-00000002.raw, ... and manifest.json. Only the manifest
marks completion. Download parts in manifest order, verify their lengths and
SHA-256 hashes, and concatenate to reconstruct the raw image. The optional
manifest field encryption_key_hash contains the base64-encoded disk key hash
alongside encryption_mode and raw_data_interpretation; no secret key is stored.
AES-XTS ciphertext requires the original disk key and a compatible restoration
or decryption path to access the filesystem. Root-KMS snapshot metadata is
normally recorded as NO_ENCRYPTION; the source metadata is preserved.

Each invocation creates a fresh UUID. Failed attempts leave their uploaded parts
for explicit inspection or cleanup; there is no automatic resume or deletion.
Keep the source snapshot or backup available for the duration; no
deletion-prevention lease is acquired. Progress and destination URIs are printed
to stderr.`

const exportSnapshotExample = `  disk-manager-admin \
      --server-config /etc/disk-manager/server-config.txt \
      snapshots export --id <snapshot-or-image-id> > image.raw

  qemu-img info image.raw
  qemu-img convert -f raw -O qcow2 image.raw image.qcow2

  for p in 1 2 3 4; do
      disk-manager-admin \
          --server-config /etc/disk-manager/server-config.txt \
          snapshots export --id <snapshot-or-image-id> \
          --partition $p --partition-count 4 >> image.raw
  done

  disk-manager-admin \
      --server-config /etc/disk-manager/server-config.txt \
      snapshots export --id <snapshot-or-image-id> \
      --s3-endpoint https://s3.example.com --s3-bucket snapshot-backups \
      --s3-credentials-file /path/to/credentials.json --s3-part-size 256MiB

  disk-manager-admin \
      --server-config /etc/disk-manager/server-config.txt \
      snapshots export --id <snapshot-id> \
      --source backup --src-disk-id <original-disk-id> \
      --s3-endpoint https://destination.example.com \
      --s3-bucket exported-images \
      --s3-credentials-file /path/to/destination-credentials.json`

func newExportSnapshotLogger(level logging.Level) logging.Logger {
	config := logzap.ConsoleConfig(level)
	config.OutputPaths = []string{"stderr"}
	return logzap.Must(config)
}

func (c *exportSnapshot) run() error {
	if err := exporter.ValidateReadWorkers(c.readWorkerCount); err != nil {
		return err
	}

	if err := exporter.ValidatePartition(
		c.partition,
		c.partitionCount,
	); err != nil {
		return err
	}

	level := logging.InfoLevel
	if c.verbose {
		level = logging.DebugLevel
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()
	ctx = logging.SetLogger(
		ctx,
		newExportSnapshotLogger(level),
	)

	var destination *persistence.S3Client
	if c.s3Mode {
		credentials, err := persistence.NewS3CredentialsFromFile(
			c.s3CredentialsFile,
		)
		if err != nil {
			return fmt.Errorf("loading destination S3 credentials: %w", err)
		}

		if credentials.ID == "" || credentials.Secret == "" {
			return fmt.Errorf(
				"destination S3 credentials must contain nonempty " +
					"id and secret",
			)
		}

		destination, err = persistence.NewS3Client(
			c.s3Endpoint,
			c.s3Region,
			credentials,
			c.s3RequestTimeout,
			metrics.NewEmptyRegistry(),
			uint64(c.s3MaxRetries),
			nil,
			nil,
			0, // maxIdleConnsPerHost
			persistence.WithS3EmptySigningRegion(),
		)
		if err != nil {
			return fmt.Errorf("initializing destination S3 client: %w", err)
		}
	}

	source, closeSource, err := c.newSource(ctx)
	if err != nil {
		return err
	}
	defer closeSource()

	if c.s3Mode {
		_, err := exporter.ExportToS3(
			ctx,
			source,
			c.snapshotID,
			destination,
			exporter.S3ExportOptions{
				Bucket:          c.s3Bucket,
				Prefix:          c.s3Prefix,
				PartSize:        c.parsedS3PartSize,
				ReadWorkerCount: c.readWorkerCount,
			},
		)
		return err
	}

	stats, err := exporter.ExportPartitionToWriterWithReadWorkers(
		ctx,
		source,
		c.snapshotID,
		os.Stdout,
		c.partition,
		c.partitionCount,
		c.readWorkerCount,
	)
	if err != nil {
		return err
	}

	logging.Info(
		ctx,
		"exported partition %v/%v of snapshot %v to stdout: "+
			"size %v bytes, %v data chunks, %v zero chunks",
		c.partition,
		c.partitionCount,
		c.snapshotID,
		stats.Size,
		stats.DataChunkCount,
		stats.ZeroChunkCount,
	)
	return nil
}

func newExportSnapshotCmd(
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	c := &exportSnapshot{
		serverConfig:    serverConfig,
		source:          "snapshot-storage",
		readWorkerCount: exporter.DefaultStreamReadWorkerCount,
		partition:       1,
		partitionCount:  1,
	}

	cmd := &cobra.Command{
		Use:     "export",
		Short:   "Exports a raw snapshot to stdout or an S3 backup prefix",
		Long:    exportSnapshotLong,
		Example: exportSnapshotExample,
		Annotations: map[string]string{
			skipClientConfigParsingAnnotation: "true",
		},

		RunE: func(cmd *cobra.Command, args []string) error {
			if err := c.validateExportOptions(cmd); err != nil {
				return err
			}

			return c.run()
		},
	}

	cmd.Flags().StringVar(
		&c.snapshotID,
		"id",
		"",
		"ID of snapshot to export; required",
	)
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	cmd.Flags().StringVar(
		&c.source,
		"source",
		"snapshot-storage",
		"Source: snapshot-storage or backup (snapshot backups only)",
	)
	cmd.Flags().StringVar(
		&c.srcDiskID,
		"src-disk-id",
		"",
		"Original disk ID; required with --source=backup, otherwise rejected",
	)

	cmd.Flags().IntVar(
		&c.readWorkerCount,
		"read-workers",
		exporter.DefaultStreamReadWorkerCount,
		"Parallel chunk read workers; up to workers * 4 pending 4 MiB chunks",
	)
	cmd.Flags().Uint32Var(
		&c.partition,
		"partition",
		1,
		"1-based partition number to export",
	)
	cmd.Flags().Uint32Var(
		&c.partitionCount,
		"partition-count",
		1,
		"Total number of contiguous snapshot partitions",
	)
	cmd.Flags().BoolVarP(
		&c.verbose,
		"verbose",
		"v",
		false,
		"Enable verbose logging",
	)
	cmd.Flags().StringVar(
		&c.s3Endpoint,
		"s3-endpoint",
		"",
		"Destination S3-compatible HTTP(S) endpoint",
	)
	cmd.Flags().StringVar(
		&c.s3Bucket,
		"s3-bucket",
		"",
		"Existing destination bucket",
	)
	cmd.Flags().StringVar(
		&c.s3CredentialsFile,
		"s3-credentials-file",
		"",
		"Destination credentials JSON: id, secret, optional session_token",
	)
	cmd.Flags().StringVar(
		&c.s3Region,
		"s3-region",
		"",
		"Signature V4 region required by the destination provider",
	)
	cmd.Flags().StringVar(
		&c.s3Prefix,
		"s3-prefix",
		"",
		"Parent prefix for the generated backup folder",
	)
	cmd.Flags().StringVar(
		&c.s3PartSize,
		"s3-part-size",
		"256MiB",
		"Maximum raw part size: multiple of 4MiB, at most 1GiB",
	)
	cmd.Flags().DurationVar(
		&c.s3RequestTimeout,
		"s3-request-timeout",
		5*time.Minute,
		"Positive per-request timeout including SDK retries",
	)
	cmd.Flags().IntVar(
		&c.s3MaxRetries,
		"s3-max-retries",
		3,
		"Maximum transient request retries; nonnegative",
	)

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type scheduleMigrateSnapshotTaskCmd struct {
	commandWithScheduler
	snapshotID string
}

func (c *scheduleMigrateSnapshotTaskCmd) run() error {
	err := c.init()
	if err != nil {
		return err
	}
	defer c.close()

	taskID, err := c.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(
			c.ctx,
			"dataplane.MigrateSnapshotTask_"+c.snapshotID+"_"+generateID(),
		),
		"dataplane.MigrateSnapshotTask",
		"",
		&dataplane_protos.MigrateSnapshotRequest{
			SrcSnapshotId: c.snapshotID,
		},
	)
	if err != nil {
		return err
	}

	fmt.Printf("Task: %v\n", taskID)
	return nil
}

func newScheduleMigrateSnapshotTaskCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	cmdWithScheduler := newCommandWithScheduler(clientConfig, serverConfig)
	c := &scheduleMigrateSnapshotTaskCmd{
		commandWithScheduler: cmdWithScheduler,
	}

	cmd := &cobra.Command{
		Use:     "schedule-migrate-snapshot-task",
		Aliases: []string{"schedule_migrate_snapshot_task"},
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(
		&c.snapshotID,
		"id",
		"",
		"ID of snapshot to migrate data to another database; required",
	)
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type migrateSnapshotDatabaseCmd struct {
	commandWithScheduler
}

func (c *migrateSnapshotDatabaseCmd) run() error {
	err := c.init()
	if err != nil {
		return err
	}
	defer c.close()

	taskID, err := c.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(
			c.ctx,
			"dataplane.MigrateSnapshotDatabaseTask_"+generateID(),
		),
		"dataplane.MigrateSnapshotDatabaseTask",
		"",
		&empty.Empty{},
	)
	if err != nil {
		return err
	}

	fmt.Printf("Task: %v\n", taskID)
	return nil
}

func newMigrateSnapshotDatabaseCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	cmdWithScheduler := newCommandWithScheduler(clientConfig, serverConfig)
	c := &migrateSnapshotDatabaseCmd{
		commandWithScheduler: cmdWithScheduler,
	}

	return &cobra.Command{
		Use:     "schedule-migrate-snapshot-database-task",
		Aliases: []string{"schedule_migrate_snapshot_database_task"},
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}
}

////////////////////////////////////////////////////////////////////////////////

func newSnapshotsCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	cmd := &cobra.Command{
		Use:     "snapshots",
		Aliases: []string{"snapshot"},
	}

	cmd.AddCommand(
		newGetSnapshotCmd(clientConfig, serverConfig),
		newListSnapshotsCmd(clientConfig, serverConfig),
		newBackupSnapshotCmd(clientConfig, serverConfig),
		newCreateSnapshotCmd(clientConfig),
		newDeleteSnapshotCmd(clientConfig),
		newExportSnapshotCmd(serverConfig),
		newExportSnapshotToS3Cmd(clientConfig),
		newScheduleMigrateSnapshotTaskCmd(
			clientConfig,
			serverConfig,
		),
		newMigrateSnapshotDatabaseCmd(
			clientConfig,
			serverConfig,
		),
	)

	return cmd
}
