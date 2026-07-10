package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/auth"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/export"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/util"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	logzap "github.com/ydb-platform/nbs/library/go/core/log/zap"
)

////////////////////////////////////////////////////////////////////////////////

func newStderrLogger(level logging.Level) logging.Logger {
	config := logzap.ConsoleConfig(level)
	config.OutputPaths = []string{"stderr"}
	return logzap.Must(config)
}

func exportSnapshot(
	ctx context.Context,
	config *server_config.ServerConfig,
	snapshotID string,
) error {

	snapshotConfig := config.GetDataplaneConfig().GetSnapshotConfig()
	if snapshotConfig == nil {
		return fmt.Errorf("dataplane snapshot config is missing in the config file")
	}

	creds := auth.NewCredentials(ctx, config.GetAuthConfig())

	db, err := persistence.NewYDBClient(
		ctx,
		snapshotConfig.GetPersistenceConfig(),
		metrics.NewEmptyRegistry(),
		persistence.WithCredentials(creds),
	)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	s3Config := snapshotConfig.GetPersistenceConfig().GetS3Config()
	var s3 *persistence.S3Client
	if s3Config != nil {
		s3, err = persistence.NewS3ClientFromConfig(
			s3Config,
			metrics.NewEmptyRegistry(),
			nil, // availabilityMonitoring
		)
		if err != nil {
			return err
		}
	}

	snapshotStorage, err := storage.NewStorage(
		snapshotConfig,
		metrics.NewEmptyRegistry(),
		db,
		s3,
	)
	if err != nil {
		return err
	}

	stats, err := export.ExportToWriter(
		ctx,
		snapshotStorage,
		snapshotID,
		os.Stdout,
	)
	if err != nil {
		return err
	}

	logging.Info(
		ctx,
		"exported snapshot %v to stdout: size %v bytes, %v data chunks, %v zero chunks",
		snapshotID,
		stats.Size,
		stats.DataChunkCount,
		stats.ZeroChunkCount,
	)
	return nil
}

////////////////////////////////////////////////////////////////////////////////

func main() {
	var configFilePath string
	var snapshotID string
	var verbose bool
	config := &server_config.ServerConfig{}

	rootCmd := &cobra.Command{
		Use:   "disk-manager-export-snapshot",
		Short: "Exports a snapshot (or an image) from the dataplane storage to stdout as a raw image stream",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return util.ParseProto(configFilePath, config)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			level := logging.InfoLevel
			if verbose {
				level = logging.DebugLevel
			}

			ctx := logging.SetLogger(
				context.Background(),
				newStderrLogger(level),
			)
			return exportSnapshot(
				ctx,
				config,
				snapshotID,
			)
		},
	}

	rootCmd.Flags().StringVar(
		&configFilePath,
		"config",
		"/etc/disk-manager/server-config.txt",
		"Path to the config file",
	)
	rootCmd.Flags().StringVar(
		&snapshotID,
		"snapshot-id",
		"",
		"ID of the snapshot (or image) to export",
	)
	rootCmd.Flags().BoolVarP(
		&verbose,
		"verbose",
		"v",
		false,
		"Enable verbose logging",
	)

	err := rootCmd.MarkFlagRequired("snapshot-id")
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	if err = rootCmd.Execute(); err != nil {
		log.Fatalf("Error: %v", err)
	}
}
