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
)

////////////////////////////////////////////////////////////////////////////////

func exportSnapshot(
	ctx context.Context,
	config *server_config.ServerConfig,
	snapshotID string,
	outputFilePath string,
	workerCount int,
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

	// os.Create truncates an existing file, so zero chunks that Export skips
	// are guaranteed to read back as zeroes.
	file, err := os.Create(outputFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	stats, err := export.Export(
		ctx,
		snapshotStorage,
		snapshotID,
		file,
		workerCount,
	)
	if err != nil {
		return err
	}

	err = file.Sync()
	if err != nil {
		return err
	}

	logging.Info(
		ctx,
		"exported snapshot %v to %v: size %v bytes, %v data chunks, %v zero chunks",
		snapshotID,
		outputFilePath,
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
	var outputFilePath string
	var workerCount int
	var verbose bool
	config := &server_config.ServerConfig{}

	rootCmd := &cobra.Command{
		Use:   "disk-manager-export-snapshot",
		Short: "Exports a snapshot (or an image) from the dataplane storage into a local raw image file",
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
				logging.NewStderrLogger(level),
			)
			return exportSnapshot(
				ctx,
				config,
				snapshotID,
				outputFilePath,
				workerCount,
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
	rootCmd.Flags().StringVar(
		&outputFilePath,
		"output",
		"",
		"Path to the output raw image file",
	)
	rootCmd.Flags().IntVar(
		&workerCount,
		"worker-count",
		32,
		"Number of chunks to read concurrently",
	)
	rootCmd.Flags().BoolVarP(
		&verbose,
		"verbose",
		"v",
		false,
		"Enable verbose logging",
	)

	for _, flagName := range []string{"snapshot-id", "output"} {
		err := rootCmd.MarkFlagRequired(flagName)
		if err != nil {
			log.Fatalf("Error: %v", err)
		}
	}

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Error: %v", err)
	}
}
