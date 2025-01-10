package schema

import (
	"context"
	"log"

	"github.com/spf13/cobra"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/auth"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/util"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

func Create(
	toolName string,
	defaultConfigFilePath string,
) {

	var configFilePath string
	var verbose bool
	var dropUnusedColumns bool
	config := &server_config.ServerConfig{}

	var rootCmd = &cobra.Command{
		Use:   toolName,
		Short: "DB schema management tool for Disk Manager Service",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return util.ParseProto(configFilePath, config)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			level := logging.InfoLevel
			if verbose {
				level = logging.DebugLevel
			}

			ctx := context.Background()
			ctx = logging.SetLogger(ctx, logging.NewStderrLogger(level))
			return create(ctx, config, dropUnusedColumns)
		},
	}

	rootCmd.Flags().StringVar(
		&configFilePath,
		"config",
		defaultConfigFilePath,
		"Path to the config file",
	)
	rootCmd.Flags().BoolVarP(
		&verbose,
		"verbose",
		"v",
		false,
		"Enable verbose logging",
	)
	rootCmd.Flags().BoolVarP(
		&dropUnusedColumns,
		"drop-unused-columns",
		"d",
		false,
		"Drops unused columns. DANGEROUS flag, use it carefully!",
	)

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Error: %v", err)
	}
}

////////////////////////////////////////////////////////////////////////////////

func create(
	ctx context.Context,
	config *server_config.ServerConfig,
	dropUnusedColumns bool,
) error {

	logging.Info(ctx, "Initiliazing schema")

	creds := auth.NewCredentials(ctx, config.GetAuthConfig())

	db, err := persistence.NewYDBClient(
		ctx,
		config.GetPersistenceConfig(),
		metrics.NewEmptyRegistry(),
		persistence.WithCredentials(creds),
	)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	if config.GetDataplaneConfig() != nil {
		err = initDataplane(ctx, config, creds, db, dropUnusedColumns)
		if err != nil {
			return err
		}

		snapshotConfig := config.GetDataplaneConfig().GetSnapshotConfig()
		if snapshotConfig.GetMigrationDestinationStorageConfig() != nil {
			migrationDestinationDb, err := persistence.NewYDBClient(
				ctx,
				snapshotConfig.GetMigrationDestinationStorageConfig(),
				metrics.NewEmptyRegistry(),
				persistence.WithCredentials(creds),
			)
			if err != nil {
				return err
			}
			defer migrationDestinationDb.Close(ctx)

			err = initDataplane(
				ctx,
				config,
				creds,
				migrationDestinationDb,
				dropUnusedColumns,
			)
			if err != nil {
				return err
			}

		}
	}

	// TODO: should not use GrpcConfig here.
	if config.GetGrpcConfig() != nil {
		err = initControlplane(ctx, config, db, dropUnusedColumns)
		if err != nil {
			return err
		}
	}

	logging.Info(ctx, "Initialized schema")
	return nil
}
