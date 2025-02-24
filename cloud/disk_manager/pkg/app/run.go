package app

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/accounting"
	internal_auth "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/auth"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/health"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/util"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/auth"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
	"github.com/ydb-platform/nbs/cloud/tasks/tracing"
)

////////////////////////////////////////////////////////////////////////////////

func Run(
	appName string,
	defaultConfigFilePath string,
	newMetricsRegistry metrics.NewRegistryFunc,
	newAuthorizer auth.NewAuthorizer,
) {

	var configFilePath string
	config := &server_config.ServerConfig{}

	var rootCmd = &cobra.Command{
		Use:   appName,
		Short: "Disk Manager Server",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return util.ParseProto(configFilePath, config)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(config, newMetricsRegistry, newAuthorizer)
		},
	}
	rootCmd.Flags().StringVar(
		&configFilePath,
		"config",
		defaultConfigFilePath,
		"Path to the config file",
	)

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Error: %v", err)
	}
}

////////////////////////////////////////////////////////////////////////////////

func run(
	config *server_config.ServerConfig,
	newMetricsRegistry metrics.NewRegistryFunc,
	newAuthorizer auth.NewAuthorizer,
) error {

	ignoreSigpipe()

	// Use cancellable context.
	ctx := context.Background()

	var hostname string

	// TODO: move hostname from GrpcConfig.
	if config.GrpcConfig != nil {
		hostname = config.GetGrpcConfig().GetHostname()
	}

	logger := logging.NewLogger(config.LoggingConfig)
	ctx = logging.SetLogger(ctx, logger)

	logging.Info(ctx, "Locking process memory")

	if config.GetLockProcessMemory() {
		err := util.LockProcessMemory()
		if err != nil {
			logging.Error(ctx, "Failed to lock process memory: %v", err)
			return err
		}
	}

	var err error

	if len(hostname) == 0 {
		hostname, err = os.Hostname()
		if err != nil {
			logging.Error(ctx, "Failed to get hostname from OS: %v", err)
			return err
		}
	}

	logging.Info(ctx, "Starting monitoring")
	mon := monitoring.NewMonitoring(config.MonitoringConfig, newMetricsRegistry)
	mon.Start(ctx)

	accounting.Init(mon.NewRegistry("accounting"))

	if config.TracingConfig != nil {
		logging.Info(ctx, "Initializing tracing")
		shutdown, err := tracing.InitTracing(ctx, config.TracingConfig)
		if err != nil {
			logging.Error(ctx, "Failed to initialize tracing: %v", err)
			return err
		}
		defer shutdown(ctx)
	}

	creds := internal_auth.NewCredentials(ctx, config.GetAuthConfig())

	logging.Info(ctx, "Initializing YDB client")
	ydbClientRegistry := mon.NewRegistry("ydb_client")
	db, err := persistence.NewYDBClient(
		ctx,
		config.PersistenceConfig,
		ydbClientRegistry,
		persistence.WithRegistry(mon.NewRegistry("ydb")),
		persistence.WithCredentials(creds),
	)
	if err != nil {
		logging.Error(ctx, "Failed to initialize YDB client: %v", err)
		return err
	}
	defer db.Close(ctx)
	logging.Info(ctx, "Initialized YDB client")

	taskMetricsRegistry := mon.NewRegistry("tasks")
	taskStorage, err := tasks_storage.NewStorage(
		config.TasksConfig,
		taskMetricsRegistry,
		db,
	)
	if err != nil {
		logging.Error(ctx, "Failed to initialize task storage: %v", err)
		return err
	}

	logging.Info(ctx, "Creating task scheduler")
	taskRegistry := tasks.NewRegistry()
	taskScheduler, err := tasks.NewScheduler(
		ctx,
		taskRegistry,
		taskStorage,
		config.TasksConfig,
		taskMetricsRegistry,
	)
	if err != nil {
		logging.Error(ctx, "Failed to create task scheduler: %v", err)
		return err
	}

	nbsClientMetricsRegistry := mon.NewRegistry("nbs_client")
	nbsSessionMetricsRegistry := mon.NewRegistry("nbs_session")
	nbsFactory, err := nbs.NewFactoryWithCreds(
		ctx,
		config.NbsConfig,
		creds,
		nbsClientMetricsRegistry,
		nbsSessionMetricsRegistry,
	)
	if err != nil {
		logging.Error(ctx, "Failed to create nbs factory: %v", err)
		return err
	}

	var s3 *persistence.S3Client
	var s3Bucket string

	dataplaneConfig := config.GetDataplaneConfig()
	if dataplaneConfig == nil {
		logging.Info(ctx, "Registering dataplane tasks")
		err = dataplane.Register(ctx, taskRegistry)
		if err != nil {
			logging.Error(ctx, "Failed to register dataplane tasks: %v", err)
			return err
		}
	} else {
		logging.Info(ctx, "Initializing YDB client for snapshot database")
		snapshotConfig := dataplaneConfig.GetSnapshotConfig()
		snapshotDB, err := persistence.NewYDBClient(
			ctx,
			snapshotConfig.GetPersistenceConfig(),
			ydbClientRegistry,
			persistence.WithCredentials(creds),
		)
		if err != nil {
			logging.Error(ctx, "Failed to initialize YDB client for snapshot database: %v", err)
			return err
		}
		defer snapshotDB.Close(ctx)

		s3Config := snapshotConfig.GetPersistenceConfig().GetS3Config()
		s3Bucket = snapshotConfig.GetS3Bucket()
		// TODO: remove when s3 will always be initialized.
		if s3Config != nil {
			s3MetricsRegistry := mon.NewRegistry("s3_client")

			availabilityMonitoringMetricsRegistry := mon.NewRegistry("availability_monitoring")
			availabilityMonitoringStorage := persistence.NewAvailabilityMonitoringStorage(
				config.TasksConfig,
				db,
			)
			successRateReportingInterval, err := time.ParseDuration(
				config.TasksConfig.GetAvailabilityMonitoringSuccessRateReportingInterval(),
			)
			if err != nil {
				return err
			}

			availabilityMonitoring := persistence.NewAvailabilityMonitoring(
				ctx,
				"s3",
				hostname,
				successRateReportingInterval,
				availabilityMonitoringStorage,
				availabilityMonitoringMetricsRegistry,
			)

			s3, err = persistence.NewS3ClientFromConfig(
				s3Config,
				s3MetricsRegistry,
				availabilityMonitoring,
			)
			if err != nil {
				return err
			}
		}

		var migrationDstDB *persistence.YDBClient
		var migrationDstS3 *persistence.S3Client
		migrationDstSnapshotConfig := dataplaneConfig.GetMigrationDstSnapshotConfig()
		if migrationDstSnapshotConfig != nil {
			migrationYdbClientRegistry := mon.NewRegistry("migration_ydb_client")
			migrationDstDB, err = persistence.NewYDBClient(
				ctx,
				migrationDstSnapshotConfig.GetPersistenceConfig(),
				migrationYdbClientRegistry,
				persistence.WithCredentials(creds),
			)
			if err != nil {
				return err
			}
			defer migrationDstDB.Close(ctx)

			migrationDstS3Config := migrationDstSnapshotConfig.GetPersistenceConfig().GetS3Config()
			if migrationDstS3Config != nil {
				registry := mon.NewRegistry("migration_s3_client")
				migrationDstS3, err = persistence.NewS3ClientFromConfig(
					migrationDstS3Config,
					registry,
					nil, // availabilityMonitoring
				)
				if err != nil {
					return err
				}
			}
		}

		err = initDataplane(
			ctx,
			config,
			mon,
			snapshotDB,
			taskRegistry,
			taskScheduler,
			nbsFactory,
			s3,
			migrationDstDB,
			migrationDstS3,
		)
		if err != nil {
			logging.Error(ctx, "Failed to initialize dataplane: %v", err)
			return err
		}
	}

	var serve func() error

	if config.GetGrpcConfig() != nil {
		logging.Info(ctx, "Initializing GRPC services")
		serve, err = initControlplane(
			ctx,
			config,
			mon,
			creds,
			newAuthorizer,
			db,
			taskStorage,
			taskRegistry,
			taskScheduler,
			nbsFactory,
		)
		if err != nil {
			logging.Error(ctx, "Failed to initialize GRPC services: %v", err)
			return err
		}
	}

	runnerMetricsRegistry := mon.NewRegistry("runners")

	controller := tasks.NewController(
		ctx,
		taskStorage,
		taskRegistry,
		runnerMetricsRegistry,
		config.GetTasksConfig(),
		hostname,
	)

	err = controller.StartRunners()
	if err != nil {
		logging.Error(ctx, "Failed to start runners: %v", err)
		return err
	}

	health.MonitorHealth(
		ctx,
		mon.NewRegistry("health"),
		db,
		nbsFactory,
		s3,
		s3Bucket,
		controller.HealthChangedCallback,
	)

	if serve != nil {
		logging.Info(ctx, "Serving requests")
		return serve()
	}

	select {}
}

////////////////////////////////////////////////////////////////////////////////

func ignoreSigpipe() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGPIPE)
}
