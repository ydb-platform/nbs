package app

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	cells_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"

	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem_snapshot"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/placementgroup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools"
	pools_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/auth"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

////////////////////////////////////////////////////////////////////////////////

func newGRPCServer(
	ctx context.Context,
	config *server_config.ServerConfig,
	mon *monitoring.Monitoring,
	creds auth.Credentials,
	newAuthorizer auth.NewAuthorizer,
) (*grpc.Server, error) {

	logging.Info(ctx, "Initializing authorizer")
	authorizer, err := newAuthorizer(config.GetAuthConfig(), creds)
	if err != nil {
		logging.Error(ctx, "Failed to initialize authorizer: %v", err)
		return nil, err
	}

	facadeMetricsRegistry := mon.NewRegistry("grpc_facade")

	keepAliveTime, err := time.ParseDuration(config.GetGrpcConfig().GetKeepAlive().GetTime())
	if err != nil {
		return nil, err
	}

	keepAliveTimeout, err := time.ParseDuration(config.GetGrpcConfig().GetKeepAlive().GetTimeout())
	if err != nil {
		return nil, err
	}

	keepAliveMinTime, err := time.ParseDuration(config.GetGrpcConfig().GetKeepAlive().GetMinTime())
	if err != nil {
		return nil, err
	}

	serverOptions := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    keepAliveTime,
			Timeout: keepAliveTimeout,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             keepAliveMinTime,
			PermitWithoutStream: config.GetGrpcConfig().GetKeepAlive().GetPermitWithoutStream(),
		}),
	}

	serverOptions = append(
		serverOptions,
		grpc.UnaryInterceptor(facade.NewInterceptor(
			logging.GetLogger(ctx),
			facadeMetricsRegistry,
			authorizer,
		)),
	)

	secure := !config.GetGrpcConfig().GetInsecure()
	if secure {
		logging.Info(ctx, "Creating GRPC transport credentials")
		certs := config.GetGrpcConfig().GetCerts()
		refreshCertsPeriod, err := time.ParseDuration(
			config.GetGrpcConfig().GetRefreshCertsPeriod(),
		)
		if err != nil {
			logging.Error(
				ctx,
				"Failed to create GRPC transport credentials: %v",
				err,
			)
			return nil, err
		}

		tlsProvider, err := common.NewGRPCServerTLSProvider(
			ctx,
			certs,
			refreshCertsPeriod,
			facadeMetricsRegistry,
		)
		if err != nil {
			logging.Error(
				ctx,
				"Failed to create GRPC TLS provider: %v",
				err,
			)
			return nil, err
		}

		transportCreds := tlsProvider.NewTransportCredentials()
		serverOptions = append(serverOptions, grpc.Creds(transportCreds))
	}

	return grpc.NewServer(serverOptions...), nil
}

////////////////////////////////////////////////////////////////////////////////

func registerControlplaneTasks(
	ctx context.Context,
	config *server_config.ServerConfig,
	mon *monitoring.Monitoring,
	creds auth.Credentials,
	db *persistence.YDBClient,
	taskStorage tasks_storage.Storage,
	taskRegistry *tasks.Registry,
	taskScheduler tasks.Scheduler,
	nbsFactory nbs.Factory,
	nfsFactory nfs.Factory,
	poolStorage pools_storage.Storage,
	poolService pools.Service,
	filesystemService filesystem.Service,
	resourceStorage resources.Storage,
	cellStorage cells_storage.Storage,
	cellSelector cells.CellSelector,
	filestoreCellsSelector cells.CellSelector,
) error {

	logging.Info(ctx, "Registering pool tasks")
	err := pools.RegisterForExecution(
		ctx,
		config.GetPoolsConfig(),
		taskRegistry,
		taskScheduler,
		poolStorage,
		nbsFactory,
		resourceStorage,
	)
	if err != nil {
		logging.Error(ctx, "Failed to register pool tasks: %v", err)
		return err
	}

	performanceConfig := config.PerformanceConfig
	if performanceConfig == nil {
		performanceConfig = &performance_config.PerformanceConfig{}
	}

	logging.Info(ctx, "Registering disk tasks")
	err = disks.RegisterForExecution(
		ctx,
		config.GetDisksConfig(),
		performanceConfig,
		resourceStorage,
		poolStorage,
		taskRegistry,
		taskScheduler,
		poolService,
		nbsFactory,
		cellSelector,
	)
	if err != nil {
		logging.Error(ctx, "Failed to register disk tasks: %v", err)
		return err
	}

	logging.Info(ctx, "Registering image tasks")
	err = images.RegisterForExecution(
		ctx,
		config.GetImagesConfig(),
		taskRegistry,
		taskScheduler,
		resourceStorage,
		nbsFactory,
		poolService,
		cellSelector,
	)
	if err != nil {
		logging.Error(ctx, "Failed to register image tasks: %v", err)
		return err
	}

	logging.Info(ctx, "Registering snapshot tasks")
	err = snapshots.RegisterForExecution(
		ctx,
		config.GetSnapshotsConfig(),
		taskRegistry,
		taskScheduler,
		resourceStorage,
		nbsFactory,
		cellSelector,
	)
	if err != nil {
		logging.Error(ctx, "Failed to register snapshot tasks: %v", err)
		return err
	}

	if config.GetFilesystemConfig() != nil {
		logging.Info(ctx, "Registering filesystem tasks")

		err = filesystem.RegisterForExecution(
			ctx,
			config.GetFilesystemConfig(),
			taskScheduler,
			taskRegistry,
			resourceStorage,
			nfsFactory,
			filestoreCellsSelector,
		)
		if err != nil {
			logging.Error(ctx, "Failed to register filesystem tasks: %v", err)
			return err
		}

		if config.GetFilesystemSnapshotsConfig() != nil {
			logging.Info(ctx, "Registering filesystem snapshot tasks")

			err = filesystem_snapshot.RegisterForExecution(
				ctx,
				config.GetFilesystemSnapshotsConfig(),
				taskRegistry,
				taskScheduler,
				filestoreCellsSelector,
				resourceStorage,
			)
			if err != nil {
				logging.Error(ctx, "Failed to register filesystem snapshot tasks: %v", err)
				return err
			}
		}
	}

	logging.Info(ctx, "Registering placementgroup tasks")
	err = placementgroup.RegisterForExecution(
		ctx,
		config.GetPlacementGroupConfig(),
		taskRegistry,
		taskScheduler,
		resourceStorage,
		nbsFactory,
		cellSelector,
	)
	if err != nil {
		logging.Error(ctx, "Failed to register placementgroup tasks: %v", err)
		return err
	}

	if config.GetCellsConfig() != nil {
		logging.Info(ctx, "Registering cells tasks")

		err = cells.RegisterForExecution(
			ctx,
			config.GetCellsConfig(),
			taskRegistry,
			taskScheduler,
			cellStorage,
			nbsFactory,
		)
		if err != nil {
			logging.Error(ctx, "Failed to register cells tasks: %v", err)
			return err
		}
	}

	return nil
}

func initControlplane(
	ctx context.Context,
	config *server_config.ServerConfig,
	mon *monitoring.Monitoring,
	creds auth.Credentials,
	newAuthorizer auth.NewAuthorizer,
	db *persistence.YDBClient,
	taskStorage tasks_storage.Storage,
	taskRegistry *tasks.Registry,
	taskScheduler tasks.Scheduler,
	nbsFactory nbs.Factory,
	nfsFactoryOptions nfs.FactoryOptions,
) (serve func() error, err error) {

	logging.Info(ctx, "Initializing pool storage")
	poolMetricsRegistry := mon.NewRegistry("pools")
	poolStorage, err := pools_storage.NewStorage(config.GetPoolsConfig(), db, poolMetricsRegistry)
	if err != nil {
		logging.Error(ctx, "Failed to initialize pool storage: %v", err)
		return nil, err
	}

	nfsClientMetricsRegistry := mon.NewRegistry("nfs_client")
	nfsSessionMetricsRegistry := mon.NewRegistry("nfs_session")
	nfsFactory := nfs.NewFactoryWithCreds(
		ctx,
		config.GetNfsConfig(),
		creds,
		nfsClientMetricsRegistry,
		nfsSessionMetricsRegistry,
		nfsFactoryOptions,
	)

	poolService := pools.NewService(taskScheduler, poolStorage)

	var filesystemService filesystem.Service
	var filesystemSnapshotService filesystem_snapshot.Service
	if config.GetFilesystemConfig() != nil && config.GetFilesystemSnapshotsConfig() != nil {
		filesystemSnapshotService = filesystem_snapshot.NewService(
			taskScheduler,
		)
	}

	filesystemStorageFolder := ""
	if config.GetFilesystemConfig() != nil {
		filesystemStorageFolder = config.GetFilesystemConfig().GetStorageFolder()
	}

	endedMigrationExpirationTimeout, err := time.ParseDuration(
		config.GetDisksConfig().GetEndedMigrationExpirationTimeout(),
	)
	if err != nil {
		return nil, err
	}

	logging.Info(ctx, "Initializing resource storage")
	resourceStorage, err := resources.NewStorage(
		config.GetDisksConfig().GetStorageFolder(),
		config.GetImagesConfig().GetStorageFolder(),
		config.GetSnapshotsConfig().GetStorageFolder(),
		filesystemStorageFolder,
		config.GetFilesystemSnapshotsConfig().GetStorageFolder(),
		config.GetPlacementGroupConfig().GetStorageFolder(),
		db,
		endedMigrationExpirationTimeout,
	)
	if err != nil {
		logging.Error(ctx, "Failed to initialize resource storage: %v", err)
		return nil, err
	}

	var cellStorage cells_storage.Storage
	if config.GetCellsConfig() != nil {
		cellStorage = cells_storage.NewStorage(config.GetCellsConfig(), db)
	}

	cellSelector := cells.NewCellSelector(
		config.GetCellsConfig(),
		cellStorage,
		nbsFactory,
		nfsFactory,
	)

	filestoreCellsSelector := cells.NewCellSelector(
		config.GetFilestoreCellsConfig(),
		cellStorage,
		nbsFactory,
		nfsFactory,
	)

	if config.GetFilesystemConfig() != nil {
		filesystemService = filesystem.NewService(
			taskScheduler,
			config.GetFilesystemConfig(),
			nfsFactory,
			resourceStorage,
			filestoreCellsSelector,
		)
	}

	err = registerControlplaneTasks(
		ctx,
		config,
		mon,
		creds,
		db,
		taskStorage,
		taskRegistry,
		taskScheduler,
		nbsFactory,
		nfsFactory,
		poolStorage,
		poolService,
		filesystemService,
		resourceStorage,
		cellStorage,
		cellSelector,
		filestoreCellsSelector,
	)
	if err != nil {
		return nil, err
	}

	logging.Info(ctx, "Initializing GRPC server")
	server, err := newGRPCServer(ctx, config, mon, creds, newAuthorizer)
	if err != nil {
		logging.Error(ctx, "Failed to initialize GRPC server: %v", err)
		return nil, err
	}

	facade.RegisterDiskService(
		server,
		taskScheduler,
		disks.NewService(
			taskScheduler,
			taskStorage,
			config.GetDisksConfig(),
			nbsFactory,
			poolService,
			resourceStorage,
			cellSelector,
		),
	)
	facade.RegisterImageService(
		server,
		taskScheduler,
		images.NewService(taskScheduler, config.GetImagesConfig()),
	)
	facade.RegisterOperationService(server, taskScheduler)
	facade.RegisterPlacementGroupService(
		server,
		taskScheduler,
		placementgroup.NewService(
			taskScheduler,
			nbsFactory,
			resourceStorage,
			cellSelector,
		),
	)
	facade.RegisterSnapshotService(
		server,
		taskScheduler,
		snapshots.NewService(
			taskScheduler,
			config.GetSnapshotsConfig(),
		),
	)
	facade.RegisterPrivateService(
		server,
		taskScheduler,
		nbsFactory,
		poolService,
		resourceStorage,
		taskStorage,
	)

	if filesystemService != nil {
		facade.RegisterFilesystemService(
			server,
			taskScheduler,
			filesystemService,
		)
	}

	if filesystemSnapshotService != nil {
		facade.RegisterFilesystemSnapshotService(
			server,
			taskScheduler,
			filesystemSnapshotService,
		)
	}

	serve = func() error {
		serverPort := config.GetGrpcConfig().GetPort()
		address := fmt.Sprintf(":%d", serverPort)

		logging.Info(ctx, "Listening on %v", address)
		listener, err := net.Listen("tcp", address)
		if err != nil {
			logging.Error(ctx, "Failed to listen on %v: %v", address, err)
			return err
		}

		logging.Info(ctx, "Serving on %v", address)
		err = server.Serve(listener)
		if err != nil {
			logging.Error(ctx, "Failed to serve on %v: %v", address, err)
			return err
		}

		return nil
	}

	return serve, nil
}
