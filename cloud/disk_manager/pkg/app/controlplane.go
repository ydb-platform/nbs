package app

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	cells_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/placementgroup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools"
	pools_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/auth"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

////////////////////////////////////////////////////////////////////////////////

func readCertificates(
	certs []*server_config.Cert,
) ([]tls.Certificate, error) {

	certificates := make([]tls.Certificate, 0, len(certs))
	for _, cert := range certs {
		certificate, err := tls.LoadX509KeyPair(
			cert.GetCertFile(),
			cert.GetPrivateKeyFile(),
		)
		if err != nil {
			return nil, errors.NewNonRetriableErrorf(
				"failed to load cert file %v: %w",
				cert.CertFile,
				err,
			)
		}

		certificates = append(certificates, certificate)
	}

	return certificates, nil
}

func newTransportCredentials(
	certificates []tls.Certificate,
) credentials.TransportCredentials {

	cfg := &tls.Config{
		Certificates: certificates,
		MinVersion:   tls.VersionTLS12,
	}
	// TODO: https://golang.org/doc/go1.14#crypto/tls
	// nolint:SA1019
	cfg.BuildNameToCertificate()

	return credentials.NewTLS(cfg)
}

////////////////////////////////////////////////////////////////////////////////

// How often certificate check should be performed.
const certificateValidationPeriod = 24 * time.Hour

// Trigger alarm if certificate will expire one week after now.
const certificateValidationThreshold = 7 * 24 * time.Hour

func checkCertificates(
	ctx context.Context,
	certs []*server_config.Cert,
	certificates []tls.Certificate,
	registry metrics.Registry,
) error {

	type expiration struct {
		path  string
		after time.Time
	}

	expirations := make([]expiration, len(certificates))

	for i, c := range certificates {
		certificate, err := x509.ParseCertificate(c.Certificate[0])
		if err != nil {
			return err
		}

		expirations[i] = expiration{
			path: certs[i].GetCertFile(), after: certificate.NotAfter,
		}
	}

	go func() {
		timer := time.NewTicker(certificateValidationPeriod)
		defer timer.Stop()

		for {
			for _, expiration := range expirations {
				gauge := registry.WithTags(
					map[string]string{"path": expiration.path},
				).Gauge(
					"certificateValidity",
				)
				if time.Until(expiration.after) <= certificateValidationThreshold {
					gauge.Set(0)
				} else {
					gauge.Set(1)
				}
			}

			select {
			case <-timer.C:
				continue
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}

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
		certificates, err := readCertificates(certs)
		if err != nil {
			logging.Error(
				ctx,
				"Failed to create GRPC transport credentials: %v",
				err,
			)
			return nil, err
		}

		transportCreds := newTransportCredentials(certificates)
		serverOptions = append(serverOptions, grpc.Creds(transportCreds))

		err = checkCertificates(ctx, certs, certificates, facadeMetricsRegistry)
		if err != nil {
			logging.Error(
				ctx,
				"Failed to parse certificates: %v",
				err,
			)
			return nil, err
		}
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
		)
		if err != nil {
			logging.Error(ctx, "Failed to register filesystem tasks: %v", err)
			return err
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
			cellStorage,
			taskRegistry,
			taskScheduler,
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
) (serve func() error, err error) {

	logging.Info(ctx, "Initializing pool storage")
	poolMetricsRegistry := mon.NewRegistry("pools")
	poolStorage, err := pools_storage.NewStorage(config.GetPoolsConfig(), db, poolMetricsRegistry)
	if err != nil {
		logging.Error(ctx, "Failed to initialize pool storage: %v", err)
		return nil, err
	}

	nfsClientMetricsRegistry := mon.NewRegistry("nfs_client")
	nfsFactory := nfs.NewFactoryWithCreds(
		ctx,
		config.GetNfsConfig(),
		creds,
		nfsClientMetricsRegistry,
	)

	poolService := pools.NewService(taskScheduler, poolStorage)

	var filesystemService filesystem.Service
	if config.GetFilesystemConfig() != nil {
		filesystemService = filesystem.NewService(
			taskScheduler,
			config.GetFilesystemConfig(),
			nfsFactory,
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
		config.GetPlacementGroupConfig().GetStorageFolder(),
		db,
		endedMigrationExpirationTimeout,
	)
	if err != nil {
		logging.Error(ctx, "Failed to initialize resource storage: %v", err)
		return nil, err
	}

	cellsConfig := config.GetCellsConfig()
	cellSelector := cells.NewCellSelector(cellsConfig, nbsFactory)

	var cellStorage cells_storage.Storage
	if config.GetCellsConfig() != nil {
		cellStorage = cells_storage.NewStorage(config.GetCellsConfig(), db)
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
		placementgroup.NewService(taskScheduler, nbsFactory),
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
