#include "bootstrap.h"

#include "config_initializer.h"
#include "options.h"

#include <cloud/blockstore/libs/cells/iface/cell_manager.h>
#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/common/caching_allocator.h>
#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/critical_events_init.h>
#include <cloud/blockstore/libs/diagnostics/fault_injection.h>
#include <cloud/blockstore/libs/diagnostics/incomplete_request_processor.h>
#include <cloud/blockstore/libs/diagnostics/probes.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/diagnostics/stats_aggregator.h>
#include <cloud/blockstore/libs/diagnostics/volume_balancer_switch.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/discovery/balancing.h>
#include <cloud/blockstore/libs/discovery/ban.h>
#include <cloud/blockstore/libs/discovery/config.h>
#include <cloud/blockstore/libs/discovery/discovery.h>
#include <cloud/blockstore/libs/discovery/fetch.h>
#include <cloud/blockstore/libs/discovery/healthcheck.h>
#include <cloud/blockstore/libs/discovery/ping.h>
#include <cloud/blockstore/libs/encryption/encryption_client.h>
#include <cloud/blockstore/libs/encryption/encryption_key.h>
#include <cloud/blockstore/libs/encryption/encryption_service.h>
#include <cloud/blockstore/libs/endpoints/endpoint_events.h>
#include <cloud/blockstore/libs/endpoints/endpoint_listener.h>
#include <cloud/blockstore/libs/endpoints/endpoint_manager.h>
#include <cloud/blockstore/libs/endpoints/service_endpoint.h>
#include <cloud/blockstore/libs/endpoints/session_manager.h>
#include <cloud/blockstore/libs/endpoints_grpc/socket_endpoint_listener.h>
#include <cloud/blockstore/libs/endpoints_nbd/nbd_server.h>
#include <cloud/blockstore/libs/endpoints_rdma/rdma_server.h>
#include <cloud/blockstore/libs/endpoints_spdk/spdk_server.h>
#include <cloud/blockstore/libs/endpoints_vhost/external_vhost_server.h>
#include <cloud/blockstore/libs/endpoints_vhost/vhost_server.h>
#include <cloud/blockstore/libs/local_nvme/device_provider.h>
#include <cloud/blockstore/libs/local_nvme/service.h>
#include <cloud/blockstore/libs/local_nvme/service_proxy.h>
#include <cloud/blockstore/libs/nbd/device.h>
#include <cloud/blockstore/libs/nbd/error_handler.h>
#include <cloud/blockstore/libs/nbd/netlink_device.h>
#include <cloud/blockstore/libs/nbd/server.h>
#include <cloud/blockstore/libs/nvme/nvme.h>
#include <cloud/blockstore/libs/rdma/config.h>
#include <cloud/blockstore/libs/server/config.h>
#include <cloud/blockstore/libs/server/server.h>
#include <cloud/blockstore/libs/service/device_handler.h>
#include <cloud/blockstore/libs/service/overlapping_requests_guard_service.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service/service_error_transform.h>
#include <cloud/blockstore/libs/service/service_filtered.h>
#include <cloud/blockstore/libs/service/service_null.h>
#include <cloud/blockstore/libs/service/split_request_service.h>
#include <cloud/blockstore/libs/service/storage_provider.h>
#include <cloud/blockstore/libs/service_local/file_io_service_provider.h>
#include <cloud/blockstore/libs/service_local/service_local.h>
#include <cloud/blockstore/libs/service_local/storage_local.h>
#include <cloud/blockstore/libs/service_local/storage_null.h>
#include <cloud/blockstore/libs/service_local/storage_rdma.h>
#include <cloud/blockstore/libs/service_local/storage_spdk.h>
#include <cloud/blockstore/libs/service_rdma/rdma_target.h>
#include <cloud/blockstore/libs/service_throttling/throttler_logger.h>
#include <cloud/blockstore/libs/service_throttling/throttler_policy.h>
#include <cloud/blockstore/libs/service_throttling/throttler_tracker.h>
#include <cloud/blockstore/libs/service_throttling/throttling.h>
#include <cloud/blockstore/libs/spdk/iface/env.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/probes.h>
#include <cloud/blockstore/libs/storage/disk_registry_proxy/model/config.h>
#include <cloud/blockstore/libs/throttling/throttler.h>
#include <cloud/blockstore/libs/throttling/throttler_logger.h>
#include <cloud/blockstore/libs/throttling/throttler_metrics.h>
#include <cloud/blockstore/libs/validation/validation.h>
#include <cloud/blockstore/libs/vhost/server.h>
#include <cloud/blockstore/libs/vhost/vhost.h>

#include <cloud/storage/core/libs/aio/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/common/proto_helpers.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/daemon/mlock.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/diagnostics/stats_fetcher.h>
#include <cloud/storage/core/libs/diagnostics/stats_updater.h>
#include <cloud/storage/core/libs/diagnostics/trace_processor.h>
#include <cloud/storage/core/libs/diagnostics/trace_processor_mon.h>
#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>
#include <cloud/storage/core/libs/endpoints/fs/fs_endpoints.h>
#include <cloud/storage/core/libs/endpoints/keyring/keyring_endpoints.h>
#include <cloud/storage/core/libs/grpc/init.h>
#include <cloud/storage/core/libs/grpc/threadpool.h>
#include <cloud/storage/core/libs/grpc/tls_certificate_provider.h>
#include <cloud/storage/core/libs/opentelemetry/iface/trace_service_client.h>
#include <cloud/storage/core/libs/opentelemetry/impl/trace_reader.h>
#include <cloud/storage/core/libs/rdma/iface/client.h>
#include <cloud/storage/core/libs/rdma/iface/server.h>
#include <cloud/storage/core/libs/version/version.h>

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>
#include <library/cpp/lwtrace/probes.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/datetime/base.h>
#include <util/stream/file.h>
#include <util/stream/str.h>
#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NServer {

using namespace NMonitoring;
using namespace NNvme;

using namespace NCloud::NBlockStore::NDiscovery;
using namespace NCloud::NBlockStore::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString TraceLoggerId = "st_trace_logger";
const TString TraceExporterId = "st_trace_exporter";
const TString SlowRequestsFilterId = "st_slow_requests_filter";

////////////////////////////////////////////////////////////////////////////////

NVhost::TServerConfig CreateVhostServerConfig(const TServerAppConfig& config)
{
    return NVhost::TServerConfig {
        .ThreadsCount = config.GetVhostThreadsCount(),
        .SocketAccessMode = config.GetSocketAccessMode(),
        .Affinity = config.GetVhostAffinity()
    };
}

TVhostEndpointThreadCounts CreateVhostEndpointThreadCounts(
    const TServerAppConfig& config)
{
    return TVhostEndpointThreadCounts{
        .SSD = config.GetVhostEndpointThreadCountSSD(),
        .HDD = config.GetVhostEndpointThreadCountHDD(),
        .NonReplicated = config.GetVhostEndpointThreadCountNonReplicated(),
        .Mirror2 = config.GetVhostEndpointThreadCountMirror2(),
        .Mirror3 = config.GetVhostEndpointThreadCountMirror3(),
    };
}

NBD::TServerConfig CreateNbdServerConfig(const TServerAppConfig& config)
{
    return NBD::TServerConfig {
        .ThreadsCount = config.GetNbdThreadsCount(),
        .LimiterEnabled = config.GetNbdLimiterEnabled(),
        .MaxInFlightBytesPerThread = config.GetMaxInFlightBytesPerThread(),
        .SocketAccessMode = config.GetSocketAccessMode(),
        .Affinity = config.GetNbdAffinity(),
    };
}

TNVMeEndpointConfig CreateNVMeEndpointConfig(const TServerAppConfig& config)
{
    return TNVMeEndpointConfig {
        .Nqn = config.GetNVMeEndpointNqn(),
        .TransportIDs = config.GetNVMeEndpointTransportIDs(),
    };
}

TSCSIEndpointConfig CreateSCSIEndpointConfig(const TServerAppConfig& config)
{
    return TSCSIEndpointConfig {
        .ListenAddress = config.GetSCSIEndpointListenAddress(),
        .ListenPort = config.GetSCSIEndpointListenPort(),
    };
}

TRdmaEndpointConfig CreateRdmaEndpointConfig(const TServerAppConfig& config)
{
    return TRdmaEndpointConfig {
        .ListenAddress = config.GetRdmaEndpointListenAddress(),
        .ListenPort = config.GetRdmaEndpointListenPort(),
    };
}

TThrottlingServiceConfig CreateThrottlingServicePolicyConfig(
    const TServerAppConfig& config)
{
    return TThrottlingServiceConfig(
        config.GetMaxReadBandwidth(),
        config.GetMaxWriteBandwidth(),
        config.GetMaxReadIops(),
        config.GetMaxWriteIops(),
        config.GetMaxBurstTime()
    );
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TBootstrapBase::TBootstrapBase(IDeviceHandlerFactoryPtr deviceHandlerFactory)
    : DeviceHandlerFactory(std::move(deviceHandlerFactory))
{}

TBootstrapBase::~TBootstrapBase()
{}

void TBootstrapBase::ParseOptions(int argc, char** argv)
{
    Y_ABORT_UNLESS(!Configs);
    Configs = InitConfigs(argc, argv);
}

// Install complete startup inputs without exposing a partially prepared bundle.
void TBootstrapBase::SetBootstrapConfig(TBootstrapConfig config)
{
    // Detect incorrect initialization order before retaining any new inputs.
    Y_ABORT_UNLESS(!BootstrapConfig, "Bootstrap configuration is already set");
    Y_ABORT_UNLESS(
        config.ServerConfig && config.EndpointConfig &&
        config.DiagnosticsConfig && config.DiskAgentConfig &&
        config.RdmaConfig && config.CellsConfig && config.SpdkEnvConfig &&
        config.DiscoveryConfig,
        "Bootstrap configuration is incomplete");

    // Retain the selected wrappers and values for the bootstrap lifetime.
    BootstrapConfig =
        std::make_unique<const TBootstrapConfig>(std::move(config));
}

void TBootstrapBase::Init()
{
    BootstrapLogging = CreateLoggingService("console", TLogSettings{});
    Log = BootstrapLogging->CreateLog("BLOCKSTORE_SERVER");
    SetCriticalEventsLog(Log);
    Configs->Log = Log;
    STORAGE_INFO("NBS server version: " << GetFullVersionString());

    Timer = CreateWallClockTimer();
    Scheduler = CreateScheduler();
    BackgroundThreadPool = CreateThreadPool("Background", 1);
    BackgroundScheduler = CreateBackgroundScheduler(
        Scheduler,
        BackgroundThreadPool);
    LongRunningTaskExecutor = CreateLongRunningTaskExecutor("LongRunning");

    Executor = TExecutor::Create("SVC");

    VolumeBalancerSwitch = CreateVolumeBalancerSwitch();
    EndpointEventHandler = CreateEndpointEventProxy();

    switch (Configs->Options->ServiceKind) {
        case TOptionsCommon::EServiceKind::Ydb:
            InitKikimrService();
            break;
        case TOptionsCommon::EServiceKind::Local:
            InitLocalService();
            break;
        case TOptionsCommon::EServiceKind::Null:
            InitNullService();
            break;
    }

    STORAGE_INFO("Service initialized");

    if (BootstrapConfig->ServerConfig->GetEnableRequestSplitter()) {
        Service = CreateSplitRequestService(std::move(Service));
    }

    if (BootstrapConfig->ServerConfig->GetEnableOverlappingRequestsGuard())
    {
        Service = CreateOverlappingRequestsGuardsService(std::move(Service));
    }

    if (BootstrapConfig->RdmaConfig->GetBlockstoreServerTargetEnabled()) {
        InitRdmaRequestServer();
        if (RdmaRequestServer) {
            RdmaTarget = CreateBlockstoreServerRdmaTarget(
                std::make_shared<TBlockstoreServerRdmaTargetConfig>(
                    BootstrapConfig->RdmaConfig
                        ->GetBlockstoreServerTarget()),
                Logging,
                GetTraceSerializer(),
                RdmaRequestServer,
                Service);
            STORAGE_INFO("RDMA Target initialized");
        }
    }

    GrpcLog = Logging->CreateLog("GRPC");
    GrpcLoggerInit(
        GrpcLog,
        Configs->Options->EnableGrpcTracing);

    auto diagnosticsConfig = BootstrapConfig->DiagnosticsConfig;
    if (TraceReaders.size()) {
        TTraceProcessorConfig traceProcessorConfig;
        traceProcessorConfig.ComponentName = "BLOCKSTORE_TRACE";
        TraceProcessor = CreateTraceProcessorMon(
            Monitoring,
            CreateTraceProcessor(
                Timer,
                BackgroundScheduler,
                Logging,
                std::move(traceProcessorConfig),
                NLwTraceMonPage::TraceManager(diagnosticsConfig->GetUnsafeLWTrace()),
                TraceReaders));

        STORAGE_INFO("TraceProcessor initialized");
    }

    auto inactiveClientsTimeout = BootstrapConfig->InactiveClientsTimeout;

    auto rootGroup = Monitoring->GetCounters()
        ->GetSubgroup("counters", "blockstore");

    auto serverGroup = rootGroup->GetSubgroup("component", "server");
    auto volumeCriticalEventsGroup =
        rootGroup->GetSubgroup("component", "critical_events");
    auto revisionGroup = serverGroup->GetSubgroup("revision", GetFullVersionString());

    auto versionCounter = revisionGroup->GetCounter(
        "version",
        false);
    *versionCounter = 1;

    InitVolumeCriticalEventsReportingMode(
        BootstrapConfig->DiagnosticsConfig
            ->GetVolumeCriticalEventsReportingMode());
    InitCriticalEventsCounter(serverGroup);
    InitVolumeCriticalEventsCounter(volumeCriticalEventsGroup);

    STORAGE_INFO("CriticalEvents counters initialized");

    CriticalEventsStatsUpdater = CreateStatsUpdater(
        Timer,
        BackgroundScheduler,
        CreateCriticalEventsStatsHandler());

    STORAGE_INFO("CriticalEventsStatsUpdater initialized");

    TVector<TCertificateFiles> certPathList;
    for (const auto& cert:
         BootstrapConfig->ServerConfig->GetCertsWithLegacyFallback())
    {
        certPathList.push_back({
            cert.CertPrivateKeyFile,
            cert.CertFile
        });
    }

    if (!BootstrapConfig->ServerConfig->GetSecurePort()) {
        CertificateProvider = CreateCertificateProviderStub();
    } else {
        Y_ENSURE(
            certPathList,
            "Secure port is configured without certificates");

        // Below we use explicit name "BLOCKSTORE_TLS_CERTIFICATE_PROVIDER"
        // because overwise it would break server_lightweight build.
        // GetComponentName() depend on kikimr which is prohibited in
        // server_lightweight.

        CertificateProvider = CreateCertificateProvider(
            Logging,
            "BLOCKSTORE_TLS_CERTIFICATE_PROVIDER",
            Scheduler,
            LongRunningTaskExecutor,
            serverGroup,
            BootstrapConfig->ServerConfig->GetRootCertsFile(),
            std::move(certPathList),
            BootstrapConfig->ServerConfig->GetRefreshCertsPeriod());
    }

    for (auto& event: PostponedCriticalEvents) {
        ReportCriticalEvent(
            event,
            "",     // message
            false); // verifyDebug
    }
    PostponedCriticalEvents.clear();

    RequestStats = CreateServerRequestStats(
        serverGroup,
        Timer,
        BootstrapConfig->DiagnosticsConfig->GetHistogramCounterOptions(),
        BootstrapConfig->DiagnosticsConfig->GetExecutionTimeSizeClasses());

    if (!VolumeStats) {
        VolumeStats = CreateVolumeStats(
            Monitoring,
            BootstrapConfig->DiagnosticsConfig,
            inactiveClientsTimeout,
            EVolumeStatsType::EServerStats,
            Timer);
    }

    ServerStats = CreateServerStats(
        BootstrapConfig->ServerConfig,
        BootstrapConfig->DiagnosticsConfig,
        Monitoring,
        ProfileLog,
        RequestStats,
        VolumeStats);

    STORAGE_INFO("Stats initialized");

    TVector<IStorageProviderPtr> storageProviders;

    if (BootstrapConfig->ServerConfig->GetNvmfInitiatorEnabled()) {
        Y_ABORT_UNLESS(Spdk);

        const auto& config = *BootstrapConfig->DiskAgentConfig;

        storageProviders.push_back(CreateSpdkStorageProvider(
            Spdk,
            CreateSyncCachingAllocator(
                Spdk->GetAllocator(),
                config.GetPageSize(),
                config.GetMaxPageCount(),
                config.GetPageDropSize()),
            ServerStats));
    }

    if (!BootstrapConfig->UseNonreplicatedRdmaActor && RdmaClient) {
        storageProviders.push_back(CreateRdmaStorageProvider(
            ServerStats,
            RdmaClient,
            ERdmaTaskQueueOpt::Use));
    }

    storageProviders.push_back(CreateDefaultStorageProvider(Service));

    StorageProvider = CreateMultiStorageProvider(
        std::move(storageProviders));

    STORAGE_INFO("StorageProvider initialized");

    const NProto::TChecksumFlags checksumFlags =
        BootstrapConfig->ServerConfig->GetChecksumFlags();
    TSessionManagerOptions sessionManagerOptions;
    sessionManagerOptions.StrictContractValidation
        = BootstrapConfig->ServerConfig->GetStrictContractValidation();
    sessionManagerOptions.DefaultClientConfig
        = BootstrapConfig->EndpointConfig->GetClientConfig();
    sessionManagerOptions.HostProfile = Configs->HostPerformanceProfile;
    sessionManagerOptions.TemporaryServer = Configs->Options->TemporaryServer;
    sessionManagerOptions.DisableClientThrottler =
        BootstrapConfig->ServerConfig->GetDisableClientThrottlers();
    sessionManagerOptions.EnableDataIntegrityClient =
        checksumFlags.GetEnableDataIntegrityClient();
    for (auto mediaKind: checksumFlags.GetMediaKindsToValidateDataIntegrity()) {
        sessionManagerOptions.MediaKindsToValidateDataIntegrity.push_back(
            static_cast<NProto::EStorageMediaKind>(mediaKind));
    }

    if (!KmsKeyProvider) {
        KmsKeyProvider = CreateKmsKeyProviderStub();
    }

    if (!RootKmsKeyProvider) {
        RootKmsKeyProvider = CreateRootKmsKeyProviderStub();
    }

    auto encryptionClientFactory = CreateEncryptionClientFactory(
        Logging,
        CreateEncryptionKeyProvider(KmsKeyProvider, RootKmsKeyProvider),
        BootstrapConfig->ServerConfig->GetEncryptZeroPolicy());

    SetupCellManager();

    auto sessionManager = CreateSessionManager(
        Timer,
        Scheduler,
        Logging,
        Monitoring,
        RequestStats,
        VolumeStats,
        ServerStats,
        Service,
        CellManager,
        StorageProvider,
        encryptionClientFactory,
        Executor,
        sessionManagerOptions);

    STORAGE_INFO("SessionManager initialized");

    THashMap<NProto::EClientIpcType, IEndpointListenerPtr> endpointListeners;

    GrpcEndpointListener = CreateSocketEndpointListener(
        Logging,
        BootstrapConfig->ServerConfig->GetUnixSocketBacklog(),
        BootstrapConfig->ServerConfig->GetSocketAccessMode());
    endpointListeners.emplace(NProto::IPC_GRPC, GrpcEndpointListener);

    STORAGE_INFO("SocketEndpointListener initialized");

    NbdErrorHandlerMap = NBD::CreateErrorHandlerMapStub();

    if (BootstrapConfig->ServerConfig->GetNbdEnabled()) {
        NbdServer = NBD::CreateServer(
            Logging,
            CreateNbdServerConfig(*BootstrapConfig->ServerConfig));

        STORAGE_INFO("NBD Server initialized");

        if (BootstrapConfig->ServerConfig->GetNbdNetlink()) {
            NbdErrorHandlerMap = NBD::CreateErrorHandlerMap();
        }

        auto nbdEndpointListener = CreateNbdEndpointListener(
            NbdServer,
            Logging,
            ServerStats,
            checksumFlags,
            BootstrapConfig->ServerConfig->GetMaxZeroBlocksSubRequestSize(),
            NbdErrorHandlerMap);

        endpointListeners.emplace(
            NProto::IPC_NBD,
            std::move(nbdEndpointListener));

        STORAGE_INFO("NBD EndpointListener initialized");
    }

    if (BootstrapConfig->ServerConfig->GetVhostEnabled()) {
        NVhost::InitVhostLog(Logging);

        if (!DeviceHandlerFactory) {
            DeviceHandlerFactory = CreateDefaultDeviceHandlerFactory();
        }

        VhostServer = NVhost::CreateServer(
            Logging,
            ServerStats,
            NVhost::CreateVhostQueueFactory(),
            DeviceHandlerFactory,
            CreateVhostServerConfig(*BootstrapConfig->ServerConfig),
            VhostCallbacks);

        STORAGE_INFO("VHOST Server initialized");

        auto vhostEndpointListener = CreateVhostEndpointListener(
            VhostServer,
            checksumFlags,
            CreateVhostEndpointThreadCounts(*BootstrapConfig->ServerConfig),
            BootstrapConfig->ServerConfig->GetVhostDiscardEnabled() ||
                BootstrapConfig->ServerConfig->GetVhostDiscardOnlyEnabled(),
            BootstrapConfig->ServerConfig->GetVhostDiscardEnabled() ||
                BootstrapConfig->ServerConfig->GetVhostWriteZeroesEnabled(),
            BootstrapConfig->ServerConfig->GetDropDiscardRequests(),
            BootstrapConfig->ServerConfig->GetMaxZeroBlocksSubRequestSize(),
            BootstrapConfig->ServerConfig->GetVhostOptimalIoSize());

        if (BootstrapConfig->ServerConfig->GetVhostServerPath()
                && !Configs->Options->TemporaryServer)
        {
            vhostEndpointListener = CreateExternalVhostEndpointListener(
                BootstrapConfig->ServerConfig,
                Logging,
                ServerStats,
                Executor,
                Configs->Options->SkipDeviceLocalityValidation
                    ? TString {}
                    : FQDNHostName(),
                RdmaClient && RdmaClient->IsAlignedDataEnabled(),
                std::move(vhostEndpointListener));

            STORAGE_INFO("VHOST External Vhost EndpointListener initialized");
        }

        endpointListeners.emplace(
            NProto::IPC_VHOST,
            std::move(vhostEndpointListener));

        STORAGE_INFO("VHOST EndpointListener initialized");
    }

    if (BootstrapConfig->ServerConfig->GetNVMeEndpointEnabled()) {
        Y_ABORT_UNLESS(Spdk);

        auto listener = CreateNVMeEndpointListener(
            Spdk,
            Logging,
            ServerStats,
            Executor,
            CreateNVMeEndpointConfig(*BootstrapConfig->ServerConfig));

        endpointListeners.emplace(
            NProto::IPC_NVME,
            std::move(listener));

        STORAGE_INFO("NVMe EndpointListener initialized");
    }

    if (BootstrapConfig->ServerConfig->GetSCSIEndpointEnabled()) {
        Y_ABORT_UNLESS(Spdk);

        auto listener = CreateSCSIEndpointListener(
            Spdk,
            Logging,
            ServerStats,
            Executor,
            CreateSCSIEndpointConfig(*BootstrapConfig->ServerConfig));

        endpointListeners.emplace(
            NProto::IPC_SCSI,
            std::move(listener));

        STORAGE_INFO("SCSI EndpointListener initialized");
    }

    if (BootstrapConfig->ServerConfig->GetRdmaEndpointEnabled()) {
        InitRdmaServer();

        STORAGE_INFO("RDMA Server initialized");

        RdmaThreadPool = CreateThreadPool("RDMA", 1);
        auto listener = CreateRdmaEndpointListener(
            RdmaServer,
            Logging,
            ServerStats,
            Executor,
            RdmaThreadPool,
            CreateRdmaEndpointConfig(*BootstrapConfig->ServerConfig));

        endpointListeners.emplace(
            NProto::IPC_RDMA,
            std::move(listener));

        STORAGE_INFO("RDMA EndpointListener initialized");
    }

    IEndpointStoragePtr endpointStorage;
    switch (BootstrapConfig->ServerConfig->GetEndpointStorageType()) {
        case NCloud::NProto::ENDPOINT_STORAGE_DEFAULT:
        case NCloud::NProto::ENDPOINT_STORAGE_KEYRING: {
            const bool notImplementedErrorIsFatal =
                BootstrapConfig->ServerConfig
                    ->GetEndpointStorageNotImplementedErrorIsFatal();

            endpointStorage = CreateKeyringEndpointStorage(
                BootstrapConfig->ServerConfig->GetRootKeyringName(),
                BootstrapConfig->ServerConfig->GetEndpointsKeyringName(),
                notImplementedErrorIsFatal);
            break;
        }
        case NCloud::NProto::ENDPOINT_STORAGE_FILE:
            endpointStorage = CreateFileEndpointStorage(
                BootstrapConfig->ServerConfig->GetEndpointStorageDir());
            break;
        default:
            Y_ABORT(
                "unsupported endpoint storage type %d",
                BootstrapConfig->ServerConfig->GetEndpointStorageType());
    }
    STORAGE_INFO("EndpointStorage initialized");

    TEndpointManagerOptions endpointManagerOptions = {
        .ClientConfig = BootstrapConfig->EndpointConfig->GetClientConfig(),
        .NbdSocketSuffix =
            BootstrapConfig->ServerConfig->GetNbdSocketSuffix(),
        .NbdDevicePrefix =
            BootstrapConfig->ServerConfig->GetNbdDevicePrefix(),
        .AutomaticNbdDeviceManagement =
            BootstrapConfig->ServerConfig
                ->GetAutomaticNbdDeviceManagement(),
    };

    NBD::IDeviceFactoryPtr nbdDeviceFactory;

    if (BootstrapConfig->ServerConfig->GetNbdNetlink()) {
        nbdDeviceFactory = NBD::CreateNetlinkDeviceFactory(
            Logging,
            BootstrapConfig->ServerConfig->GetNbdRequestTimeout(),
            BootstrapConfig->ServerConfig->GetNbdConnectionTimeout());
    }

    if (!nbdDeviceFactory) {
        nbdDeviceFactory = NBD::CreateDeviceFactory(
            Logging,
            BootstrapConfig->ServerConfig
                ->GetNbdConnectionTimeout());  // timeout
    }

    EndpointManager = CreateEndpointManager(
        Timer,
        Scheduler,
        Logging,
        RequestStats,
        VolumeStats,
        ServerStats,
        Executor,
        EndpointEventHandler,
        std::move(sessionManager),
        std::move(endpointStorage),
        std::move(endpointListeners),
        std::move(nbdDeviceFactory),
        NbdErrorHandlerMap,
        Service,
        std::move(endpointManagerOptions));

    STORAGE_INFO("EndpointManager initialized");

    Service = CreateMultipleEndpointService(
        std::move(Service),
        Timer,
        Scheduler,
        EndpointManager);

    STORAGE_INFO("MultipleEndpointService initialized");

    Service = CreateMultipleEncryptionService(
        std::move(Service),
        Logging,
        std::move(encryptionClientFactory));

    STORAGE_INFO("MultipleEncryptionService initialized");

    if (BootstrapConfig->ServerConfig->GetThrottlingEnabled()) {
        Service = CreateThrottlingService(
            std::move(Service),
            CreateThrottler(
                CreateServiceThrottlerLogger(RequestStats, Logging),
                CreateThrottlerMetricsStub(),
                CreateServiceThrottlerPolicy(
                    CreateThrottlingServicePolicyConfig(
                        *BootstrapConfig->ServerConfig)),
                CreateServiceThrottlerTracker(),
                Timer,
                Scheduler,
                VolumeStats));

        STORAGE_INFO("ThrottlingService initialized");
    }

    auto udsService = Service;
    if (!BootstrapConfig->ServerConfig->GetAllowAllRequestsViaUDS()) {
        udsService = CreateFilteredService(Service, {
            EBlockStoreRequest::Ping,
            EBlockStoreRequest::QueryAvailableStorage,
            EBlockStoreRequest::DescribeVolume,
            EBlockStoreRequest::KickEndpoint,
            EBlockStoreRequest::StopEndpoint,
            EBlockStoreRequest::RefreshEndpoint,
            EBlockStoreRequest::CreateVolumeFromDevice,
            EBlockStoreRequest::ResumeDevice
        });
    }

    IBlockStorePtr cellForwardTrusted;
    if (BootstrapConfig->CellsConfig->GetCellsEnabled()) {
        cellForwardTrusted = Service;
    }

    InitAuthService();

    if (BootstrapConfig->CellsConfig->GetCellsEnabled()) {
        Service = WrapServiceForInterCellForward(
            std::move(Service),
            std::move(cellForwardTrusted));
    }

    if (BootstrapConfig->ServerConfig->GetStrictContractValidation()) {
        Service = CreateValidationService(
            Logging,
            Monitoring,
            std::move(Service),
            CreateCrcDigestCalculator(),
            inactiveClientsTimeout);

        STORAGE_INFO("ValidationService initialized");
    }

    if (LocalNVMeService) {
        Service =
            CreateLocalNVMeServiceProxy(std::move(Service), LocalNVMeService);

        udsService = CreateLocalNVMeServiceProxy(
            std::move(udsService),
            LocalNVMeService);
    }

    Server = CreateServer(
        BootstrapConfig->ServerConfig,
        Logging,
        ServerStats,
        Service,
        std::move(udsService),
        TServerOptions {
            // Enables cell id checking in DescribeVolume requests
            // only if "cells" feature is on
            .CellId = BootstrapConfig->CellsConfig->GetCellsEnabled() ?
                BootstrapConfig->CellsConfig->GetCellId() :
                ""
        },
        CertificateProvider);

    STORAGE_INFO("Server initialized");

    GrpcEndpointListener->SetClientStorageFactory(
        Server->GetClientStorageFactory());

    TVector<IIncompleteRequestProviderPtr> requestProviders = {
        Server,
        EndpointManager
    };

    if (NbdServer) {
        requestProviders.push_back(NbdServer);
    }

    if (VhostServer) {
        requestProviders.push_back(VhostServer);
    }

    ServerStatsUpdater = CreateStatsUpdater(
        Timer,
        BackgroundScheduler,
        CreateIncompleteRequestProcessor(
            ServerStats,
            std::move(requestProviders)));

    STORAGE_INFO("ServerStatsUpdater initialized");
}

void TBootstrapBase::InitProfileLog()
{
    if (Configs->Options->ProfileFile) {
        ProfileLog = CreateProfileLog(
            {
                Configs->Options->ProfileFile,
                BootstrapConfig->DiagnosticsConfig
                    ->GetProfileLogTimeThreshold(),
            },
            Timer,
            BackgroundScheduler
        );
    } else {
        ProfileLog = CreateProfileLogStub();
    }
}

void TBootstrapBase::InitDbgConfigs()
{
    Configs->InitServerConfig();
    Configs->InitEndpointConfig();
    Configs->InitHostPerformanceProfile();
    Configs->InitDiskAgentConfig();
    // InitRdmaConfig should be called after InitDiskAgentConfig and
    // InitServerConfig to backport legacy RDMA config
    Configs->InitRdmaConfig();
    Configs->InitDiskRegistryProxyConfig();
    Configs->InitDiagnosticsConfig();
    Configs->InitDiscoveryConfig();
    Configs->InitSpdkEnvConfig();
    Configs->InitCellsConfig();

    // Select local inputs before tracing and transport initialization use them.
    SetBootstrapConfig({
        .ServerConfig = Configs->ServerConfig,
        .EndpointConfig = Configs->EndpointConfig,
        .DiagnosticsConfig = Configs->DiagnosticsConfig,
        .DiskAgentConfig = Configs->DiskAgentConfig,
        .RdmaConfig = Configs->RdmaConfig,
        .CellsConfig = Configs->CellsConfig,
        .SpdkEnvConfig = Configs->SpdkEnvConfig,
        .DiscoveryConfig = Configs->DiscoveryConfig,
        .UseNonreplicatedRdmaActor = Configs->GetUseNonreplicatedRdmaActor(),
        .InactiveClientsTimeout = Configs->GetInactiveClientsTimeout(),
    });

    TLogSettings logSettings;
    logSettings.FiltrationLevel =
        static_cast<ELogPriority>(Configs->GetLogDefaultLevel());

    Logging = CreateLoggingService("console", logSettings);

    InitLWTrace({});

    auto monPort = Configs->GetMonitoringPort();
    if (monPort) {
        auto monAddress = Configs->GetMonitoringAddress();
        auto threadsCount = Configs->GetMonitoringThreads();
        Monitoring = CreateMonitoringService(monPort, monAddress, threadsCount);
    } else {
        Monitoring = CreateMonitoringServiceStub();
    }
}

void TBootstrapBase::InitLocalService()
{
    InitDbgConfigs();
    InitRdmaClient();
    InitSpdk();
    InitProfileLog();

    DiscoveryService = CreateDiscoveryServiceStub(
        FQDNHostName(),
        BootstrapConfig->DiscoveryConfig->GetConductorInstancePort(),
        BootstrapConfig->DiscoveryConfig->GetConductorSecureInstancePort());

    const auto& config =
        BootstrapConfig->ServerConfig->GetLocalServiceConfig()
            ? *BootstrapConfig->ServerConfig->GetLocalServiceConfig()
            : NProto::TLocalServiceConfig();

    FileIOServiceProvider =
        CreateSingleFileIOServiceProvider(CreateAIOService());

    NvmeManager = CreateNvmeManager(
        Logging,
        BootstrapConfig->DiskAgentConfig->GetSecureEraseTimeout(),
        BootstrapConfig->DiskAgentConfig->GetNVMeAdminCmdTimeout());

    Service = CreateLocalService(
        config,
        DiscoveryService,
        CreateLocalStorageProvider(
            FileIOServiceProvider,
            NvmeManager,
            TLocalStorageProviderParams{
                .DirectIO = false,
                .UseSubmissionThread = false,
                .ValidatedBlocksRatio =
                    BootstrapConfig->DiskAgentConfig
                        ->GetValidatedBlocksRatio(),
                .DataIntegrityValidationPolicy =
                    BootstrapConfig->DiskAgentConfig
                        ->GetDataIntegrityValidationPolicyForDrBasedDisks()}));
}

void TBootstrapBase::InitNullService()
{
    InitDbgConfigs();
    InitRdmaClient();
    InitSpdk();
    InitProfileLog();

    const auto& config =
        BootstrapConfig->ServerConfig->GetNullServiceConfig()
            ? *BootstrapConfig->ServerConfig->GetNullServiceConfig()
            : NProto::TNullServiceConfig();

    Service = CreateNullService(config);
}

void TBootstrapBase::InitLWTrace(const TString& serviceNameForExporter)
{
    auto& probes = NLwTraceMonPage::ProbeRegistry();
    probes.AddProbesList(LWTRACE_GET_PROBES(BLOCKSTORE_SERVER_PROVIDER));
    probes.AddProbesList(LWTRACE_GET_PROBES(LWTRACE_INTERNAL_PROVIDER));

    if (BootstrapConfig->DiskAgentConfig->GetEnabled()) {
        probes.AddProbesList(LWTRACE_GET_PROBES(BLOCKSTORE_DISK_AGENT_PROVIDER));
    }

    auto diagnosticsConfig = BootstrapConfig->DiagnosticsConfig;
    auto& lwManager = NLwTraceMonPage::TraceManager(diagnosticsConfig->GetUnsafeLWTrace());

    const TVector<std::tuple<TString, TString>> desc = {
        {"RequestStarted",                  "BLOCKSTORE_SERVER_PROVIDER"},
        {"BackgroundTaskStarted_Partition", "BLOCKSTORE_STORAGE_PROVIDER"},
        {"RequestReceived_DiskAgent",       "BLOCKSTORE_STORAGE_PROVIDER"},
    };

    auto traceLog = CreateUnifiedAgentLoggingService(
        Logging,
        diagnosticsConfig->GetTracesUnifiedAgentEndpoint(),
        diagnosticsConfig->GetTracesSyslogIdentifier()
    );

    if (const auto samplingRate = diagnosticsConfig->GetSamplingRate()) {
        NLWTrace::TQuery query = ProbabilisticQuery(
            desc,
            samplingRate,
            diagnosticsConfig->GetLWTraceShuttleCount());
        lwManager.New(TraceLoggerId, query);

        ITraceReaderPtr reader;
        if (serviceNameForExporter) {
            reader = SetupTraceReaderWithOpentelemetryExport(
                TraceLoggerId,
                traceLog,
                "BLOCKSTORE_TRACE",
                "AllRequests",
                GetTraceServiceClient(),
                serviceNameForExporter,
                TLOG_INFO);
        } else {
            reader = SetupTraceReaderWithLog(
                TraceLoggerId,
                traceLog,
                "BLOCKSTORE_TRACE",
                "AllRequests");
        }

        TraceReaders.push_back(std::move(reader));
    }

    if (auto samplingRate = diagnosticsConfig->GetSlowRequestSamplingRate()) {
        NLWTrace::TQuery query = ProbabilisticQuery(
            desc,
            samplingRate,
            diagnosticsConfig->GetLWTraceShuttleCount());
        lwManager.New(SlowRequestsFilterId, query);

        ITraceReaderPtr reader;
        if (serviceNameForExporter) {
            reader = SetupTraceReaderForSlowRequestsWithOpentelemetryExport(
                SlowRequestsFilterId,
                traceLog,
                "BLOCKSTORE_TRACE",
                GetTraceServiceClient(),
                serviceNameForExporter,
                diagnosticsConfig->GetRequestThresholds(),
                "SlowRequests");
        } else {
            reader = SetupTraceReaderForSlowRequests(
                SlowRequestsFilterId,
                traceLog,
                "BLOCKSTORE_TRACE",
                diagnosticsConfig->GetRequestThresholds(),
                "SlowRequests");
        }

        TraceReaders.push_back(std::move(reader));
    }

    lwManager.RegisterCustomAction(
        "ServiceErrorAction", &CreateServiceErrorActionExecutor);

    if (diagnosticsConfig->GetLWTraceDebugInitializationQuery()) {
        NLWTrace::TQuery query;
        ParseProtoTextFromFile(
            diagnosticsConfig->GetLWTraceDebugInitializationQuery(),
            query);

        lwManager.New("diagnostics", query);
    }
}

void TBootstrapBase::Start()
{
#define START_COMMON_COMPONENT(c)                                              \
    if (c) {                                                                   \
        STORAGE_INFO("Starting " << #c << " ...");                             \
        c->Start();                                                            \
        STORAGE_INFO("Started " << #c);                                        \
    }                                                                          \
// START_COMMON_COMPONENT

#define START_KIKIMR_COMPONENT(c)                                              \
    if (Get##c()) {                                                            \
        STORAGE_INFO("Starting " << #c << " ...");                             \
        Get##c()->Start();                                                     \
        STORAGE_INFO("Started " << #c);                                        \
    }                                                                          \
// START_KIKIMR_COMPONENT

    START_KIKIMR_COMPONENT(AsyncLogger);
    START_COMMON_COMPONENT(Logging);
    START_COMMON_COMPONENT(NvmeManager);
    START_KIKIMR_COMPONENT(LogbrokerService);
    START_KIKIMR_COMPONENT(NotifyService);
    START_COMMON_COMPONENT(Monitoring);
    START_COMMON_COMPONENT(ProfileLog);
    START_COMMON_COMPONENT(DiscoveryService);
    START_COMMON_COMPONENT(TraceProcessor);
    START_KIKIMR_COMPONENT(TraceSerializer);
    START_KIKIMR_COMPONENT(ClientPercentiles);
    START_KIKIMR_COMPONENT(StatsAggregator);
    START_KIKIMR_COMPONENT(IamTokenClient);
    START_KIKIMR_COMPONENT(ComputeClient);
    START_KIKIMR_COMPONENT(KmsClient);
    START_KIKIMR_COMPONENT(RootKmsClient);
    START_KIKIMR_COMPONENT(YdbStorage);
    START_KIKIMR_COMPONENT(StatsUploader);
    START_COMMON_COMPONENT(Spdk);
    START_COMMON_COMPONENT(FileIOServiceProvider);
    START_KIKIMR_COMPONENT(ActorSystem);
    START_COMMON_COMPONENT(EndpointManager);
    START_COMMON_COMPONENT(Service);
    START_COMMON_COMPONENT(VhostServer);
    START_COMMON_COMPONENT(NbdServer);
    START_COMMON_COMPONENT(CertificateProvider);
    START_COMMON_COMPONENT(GrpcEndpointListener);
    START_COMMON_COMPONENT(Executor);
    START_COMMON_COMPONENT(Server);
    START_COMMON_COMPONENT(CriticalEventsStatsUpdater);
    START_COMMON_COMPONENT(ServerStatsUpdater);
    START_COMMON_COMPONENT(BackgroundThreadPool);
    START_COMMON_COMPONENT(RdmaClient);
    START_COMMON_COMPONENT(GetTraceServiceClient());
    START_COMMON_COMPONENT(RdmaRequestServer);
    START_COMMON_COMPONENT(RdmaTarget);
    START_COMMON_COMPONENT(CellManager);
    START_COMMON_COMPONENT(LongRunningTaskExecutor);
    START_COMMON_COMPONENT(LocalNVMeDeviceProvider);
    START_COMMON_COMPONENT(LocalNVMeService);

    // we need to start scheduler after all other components for 2 reasons:
    // 1) any component can schedule a task that uses a dependency that hasn't
    // started yet
    // 2) we have loops in our dependencies, so there is no 'correct' starting
    // order
    START_COMMON_COMPONENT(Scheduler);

    // register the cells mon page only now: its disk search needs the cell
    // manager's gRPC client (started above) to have executors to run on
    SetupCellMonitoringActor();

    if (!Configs->Options->TemporaryServer) {
        WarmupBSGroupConnections();
    }

    auto restoreFuture = EndpointManager->RestoreEndpoints();
    if (!Configs->Options->TemporaryServer) {
        auto balancerSwitch = VolumeBalancerSwitch;
        restoreFuture.Subscribe([=] (const auto& future) {
            Y_UNUSED(future);
            balancerSwitch->EnableVolumeBalancer();
        });
    }
    STORAGE_INFO("Started endpoints restoring");

    if (Configs->Options->MemLock) {
        LockProcessMemory(Log);
        STORAGE_INFO("Process memory locked");
    }

#undef START_COMMON_COMPONENT
#undef START_KIKIMR_COMPONENT
}

void TBootstrapBase::Stop()
{
#define STOP_COMMON_COMPONENT(c)                                               \
    if (c) {                                                                   \
        STORAGE_INFO("Stopping " << #c << "...");                              \
        c->Stop();                                                             \
        STORAGE_INFO("Stopped " << #c);                                        \
    }                                                                          \
// STOP_COMMON_COMPONENT

#define STOP_KIKIMR_COMPONENT(c)                                               \
    if (Get##c()) {                                                            \
        STORAGE_INFO("Stopping " << #c << "...");                              \
        Get##c()->Stop();                                                      \
        STORAGE_INFO("Stopped " << #c);                                        \
    }                                                                          \
// STOP_KIKIMR_COMPONENT

    // stopping scheduler before all other components to avoid races between
    // scheduled tasks and shutting down of component dependencies
    STOP_COMMON_COMPONENT(Scheduler);
    STOP_COMMON_COMPONENT(LocalNVMeService);
    STOP_COMMON_COMPONENT(LocalNVMeDeviceProvider);
    STOP_COMMON_COMPONENT(LongRunningTaskExecutor);
    STOP_COMMON_COMPONENT(CellManager);
    STOP_COMMON_COMPONENT(RdmaTarget);
    STOP_COMMON_COMPONENT(RdmaRequestServer);
    STOP_COMMON_COMPONENT(GetTraceServiceClient());
    STOP_COMMON_COMPONENT(RdmaClient);
    STOP_COMMON_COMPONENT(BackgroundThreadPool);
    STOP_COMMON_COMPONENT(ServerStatsUpdater);
    STOP_COMMON_COMPONENT(CriticalEventsStatsUpdater);
    STOP_COMMON_COMPONENT(Server);
    STOP_COMMON_COMPONENT(CertificateProvider);
    STOP_COMMON_COMPONENT(Executor);
    STOP_COMMON_COMPONENT(GrpcEndpointListener);
    STOP_COMMON_COMPONENT(NbdServer);
    STOP_COMMON_COMPONENT(VhostServer);
    STOP_COMMON_COMPONENT(Service);
    STOP_COMMON_COMPONENT(EndpointManager);

    STOP_KIKIMR_COMPONENT(ActorSystem);

    // stop FileIOServiceProvider after ActorSystem to ensure that there are no
    // in-flight I/O requests from TDiskAgentActor
    STOP_COMMON_COMPONENT(FileIOServiceProvider);

    STOP_COMMON_COMPONENT(Spdk);
    STOP_KIKIMR_COMPONENT(StatsUploader);
    STOP_KIKIMR_COMPONENT(YdbStorage);
    STOP_KIKIMR_COMPONENT(RootKmsClient);
    STOP_KIKIMR_COMPONENT(KmsClient);
    STOP_KIKIMR_COMPONENT(ComputeClient);
    STOP_KIKIMR_COMPONENT(IamTokenClient);
    STOP_KIKIMR_COMPONENT(StatsAggregator);
    STOP_KIKIMR_COMPONENT(ClientPercentiles);
    STOP_KIKIMR_COMPONENT(TraceSerializer);
    STOP_COMMON_COMPONENT(TraceProcessor);
    STOP_COMMON_COMPONENT(DiscoveryService);
    STOP_COMMON_COMPONENT(ProfileLog);
    STOP_COMMON_COMPONENT(Monitoring);
    STOP_KIKIMR_COMPONENT(LogbrokerService);
    STOP_COMMON_COMPONENT(NvmeManager);
    STOP_COMMON_COMPONENT(Logging);
    STOP_KIKIMR_COMPONENT(AsyncLogger);

#undef STOP_COMMON_COMPONENT
#undef STOP_KIKIMR_COMPONENT
}

IBlockStorePtr TBootstrapBase::GetBlockStoreService()
{
    return Service;
}

}   // namespace NCloud::NBlockStore::NServer
