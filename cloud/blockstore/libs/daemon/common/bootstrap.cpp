#include "bootstrap.h"

#include "config_initializer.h"
#include "options.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/common/caching_allocator.h>
#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
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
#include <cloud/blockstore/libs/endpoint_proxy/client/client.h>
#include <cloud/blockstore/libs/endpoint_proxy/client/device_factory.h>
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
#include <cloud/blockstore/libs/nbd/device.h>
#include <cloud/blockstore/libs/nbd/netlink_device.h>
#include <cloud/blockstore/libs/nbd/server.h>
#include <cloud/blockstore/libs/nbd/error_handler.h>
#include <cloud/blockstore/libs/nvme/nvme.h>
#include <cloud/blockstore/libs/rdma/iface/client.h>
#include <cloud/blockstore/libs/rdma/iface/server.h>
#include <cloud/blockstore/libs/server/config.h>
#include <cloud/blockstore/libs/server/server.h>
#include <cloud/blockstore/libs/service/device_handler.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service/service_error_transform.h>
#include <cloud/blockstore/libs/service/service_filtered.h>
#include <cloud/blockstore/libs/service/service_null.h>
#include <cloud/blockstore/libs/service/storage_provider.h>
#include <cloud/blockstore/libs/service_local/file_io_service_provider.h>
#include <cloud/blockstore/libs/service_local/service_local.h>
#include <cloud/blockstore/libs/service_local/storage_local.h>
#include <cloud/blockstore/libs/service_local/storage_null.h>
#include <cloud/blockstore/libs/service_local/storage_rdma.h>
#include <cloud/blockstore/libs/service_local/storage_spdk.h>
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
#include <cloud/storage/core/libs/diagnostics/stats_fetcher.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/diagnostics/stats_updater.h>
#include <cloud/storage/core/libs/diagnostics/trace_processor_mon.h>
#include <cloud/storage/core/libs/diagnostics/trace_processor.h>
#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>
#include <cloud/storage/core/libs/grpc/init.h>
#include <cloud/storage/core/libs/grpc/threadpool.h>
#include <cloud/storage/core/libs/endpoints/fs/fs_endpoints.h>
#include <cloud/storage/core/libs/endpoints/keyring/keyring_endpoints.h>
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

NBD::TServerConfig CreateNbdServerConfig(const TServerAppConfig& config)
{
    return NBD::TServerConfig {
        .ThreadsCount = config.GetNbdThreadsCount(),
        .LimiterEnabled = config.GetNbdLimiterEnabled(),
        .MaxInFlightBytesPerThread = config.GetMaxInFlightBytesPerThread(),
        .SocketAccessMode = config.GetSocketAccessMode(),
        .Affinity = config.GetNbdAffinity()
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

void TBootstrapBase::Init()
{
    InitLogs();
    STORAGE_INFO("NBS server version: " << GetFullVersionString());

    Timer = CreateWallClockTimer();
    Scheduler = CreateScheduler();
    BackgroundThreadPool = CreateThreadPool("Background", 1);
    BackgroundScheduler = CreateBackgroundScheduler(
        Scheduler,
        BackgroundThreadPool);

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

    GrpcLog = Logging->CreateLog("GRPC");
    GrpcLoggerInit(
        GrpcLog,
        Configs->Options->EnableGrpcTracing);

    auto diagnosticsConfig = Configs->DiagnosticsConfig;
    if (TraceReaders.size()) {
        TraceProcessor = CreateTraceProcessorMon(
            Monitoring,
            CreateTraceProcessor(
                Timer,
                BackgroundScheduler,
                Logging,
                "BLOCKSTORE_TRACE",
                NLwTraceMonPage::TraceManager(diagnosticsConfig->GetUnsafeLWTrace()),
                TraceReaders));

        STORAGE_INFO("TraceProcessor initialized");
    }

    auto clientInactivityTimeout = Configs->GetInactiveClientsTimeout();

    auto rootGroup = Monitoring->GetCounters()
        ->GetSubgroup("counters", "blockstore");

    auto serverGroup = rootGroup->GetSubgroup("component", "server");
    auto revisionGroup = serverGroup->GetSubgroup("revision", GetFullVersionString());

    auto versionCounter = revisionGroup->GetCounter(
        "version",
        false);
    *versionCounter = 1;

    InitCriticalEventsCounter(serverGroup);

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
        Configs->DiagnosticsConfig->GetHistogramCounterOptions());

    if (!VolumeStats) {
        VolumeStats = CreateVolumeStats(
            Monitoring,
            Configs->DiagnosticsConfig,
            clientInactivityTimeout,
            EVolumeStatsType::EServerStats,
            Timer);
    }

    ServerStats = CreateServerStats(
        Configs->ServerConfig,
        Configs->DiagnosticsConfig,
        Monitoring,
        ProfileLog,
        RequestStats,
        VolumeStats);

    STORAGE_INFO("Stats initialized");

    TVector<IStorageProviderPtr> storageProviders;

    if (Configs->ServerConfig->GetNvmfInitiatorEnabled()) {
        Y_ABORT_UNLESS(Spdk);

        const auto& config = *Configs->DiskAgentConfig;

        storageProviders.push_back(CreateSpdkStorageProvider(
            Spdk,
            CreateSyncCachingAllocator(
                Spdk->GetAllocator(),
                config.GetPageSize(),
                config.GetMaxPageCount(),
                config.GetPageDropSize()),
            ServerStats));
    }

    if (!Configs->GetUseNonreplicatedRdmaActor() && RdmaClient) {
        storageProviders.push_back(CreateRdmaStorageProvider(
            ServerStats,
            RdmaClient,
            ERdmaTaskQueueOpt::Use));
    }

    storageProviders.push_back(CreateDefaultStorageProvider(Service));
    StorageProvider = CreateMultiStorageProvider(std::move(storageProviders));

    STORAGE_INFO("StorageProvider initialized");

    TSessionManagerOptions sessionManagerOptions;
    sessionManagerOptions.StrictContractValidation
        = Configs->ServerConfig->GetStrictContractValidation();
    sessionManagerOptions.DefaultClientConfig
        = Configs->EndpointConfig->GetClientConfig();
    sessionManagerOptions.HostProfile = Configs->HostPerformanceProfile;
    sessionManagerOptions.TemporaryServer = Configs->Options->TemporaryServer;
    sessionManagerOptions.DisableClientThrottler =
        Configs->ServerConfig->GetDisableClientThrottlers();

    if (!KmsKeyProvider) {
        KmsKeyProvider = CreateKmsKeyProviderStub();
    }

    if (!RootKmsKeyProvider) {
        RootKmsKeyProvider = CreateRootKmsKeyProviderStub();
    }

    auto encryptionClientFactory = CreateEncryptionClientFactory(
        Logging,
        CreateEncryptionKeyProvider(KmsKeyProvider, RootKmsKeyProvider));

    auto sessionManager = CreateSessionManager(
        Timer,
        Scheduler,
        Logging,
        Monitoring,
        RequestStats,
        VolumeStats,
        ServerStats,
        Service,
        StorageProvider,
        encryptionClientFactory,
        Executor,
        sessionManagerOptions);

    STORAGE_INFO("SessionManager initialized");

    THashMap<NProto::EClientIpcType, IEndpointListenerPtr> endpointListeners;

    GrpcEndpointListener = CreateSocketEndpointListener(
        Logging,
        Configs->ServerConfig->GetUnixSocketBacklog(),
        Configs->ServerConfig->GetSocketAccessMode());
    endpointListeners.emplace(NProto::IPC_GRPC, GrpcEndpointListener);

    STORAGE_INFO("SocketEndpointListener initialized");

    NbdErrorHandlerMap = NBD::CreateErrorHandlerMapStub();

    if (Configs->ServerConfig->GetNbdEnabled()) {
        NbdServer = NBD::CreateServer(
            Logging,
            CreateNbdServerConfig(*Configs->ServerConfig));

        STORAGE_INFO("NBD Server initialized");

        if (Configs->ServerConfig->GetNbdNetlink()) {
            NbdErrorHandlerMap = NBD::CreateErrorHandlerMap();
        }

        auto nbdEndpointListener = CreateNbdEndpointListener(
            NbdServer,
            Logging,
            ServerStats,
            Configs->ServerConfig->GetChecksumFlags(),
            Configs->ServerConfig->GetMaxZeroBlocksSubRequestSize(),
            NbdErrorHandlerMap);

        endpointListeners.emplace(
            NProto::IPC_NBD,
            std::move(nbdEndpointListener));

        STORAGE_INFO("NBD EndpointListener initialized");
    }

    if (Configs->ServerConfig->GetVhostEnabled()) {
        NVhost::InitVhostLog(Logging);

        if (!DeviceHandlerFactory) {
            DeviceHandlerFactory = CreateDefaultDeviceHandlerFactory();
        }

        VhostServer = NVhost::CreateServer(
            Logging,
            ServerStats,
            NVhost::CreateVhostQueueFactory(),
            DeviceHandlerFactory,
            CreateVhostServerConfig(*Configs->ServerConfig),
            VhostCallbacks);

        STORAGE_INFO("VHOST Server initialized");

        auto vhostEndpointListener = CreateVhostEndpointListener(
            VhostServer,
            Configs->ServerConfig->GetChecksumFlags(),
            Configs->ServerConfig->GetVhostDiscardEnabled(),
            Configs->ServerConfig->GetMaxZeroBlocksSubRequestSize());

        if (Configs->ServerConfig->GetVhostServerPath()
                && !Configs->Options->TemporaryServer)
        {
            vhostEndpointListener = CreateExternalVhostEndpointListener(
                Configs->ServerConfig,
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

    if (Configs->ServerConfig->GetNVMeEndpointEnabled()) {
        Y_ABORT_UNLESS(Spdk);

        auto listener = CreateNVMeEndpointListener(
            Spdk,
            Logging,
            ServerStats,
            Executor,
            CreateNVMeEndpointConfig(*Configs->ServerConfig));

        endpointListeners.emplace(
            NProto::IPC_NVME,
            std::move(listener));

        STORAGE_INFO("NVMe EndpointListener initialized");
    }

    if (Configs->ServerConfig->GetSCSIEndpointEnabled()) {
        Y_ABORT_UNLESS(Spdk);

        auto listener = CreateSCSIEndpointListener(
            Spdk,
            Logging,
            ServerStats,
            Executor,
            CreateSCSIEndpointConfig(*Configs->ServerConfig));

        endpointListeners.emplace(
            NProto::IPC_SCSI,
            std::move(listener));

        STORAGE_INFO("SCSI EndpointListener initialized");
    }

    if (Configs->ServerConfig->GetRdmaEndpointEnabled()) {
        InitRdmaServer();

        STORAGE_INFO("RDMA Server initialized");

        RdmaThreadPool = CreateThreadPool("RDMA", 1);
        auto listener = CreateRdmaEndpointListener(
            RdmaServer,
            Logging,
            ServerStats,
            Executor,
            RdmaThreadPool,
            CreateRdmaEndpointConfig(*Configs->ServerConfig));

        endpointListeners.emplace(
            NProto::IPC_RDMA,
            std::move(listener));

        STORAGE_INFO("RDMA EndpointListener initialized");
    }

    IEndpointStoragePtr endpointStorage;
    switch (Configs->ServerConfig->GetEndpointStorageType()) {
        case NCloud::NProto::ENDPOINT_STORAGE_DEFAULT:
        case NCloud::NProto::ENDPOINT_STORAGE_KEYRING: {
            const bool notImplementedErrorIsFatal = Configs->ServerConfig
                ->GetEndpointStorageNotImplementedErrorIsFatal();

            endpointStorage = CreateKeyringEndpointStorage(
                Configs->ServerConfig->GetRootKeyringName(),
                Configs->ServerConfig->GetEndpointsKeyringName(),
                notImplementedErrorIsFatal);
            break;
        }
        case NCloud::NProto::ENDPOINT_STORAGE_FILE:
            endpointStorage = CreateFileEndpointStorage(
                Configs->ServerConfig->GetEndpointStorageDir());
            break;
        default:
            Y_ABORT(
                "unsupported endpoint storage type %d",
                Configs->ServerConfig->GetEndpointStorageType());
    }
    STORAGE_INFO("EndpointStorage initialized");

    TEndpointManagerOptions endpointManagerOptions = {
        .ClientConfig = Configs->EndpointConfig->GetClientConfig(),
        .NbdSocketSuffix = Configs->ServerConfig->GetNbdSocketSuffix(),
        .NbdDevicePrefix = Configs->ServerConfig->GetNbdDevicePrefix(),
        .AutomaticNbdDeviceManagement =
            Configs->ServerConfig->GetAutomaticNbdDeviceManagement(),
    };

    NBD::IDeviceFactoryPtr nbdDeviceFactory;

    if (Configs->ServerConfig->GetEndpointProxySocketPath()) {
        EndpointProxyClient = NClient::CreateClient(
            {
                "", // Host
                0,  // Port
                0,  // SecurePort
                "", // RootCertsFile
                Configs->ServerConfig->GetEndpointProxySocketPath(),
                {}, // RetryPolicy
            },
            Scheduler,
            Timer,
            Logging);

        const ui32 defaultSectorSize = 4_KB;

        nbdDeviceFactory = NClient::CreateProxyDeviceFactory(
            {defaultSectorSize,
             Configs->ServerConfig->GetMaxZeroBlocksSubRequestSize()},
            EndpointProxyClient);
    }

    if (!nbdDeviceFactory && Configs->ServerConfig->GetNbdNetlink()) {
        nbdDeviceFactory = NBD::CreateNetlinkDeviceFactory(
            Logging,
            Configs->ServerConfig->GetNbdRequestTimeout(),
            Configs->ServerConfig->GetNbdConnectionTimeout());
    }

    // The only case we want kernel to retry requests is when the socket is dead
    // due to nbd server restart. And since we can't configure ioctl device to
    // use a new socket, request timeout effectively becomes connection timeout
    if (!nbdDeviceFactory) {
        nbdDeviceFactory = NBD::CreateDeviceFactory(
            Logging,
            Configs->ServerConfig->GetNbdConnectionTimeout());  // timeout
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

    if (Configs->ServerConfig->GetThrottlingEnabled()) {
        Service = CreateThrottlingService(
            std::move(Service),
            CreateThrottler(
                CreateServiceThrottlerLogger(RequestStats, Logging),
                CreateThrottlerMetricsStub(),
                CreateServiceThrottlerPolicy(
                    CreateThrottlingServicePolicyConfig(
                        *Configs->ServerConfig)),
                CreateServiceThrottlerTracker(),
                Timer,
                Scheduler,
                VolumeStats));

        STORAGE_INFO("ThrottlingService initialized");
    }

    auto udsService = Service;
    if (!Configs->ServerConfig->GetAllowAllRequestsViaUDS()) {
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

    InitAuthService();

    if (Configs->ServerConfig->GetStrictContractValidation()) {
        Service = CreateValidationService(
            Logging,
            Monitoring,
            std::move(Service),
            CreateCrcDigestCalculator(),
            clientInactivityTimeout);

        STORAGE_INFO("ValidationService initialized");
    }

    Server = CreateServer(
        Configs->ServerConfig,
        Logging,
        ServerStats,
        Service,
        std::move(udsService));

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
                Configs->DiagnosticsConfig->GetProfileLogTimeThreshold(),
            },
            Timer,
            BackgroundScheduler
        );
    } else {
        ProfileLog = CreateProfileLogStub();
    }
}

void TBootstrapBase::InitLogs()
{
    TLogSettings logSettings;
    logSettings.BackendFileName = Configs->GetLogBackendFileName();

    BootstrapLogging = CreateLoggingService("console", logSettings);
    Log = BootstrapLogging->CreateLog("BLOCKSTORE_SERVER");
    SetCriticalEventsLog(Log);
    Configs->Log = Log;
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

    TLogSettings logSettings;
    logSettings.FiltrationLevel =
        static_cast<ELogPriority>(Configs->GetLogDefaultLevel());
    logSettings.BackendFileName = Configs->GetLogBackendFileName();

    Logging = CreateLoggingService("console", logSettings);

    InitLWTrace();

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
        Configs->DiscoveryConfig->GetConductorInstancePort(),
        Configs->DiscoveryConfig->GetConductorSecureInstancePort()
    );

    const auto& config = Configs->ServerConfig->GetLocalServiceConfig()
        ? *Configs->ServerConfig->GetLocalServiceConfig()
        : NProto::TLocalServiceConfig();

    FileIOServiceProvider =
        CreateSingleFileIOServiceProvider(CreateAIOService());

    NvmeManager = CreateNvmeManager(
        Configs->DiskAgentConfig->GetSecureEraseTimeout());

    Service = CreateLocalService(
        config,
        DiscoveryService,
        CreateLocalStorageProvider(
            FileIOServiceProvider,
            NvmeManager,
            {.DirectIO = false, .UseSubmissionThread = false}));
}

void TBootstrapBase::InitNullService()
{
    InitDbgConfigs();
    InitRdmaClient();
    InitSpdk();
    InitProfileLog();

    const auto& config = Configs->ServerConfig->GetNullServiceConfig()
        ? *Configs->ServerConfig->GetNullServiceConfig()
        : NProto::TNullServiceConfig();

    Service = CreateNullService(config);
}

void TBootstrapBase::InitLWTrace()
{
    auto& probes = NLwTraceMonPage::ProbeRegistry();
    probes.AddProbesList(LWTRACE_GET_PROBES(BLOCKSTORE_SERVER_PROVIDER));
    probes.AddProbesList(LWTRACE_GET_PROBES(LWTRACE_INTERNAL_PROVIDER));

    if (Configs->DiskAgentConfig->GetEnabled()) {
        probes.AddProbesList(LWTRACE_GET_PROBES(BLOCKSTORE_DISK_AGENT_PROVIDER));
    }

    auto diagnosticsConfig = Configs->DiagnosticsConfig;
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

    if (auto samplingRate = diagnosticsConfig->GetSamplingRate()) {
        NLWTrace::TQuery query = ProbabilisticQuery(
            desc,
            samplingRate,
            diagnosticsConfig->GetLWTraceShuttleCount());
        lwManager.New(TraceLoggerId, query);
        TraceReaders.push_back(CreateTraceLogger(
            TraceLoggerId,
            traceLog,
            "BLOCKSTORE_TRACE"
        ));
    }

    if (auto samplingRate = diagnosticsConfig->GetSlowRequestSamplingRate()) {
        NLWTrace::TQuery query = ProbabilisticQuery(
            desc,
            samplingRate,
            diagnosticsConfig->GetLWTraceShuttleCount());
        lwManager.New(SlowRequestsFilterId, query);
        TraceReaders.push_back(CreateSlowRequestsFilter(
            SlowRequestsFilterId,
            traceLog,
            "BLOCKSTORE_TRACE",
            diagnosticsConfig->GetRequestThresholds()));
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
    START_KIKIMR_COMPONENT(LogbrokerService);
    START_KIKIMR_COMPONENT(NotifyService);
    START_COMMON_COMPONENT(Monitoring);
    START_COMMON_COMPONENT(ProfileLog);
    START_KIKIMR_COMPONENT(StatsFetcher);
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
    START_COMMON_COMPONENT(EndpointProxyClient);
    START_COMMON_COMPONENT(EndpointManager);
    START_COMMON_COMPONENT(Service);
    START_COMMON_COMPONENT(VhostServer);
    START_COMMON_COMPONENT(NbdServer);
    START_COMMON_COMPONENT(GrpcEndpointListener);
    START_COMMON_COMPONENT(Executor);
    START_COMMON_COMPONENT(Server);
    START_COMMON_COMPONENT(ServerStatsUpdater);
    START_COMMON_COMPONENT(BackgroundThreadPool);
    START_COMMON_COMPONENT(RdmaClient);

    // we need to start scheduler after all other components for 2 reasons:
    // 1) any component can schedule a task that uses a dependency that hasn't
    // started yet
    // 2) we have loops in our dependencies, so there is no 'correct' starting
    // order
    START_COMMON_COMPONENT(Scheduler);

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

    STOP_COMMON_COMPONENT(RdmaClient);
    STOP_COMMON_COMPONENT(BackgroundThreadPool);
    STOP_COMMON_COMPONENT(ServerStatsUpdater);
    STOP_COMMON_COMPONENT(Server);
    STOP_COMMON_COMPONENT(Executor);
    STOP_COMMON_COMPONENT(GrpcEndpointListener);
    STOP_COMMON_COMPONENT(NbdServer);
    STOP_COMMON_COMPONENT(VhostServer);
    STOP_COMMON_COMPONENT(Service);
    STOP_COMMON_COMPONENT(EndpointManager);
    STOP_COMMON_COMPONENT(EndpointProxyClient);


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
    STOP_KIKIMR_COMPONENT(StatsFetcher);
    STOP_COMMON_COMPONENT(ProfileLog);
    STOP_COMMON_COMPONENT(Monitoring);
    STOP_KIKIMR_COMPONENT(LogbrokerService);
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
