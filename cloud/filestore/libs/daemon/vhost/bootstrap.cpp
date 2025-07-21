#include "bootstrap.h"

#include "config_initializer.h"
#include "options.h"

#include <cloud/filestore/libs/client/client.h>
#include <cloud/filestore/libs/client/config.h>
#include <cloud/filestore/libs/client/durable.h>
#include <cloud/filestore/libs/client/probes.h>
#include <cloud/filestore/libs/diagnostics/config.h>
#include <cloud/filestore/libs/diagnostics/profile_log.h>
#include <cloud/filestore/libs/diagnostics/request_stats.h>
#include <cloud/filestore/libs/endpoint/endpoint_manager.h>
#include <cloud/filestore/libs/endpoint/listener.h>
#include <cloud/filestore/libs/endpoint/service_auth.h>
#include <cloud/filestore/libs/endpoint_vhost/config.h>
#include <cloud/filestore/libs/endpoint_vhost/listener.h>
#include <cloud/filestore/libs/server/config.h>
#include <cloud/filestore/libs/server/probes.h>
#include <cloud/filestore/libs/server/server.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/endpoint.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/libs/service/service_auth.h>
#include <cloud/filestore/libs/service_kikimr/auth_provider_kikimr.h>
#include <cloud/filestore/libs/service_kikimr/service.h>
#include <cloud/filestore/libs/service_local/config.h>
#include <cloud/filestore/libs/service_local/service.h>
#include <cloud/filestore/libs/service_null/service.h>
#include <cloud/filestore/libs/storage/core/probes.h>
#include <cloud/filestore/libs/vfs/probes.h>
#include <cloud/filestore/libs/vhost/server.h>

#include <cloud/storage/core/libs/aio/service.h>
#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/daemon/mlock.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/diagnostics/stats_updater.h>
#include <cloud/storage/core/libs/diagnostics/trace_reader.h>
#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>
#include <cloud/storage/core/libs/endpoints/fs/fs_endpoints.h>
#include <cloud/storage/core/libs/endpoints/keyring/keyring_endpoints.h>
#include <cloud/storage/core/libs/io_uring/service.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>
#include <cloud/storage/core/libs/user_stats/counter/user_counter.h>

#include <contrib/ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>
#include <contrib/ydb/core/tablet_flat/probes.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/generic/map.h>
#include <util/generic/overloaded.h>
#include <util/stream/file.h>
#include <util/system/fs.h>
#include <util/system/sysstat.h>

namespace NCloud::NFileStore::NDaemon {

using namespace NCloud::NFileStore::NVhost;
using namespace NCloud::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TFileStoreEndpoints final
    : public IFileStoreEndpoints
{
private:
    using TEndpointsMap = TMap<TString, IFileStoreServicePtr>;

private:
    ITimerPtr Timer;
    ISchedulerPtr Scheduler;
    ILoggingServicePtr Logging;
    IFileStoreServicePtr LocalService;
    IActorSystemPtr ActorSystem;

    TEndpointsMap Endpoints;

public:
    TFileStoreEndpoints(
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            ILoggingServicePtr logging,
            IFileStoreServicePtr localService,
            IActorSystemPtr actorSystem)
        : Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Logging(std::move(logging))
        , LocalService(std::move(localService))
        , ActorSystem(std::move(actorSystem))
    {}

    void Start() override
    {
        for (const auto& [name, endpoint]: Endpoints) {
            endpoint->Start();
        }
    }

    void Stop() override
    {
        for (const auto& [name, endpoint]: Endpoints) {
            endpoint->Stop();
        }
    }

    IFileStoreServicePtr GetEndpoint(const TString& name) override
    {
        const auto* p = Endpoints.FindPtr(name);
        return p ? *p : nullptr;
    }

    bool AddEndpoint(
        const TString& name,
        const NProto::TClientConfig& config,
        NDaemon::EServiceKind kind)
    {
        auto clientConfig = std::make_shared<NClient::TClientConfig>(config);
        IFileStoreServicePtr fileStore;
        switch (kind) {
            case NDaemon::EServiceKind::Null: {
                fileStore = CreateNullFileStore();
                break;
            }

            case NDaemon::EServiceKind::Kikimr: {
                fileStore = CreateKikimrFileStore(ActorSystem);
                break;
            }

            default: {
                if (LocalService) {
                    fileStore = LocalService;
                } else {
                    fileStore = NClient::CreateFileStoreClient(
                        clientConfig,
                        Logging);
                }
                break;
            }
        }

        return AddEndpoint(
            name,
            std::move(clientConfig),
            std::move(fileStore));
    }

    bool Empty() const
    {
        return Endpoints.empty();
    }

private:
    bool AddEndpoint(
        const TString& name,
        std::shared_ptr<NClient::TClientConfig> clientConfig,
        IFileStoreServicePtr filestore)
    {
        if (Endpoints.contains(name)) {
            return false;
        }

        auto client = NClient::CreateDurableClient(
            Logging,
            Timer,
            Scheduler,
            NClient::CreateRetryPolicy(std::move(clientConfig)),
            std::move(filestore));

        Endpoints.emplace(name, std::move(client));
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

IFileIOServicePtr CreateFileIOService(const TLocalFileStoreConfig& config)
{
    return std::visit(
        TOverloaded{
            [&](const TAioConfig& aio)
            {
                return CreateThreadedAIOService(
                    config.GetNumThreads(),
                    {
                        .MaxEvents = aio.GetEntries(),
                    });
            },
            [&](const TIoUringConfig& ring)
            {
                IFileIOServiceFactoryPtr factory = CreateIoUringServiceFactory({
                    .SubmissionQueueEntries = ring.GetEntries(),
                    .MaxKernelWorkersCount = ring.GetMaxKernelWorkersCount(),
                    .ShareKernelWorkers = ring.GetShareKernelWorkers(),
                    .ForceAsyncIO = ring.GetForceAsyncIO(),
                });

                if (config.GetNumThreads() <= 1) {
                    return factory->CreateFileIOService();
                }

                return CreateRoundRobinFileIOService(
                    config.GetNumThreads(),
                    *factory);
            },
        },
        config.GetFileIOConfig());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TBootstrapVhost::TBootstrapVhost(
        std::shared_ptr<NKikimr::TModuleFactories> kikimrFactories,
        TVhostModuleFactoriesPtr vhostFactories)
    : TBootstrapCommon(
        std::move(kikimrFactories),
        "NFS_VHOST",
        "client",
        NUserStats::CreateUserCounterSupplier())
    , VhostModuleFactories(std::move(vhostFactories))
{}

TBootstrapVhost::~TBootstrapVhost() = default;

TConfigInitializerCommonPtr TBootstrapVhost::InitConfigs(int argc, char** argv)
{
    auto options = std::make_shared<TOptionsVhost>();
    options->Parse(argc, argv);

    Configs = std::make_shared<TConfigInitializerVhost>(std::move(options));
    return Configs;
}

void TBootstrapVhost::InitComponents()
{
    InitConfig();

    if (Configs->Options->Service == NDaemon::EServiceKind::Local) {
        // clean umask to allow passing permissions to local file system
        Umask(0);
    }

    NVhost::InitLog(Logging);
    switch (Configs->VhostServiceConfig->GetEndpointStorageType()) {
        case NCloud::NProto::ENDPOINT_STORAGE_DEFAULT:
        case NCloud::NProto::ENDPOINT_STORAGE_KEYRING: {
            const bool notImplementedErrorIsFatal =
                Configs->VhostServiceConfig->GetEndpointStorageNotImplementedErrorIsFatal();
            EndpointStorage = CreateKeyringEndpointStorage(
                Configs->VhostServiceConfig->GetRootKeyringName(),
                Configs->VhostServiceConfig->GetEndpointsKeyringName(),
                notImplementedErrorIsFatal);
            break;
        }
        case NCloud::NProto::ENDPOINT_STORAGE_FILE:
            EndpointStorage = CreateFileEndpointStorage(
                Configs->VhostServiceConfig->GetEndpointStorageDir());
            break;
        default:
            Y_ABORT(
                "unsupported endpoint storage type %d",
                Configs->VhostServiceConfig->GetEndpointStorageType());
    }

    switch (Configs->Options->Service) {
        case NDaemon::EServiceKind::Local:
        case NDaemon::EServiceKind::Kikimr:
            InitEndpoints();
            break;
        case NDaemon::EServiceKind::Null:
            InitNullEndpoints();
            break;
    }

    if (Configs->Options->Service == EServiceKind::Kikimr) {
        EndpointManager = CreateAuthService(
            std::move(EndpointManager),
            CreateKikimrAuthProvider(ActorSystem));
    }

    Server = CreateServer(
        Configs->ServerConfig,
        Logging,
        StatsRegistry->GetRequestStats(),
        EndpointManager);
    RegisterServer(Server);

    if (LocalService) {
        auto serverConfigProto = Configs->ServerConfig->GetProto();
        serverConfigProto.ClearSecurePort();
        serverConfigProto.SetPort(Configs->Options->LocalServicePort);
        LocalServiceServer = CreateServer(
            std::make_shared<NServer::TServerConfig>(serverConfigProto),
            Logging,
            StatsRegistry->GetRequestStats(),
            ProfileLog,
            LocalService);

        STORAGE_INFO("initialized LocalServiceServer: %s",
            serverConfigProto.Utf8DebugString().Quote().c_str());
    }

    InitLWTrace();
}

void TBootstrapVhost::InitConfig()
{
    Configs->InitAppConfig();
}

void TBootstrapVhost::InitEndpoints()
{
    const auto* localServiceConfig =
        Configs->VhostServiceConfig->GetLocalServiceConfig();
    if (localServiceConfig) {
        auto serviceConfig = std::make_shared<TLocalFileStoreConfig>(
            *localServiceConfig);
        ThreadPool = CreateThreadPool("svc", serviceConfig->GetNumThreads());
        FileIOService = CreateFileIOService(*serviceConfig);
        LocalService = CreateLocalFileStore(
            std::move(serviceConfig),
            Timer,
            Scheduler,
            Logging,
            FileIOService,
            ThreadPool,
            ProfileLog);

        STORAGE_INFO("initialized LocalService: %s",
            localServiceConfig->Utf8DebugString().Quote().c_str());
    }

    auto endpoints = std::make_shared<TFileStoreEndpoints>(
        Timer,
        Scheduler,
        Logging,
        LocalService,
        ActorSystem);

    for (const auto& endpoint: Configs->VhostServiceConfig->GetServiceEndpoints()) {
        bool inserted = endpoints->AddEndpoint(
            endpoint.GetName(),
            endpoint.GetClientConfig(),
            Configs->Options->Service);

        if (inserted) {
            STORAGE_INFO("configured endpoint type %s -> %s",
                ToString<EServiceKind>(Configs->Options->Service).c_str(),
                endpoint.ShortDebugString().c_str());
        } else {
            STORAGE_ERROR("duplicated client config: '" << endpoint.GetName() << "'");
        }
    }

    if (endpoints->Empty()) {
        // TODO: ReportCritEvent()
        STORAGE_ERROR("Empty endpoints config");
    }

    FileStoreEndpoints = std::move(endpoints);

    EndpointListener = NVhost::CreateEndpointListener(
        Logging,
        Timer,
        Scheduler,
        FileStoreEndpoints,
        VhostModuleFactories->LoopFactory(
            Logging,
            Timer,
            Scheduler,
            StatsRegistry,
            ProfileLog),
        THandleOpsQueueConfig{
            .PathPrefix = Configs->VhostServiceConfig->GetHandleOpsQueuePath(),
            .MaxQueueSize = Configs->VhostServiceConfig->GetHandleOpsQueueSize(),
        },
        TWriteBackCacheConfig{
            .PathPrefix = Configs->VhostServiceConfig->GetWriteBackCachePath(),
            .Capacity =
                Configs->VhostServiceConfig->GetWriteBackCacheCapacity(),
            .AutomaticFlushPeriod =
                Configs->VhostServiceConfig
                    ->GetWriteBackCacheAutomaticFlushPeriod()
        }
    );

    EndpointManager = CreateEndpointManager(
        Logging,
        EndpointStorage,
        EndpointListener,
        Configs->VhostServiceConfig->GetSocketAccessMode());
}

void TBootstrapVhost::InitNullEndpoints()
{
    EndpointManager = CreateNullEndpointManager();
}

void TBootstrapVhost::InitLWTrace()
{
    TVector<NLWTrace::TProbe**> probes = {
        LWTRACE_GET_PROBES(BLOBSTORAGE_PROVIDER),
        LWTRACE_GET_PROBES(FILESTORE_CLIENT_PROVIDER),
        LWTRACE_GET_PROBES(FILESTORE_SERVER_PROVIDER),
        LWTRACE_GET_PROBES(FILESTORE_STORAGE_PROVIDER),
        LWTRACE_GET_PROBES(FILESTORE_VFS_PROVIDER),
        LWTRACE_GET_PROBES(LWTRACE_INTERNAL_PROVIDER),
        LWTRACE_GET_PROBES(TABLET_FLAT_PROVIDER),
    };

    const TVector<std::tuple<TString, TString>> probesToTrace = {
        {"RequestReceived",              "FILESTORE_VFS_PROVIDER"},
    };

    TBootstrapCommon::InitLWTrace(probes, probesToTrace);
}

void TBootstrapVhost::StartComponents()
{
    NVhost::StartServer();

    FILESTORE_LOG_START_COMPONENT(ThreadPool);
    // LocalService is started inside FileStoreEndpoints
    FILESTORE_LOG_START_COMPONENT(FileStoreEndpoints);
    FILESTORE_LOG_START_COMPONENT(EndpointManager);
    FILESTORE_LOG_START_COMPONENT(Server);
    FILESTORE_LOG_START_COMPONENT(LocalServiceServer);

    if (EndpointManager) {
        EndpointManager->RestoreEndpoints();
    }
}

void TBootstrapVhost::StopComponents()
{
    FILESTORE_LOG_STOP_COMPONENT(LocalServiceServer);
    FILESTORE_LOG_STOP_COMPONENT(Server);
    FILESTORE_LOG_STOP_COMPONENT(EndpointManager);
    FILESTORE_LOG_STOP_COMPONENT(FileStoreEndpoints);
    // LocalService is stopped inside FileStoreEndpoints
    FILESTORE_LOG_STOP_COMPONENT(ThreadPool);

    NVhost::StopServer();
}

void TBootstrapVhost::Drain()
{
    if (EndpointManager) {
        EndpointManager->Drain();
    }
}

}   // namespace NCloud::NFileStore::NDaemon
