#include "bootstrap.h"

#include "config_initializer.h"
#include "options.h"

#include <cloud/filestore/libs/daemon/common/bootstrap.h>
#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/diagnostics/profile_log.h>
#include <cloud/filestore/libs/diagnostics/request_stats.h>
#include <cloud/filestore/libs/server/config.h>
#include <cloud/filestore/libs/server/probes.h>
#include <cloud/filestore/libs/server/server.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/service/service_auth.h>
#include <cloud/filestore/libs/service_kikimr/auth_provider_kikimr.h>
#include <cloud/filestore/libs/service_kikimr/service.h>
#include <cloud/filestore/libs/service_local/config.h>
#include <cloud/filestore/libs/service_local/service.h>
#include <cloud/filestore/libs/service_null/service.h>
#include <cloud/filestore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/diagnostics/stats_updater.h>
#include <cloud/storage/core/libs/diagnostics/trace_reader.h>
#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>
#include <cloud/storage/core/libs/user_stats/counter/user_counter.h>

#include <contrib/ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>
#include <contrib/ydb/core/tablet_flat/probes.h>

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>

namespace NCloud::NFileStore::NDaemon {

using namespace NActors;
using namespace NKikimr;

using namespace NCloud::NStorage;

////////////////////////////////////////////////////////////////////////////////

TBootstrapServer::TBootstrapServer(std::shared_ptr<NKikimr::TModuleFactories> moduleFactories)
    : TBootstrapCommon(
        std::move(moduleFactories),
        "NFS_SERVER",
        "server",
        NUserStats::CreateUserCounterSupplierStub())
{}

TBootstrapServer::~TBootstrapServer()
{}

////////////////////////////////////////////////////////////////////////////////

void TBootstrapServer::StartComponents()
{
    FILESTORE_LOG_START_COMPONENT(ThreadPool);
    FILESTORE_LOG_START_COMPONENT(Service);
    FILESTORE_LOG_START_COMPONENT(Server);
}

void TBootstrapServer::Drain()
{}

void TBootstrapServer::StopComponents()
{
    FILESTORE_LOG_STOP_COMPONENT(Server);
    FILESTORE_LOG_STOP_COMPONENT(Service);
    FILESTORE_LOG_STOP_COMPONENT(ThreadPool);
}

TConfigInitializerCommonPtr TBootstrapServer::InitConfigs(int argc, char** argv)
{
    auto options = std::make_shared<TOptionsServer>();
    options->Parse(argc, argv);

    Configs = std::make_shared<TConfigInitializerServer>(std::move(options));
    return Configs;
}

void TBootstrapServer::InitComponents()
{
    InitConfigs();

    switch (Configs->Options->Service) {
        case NDaemon::EServiceKind::Kikimr:
            InitKikimrService();
            break;
        case NDaemon::EServiceKind::Local:
            InitLocalService();
            break;
        case NDaemon::EServiceKind::Null:
            InitNullService();
            break;
    }

    Server = NServer::CreateServer(
        Configs->ServerConfig,
        Logging,
        StatsRegistry->GetRequestStats(),
        ProfileLog,
        Service);
    RegisterServer(Server);

    InitLWTrace();
}

void TBootstrapServer::InitConfigs()
{
    Configs->InitAppConfig();
    Configs->SetupGrpcThreadsLimit();
}

void TBootstrapServer::InitLWTrace()
{
    TVector<NLWTrace::TProbe**> probes = {
        LWTRACE_GET_PROBES(BLOBSTORAGE_PROVIDER),
        LWTRACE_GET_PROBES(FILESTORE_SERVER_PROVIDER),
        LWTRACE_GET_PROBES(FILESTORE_STORAGE_PROVIDER),
        LWTRACE_GET_PROBES(LWTRACE_INTERNAL_PROVIDER),
        LWTRACE_GET_PROBES(TABLET_FLAT_PROVIDER),
    };

    const TVector<std::tuple<TString, TString>> probesToTrace = {
        // internal operation started by tablet
        {"BackgroundTaskStarted_Tablet", "FILESTORE_STORAGE_PROVIDER"},
        // request accepted by tablet
        {"RequestReceived_Tablet",       "FILESTORE_STORAGE_PROVIDER"},
        // request accepted by grpc server
        {"ExecuteRequest",               "FILESTORE_SERVER_PROVIDER"},
    };

    TBootstrapCommon::InitLWTrace(probes, probesToTrace);
}

void TBootstrapServer::InitKikimrService()
{
    Y_ABORT_UNLESS(ActorSystem, "Actor system MUST be initialized to create kikimr filestore");
    Service = CreateKikimrFileStore(ActorSystem);

    Service = CreateAuthService(
        std::move(Service),
        CreateKikimrAuthProvider(ActorSystem),
        Configs->ServerConfig->GetActionsNoAuth());

    STORAGE_INFO("AuthService initialized");
}

void TBootstrapServer::InitLocalService()
{
    auto serviceConfig = std::make_shared<TLocalFileStoreConfig>(
        Configs->AppConfig.GetLocalServiceConfig());

    ThreadPool = CreateThreadPool("svc", serviceConfig->GetNumThreads());
    Service = CreateLocalFileStore(
        std::move(serviceConfig),
        Timer,
        Scheduler,
        Logging,
        FileIOService,
        ThreadPool,
        nullptr   // no profile log
    );
}

void TBootstrapServer::InitNullService()
{
    Service = CreateNullFileStore();
}

}   // namespace NCloud::NFileStore::NDaemon
