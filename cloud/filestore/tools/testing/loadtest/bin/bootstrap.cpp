#include "bootstrap.h"

#include "options.h"

#include <cloud/filestore/tools/testing/loadtest/lib/client.h>

#include <cloud/filestore/libs/client/client.h>
#include <cloud/filestore/libs/client/config.h>
#include <cloud/filestore/libs/client/durable.h>
#include <cloud/filestore/libs/service/filestore.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/grpc/init.h>
#include <cloud/storage/core/libs/grpc/utils.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/generic/vector.h>
#include <util/stream/file.h>

namespace NCloud::NFileStore::NLoadTest {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TClientFactory final
    : public IClientFactory
{
private:
    const ILoggingServicePtr Logging;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const NClient::TClientConfigPtr ClientConfig;

    TVector<IFileStoreServicePtr> Clients;

public:
    TClientFactory(
            ILoggingServicePtr logging,
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            NClient::TClientConfigPtr clientConfig)
        : Logging(std::move(logging))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , ClientConfig(std::move(clientConfig))
    {}

    void Start() override
    {
        for (auto& client: Clients) {
            client->Start();
        }
    }

    void Stop() override
    {
        for (auto& client: Clients) {
            client->Stop();
        }
    }

    IFileStoreServicePtr CreateClient() override
    {
        auto client = NClient::CreateFileStoreClient(
            ClientConfig,
            Logging);

        client = NClient::CreateDurableClient(
            Logging,
            Timer,
            Scheduler,
            CreateRetryPolicy(ClientConfig),
            client);

        Clients.push_back(client);
        return client;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(TOptionsPtr options)
    : Options(std::move(options))
{}

TBootstrap::~TBootstrap()
{}

void TBootstrap::Init()
{
    InitDbgConfig();
    InitClientConfig();

    Timer = CreateWallClockTimer();
    Scheduler = CreateScheduler();

    ClientFactory = std::make_shared<TClientFactory>(
        Logging,
        Timer,
        Scheduler,
        ClientConfig);
}

void TBootstrap::Start()
{
    if (Logging) {
        Logging->Start();
    }

    if (Monitoring) {
        Monitoring->Start();
    }

    if (Scheduler) {
        Scheduler->Start();
    }

    if (ClientFactory) {
        ClientFactory->Start();
    }
}

void TBootstrap::Stop()
{
    if (ClientFactory) {
        ClientFactory->Stop();
    }

    if (Scheduler) {
        Scheduler->Stop();
    }

    if (Monitoring) {
        Monitoring->Stop();
    }

    if (Logging) {
        Logging->Stop();
    }
}

////////////////////////////////////////////////////////////////////////////////

void TBootstrap::InitDbgConfig()
{
    TLogSettings logSettings;

    if (Options->VerboseLevel) {
        auto level = GetLogLevel(Options->VerboseLevel);
        Y_ENSURE(level, "unknown log level: " << Options->VerboseLevel.Quote());
        logSettings.FiltrationLevel = *level;
    }

    Logging = CreateLoggingService("console", logSettings);
    Log = Logging->CreateLog("BOOT");
    GrpcLog = Logging->CreateLog("GRPC");
    GrpcLoggerInit(GrpcLog, Options->EnableGrpcTracing);

    if (Options->MonitoringPort) {
        Monitoring = CreateMonitoringService(
            Options->MonitoringPort,
            Options->MonitoringAddress,
            Options->MonitoringThreads);
    } else {
        Monitoring = CreateMonitoringServiceStub();
    }
}

void TBootstrap::InitClientConfig()
{
    NProto::TClientConfig clientConfig;

    if (Options->ConfigFile) {
        ParseFromTextFormat(Options->ConfigFile, clientConfig);
    }
    if (Options->Host) {
        clientConfig.SetHost(Options->Host);
    }
    if (Options->InsecurePort) {
        clientConfig.SetPort(Options->InsecurePort);
    }
    if (Options->SecurePort) {
        clientConfig.SetSecurePort(Options->SecurePort);
    }
    if (Options->UnixSocketPath) {
        clientConfig.SetUnixSocketPath(Options->UnixSocketPath);
    }

    ClientConfig = std::make_shared<NClient::TClientConfig>(clientConfig);

    SetGrpcThreadsLimit(ClientConfig->GetGrpcThreadsLimit());

    TStringStream ss;
    ClientConfig->Dump(ss);

    STORAGE_INFO("client config:\n" << ss.Str());
}

}   // namespace NCloud::NFileStore::NServer
