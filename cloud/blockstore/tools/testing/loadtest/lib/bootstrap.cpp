#include "bootstrap.h"

#include "filesystem_client.h"
#include "options.h"

#include <cloud/blockstore/config/spdk.pb.h>
#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/durable.h>
#include <cloud/blockstore/libs/client/throttling.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>
#include <cloud/blockstore/libs/client_spdk/spdk_client.h>
#include <cloud/blockstore/libs/diagnostics/probes.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/nbd/client.h>
#include <cloud/blockstore/libs/nbd/client_handler.h>
#include <cloud/blockstore/libs/rdma/impl/client.h>
#include <cloud/blockstore/libs/rdma/impl/verbs.h>
#include <cloud/blockstore/libs/rdma_test/client_test.h>
#include <cloud/blockstore/libs/spdk/iface/config.h>
#include <cloud/blockstore/libs/spdk/iface/env.h>
#include <cloud/blockstore/libs/throttling/throttler.h>
#include <cloud/blockstore/libs/throttling/throttler_metrics.h>
#include <cloud/blockstore/libs/validation/validation.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>
#include <cloud/storage/core/libs/grpc/init.h>
#include <cloud/storage/core/libs/grpc/threadpool.h>
#include <cloud/storage/core/libs/grpc/utils.h>
#include <cloud/storage/core/libs/version/version.h>

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/generic/guid.h>

namespace NCloud::NBlockStore::NLoadTest {

using namespace NThreading;

using namespace NCloud::NBlockStore::NClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString TestInstanceId = "TestInstanceId";

////////////////////////////////////////////////////////////////////////////////

struct TRetryPolicyImpl final
    : IRetryPolicy
{
    IRetryPolicyPtr Policy;
    TVector<ui32> NonretriableErrorCodes;

    TRetryPolicyImpl(
            IRetryPolicyPtr policy,
            TVector<ui32> nonretriableErrorCodes)
        : Policy(std::move(policy))
        , NonretriableErrorCodes(std::move(nonretriableErrorCodes))
    {
    }

    TRetrySpec ShouldRetry(
        TRetryState& state,
        const NProto::TError& error) override
    {
        const auto it = Find(
            NonretriableErrorCodes.begin(),
            NonretriableErrorCodes.end(),
            error.GetCode()
        );

        if (it != NonretriableErrorCodes.end()) {
            return {};
        }

        return Policy->ShouldRetry(state, error);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TDigestCalculatorImpl final
    : IBlockDigestCalculator
{
    ui64 Calculate(ui64 blockIndex, const TStringBuf block) const override
    {
        Y_UNUSED(blockIndex);

        if (!block) {
            return 0;
        }

        ui64 result;
        Y_ABORT_UNLESS(block.size() >= sizeof(result));
        memcpy(&result, block.data(), sizeof(result));
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

TNVMeEndpointConfig CreateNVMeEndpointConfig(
    const TClientAppConfig& config,
    const TString& socketPath)
{
    return TNVMeEndpointConfig {
        .DeviceTransportId = config.GetNvmeDeviceTransportId(),
        .DeviceNqn = config.GetNvmeDeviceNqn(),
        .SocketPath = socketPath,
    };
}

TSCSIEndpointConfig CreateSCSIEndpointConfig(
    const TClientAppConfig& config,
    const TString& socketPath)
{
    return TSCSIEndpointConfig {
        .DeviceUrl = config.GetScsiDeviceUrl(),
        .InitiatorIqn = config.GetScsiInitiatorIqn(),
        .SocketPath = socketPath,
    };
}

TRdmaEndpointConfig CreateRdmaEndpointConfig(const TClientAppConfig& config)
{
    return TRdmaEndpointConfig {
        .Address = config.GetRdmaDeviceAddress(),
        .Port = config.GetRdmaDevicePort(),
    };
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(
        TOptionsPtr options,
        std::shared_ptr<TModuleFactories> moduleFactories)
    : Options(std::move(options))
    , ModuleFactories(std::move(moduleFactories))
{}

TBootstrap::~TBootstrap()
{}

void TBootstrap::Init()
{
    InitLWTrace();

    InitClientConfig();

    Timer = CreateWallClockTimer();
    Scheduler = CreateScheduler();

    const auto& logConfig = ClientConfig->GetLogConfig();
    const auto& monConfig = ClientConfig->GetMonitoringConfig();

    TLogSettings logSettings;

    if (logConfig.HasLogLevel()) {
        logSettings.FiltrationLevel = static_cast<ELogPriority>(
            logConfig.GetLogLevel());
    }

    if (Options->VerboseLevel) {
        auto level = GetLogLevel(Options->VerboseLevel);
        if (!level) {
            ythrow yexception()
                << "unknown log level: " << Options->VerboseLevel.Quote();
        }
        logSettings.FiltrationLevel = *level;
    }

    Logging = CreateLoggingService("console", logSettings);
    GrpcLog = Logging->CreateLog("GRPC");

    GrpcLoggerInit(
        GrpcLog,
        Options->EnableGrpcTracing || logConfig.GetEnableGrpcTracing());

    SetGrpcThreadsLimit(ClientConfig->GetGrpcThreadsLimit());

    ui32 monPort = monConfig.GetPort();
    if (monPort) {
        const TString& monAddress = monConfig.GetAddress();
        ui32 threadsCount = monConfig.GetThreadsCount();
        Monitoring = CreateMonitoringService(monPort, monAddress, threadsCount);
    } else {
        Monitoring = CreateMonitoringServiceStub();
    }

    auto rootGroup = Monitoring->GetCounters()
        ->GetSubgroup("counters", "blockstore");

    auto clientGroup = rootGroup->GetSubgroup("component", "client");
    auto versionCounter = clientGroup->GetNamedCounter(
        "version",
        GetFullVersionString(),
        false);
    *versionCounter = 1;

    RequestStats = CreateClientRequestStats(
        clientGroup,
        Timer,
        EHistogramCounterOption::ReportMultipleCounters);

    VolumeStats = CreateVolumeStats(
        Monitoring,
        {},
        EVolumeStatsType::EClientStats,
        Timer);

    if (Options->SpdkConfig) {
        NProto::TSpdkEnvConfig config;
        ParseFromTextFormat(Options->SpdkConfig, config);
        auto spdkConfig = std::make_shared<NSpdk::TSpdkEnvConfig>(config);

        auto spdkParts = ModuleFactories->SpdkFactory(spdkConfig);
        Spdk = std::move(spdkParts.Env);
        SpdkLog = Logging->CreateLog("BLOCKSTORE_SPDK");
        spdkParts.LogInitializer(SpdkLog);
    }
}

IBlockStoreValidationClientPtr TBootstrap::CreateValidationClient(
    IBlockStorePtr client,
    IValidationCallbackPtr callback,
    TString loggingTag,
    TBlockRange64 validationRange)
{
    return NClient::CreateValidationClient(
        Logging,
        Monitoring,
        std::move(client),
        CreateDigestCalculator(),
        std::move(callback),
        std::move(loggingTag),
        validationRange);
}

IBlockDigestCalculatorPtr TBootstrap::CreateDigestCalculator()
{
    return std::make_shared<TDigestCalculatorImpl>();
}

TClientAppConfigPtr CreateClientConfig(
    const TClientAppConfigPtr& config,
    TString clientId)
{
    NProto::TClientAppConfig appConfig;
    appConfig.MutableClientConfig()->CopyFrom(config->GetClientConfig());
    appConfig.MutableLogConfig()->CopyFrom(config->GetLogConfig());
    appConfig.MutableMonitoringConfig()->CopyFrom(config->GetMonitoringConfig());

    auto& clientConfig = *appConfig.MutableClientConfig();

    if (!clientId) {
        clientId = CreateGuidAsString();
    }
    clientConfig.SetClientId(clientId);

    return std::make_shared<TClientAppConfig>(appConfig);
}

IClientPtr TBootstrap::CreateAndStartGrpcClient(TString clientId)
{
    auto config = CreateClientConfig(ClientConfig, std::move(clientId));

    auto clientStats = CreateClientStats(
        config,
        Monitoring,
        RequestStats,
        VolumeStats,
        TestInstanceId);

    auto [client, error] = NClient::CreateClient(
        std::move(config),
        Timer,
        Scheduler,
        Logging,
        Monitoring,
        std::move(clientStats));

    Y_ABORT_UNLESS(!HasError(error));

    client->Start();
    Clients.Enqueue(client);

    return client;
}

IBlockStorePtr TBootstrap::CreateAndStartFilesystemClient()
{
    auto clientStats = CreateClientStats(
        CreateClientConfig(ClientConfig, "filesystem-client"),
        Monitoring,
        RequestStats,
        VolumeStats,
        TestInstanceId);

    auto client = NFilesystemClient::CreateClient(
        Timer,
        Scheduler,
        Logging,
        Monitoring,
        std::move(clientStats));

    client->Start();
    Clients.Enqueue(client);

    return client;
}

NBD::IClientPtr TBootstrap::CreateAndStartNbdClient(TString clientId)
{
    auto config = CreateClientConfig(ClientConfig, std::move(clientId));

    auto clientStats = CreateClientStats(
        config,
        Monitoring,
        RequestStats,
        VolumeStats,
        TestInstanceId);

    // TODO
    Y_UNUSED(clientStats);

    auto client = NBD::CreateClient(
        Logging,
        config->GetNbdThreadsCount());

    client->Start();
    Clients.Enqueue(client);

    return client;
}

NRdma::IClientPtr TBootstrap::CreateAndStartRdmaClient(TString clientId)
{
    auto config = CreateClientConfig(ClientConfig, std::move(clientId));

    auto clientStats = CreateClientStats(
        config,
        Monitoring,
        RequestStats,
        VolumeStats,
        TestInstanceId);

    // TODO
    Y_UNUSED(clientStats);

    auto rdmaConfig = std::make_shared<NRdma::TClientConfig>();
    // TODO

    auto client = NRdma::CreateClient(
        NRdma::NVerbs::CreateVerbs(),
        Logging,
        Monitoring,
        std::move(rdmaConfig));

    client->Start();
    Clients.Enqueue(client);

    return client;
}

IBlockStorePtr TBootstrap::CreateClient(TVector<ui32> nonretriableErrorCodes)
{
    if (Options->Host == "filesystem") {
        return CreateAndStartFilesystemClient();
    }

    auto client = CreateAndStartGrpcClient();

    auto clientEndpoint = client->CreateEndpoint();

    auto retryPolicy = std::make_shared<TRetryPolicyImpl>(
        CreateRetryPolicy(ClientConfig, std::nullopt),
        std::move(nonretriableErrorCodes));

    return CreateDurableClient(
        ClientConfig,
        std::move(clientEndpoint),
        std::move(retryPolicy),
        Logging,
        Timer,
        Scheduler,
        RequestStats,
        VolumeStats);
}

IBlockStorePtr TBootstrap::CreateEndpointDataClient(
    NProto::EClientIpcType ipcType,
    const TString& socketPath,
    const TString& clientId,
    TVector<ui32> nonretriableErrorCodes)
{
    IBlockStorePtr clientEndpoint;
    switch (ipcType) {
        case NProto::IPC_GRPC: {
            auto client = CreateAndStartGrpcClient(clientId);
            clientEndpoint = client->CreateDataEndpoint(socketPath);
            break;
        }

        case NProto::IPC_NBD: {
            Y_ABORT_UNLESS(ClientConfig->GetNbdSocketSuffix());
            auto connectAddress = TNetworkAddress(TUnixSocketPath(
                socketPath + ClientConfig->GetNbdSocketSuffix()));

            auto client = CreateAndStartGrpcClient(clientId);
            auto dataEndpoint = client->CreateDataEndpoint(socketPath);

            auto nbdClient = CreateAndStartNbdClient(clientId);
            clientEndpoint = nbdClient->CreateEndpoint(
                connectAddress,
                NBD::CreateClientHandler(Logging),
                std::move(dataEndpoint));
            break;
        }

        case NProto::IPC_NVME:
        case NProto::IPC_SCSI: {
            Y_ABORT_UNLESS(Spdk);

            auto client = CreateAndStartGrpcClient(clientId);
            auto dataEndpoint = client->CreateEndpoint();

            if (ipcType == NProto::IPC_NVME) {
                clientEndpoint = CreateNVMeEndpointClient(
                    Spdk,
                    std::move(dataEndpoint),
                    CreateNVMeEndpointConfig(*ClientConfig, socketPath));
            } else if (ipcType == NProto::IPC_SCSI) {
                clientEndpoint = CreateSCSIEndpointClient(
                    Spdk,
                    std::move(dataEndpoint),
                    CreateSCSIEndpointConfig(*ClientConfig, socketPath));
            }
            break;
        }

        case NProto::IPC_RDMA: {
            auto client = CreateAndStartGrpcClient(clientId);
            auto dataEndpoint = client->CreateEndpoint();

            auto rdmaClient = CreateAndStartRdmaClient(clientId);
            clientEndpoint = CreateRdmaEndpointClient(
                Logging,
                rdmaClient,
                std::move(dataEndpoint),
                CreateTraceSerializerStub(),
                CreateTaskQueueStub(),
                CreateRdmaEndpointConfig(*ClientConfig));
            break;
        }

        default:
            // not supported
            break;
    }

    if (!clientEndpoint) {
        ythrow yexception()
            << "unsupported client ipc type: " << static_cast<ui32>(ipcType);
    }

    return CreateDurableDataClient(
        std::move(clientEndpoint),
        std::move(nonretriableErrorCodes));
}

IBlockStorePtr TBootstrap::CreateDurableDataClient(
    IBlockStorePtr dataClient,
    TVector<ui32> nonretriableErrorCodes)
{
    auto retryPolicy = std::make_shared<TRetryPolicyImpl>(
        CreateRetryPolicy(ClientConfig, std::nullopt),
        std::move(nonretriableErrorCodes));

    return CreateDurableClient(
        ClientConfig,
        std::move(dataClient),
        std::move(retryPolicy),
        Logging,
        Timer,
        Scheduler,
        RequestStats,
        VolumeStats);
}

IBlockStorePtr TBootstrap::CreateThrottlingClient(
    IBlockStorePtr client,
    NProto::TClientPerformanceProfile performanceProfile)
{
    auto throttler = CreateThrottler(
        CreateClientThrottlerLogger(RequestStats, Logging),
        CreateThrottlerMetrics(
            Timer,
            Monitoring->GetCounters()->GetSubgroup("counters", "blockstore"),
            "client"),
        CreateClientThrottlerPolicy(std::move(performanceProfile)),
        CreateClientThrottlerTracker(),
        Timer,
        Scheduler,
        VolumeStats);

    return NClient::CreateThrottlingClient(
        std::move(client),
        std::move(throttler));
}

const TString& TBootstrap::GetEndpointStorageDir() const
{
    return Options->EndpointStorageDir;
}

void TBootstrap::InitClientConfig()
{
    NProto::TClientAppConfig appConfig;

    auto& clientConfig = *appConfig.MutableClientConfig();
    if (Options->ClientConfig) {
        ParseFromTextFormat(Options->ClientConfig, clientConfig);
    }
    if (Options->Host) {
        clientConfig.SetHost(Options->Host);
    }
    if (Options->InsecurePort) {
        clientConfig.SetInsecurePort(Options->InsecurePort);
    }
    if (Options->SecurePort) {
        clientConfig.SetSecurePort(Options->SecurePort);
    }

    if (!clientConfig.GetNbdSocketSuffix()) {
        clientConfig.SetNbdSocketSuffix("_nbd");
        clientConfig.SetNbdUseNbsErrors(true);
    }

    auto& monConfig = *appConfig.MutableMonitoringConfig();
    if (Options->MonitoringConfig) {
        ParseFromTextFormat(Options->MonitoringConfig, monConfig);
    }
    if (Options->MonitoringAddress) {
        monConfig.SetAddress(Options->MonitoringAddress);
    }
    if (Options->MonitoringPort) {
        monConfig.SetPort(Options->MonitoringPort);
    }
    if (Options->MonitoringThreads) {
        monConfig.SetThreadsCount(Options->MonitoringThreads);
    }
    if (!monConfig.GetThreadsCount()) {
        monConfig.SetThreadsCount(1);  // reasonable defaults
    }

    ClientConfig = std::make_shared<TClientAppConfig>(appConfig);
}

void TBootstrap::InitLWTrace()
{
    auto& probes = NLwTraceMonPage::ProbeRegistry();
    probes.AddProbesList(LWTRACE_GET_PROBES(BLOCKSTORE_SERVER_PROVIDER));
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

    if (Spdk) {
        Spdk->Start();
    }
}

void TBootstrap::Stop()
{
    IStartablePtr client;
    while (Clients.Dequeue(&client)) {
        client->Stop();
    }

    if (Spdk) {
        Spdk->Stop();
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

}   // namespace NCloud::NBlockStore::NLoadTest
