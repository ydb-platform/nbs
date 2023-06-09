#include "bootstrap.h"

#include "options.h"

#include <cloud/blockstore/tools/testing/loadtest/lib/filesystem_client.h>

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
#include <cloud/blockstore/libs/encryption/encryption_client.h>
#include <cloud/blockstore/libs/nbd/client.h>
#include <cloud/blockstore/libs/nbd/client_handler.h>
#include <cloud/blockstore/libs/rdma/impl/client.h>
#include <cloud/blockstore/libs/rdma/impl/verbs.h>
#include <cloud/blockstore/libs/spdk/iface/config.h>
#include <cloud/blockstore/libs/spdk/iface/env.h>
#include <cloud/blockstore/libs/spdk/iface/env_stub.h>
//#include <cloud/blockstore/libs/spdk/impl/env.h>
#include <cloud/blockstore/libs/throttling/throttler.h>
#include <cloud/blockstore/libs/throttling/throttler_metrics.h>
#include <cloud/blockstore/libs/validation/validation.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/grpc/executor.h>
#include <cloud/storage/core/libs/grpc/logging.h>
#include <cloud/storage/core/libs/grpc/threadpool.h>
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
        Y_VERIFY(block.Size() >= sizeof(result));
        memcpy(&result, block.Data(), sizeof(result));
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

TBootstrap::TBootstrap(TOptionsPtr options)
    : Options(std::move(options))
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
    logSettings.UseLocalTimestamps = true;

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

    ui32 maxThreads = ClientConfig->GetGrpcThreadsLimit();
    SetExecutorThreadsLimit(maxThreads);
    SetDefaultThreadPoolLimit(maxThreads);

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

    RequestStats = CreateClientRequestStats(clientGroup, Timer);

    VolumeStats = CreateVolumeStats(
        Monitoring,
        {},
        EVolumeStatsType::EClientStats,
        Timer);

    if (Options->SpdkConfig) {
#if 0
        // TODO: create a separate build target with spdk-capable loadtest
        NSpdk::InitLogging(Logging->CreateLog("BLOCKSTORE_SPDK"));

        NProto::TSpdkEnvConfig config;
        ParseFromTextFormat(Options->SpdkConfig, config);

        auto spdkConfig = std::make_shared<NSpdk::TSpdkEnvConfig>(config);
        Spdk = NSpdk::CreateEnv(spdkConfig);
#else
        Spdk = NSpdk::CreateEnvStub();
#endif
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

    Y_VERIFY(!HasError(error));

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

IBlockStorePtr TBootstrap::CreateClient(
    TVector<ui32> nonretriableErrorCodes,
    const NProto::TEncryptionSpec& encryptionSpec)
{
    if (Options->Host == "filesystem") {
        return CreateAndStartFilesystemClient();
    }

    auto client = CreateAndStartGrpcClient();

    auto clientEndpoint = client->CreateEndpoint();

    auto retryPolicy = std::make_shared<TRetryPolicyImpl>(
        CreateRetryPolicy(ClientConfig),
        std::move(nonretriableErrorCodes));

    clientEndpoint = CreateDurableClient(
        ClientConfig,
        std::move(clientEndpoint),
        std::move(retryPolicy),
        Logging,
        Timer,
        Scheduler,
        RequestStats,
        VolumeStats);

    auto clientOrError = TryToCreateEncryptionClient(
        std::move(clientEndpoint),
        Logging,
        encryptionSpec);

    CheckError(clientOrError);
    return clientOrError.GetResult();
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
            Y_VERIFY(ClientConfig->GetNbdSocketSuffix());
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
            Y_VERIFY(Spdk);

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
        std::move(nonretriableErrorCodes),
        {});
}

IBlockStorePtr TBootstrap::CreateDurableDataClient(
    IBlockStorePtr dataClient,
    TVector<ui32> nonretriableErrorCodes,
    const NProto::TEncryptionSpec& encryptionSpec)
{
    auto retryPolicy = std::make_shared<TRetryPolicyImpl>(
        CreateRetryPolicy(ClientConfig),
        std::move(nonretriableErrorCodes));

    dataClient = CreateDurableClient(
        ClientConfig,
        std::move(dataClient),
        std::move(retryPolicy),
        Logging,
        Timer,
        Scheduler,
        RequestStats,
        VolumeStats);

    auto clientOrError = TryToCreateEncryptionClient(
        std::move(dataClient),
        Logging,
        encryptionSpec);

    CheckError(clientOrError);
    return clientOrError.GetResult();
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
