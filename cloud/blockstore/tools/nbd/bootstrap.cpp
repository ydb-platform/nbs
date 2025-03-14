#include "bootstrap.h"

#include "options.h"

#include <cloud/blockstore/config/server.pb.h>

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/durable.h>
#include <cloud/blockstore/libs/client/session.h>
#include <cloud/blockstore/libs/client/throttling.h>
#include <cloud/blockstore/libs/diagnostics/probes.h>
#include <cloud/blockstore/libs/diagnostics/incomplete_request_processor.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/nbd/device.h>
#include <cloud/blockstore/libs/nbd/error_handler.h>
#include <cloud/blockstore/libs/nbd/netlink_device.h>
#include <cloud/blockstore/libs/nbd/server.h>
#include <cloud/blockstore/libs/nbd/server_handler.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/device_handler.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service/service_null.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/diagnostics/stats_updater.h>
#include <cloud/storage/core/libs/grpc/init.h>
#include <cloud/storage/core/libs/grpc/threadpool.h>
#include <cloud/storage/core/libs/grpc/utils.h>
#include <cloud/storage/core/libs/version/version.h>

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/datetime/base.h>
#include <util/folder/dirut.h>
#include <util/generic/guid.h>
#include <util/stream/file.h>
#include <util/string/strip.h>
#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NBD {

using namespace NThreading;

using namespace NCloud::NBlockStore::NClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString DefaultConfigFile = "/Berkanavt/nbs-server/cfg/nbs-client.txt";
const TString DefaultIamTokenFile = "~/.nbs-client/iam-token";

////////////////////////////////////////////////////////////////////////////////

TString ResolvePath(const TString& path)
{
    if (path.StartsWith('~')) {
        return TStringBuilder() << GetHomeDir() << path.substr(1);
    }

    return path;
}

TString GetIamToken(const TString& iamTokenFile)
{
    auto filename = ResolvePath(iamTokenFile);
    TFile file;
    try {
        file = TFile(filename, EOpenModeFlag::OpenExisting | EOpenModeFlag::RdOnly);
    } catch (...) {
        return {};
    }

    if (!file.IsOpen()) {
        return {};
    }

    return Strip(TFileInput(file).ReadAll());
}

////////////////////////////////////////////////////////////////////////////////

static const TDuration WaitTimeout = TDuration::Seconds(10);

TNetworkAddress CreateListenAddress(const TOptions& options)
{
    if (options.ListenUnixSocketPath) {
        return TNetworkAddress(TUnixSocketPath(options.ListenUnixSocketPath));
    } else if (options.ListenAddress) {
        return TNetworkAddress(options.ListenAddress, options.ListenPort);
    } else {
        return TNetworkAddress(options.ListenPort);
    }
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

    switch (Options->DeviceMode) {
        case EDeviceMode::Null:
            InitNullClient();
            InitClientSession();
            break;

        case EDeviceMode::Proxy:
            InitControlClient();
            InitClientSession();
            break;

        case EDeviceMode::Endpoint:
            InitControlClient();
            break;
    }
}

void TBootstrap::InitClientConfig()
{
    NProto::TClientAppConfig appConfig;
    if (Options->ConfigFile) {
        ParseFromTextFormat(Options->ConfigFile, appConfig);
    } else if (NFs::Exists(DefaultConfigFile)) {
        ParseFromTextFormat(DefaultConfigFile, appConfig);
    }

    auto& clientConfig = *appConfig.MutableClientConfig();
    if (Options->Host) {
        clientConfig.SetHost(Options->Host);
    }
    if (Options->InsecurePort) {
        clientConfig.SetInsecurePort(Options->InsecurePort);
    }
    if (Options->SecurePort) {
        clientConfig.SetSecurePort(Options->SecurePort);
    }
    if (clientConfig.GetHost() == "localhost" &&
        clientConfig.GetSecurePort() != 0)
    {
        // With TLS on transform localhost into fully qualified domain name.
        clientConfig.SetHost(FQDNHostName());
    }
    // AuthToken is set up using AuthConfig.
    clientConfig.ClearAuthToken();

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

    auto iamTokenFile = Options->IamTokenFile;
    if (!iamTokenFile) {
        auto& authConfig = appConfig.GetAuthConfig();
        if (authConfig.HasIamTokenFile()) {
            iamTokenFile = authConfig.GetIamTokenFile();
        } else {
            iamTokenFile = DefaultIamTokenFile;
        }
    }

    // Do not send token via insecure channel.
    if (clientConfig.GetSecurePort() != 0) {
        clientConfig.SetAuthToken(GetIamToken(iamTokenFile));
    }

    if (!clientConfig.GetClientId()) {
        clientConfig.SetClientId(CreateGuidAsString());
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

    if (Client) {
        Client->Start();
    }

    if (StatsUpdater) {
        StatsUpdater->Start();
    }

    if (ClientEndpoint) {
        ClientEndpoint->Start();
    }

    if (Scheduler) {
        Scheduler->Start();
    }

    auto listenAddress = CreateListenAddress(*Options);

    switch (Options->DeviceMode) {
        case EDeviceMode::Null:
        case EDeviceMode::Proxy:
            StartNbdServer(listenAddress);
            break;

        case EDeviceMode::Endpoint:
            StartNbdEndpoint();
            break;
    }

    if (Options->ConnectDevice) {
#if defined(_linux_)
        if (Options->Netlink) {
            NbdDevice = CreateNetlinkDevice(
                Logging,
                listenAddress,
                Options->ConnectDevice,
                Options->RequestTimeout,
                Options->ConnectionTimeout,
                Options->Reconfigure);
        } else {
            // The only case we want kernel to retry requests is when the socket
            // is dead due to nbd server restart. And since we can't configure
            // ioctl device to use a new socket, request timeout effectively
            // becomes connection timeout
            NbdDevice = CreateDevice(
                Logging,
                listenAddress,
                Options->ConnectDevice,
                Options->ConnectionTimeout);
        }
        auto future = NbdDevice->Start();
        const auto& status = future.GetValue();
        if (HasError(status)) {
            ythrow yexception() << status.GetMessage();
        }
#else
        ythrow yexception() << "unsupported platform";
#endif
    }
}

void TBootstrap::Stop()
{
    if (NbdDevice) {
        NbdDevice->Stop(Options->Disconnect);
    }

    switch (Options->DeviceMode) {
        case EDeviceMode::Null:
        case EDeviceMode::Proxy:
            StopNbdServer();
            break;

        case EDeviceMode::Endpoint:
            StopNbdEndpoint();
            break;
    }

    if (Session) {
        auto future = Session->UnmountVolume();
        const auto& response = future.GetValue(WaitTimeout);
        CheckError(response);
    }

    if (Scheduler) {
        Scheduler->Stop();
    }

    if (ClientEndpoint) {
        ClientEndpoint->Stop();
    }

    if (StatsUpdater) {
        StatsUpdater->Stop();
    }

    if (Client) {
        Client->Stop();
    }

    if (Monitoring) {
        Monitoring->Stop();
    }

    if (Logging) {
        Logging->Stop();
    }
}

void TBootstrap::InitNullClient()
{
    NProto::TNullServiceConfig config;
    config.SetDiskBlockSize(Options->NullBlockSize);
    config.SetDiskBlocksCount(Options->NullBlocksCount);

    ClientEndpoint = CreateNullService(config);
}

void TBootstrap::InitControlClient()
{
    auto rootGroup = Monitoring->GetCounters()
        ->GetSubgroup("counters", "blockstore");

    auto clientGroup = rootGroup->GetSubgroup("component", "client");

    RequestStats = CreateClientRequestStats(
        clientGroup,
        Timer,
        EHistogramCounterOption::ReportMultipleCounters);

    VolumeStats = CreateVolumeStats(
        Monitoring,
        {},
        EVolumeStatsType::EClientStats,
        Timer);

    ClientStats = CreateClientStats(
        ClientConfig,
        Monitoring,
        RequestStats,
        VolumeStats,
        ClientConfig->GetInstanceId());

    auto [client, error] = CreateClient(
        ClientConfig,
        Timer,
        Scheduler,
        Logging,
        Monitoring,
        ClientStats);

    Y_ABORT_UNLESS(!HasError(error));
    Client = std::move(client);

    StatsUpdater = CreateStatsUpdater(
        Timer,
        Scheduler,
        CreateIncompleteRequestProcessor(
            ClientStats,
            {})  // TODO: fill incompleteRequestProviders (NBS-2167)
    );

    ClientEndpoint = Client->CreateEndpoint();

    auto retryPolicy = CreateRetryPolicy(ClientConfig, std::nullopt);

    ClientEndpoint = CreateDurableClient(
        ClientConfig,
        std::move(ClientEndpoint),
        std::move(retryPolicy),
        Logging,
        Timer,
        Scheduler,
        RequestStats,
        VolumeStats);
}

void TBootstrap::InitClientSession()
{
    TSessionConfig sessionConfig;
    sessionConfig.DiskId = Options->DiskId;
    sessionConfig.MountToken = Options->MountToken;
    sessionConfig.AccessMode = Options->AccessMode;
    sessionConfig.MountMode = Options->MountMode;
    if (Options->ThrottlingDisabled) {
        SetProtoFlag(
            sessionConfig.MountFlags,
            NProto::MF_THROTTLING_DISABLED);
    }
    sessionConfig.ClientVersionInfo = GetFullVersionString();

    NProto::TEncryptionSpec encryptionSpec;
    encryptionSpec.SetMode(Options->EncryptionMode);
    if (Options->EncryptionMode != NProto::NO_ENCRYPTION) {
        encryptionSpec.MutableKeyPath()->SetFilePath(
            Options->EncryptionKeyPath);
    }
    sessionConfig.EncryptionSpec = encryptionSpec;

    Session = CreateSession(
        Timer,
        Scheduler,
        Logging,
        RequestStats,
        VolumeStats,
        ClientEndpoint,
        ClientConfig,
        sessionConfig);
}

void TBootstrap::StartNbdServer(TNetworkAddress listenAddress)
{
    auto mountFuture = Session->MountVolume();
    const auto& mountResponse = mountFuture.GetValue(WaitTimeout);
    CheckError(mountResponse);

    TStorageOptions options;

    const auto& volume = mountResponse.GetVolume();
    options.BlockSize = volume.GetBlockSize();
    options.BlocksCount = volume.GetBlocksCount();
    options.CheckpointId = Options->CheckpointId;

    auto handlerFactory = CreateServerHandlerFactory(
        CreateDefaultDeviceHandlerFactory(),
        Logging,
        Session,
        CreateServerStatsStub(),
        CreateErrorHandlerStub(),
        options);

    TServerConfig serverConfig {
        .ThreadsCount = 1,  // there will be just one endpoint
        .MaxInFlightBytesPerThread = Options->MaxInFlightBytes,
        .Affinity = {}
    };

    NbdServer = CreateServer(Logging, serverConfig);
    NbdServer->Start();

    auto future = NbdServer->StartEndpoint(
        listenAddress,
        std::move(handlerFactory));
    CheckError(future.GetValue(WaitTimeout));
}

void TBootstrap::StopNbdServer()
{
    if (NbdServer) {
        NbdServer->Stop();
    }
}

void TBootstrap::StartNbdEndpoint()
{
    auto ctx = MakeIntrusive<TCallContext>();
    auto request = std::make_shared<NProto::TStartEndpointRequest>();
    request->SetUnixSocketPath(Options->ListenUnixSocketPath);
    request->SetDiskId(Options->DiskId);
    request->SetIpcType(NProto::IPC_NBD);
    request->SetClientId(CreateGuidAsString());
    request->SetVolumeAccessMode(Options->AccessMode);
    request->SetVolumeMountMode(Options->MountMode);
    ui32 mountFlags = 0;
    if (Options->ThrottlingDisabled) {
        SetProtoFlag(
            mountFlags,
            NProto::MF_THROTTLING_DISABLED);
    }
    request->SetMountFlags(mountFlags);
    request->SetUnalignedRequestsDisabled(Options->UnalignedRequestsDisabled);
    request->SetClientVersionInfo(GetFullVersionString());

    auto& encryptionSpec = *request->MutableEncryptionSpec();
    encryptionSpec.SetMode(Options->EncryptionMode);
    if (Options->EncryptionMode != NProto::NO_ENCRYPTION) {
        encryptionSpec.MutableKeyPath()->SetFilePath(
            Options->EncryptionKeyPath);
    }

    auto future = ClientEndpoint->StartEndpoint(
        std::move(ctx),
        std::move(request));
    CheckError(future.GetValue(WaitTimeout));
}

void TBootstrap::StopNbdEndpoint()
{
    auto ctx = MakeIntrusive<TCallContext>();
    auto request = std::make_shared<NProto::TStopEndpointRequest>();
    request->SetUnixSocketPath(Options->ListenUnixSocketPath);

    auto future = ClientEndpoint->StopEndpoint(
        std::move(ctx),
        std::move(request));
    CheckError(future.GetValue(WaitTimeout));
}

}   // namespace NCloud::NBlockStore::NBD
