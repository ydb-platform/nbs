#include "plugin.h"

#include <cloud/vm/api/blockstore-plugin.h>

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/session.h>
#include <cloud/blockstore/libs/client/throttling.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/nbd/client.h>
#include <cloud/blockstore/libs/service/service_test.h>
#include <cloud/blockstore/libs/throttling/throttler.h>
#include <cloud/blockstore/libs/throttling/throttler_metrics.h>
#include <cloud/blockstore/libs/throttling/throttler_policy.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/mutex.h>

#include <functional>

namespace NCloud::NBlockStore::NPlugin {

using namespace NClient;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

static const TString DefaultDiskId = "path/to/test_volume";
static const TString DefaultMountToken = "mountToken";
static const TString DefaultSocketPath = "path/to/test_socket";
static constexpr ui32 DefaultBlocksCount = 1024;

static const TDuration RequestTimeout = TDuration::Seconds(5);

////////////////////////////////////////////////////////////////////////////////

enum class EEndpointType
{
    Tcp,
    UdsGrpc,
    UdsNbd,
};

////////////////////////////////////////////////////////////////////////////////

NProto::EClientIpcType GetIpcType(EEndpointType endpointType)
{
    switch (endpointType) {
        case EEndpointType::Tcp:
        case EEndpointType::UdsGrpc:
            return NProto::IPC_GRPC;
        case EEndpointType::UdsNbd:
            return NProto::IPC_NBD;
        default:
            Y_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TTestClient final: public IClient
{
    IBlockStorePtr TcpEndpoint;
    IBlockStorePtr UdsEndpoint;

    void Start() override
    {}
    void Stop() override
    {}

    IBlockStorePtr CreateEndpoint() override
    {
        Y_ABORT("Not implemented");
    }

    IBlockStorePtr CreateDataEndpoint() override
    {
        UNIT_ASSERT(TcpEndpoint);
        return TcpEndpoint;
    }

    IBlockStorePtr CreateDataEndpoint(const TString& socketPath) override
    {
        Y_UNUSED(socketPath);

        UNIT_ASSERT(UdsEndpoint);
        return UdsEndpoint;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestThrottlerPolicy final: public IThrottlerPolicy
{
    ui32 Count = 0;

    TDuration SuggestDelay(
        TInstant now,
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        size_t byteCount) override
    {
        Y_UNUSED(now);
        Y_UNUSED(mediaKind);
        Y_UNUSED(requestType);
        Y_UNUSED(byteCount);

        ++Count;
        return TDuration::Zero();
    }

    double CalculateCurrentSpentBudgetShare(TInstant ts) const override
    {
        Y_UNUSED(ts);

        return 0.0;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestNbdClient final: public NBD::IClient
{
    IBlockStorePtr Endpoint;

    void Start() override
    {}
    void Stop() override
    {}

    IBlockStorePtr CreateEndpoint(
        const TNetworkAddress& connectAddress,
        NBD::IClientHandlerPtr handler,
        IBlockStorePtr volumeClient) override
    {
        Y_UNUSED(connectAddress);
        Y_UNUSED(handler);
        Y_UNUSED(volumeClient);

        UNIT_ASSERT(Endpoint);
        return Endpoint;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
private:
    ITimerPtr Timer;
    ISchedulerPtr Scheduler;
    ILoggingServicePtr Logging;
    IMonitoringServicePtr Monitoring;
    IRequestStatsPtr RequestStats;
    IVolumeStatsPtr VolumeStats;
    IServerStatsPtr ClientStats;
    IThrottlerPtr Throttler;
    TClientAppConfigPtr Config;

    std::shared_ptr<TTestClient> Client;
    std::shared_ptr<TTestNbdClient> NbdClient;

public:
    TBootstrap(
        EEndpointType endpointType,
        IBlockStorePtr clientEndpoint,
        ITimerPtr timer,
        ISchedulerPtr scheduler)
        : Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Logging(CreateLoggingService("console"))
        , Monitoring(CreateMonitoringServiceStub())
        , RequestStats(CreateRequestStatsStub())
        , VolumeStats(CreateVolumeStatsStub())
        , ClientStats(CreateServerStatsStub())
    {
        NProto::TClientAppConfig clientAppConfig;
        auto& clientConfig = *clientAppConfig.MutableClientConfig();
        clientConfig.SetIpcType(GetIpcType(endpointType));
        clientConfig.SetClientId("testClientId");
        Config = std::make_shared<TClientAppConfig>(std::move(clientAppConfig));

        Client = std::make_shared<TTestClient>();
        NbdClient = std::make_shared<TTestNbdClient>();

        switch (endpointType) {
            case EEndpointType::Tcp:
                Client->TcpEndpoint = std::move(clientEndpoint);
                break;
            case EEndpointType::UdsGrpc:
                Client->UdsEndpoint = std::move(clientEndpoint);
                break;
            case EEndpointType::UdsNbd:
                NbdClient->Endpoint = clientEndpoint;
                Client->UdsEndpoint = clientEndpoint;
                break;
        }
    }

    void Start()
    {
        if (Scheduler) {
            Scheduler->Start();
        }

        if (Logging) {
            Logging->Start();
        }

        if (Monitoring) {
            Monitoring->Start();
        }

        if (Client) {
            Client->Start();
        }

        if (NbdClient) {
            NbdClient->Start();
        }

        if (Throttler) {
            Throttler->Start();
        }
    }

    void Stop()
    {
        if (Throttler) {
            Throttler->Stop();
        }

        if (NbdClient) {
            NbdClient->Stop();
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

        if (Scheduler) {
            Scheduler->Stop();
        }
    }

    ITimerPtr GetTimer()
    {
        return Timer;
    }

    ISchedulerPtr GetScheduler()
    {
        return Scheduler;
    }

    ILoggingServicePtr GetLogging()
    {
        return Logging;
    }

    IMonitoringServicePtr GetMonitoring()
    {
        return Monitoring;
    }

    IVolumeStatsPtr GetVolumeStats()
    {
        return VolumeStats;
    }

    IServerStatsPtr GetClientStats()
    {
        return ClientStats;
    }

    IRequestStatsPtr GetRequestStats()
    {
        return RequestStats;
    }

    IThrottlerPtr GetThrottler()
    {
        return Throttler;
    }

    IClientPtr GetClient()
    {
        return Client;
    }

    NBD::IClientPtr GetNbdClient()
    {
        return NbdClient;
    }

    TClientAppConfigPtr GetConfig()
    {
        return Config;
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TBootstrap> CreateBootstrap(
    EEndpointType endpointType,
    std::shared_ptr<TTestService> client,
    ISchedulerPtr scheduler = {})
{
    if (!scheduler) {
        scheduler = CreateScheduler();
    }

    return std::make_unique<TBootstrap>(
        endpointType,
        std::move(client),
        CreateWallClockTimer(),
        std::move(scheduler));
}

////////////////////////////////////////////////////////////////////////////////

struct TCompletion: BlockPlugin_Completion
{
    using TCallbackFn = void(BlockPlugin_Completion* comp);
    using TCallback = std::function<TCallbackFn>;

    TCompletion(TCallback callback)
    {
        custom_data = new TCallback(std::move(callback));
    }

    ~TCompletion()
    {
        delete static_cast<TCallback*>(custom_data);
    }

    static int CompleteRequest(
        BlockPluginHost* host,
        BlockPlugin_Completion* comp)
    {
        Y_UNUSED(host);

        auto* callback = static_cast<TCallback*>(comp->custom_data);
        if (callback) {
            (*callback)(comp);
        }

        Cerr << "completed " << comp->id << ": " << comp->status << Endl;
        return BLOCK_PLUGIN_E_OK;
    }
};

////////////////////////////////////////////////////////////////////////////////

void LogMessage(BlockPluginHost* host, const char* message)
{
    Y_UNUSED(host);
    Cerr << message << Endl;
}

int CompleteRequest(BlockPluginHost* host, BlockPlugin_Completion* comp)
{
    Y_UNUSED(host);
    Cerr << "completed " << comp->id << Endl;
    return BLOCK_PLUGIN_E_OK;
}

BlockPluginHost gBlockPluginHost = {
    .magic = BLOCK_PLUGIN_MAGIC,
    .version_major = BLOCK_PLUGIN_API_VERSION_MAJOR,
    .version_minor = BLOCK_PLUGIN_API_VERSION_MINOR,

    .state = nullptr,

    .complete_request = CompleteRequest,
    .log_message = LogMessage,
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPluginTest)
{
    void ShouldMountVolumeSync(EEndpointType endpointType)
    {
        auto client = std::make_shared<TTestService>();

        bool started = false;
        bool stopped = false;
        client->StartHandler = [&]()
        {
            started = true;
        };
        client->StopHandler = [&]()
        {
            stopped = true;
        };

        client->MountVolumeHandler =
            [](std::shared_ptr<NProto::TMountVolumeRequest> request)
        {
            UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
            UNIT_ASSERT_EQUAL(request->GetToken(), DefaultMountToken);

            NProto::TMountVolumeResponse response;
            response.SetSessionId("1");

            auto& volume = *response.MutableVolume();
            volume.SetDiskId(request->GetDiskId());
            volume.SetBlockSize(DefaultBlockSize);
            volume.SetBlocksCount(DefaultBlocksCount);

            return MakeFuture(response);
        };

        client->UnmountVolumeHandler =
            [](std::shared_ptr<NProto::TUnmountVolumeRequest> request)
        {
            UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
            UNIT_ASSERT_EQUAL(request->GetSessionId(), "1");
            NProto::TUnmountVolumeResponse response;
            return MakeFuture(response);
        };

        auto bootstrap = CreateBootstrap(endpointType, client);
        bootstrap->Start();

        auto plugin = CreatePlugin(
            &gBlockPluginHost,
            bootstrap->GetTimer(),
            bootstrap->GetScheduler(),
            bootstrap->GetLogging(),
            bootstrap->GetMonitoring(),
            bootstrap->GetRequestStats(),
            bootstrap->GetVolumeStats(),
            bootstrap->GetClientStats(),
            bootstrap->GetThrottler(),
            bootstrap->GetClient(),
            bootstrap->GetNbdClient(),
            bootstrap->GetConfig());

        NProto::TPluginMountConfig mountConfig;
        mountConfig.SetDiskId(DefaultDiskId);
        if (endpointType != EEndpointType::Tcp) {
            mountConfig.SetUnixSocketPath(DefaultSocketPath);
        }

        BlockPlugin_Volume volume = {};

        TSessionConfig sessionConfig;
        sessionConfig.DiskId = mountConfig.GetDiskId();
        sessionConfig.MountToken = DefaultMountToken;
        sessionConfig.IpcType = GetIpcType(endpointType);

        UNIT_ASSERT(!started);
        UNIT_ASSERT(!stopped);

        int error = plugin->MountVolume(mountConfig, sessionConfig, &volume);
        UNIT_ASSERT_EQUAL(error, BLOCK_PLUGIN_E_OK);
        UNIT_ASSERT(started);
        UNIT_ASSERT(!stopped);

        error = plugin->UnmountVolume(&volume);
        UNIT_ASSERT_EQUAL(error, BLOCK_PLUGIN_E_OK);
        UNIT_ASSERT(stopped);

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldMountVolumeSync_Tcp)
    {
        ShouldMountVolumeSync(EEndpointType::Tcp);
    }

    Y_UNIT_TEST(ShouldMountVolumeSync_UdsGrpc)
    {
        ShouldMountVolumeSync(EEndpointType::UdsGrpc);
    }

    Y_UNIT_TEST(ShouldMountVolumeSync_UdsNbd)
    {
        ShouldMountVolumeSync(EEndpointType::UdsNbd);
    }

    void ShouldMountVolumeAsync(EEndpointType endpointType)
    {
        auto client = std::make_shared<TTestService>();

        bool started = false;
        bool stopped = false;
        client->StartHandler = [&]()
        {
            started = true;
        };
        client->StopHandler = [&]()
        {
            stopped = true;
        };

        client->MountVolumeHandler =
            [](std::shared_ptr<NProto::TMountVolumeRequest> request)
        {
            UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
            UNIT_ASSERT_EQUAL(request->GetToken(), DefaultMountToken);

            NProto::TMountVolumeResponse response;
            response.SetSessionId("1");

            auto& volume = *response.MutableVolume();
            volume.SetDiskId(request->GetDiskId());
            volume.SetBlockSize(DefaultBlockSize);
            volume.SetBlocksCount(DefaultBlocksCount);

            return MakeFuture(response);
        };

        client->UnmountVolumeHandler =
            [](std::shared_ptr<NProto::TUnmountVolumeRequest> request)
        {
            UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
            UNIT_ASSERT_EQUAL(request->GetSessionId(), "1");
            NProto::TUnmountVolumeResponse response;
            return MakeFuture(response);
        };

        auto bootstrap = CreateBootstrap(endpointType, client);
        bootstrap->Start();

        NProto::TPluginMountConfig mountConfig;
        mountConfig.SetDiskId(DefaultDiskId);
        if (endpointType != EEndpointType::Tcp) {
            mountConfig.SetUnixSocketPath(DefaultSocketPath);
        }

        BlockPlugin_Volume volume = {};

        BlockPluginHost host = gBlockPluginHost;
        host.complete_request = TCompletion::CompleteRequest;

        auto plugin = CreatePlugin(
            &host,
            bootstrap->GetTimer(),
            bootstrap->GetScheduler(),
            bootstrap->GetLogging(),
            bootstrap->GetMonitoring(),
            bootstrap->GetRequestStats(),
            bootstrap->GetVolumeStats(),
            bootstrap->GetClientStats(),
            bootstrap->GetThrottler(),
            bootstrap->GetClient(),
            bootstrap->GetNbdClient(),
            bootstrap->GetConfig());

        UNIT_ASSERT(!started);
        UNIT_ASSERT(!stopped);

        {
            TPromise<int> completionPromise = NewPromise<int>();
            TCompletion comp([&](BlockPlugin_Completion* comp)
                             { completionPromise.SetValue(comp->status); });

            TSessionConfig sessionConfig;
            sessionConfig.DiskId = mountConfig.GetDiskId();
            sessionConfig.MountToken = DefaultMountToken;
            sessionConfig.IpcType = GetIpcType(endpointType);

            int error = plugin->MountVolumeAsync(
                mountConfig,
                sessionConfig,
                &volume,
                &comp);
            UNIT_ASSERT_EQUAL(error, BLOCK_PLUGIN_E_OK);

            int status = completionPromise.GetFuture().GetValue(RequestTimeout);
            UNIT_ASSERT_EQUAL(status, BP_COMPLETION_MOUNT_FINISHED);

            UNIT_ASSERT(started);
            UNIT_ASSERT(!stopped);
        }

        {
            TPromise<int> completionPromise = NewPromise<int>();
            TCompletion comp([&](BlockPlugin_Completion* comp)
                             { completionPromise.SetValue(comp->status); });

            int error = plugin->UnmountVolumeAsync(&volume, &comp);
            UNIT_ASSERT_EQUAL(error, BLOCK_PLUGIN_E_OK);

            int status = completionPromise.GetFuture().GetValue(RequestTimeout);
            UNIT_ASSERT_EQUAL(status, BP_COMPLETION_UNMOUNT_FINISHED);

            UNIT_ASSERT(stopped);
        }

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldMountVolumeAsync_Tcp)
    {
        ShouldMountVolumeAsync(EEndpointType::Tcp);
    }

    Y_UNIT_TEST(ShouldMountVolumeAsync_UdsGrpc)
    {
        ShouldMountVolumeAsync(EEndpointType::UdsGrpc);
    }

    Y_UNIT_TEST(ShouldMountVolumeAsync_UdsNbd)
    {
        ShouldMountVolumeAsync(EEndpointType::UdsNbd);
    }

    void ShouldReadWriteBlocks(EEndpointType endpointType)
    {
        auto client = std::make_shared<TTestService>();

        client->MountVolumeHandler =
            [](std::shared_ptr<NProto::TMountVolumeRequest> request)
        {
            UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
            UNIT_ASSERT_EQUAL(request->GetToken(), DefaultMountToken);

            NProto::TMountVolumeResponse response;
            response.SetSessionId("1");

            auto& volume = *response.MutableVolume();
            volume.SetDiskId(request->GetDiskId());
            volume.SetBlockSize(DefaultBlockSize);
            volume.SetBlocksCount(1024);

            return MakeFuture(response);
        };

        client->UnmountVolumeHandler =
            [](std::shared_ptr<NProto::TUnmountVolumeRequest> request)
        {
            UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
            UNIT_ASSERT_EQUAL(request->GetSessionId(), "1");
            NProto::TUnmountVolumeResponse response;
            return MakeFuture(response);
        };

        client->ReadBlocksLocalHandler =
            [](std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
            UNIT_ASSERT_EQUAL(request->GetSessionId(), "1");
            return MakeFuture(NProto::TReadBlocksLocalResponse());
        };

        client->WriteBlocksLocalHandler =
            [](std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
        {
            UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
            UNIT_ASSERT_EQUAL(request->GetSessionId(), "1");
            return MakeFuture(NProto::TWriteBlocksLocalResponse());
        };

        client->ZeroBlocksHandler =
            [](std::shared_ptr<NProto::TZeroBlocksRequest> request)
        {
            UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
            UNIT_ASSERT_EQUAL(request->GetSessionId(), "1");
            return MakeFuture(NProto::TZeroBlocksResponse());
        };

        auto bootstrap = CreateBootstrap(endpointType, client);
        bootstrap->Start();

        NProto::TPluginMountConfig mountConfig;
        mountConfig.SetDiskId(DefaultDiskId);
        if (endpointType != EEndpointType::Tcp) {
            mountConfig.SetUnixSocketPath(DefaultSocketPath);
        }

        BlockPlugin_Volume volume = {};
        BlockPluginHost host = gBlockPluginHost;
        host.complete_request = TCompletion::CompleteRequest;

        auto plugin = CreatePlugin(
            &host,
            bootstrap->GetTimer(),
            bootstrap->GetScheduler(),
            bootstrap->GetLogging(),
            bootstrap->GetMonitoring(),
            bootstrap->GetRequestStats(),
            bootstrap->GetVolumeStats(),
            bootstrap->GetClientStats(),
            bootstrap->GetThrottler(),
            bootstrap->GetClient(),
            bootstrap->GetNbdClient(),
            bootstrap->GetConfig());

        TSessionConfig sessionConfig;
        sessionConfig.DiskId = mountConfig.GetDiskId();
        sessionConfig.MountToken = DefaultMountToken;
        sessionConfig.IpcType = GetIpcType(endpointType);

        int error = plugin->MountVolume(mountConfig, sessionConfig, &volume);
        UNIT_ASSERT_EQUAL(error, BLOCK_PLUGIN_E_OK);

        {
            BlockPlugin_IOVector vector = {};

            BlockPlugin_ReadBlocks request = {};
            request.header.type = BLOCK_PLUGIN_READ_BLOCKS;
            request.header.size = sizeof(request);
            request.bp_iov = &vector;

            TPromise<int> completionPromise = NewPromise<int>();
            TCompletion comp([&](BlockPlugin_Completion* comp)
                             { completionPromise.SetValue(comp->status); });

            error = plugin->SubmitRequest(&volume, &request.header, &comp);
            UNIT_ASSERT_EQUAL(error, BLOCK_PLUGIN_E_OK);

            int status = completionPromise.GetFuture().GetValue(RequestTimeout);
            UNIT_ASSERT_EQUAL(status, BP_COMPLETION_READ_FINISHED);
        }

        {
            BlockPlugin_IOVector vector = {};

            BlockPlugin_WriteBlocks request = {};
            request.header.type = BLOCK_PLUGIN_WRITE_BLOCKS;
            request.header.size = sizeof(request);
            request.bp_iov = &vector;

            TPromise<int> completionPromise = NewPromise<int>();
            TCompletion comp([&](BlockPlugin_Completion* comp)
                             { completionPromise.SetValue(comp->status); });

            error = plugin->SubmitRequest(&volume, &request.header, &comp);
            UNIT_ASSERT_EQUAL(error, BLOCK_PLUGIN_E_OK);

            int status = completionPromise.GetFuture().GetValue(RequestTimeout);
            UNIT_ASSERT_EQUAL(status, BP_COMPLETION_WRITE_FINISHED);
        }

        {
            BlockPlugin_ZeroBlocks request = {};
            request.header.type = BLOCK_PLUGIN_ZERO_BLOCKS;
            request.header.size = sizeof(request);

            TPromise<int> completionPromise = NewPromise<int>();
            TCompletion comp([&](BlockPlugin_Completion* comp)
                             { completionPromise.SetValue(comp->status); });

            error = plugin->SubmitRequest(&volume, &request.header, &comp);
            UNIT_ASSERT_EQUAL(error, BLOCK_PLUGIN_E_OK);

            int status = completionPromise.GetFuture().GetValue(RequestTimeout);
            UNIT_ASSERT_EQUAL(status, BP_COMPLETION_ZERO_FINISHED);
        }

        error = plugin->UnmountVolume(&volume);
        UNIT_ASSERT_EQUAL(error, BLOCK_PLUGIN_E_OK);

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldReadWriteBlocks_Tcp)
    {
        ShouldReadWriteBlocks(EEndpointType::Tcp);
    }

    Y_UNIT_TEST(ShouldReadWriteBlocks_UdsGrpc)
    {
        ShouldReadWriteBlocks(EEndpointType::UdsGrpc);
    }

    Y_UNIT_TEST(ShouldReadWriteBlocks_UdsNbd)
    {
        ShouldReadWriteBlocks(EEndpointType::UdsNbd);
    }

    void ShouldHandleRepeatedMountWithoutUnmount(EEndpointType endpointType)
    {
        auto client = std::make_shared<TTestService>();

        client->MountVolumeHandler =
            [](std::shared_ptr<NProto::TMountVolumeRequest> request)
        {
            UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
            UNIT_ASSERT_EQUAL(request->GetToken(), DefaultMountToken);

            NProto::TMountVolumeResponse response;
            response.SetSessionId("1");

            auto& volume = *response.MutableVolume();
            volume.SetDiskId(request->GetDiskId());
            volume.SetBlockSize(DefaultBlockSize);
            volume.SetBlocksCount(DefaultBlocksCount);

            return MakeFuture(response);
        };

        client->UnmountVolumeHandler =
            [](std::shared_ptr<NProto::TUnmountVolumeRequest> request)
        {
            UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
            UNIT_ASSERT_EQUAL(request->GetSessionId(), "1");
            NProto::TUnmountVolumeResponse response;
            return MakeFuture(response);
        };

        auto bootstrap = CreateBootstrap(endpointType, client);
        bootstrap->Start();

        auto plugin = CreatePlugin(
            &gBlockPluginHost,
            bootstrap->GetTimer(),
            bootstrap->GetScheduler(),
            bootstrap->GetLogging(),
            bootstrap->GetMonitoring(),
            bootstrap->GetRequestStats(),
            bootstrap->GetVolumeStats(),
            bootstrap->GetClientStats(),
            bootstrap->GetThrottler(),
            bootstrap->GetClient(),
            bootstrap->GetNbdClient(),
            bootstrap->GetConfig());

        NProto::TPluginMountConfig mountConfig;
        mountConfig.SetDiskId(DefaultDiskId);
        if (endpointType != EEndpointType::Tcp) {
            mountConfig.SetUnixSocketPath(DefaultSocketPath);
        }

        BlockPlugin_Volume volume = {};

        TSessionConfig sessionConfig;
        sessionConfig.DiskId = mountConfig.GetDiskId();
        sessionConfig.MountToken = DefaultMountToken;
        sessionConfig.IpcType = GetIpcType(endpointType);
        sessionConfig.AccessMode = NProto::VOLUME_ACCESS_READ_ONLY;
        sessionConfig.MountMode = NProto::VOLUME_MOUNT_REMOTE;

        int error = plugin->MountVolume(mountConfig, sessionConfig, &volume);
        UNIT_ASSERT_EQUAL(error, BLOCK_PLUGIN_E_OK);

        sessionConfig.AccessMode = NProto::VOLUME_ACCESS_READ_WRITE;
        sessionConfig.MountMode = NProto::VOLUME_MOUNT_LOCAL;

        error = plugin->MountVolume(mountConfig, sessionConfig, &volume);
        UNIT_ASSERT_EQUAL(error, BLOCK_PLUGIN_E_OK);

        error = plugin->UnmountVolume(&volume);
        UNIT_ASSERT_EQUAL(error, BLOCK_PLUGIN_E_OK);

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldHandleRepeatedMountWithoutUnmount_Tcp)
    {
        ShouldHandleRepeatedMountWithoutUnmount(EEndpointType::Tcp);
    }

    Y_UNIT_TEST(ShouldHandleRepeatedMountWithoutUnmount_UdsGrpc)
    {
        ShouldHandleRepeatedMountWithoutUnmount(EEndpointType::UdsGrpc);
    }

    Y_UNIT_TEST(ShouldHandleRepeatedMountWithoutUnmount_UdsNbd)
    {
        ShouldHandleRepeatedMountWithoutUnmount(EEndpointType::UdsNbd);
    }

    Y_UNIT_TEST(ClientShouldProvideIncompleteRequests)
    {
        auto client = std::make_shared<TTestService>();
        auto trigger = NewPromise<void>();

        client->MountVolumeHandler =
            [](std::shared_ptr<NProto::TMountVolumeRequest> request)
        {
            Y_UNUSED(request);
            return MakeFuture(NProto::TMountVolumeResponse());
        };

        client->UnmountVolumeHandler =
            [](std::shared_ptr<NProto::TUnmountVolumeRequest> request)
        {
            Y_UNUSED(request);
            return MakeFuture(NProto::TUnmountVolumeResponse());
        };

        client->ReadBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            Y_UNUSED(request);
            return trigger.GetFuture().Apply(
                [](const auto&) { return NProto::TReadBlocksLocalResponse(); });
        };

        client->WriteBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
        {
            Y_UNUSED(request);
            return trigger.GetFuture().Apply(
                [](const auto&)
                { return NProto::TWriteBlocksLocalResponse(); });
        };

        client->ZeroBlocksHandler =
            [&](std::shared_ptr<NProto::TZeroBlocksRequest> request)
        {
            Y_UNUSED(request);
            return trigger.GetFuture().Apply(
                [](const auto&) { return NProto::TZeroBlocksResponse(); });
        };

        auto bootstrap = CreateBootstrap(EEndpointType::Tcp, client);
        bootstrap->Start();

        NProto::TPluginMountConfig mountConfig;
        mountConfig.SetDiskId(DefaultDiskId);

        BlockPlugin_Volume volume = {};
        BlockPluginHost host = gBlockPluginHost;
        host.complete_request = TCompletion::CompleteRequest;

        auto plugin = CreatePlugin(
            &host,
            bootstrap->GetTimer(),
            bootstrap->GetScheduler(),
            bootstrap->GetLogging(),
            bootstrap->GetMonitoring(),
            bootstrap->GetRequestStats(),
            bootstrap->GetVolumeStats(),
            bootstrap->GetClientStats(),
            bootstrap->GetThrottler(),
            bootstrap->GetClient(),
            bootstrap->GetNbdClient(),
            bootstrap->GetConfig());

        TSessionConfig sessionConfig;
        sessionConfig.DiskId = mountConfig.GetDiskId();
        sessionConfig.MountToken = DefaultMountToken;
        sessionConfig.IpcType = NProto::IPC_GRPC;

        int error = plugin->MountVolume(mountConfig, sessionConfig, &volume);
        UNIT_ASSERT_EQUAL(error, BLOCK_PLUGIN_E_OK);

        TPromise<int> zeroPromise = NewPromise<int>();
        TCompletion zeroComp([&](BlockPlugin_Completion* comp)
                             { zeroPromise.SetValue(comp->status); });
        {
            BlockPlugin_ZeroBlocks request = {};
            request.header.type = BLOCK_PLUGIN_ZERO_BLOCKS;
            request.header.size = sizeof(request);

            error = plugin->SubmitRequest(&volume, &request.header, &zeroComp);
            UNIT_ASSERT_EQUAL(error, BLOCK_PLUGIN_E_OK);
        }

        TPromise<int> writePromise = NewPromise<int>();
        TCompletion writeComp([&](BlockPlugin_Completion* comp)
                              { writePromise.SetValue(comp->status); });
        {
            BlockPlugin_IOVector vector = {};

            BlockPlugin_WriteBlocks request = {};
            request.header.type = BLOCK_PLUGIN_WRITE_BLOCKS;
            request.header.size = sizeof(request);
            request.bp_iov = &vector;

            error = plugin->SubmitRequest(&volume, &request.header, &writeComp);
            UNIT_ASSERT_EQUAL(error, BLOCK_PLUGIN_E_OK);
        }

        TPromise<int> readPromise = NewPromise<int>();
        TCompletion readComp([&](BlockPlugin_Completion* comp)
                             { readPromise.SetValue(comp->status); });
        {
            BlockPlugin_IOVector vector = {};

            BlockPlugin_ReadBlocks request = {};
            request.header.type = BLOCK_PLUGIN_READ_BLOCKS;
            request.header.size = sizeof(request);
            request.bp_iov = &vector;

            error = plugin->SubmitRequest(&volume, &request.header, &readComp);
            UNIT_ASSERT_EQUAL(error, BLOCK_PLUGIN_E_OK);
        }

        TIncompleteRequestsCollector collector =
            [](TCallContext& callContext,
               IVolumeInfoPtr volumeInfo,
               NCloud::NProto::EStorageMediaKind mediaKind,
               EBlockStoreRequest requestType,
               TRequestTime time) -> size_t
        {
            Y_UNUSED(callContext);
            Y_UNUSED(volumeInfo);
            Y_UNUSED(requestType);
            Y_UNUSED(mediaKind);
            Y_UNUSED(time);
            return 1;
        };

        auto requestCount = plugin->CollectRequests(collector);
        UNIT_ASSERT_VALUES_EQUAL(3, requestCount);

        trigger.SetValue();

        int zeroStatus = zeroPromise.GetFuture().GetValue(RequestTimeout);
        UNIT_ASSERT_EQUAL(zeroStatus, BP_COMPLETION_ZERO_FINISHED);
        int writeStatus = writePromise.GetFuture().GetValue(RequestTimeout);
        UNIT_ASSERT_EQUAL(writeStatus, BP_COMPLETION_WRITE_FINISHED);
        int readStatus = readPromise.GetFuture().GetValue(RequestTimeout);
        UNIT_ASSERT_EQUAL(readStatus, BP_COMPLETION_READ_FINISHED);

        UNIT_ASSERT_VALUES_EQUAL(0, plugin->CollectRequests(collector));

        error = plugin->UnmountVolume(&volume);
        UNIT_ASSERT_EQUAL(error, BLOCK_PLUGIN_E_OK);

        bootstrap->Stop();
    }

    void ShouldThrottleRequests(EEndpointType endpointType)
    {
        auto client = std::make_shared<TTestService>();

        client->MountVolumeHandler =
            [](std::shared_ptr<NProto::TMountVolumeRequest> request)
        {
            UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
            UNIT_ASSERT_EQUAL(request->GetToken(), DefaultMountToken);

            NProto::TMountVolumeResponse response;
            response.SetSessionId("testSessionId");

            auto& volume = *response.MutableVolume();
            volume.SetDiskId(request->GetDiskId());
            volume.SetBlockSize(DefaultBlockSize);
            volume.SetBlocksCount(1024);

            return MakeFuture(response);
        };

        client->UnmountVolumeHandler =
            [](std::shared_ptr<NProto::TUnmountVolumeRequest> request)
        {
            UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
            UNIT_ASSERT_EQUAL(request->GetSessionId(), "testSessionId");
            return MakeFuture(NProto::TUnmountVolumeResponse());
        };

        client->ReadBlocksLocalHandler =
            [](std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
            UNIT_ASSERT_EQUAL(request->GetSessionId(), "testSessionId");
            return MakeFuture(NProto::TReadBlocksLocalResponse());
        };

        client->WriteBlocksLocalHandler =
            [](std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
        {
            UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
            UNIT_ASSERT_EQUAL(request->GetSessionId(), "testSessionId");
            return MakeFuture(NProto::TWriteBlocksLocalResponse());
        };

        client->ZeroBlocksHandler =
            [](std::shared_ptr<NProto::TZeroBlocksRequest> request)
        {
            UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
            UNIT_ASSERT_EQUAL(request->GetSessionId(), "testSessionId");
            return MakeFuture(NProto::TZeroBlocksResponse());
        };

        auto bootstrap = CreateBootstrap(endpointType, client);
        bootstrap->Start();

        NProto::TPluginMountConfig mountConfig;
        mountConfig.SetDiskId(DefaultDiskId);
        if (endpointType != EEndpointType::Tcp) {
            mountConfig.SetUnixSocketPath(DefaultSocketPath);
        }

        BlockPlugin_Volume volume = {};
        BlockPluginHost host = gBlockPluginHost;
        host.complete_request = TCompletion::CompleteRequest;

        auto volumeStats = CreateVolumeStats(
            bootstrap->GetMonitoring(),
            TDuration::Zero(),
            EVolumeStatsType::EClientStats,
            bootstrap->GetTimer());

        auto throttlerMetrics = CreateThrottlerMetrics(
            bootstrap->GetTimer(),
            bootstrap->GetMonitoring()->GetCounters(),
            "client");
        auto throttlerPolicy = std::make_shared<TTestThrottlerPolicy>();
        auto throttler = CreateThrottler(
            CreateClientThrottlerLogger(
                bootstrap->GetRequestStats(),
                bootstrap->GetLogging()),
            std::move(throttlerMetrics),
            throttlerPolicy,
            CreateClientThrottlerTracker(),
            bootstrap->GetTimer(),
            bootstrap->GetScheduler(),
            volumeStats);

        auto plugin = CreatePlugin(
            &host,
            bootstrap->GetTimer(),
            bootstrap->GetScheduler(),
            bootstrap->GetLogging(),
            bootstrap->GetMonitoring(),
            bootstrap->GetRequestStats(),
            volumeStats,
            bootstrap->GetClientStats(),
            std::move(throttler),
            bootstrap->GetClient(),
            bootstrap->GetNbdClient(),
            bootstrap->GetConfig());

        TSessionConfig sessionConfig;
        sessionConfig.DiskId = mountConfig.GetDiskId();
        sessionConfig.MountToken = DefaultMountToken;
        sessionConfig.IpcType = GetIpcType(endpointType);

        int error = plugin->MountVolume(mountConfig, sessionConfig, &volume);
        UNIT_ASSERT_EQUAL(error, BLOCK_PLUGIN_E_OK);

        NProto::TVolume tempVolume;
        tempVolume.SetDiskId(mountConfig.GetDiskId());
        volumeStats->MountVolume(
            tempVolume,
            bootstrap->GetConfig()->GetClientConfig().GetClientId(),
            "");

        UNIT_ASSERT_VALUES_EQUAL(0, throttlerPolicy->Count);

        {
            BlockPlugin_IOVector vector = {};

            BlockPlugin_ReadBlocks request = {};
            request.header.type = BLOCK_PLUGIN_READ_BLOCKS;
            request.header.size = sizeof(request);
            request.bp_iov = &vector;

            TPromise<int> completionPromise = NewPromise<int>();
            TCompletion comp([&](BlockPlugin_Completion* comp)
                             { completionPromise.SetValue(comp->status); });

            error = plugin->SubmitRequest(&volume, &request.header, &comp);
            UNIT_ASSERT_EQUAL(error, BLOCK_PLUGIN_E_OK);

            int status = completionPromise.GetFuture().GetValue(RequestTimeout);
            UNIT_ASSERT_EQUAL(status, BP_COMPLETION_READ_FINISHED);
        }

        UNIT_ASSERT_VALUES_EQUAL(1, throttlerPolicy->Count);

        {
            BlockPlugin_IOVector vector = {};

            BlockPlugin_WriteBlocks request = {};
            request.header.type = BLOCK_PLUGIN_WRITE_BLOCKS;
            request.header.size = sizeof(request);
            request.bp_iov = &vector;

            TPromise<int> completionPromise = NewPromise<int>();
            TCompletion comp([&](BlockPlugin_Completion* comp)
                             { completionPromise.SetValue(comp->status); });

            error = plugin->SubmitRequest(&volume, &request.header, &comp);
            UNIT_ASSERT_EQUAL(error, BLOCK_PLUGIN_E_OK);

            int status = completionPromise.GetFuture().GetValue(RequestTimeout);
            UNIT_ASSERT_EQUAL(status, BP_COMPLETION_WRITE_FINISHED);
        }

        UNIT_ASSERT_VALUES_EQUAL(2, throttlerPolicy->Count);

        {
            BlockPlugin_ZeroBlocks request = {};
            request.header.type = BLOCK_PLUGIN_ZERO_BLOCKS;
            request.header.size = sizeof(request);

            TPromise<int> completionPromise = NewPromise<int>();
            TCompletion comp([&](BlockPlugin_Completion* comp)
                             { completionPromise.SetValue(comp->status); });

            error = plugin->SubmitRequest(&volume, &request.header, &comp);
            UNIT_ASSERT_EQUAL(error, BLOCK_PLUGIN_E_OK);

            int status = completionPromise.GetFuture().GetValue(RequestTimeout);
            UNIT_ASSERT_EQUAL(status, BP_COMPLETION_ZERO_FINISHED);
        }

        UNIT_ASSERT_VALUES_EQUAL(3, throttlerPolicy->Count);

        error = plugin->UnmountVolume(&volume);
        UNIT_ASSERT_EQUAL(error, BLOCK_PLUGIN_E_OK);

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldThrottleRequests_Tcp)
    {
        ShouldThrottleRequests(EEndpointType::Tcp);
    }

    Y_UNIT_TEST(ShouldThrottleRequests_UdsGrpc)
    {
        ShouldThrottleRequests(EEndpointType::UdsGrpc);
    }

    Y_UNIT_TEST(ShouldThrottleRequests_UdsNbd)
    {
        ShouldThrottleRequests(EEndpointType::UdsNbd);
    }
}

}   // namespace NCloud::NBlockStore::NPlugin
