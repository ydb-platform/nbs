#include "socket_endpoint_listener.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/durable.h>
#include <cloud/blockstore/libs/client/session_test.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/endpoints/endpoint_listener.h>
#include <cloud/blockstore/libs/server/client_storage_factory.h>
#include <cloud/blockstore/libs/server/server.h>
#include <cloud/blockstore/libs/server/server_test.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service/service_test.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <google/protobuf/util/message_differencer.h>

#include <util/folder/path.h>
#include <util/generic/guid.h>
#include <util/generic/scope.h>
#include <util/system/mutex.h>

namespace NCloud::NBlockStore::NServer {

using namespace NThreading;

using namespace NCloud::NBlockStore::NClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

static constexpr int MODE0660 = S_IRGRP | S_IWGRP | S_IRUSR | S_IWUSR;

////////////////////////////////////////////////////////////////////////////////

using NStorage::NServer::IClientStorage;
using NStorage::NServer::IClientStoragePtr;

class TTestClientStorage
    : public IClientStorageFactory
    , public IClientStorage
    , public std::enable_shared_from_this<TTestClientStorage>
{
private:
    TMutex Lock;
    TSet<SOCKET> Sessions;

public:
    IClientStoragePtr CreateClientStorage(IBlockStorePtr service) override
    {
        Y_UNUSED(service);
        return shared_from_this();
    }

    void AddClient(
        const TSocketHolder& socket,
        NProto::ERequestSource source) override
    {
        Y_UNUSED(source);

        with_lock (Lock) {
            auto [it, inserted] = Sessions.emplace(socket);
            UNIT_ASSERT(inserted);
        }
    }

    void RemoveClient(const TSocketHolder& socket) override
    {
        with_lock (Lock) {
            auto it = Sessions.find(socket);
            UNIT_ASSERT(it != Sessions.end());
            Sessions.erase(it);
        }
    }

    void WaitForSessionCount(ui32 sessionCount) {
        while (true) {
            with_lock (Lock) {
                if (Sessions.size() == sessionCount) {
                    break;
                }
            }
            Sleep(TDuration::MilliSeconds(100));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(
    ILoggingServicePtr logging,
    const TString& unixSocketPath)
{
    NProto::TClientAppConfig clientAppConfig;
    auto& clientConfig = *clientAppConfig.MutableClientConfig();
    clientConfig.SetUnixSocketPath(unixSocketPath);
    clientConfig.SetClientId(CreateGuidAsString());

    auto monitoring = CreateMonitoringServiceStub();

    auto clientStats = CreateServerStatsStub();

    auto [client, error] = CreateClient(
        std::make_shared<TClientAppConfig>(clientAppConfig),
        CreateWallClockTimer(),
        CreateSchedulerStub(),
        std::move(logging),
        std::move(monitoring),
        std::move(clientStats));

    UNIT_ASSERT(!HasError(error));
    return client;
}

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
private:
    IServerPtr Server;
    ISocketEndpointListenerPtr EndpointListener;

    IClientPtr Client;
    IBlockStorePtr ClientEndpoint;

    std::shared_ptr<TTestSession> TestSession;

    ITimerPtr Timer;
    ISchedulerPtr Scheduler;

public:
    TBootstrap(
            IServerPtr server,
            ISocketEndpointListenerPtr endpointListener,
            IClientPtr client,
            IBlockStorePtr clientEndpoint,
            std::shared_ptr<TTestSession> testSession,
            ITimerPtr timer,
            ISchedulerPtr scheduler)
        : Server(std::move(server))
        , EndpointListener(std::move(endpointListener))
        , Client(std::move(client))
        , ClientEndpoint(std::move(clientEndpoint))
        , TestSession(std::move(testSession))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
    {}

    void Start()
    {
        if (Server) {
            Server->Start();
        }

        if (EndpointListener) {
            EndpointListener->Start();
        }

        if (Client) {
            Client->Start();
        }

        if (ClientEndpoint) {
            ClientEndpoint->Start();
        }

        if (Scheduler) {
            Scheduler->Start();
        }
    }

    void Stop()
    {
        if (Scheduler) {
            Scheduler->Stop();
        }

        if (ClientEndpoint) {
            ClientEndpoint->Stop();
        }

        if (Client) {
            Client->Stop();
        }

        if (EndpointListener) {
            EndpointListener->Stop();
        }

        if (Server) {
            Server->Stop();
        }
    }

    IServerPtr GetServer()
    {
        return Server;
    }

    ISocketEndpointListenerPtr GetEndpointListener()
    {
        return EndpointListener;
    }

    IClientPtr GetClient()
    {
        return Client;
    }

    IBlockStorePtr GetClientEndpoint()
    {
        return ClientEndpoint;
    }

    std::shared_ptr<TTestSession> GetTestSession()
    {
        return TestSession;
    }

    ITimerPtr GetTimer()
    {
        return Timer;
    }

    ISchedulerPtr GetScheduler()
    {
        return Scheduler;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    TString UnixSocketPath = CreateGuidAsString() + ".sock";
    TString ClientId = "testClientId";
    TString DiskId = "testDiskId";
    ui32 UnixSocketBacklog = 16;
    ui32 BlockSize = DefaultBlockSize;
    ui64 BlocksCount = 42;
};

////////////////////////////////////////////////////////////////////////////////

NProto::TMountVolumeResponse CreateMountVolumeResponse(TOptions options)
{
    NProto::TMountVolumeResponse response;
    response.MutableVolume()->SetDiskId(options.DiskId);
    response.MutableVolume()->SetBlockSize(options.BlockSize);
    response.MutableVolume()->SetBlocksCount(options.BlocksCount);
    return response;
}

////////////////////////////////////////////////////////////////////////////////

TBootstrap CreateBootstrap(const TOptions& options)
{
    auto testSession = std::make_shared<TTestSession>();

    testSession->EnsureVolumeMountedHandler =
        [=] () {
            return MakeFuture(CreateMountVolumeResponse(options));
        };

    TPortManager portManager;
    ui16 port = portManager.GetPort(9001);
    ui16 dataPort = portManager.GetPort(9002);

    TTestFactory testFactory;

    auto server = testFactory.CreateServerBuilder()
        .SetPort(port)
        .SetDataPort(dataPort)
        .BuildServer(std::make_shared<TTestService>());

    auto endpointListener = CreateSocketEndpointListener(
        testFactory.Logging,
        options.UnixSocketBacklog,
        MODE0660);
    endpointListener->SetClientStorageFactory(
        server->GetClientStorageFactory());

    NProto::TStartEndpointRequest request;
    request.SetUnixSocketPath(options.UnixSocketPath);
    request.SetDiskId(options.DiskId);

    NProto::TVolume volume;
    volume.SetDiskId(options.DiskId);
    volume.SetBlockSize(options.BlockSize);
    volume.SetBlocksCount(options.BlocksCount);

    {
        auto future = endpointListener->StartEndpoint(
            request,
            volume,
            testSession);
        const auto& error = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(error), error);
    }

    auto client = testFactory.CreateClientBuilder()
        .SetUnixSocketPath(options.UnixSocketPath)
        .SetPort(port)
        .SetDataPort(dataPort)
        .SetClientId(options.ClientId)
        .BuildClient();

    auto clientEndpoint = client->CreateDataEndpoint(options.UnixSocketPath);

    return TBootstrap(
        std::move(server),
        std::move(endpointListener),
        std::move(client),
        std::move(clientEndpoint),
        std::move(testSession),
        CreateWallClockTimer(),
        CreateScheduler());
};

////////////////////////////////////////////////////////////////////////////////

NProto::TError ReadBlocksLocal(IBlockStorePtr client, const TString& diskId)
{
    auto request = std::make_shared<NProto::TReadBlocksRequest>();
    request->SetDiskId(diskId);

    auto future = client->ReadBlocks(
        MakeIntrusive<TCallContext>(),
        std::move(request));

    const auto& response = future.GetValue(TDuration::Seconds(5));
    return response.GetError();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSocketEndpointListenerTest)
{
    Y_UNIT_TEST(ShouldHandleStartStopEndpoint)
    {
        auto logging = CreateLoggingService("console");
        TFsPath unixSocket(CreateGuidAsString() + ".sock");

        auto clientStorage = std::make_shared<TTestClientStorage>();
        auto listener = CreateSocketEndpointListener(logging, 16, MODE0660);
        listener->SetClientStorageFactory(clientStorage);
        listener->Start();
        Y_DEFER {
            listener->Stop();
        };

        {
            NProto::TStartEndpointRequest request;
            request.SetUnixSocketPath(unixSocket.GetPath());
            auto future = listener->StartEndpoint(request, {}, {});
            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(error), error);
        }

        auto client = CreateClient(logging, unixSocket.GetPath());

        auto clientEndpoint = client->CreateEndpoint();

        client->Start();
        clientEndpoint->Start();
        Y_DEFER {
            clientEndpoint->Stop();
            client->Stop();
        };

        clientStorage->WaitForSessionCount(1);

        {
            auto future = listener->StopEndpoint(unixSocket.GetPath());
            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(error), error);
        }

        clientStorage->WaitForSessionCount(0);
    }

    Y_UNIT_TEST(ShouldHandleClientDisconnection)
    {
        auto logging = CreateLoggingService("console");
        TFsPath unixSocket(CreateGuidAsString() + ".sock");

        auto clientStorage = std::make_shared<TTestClientStorage>();
        auto listener = CreateSocketEndpointListener(logging, 16, MODE0660);
        listener->SetClientStorageFactory(clientStorage);
        listener->Start();
        Y_DEFER {
            listener->Stop();
        };

        NProto::TStartEndpointRequest request;
        request.SetUnixSocketPath(unixSocket.GetPath());
        auto future = listener->StartEndpoint(request, {}, {});
        const auto& error = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(error), error);

        auto client = CreateClient(logging, unixSocket.GetPath());
        client->Start();
        Y_DEFER {
            if (client) {
                client->Stop();
            }
        };

        auto clientEndpoint = client->CreateEndpoint();
        clientEndpoint->Start();
        Y_DEFER {
            if (clientEndpoint) {
                clientEndpoint->Stop();
            }
        };

        clientStorage->WaitForSessionCount(1);

        clientEndpoint->Stop();
        clientEndpoint.reset();

        client->Stop();
        client.reset();

        clientStorage->WaitForSessionCount(0);
    }

    Y_UNIT_TEST(ShouldNotAcceptClientAfterServerStopped)
    {
        TString clientId = "testClientId";
        TString diskId = "testDiskId";
        ui32 unixSocketBacklog = 16;
        auto blocksCount = 42;
        TFsPath unixSocket(CreateGuidAsString() + ".sock");

        TPortManager portManager;
        ui16 dataPort = portManager.GetPort(9001);

        TTestFactory testFactory;

        auto server = testFactory.CreateServerBuilder()
            .SetDataPort(dataPort)
            .BuildServer(std::make_shared<TTestService>());

        auto endpointListener = CreateSocketEndpointListener(
            testFactory.Logging,
            unixSocketBacklog,
            MODE0660);
        endpointListener->SetClientStorageFactory(
            server->GetClientStorageFactory());

        endpointListener->Start();
        server->Start();

        NProto::TStartEndpointRequest request;
        request.SetUnixSocketPath(unixSocket.GetPath());
        request.SetDiskId(diskId);

        NProto::TVolume volume;
        volume.SetDiskId(diskId);
        volume.SetBlockSize(DefaultBlockSize);
        volume.SetBlocksCount(blocksCount);

        {
            auto future = endpointListener->StartEndpoint(
                request,
                volume,
                std::make_shared<TTestSession>());
            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(error), error);
        }

        auto client = testFactory.CreateClientBuilder()
            .SetDataPort(dataPort)
            .SetClientId(clientId)
            .BuildClient();
        client->Start();

        server->Stop();

        TVector<IBlockStorePtr> clientEndpoints;
        for (size_t i = 0; i < 100; ++i) {
            auto clientEndpoint = client->CreateDataEndpoint(unixSocket.GetPath());
            clientEndpoint->Start();
            clientEndpoints.push_back(std::move(clientEndpoint));
        }

        client->Stop();

        endpointListener->Stop();
    }

    Y_UNIT_TEST(ShouldEmulateMountUnmountResponseForSocketEndpoints)
    {
        TOptions options;
        auto bootstrap = CreateBootstrap(options);
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        auto client = bootstrap.GetClientEndpoint();
        const auto& clientId = options.ClientId;
        const auto& diskId = options.DiskId;

        {
            auto request = std::make_shared<NProto::TMountVolumeRequest>();
            request->MutableHeaders()->SetClientId(clientId);
            request->SetDiskId(diskId);

            auto future = client->MountVolume(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response.GetError());

            google::protobuf::util::MessageDifferencer comparator;
            UNIT_ASSERT(comparator.Equals(
                CreateMountVolumeResponse(options),
                response));
        }

        {
            auto request = std::make_shared<NProto::TUnmountVolumeRequest>();
            request->MutableHeaders()->SetClientId(clientId);
            request->SetDiskId(diskId);

            auto future = client->UnmountVolume(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response.GetError());
            UNIT_ASSERT(response.GetError().GetCode() == S_FALSE);
        }
    }

    Y_UNIT_TEST(ShouldHandleStorageRequestsForSocketEndpoints)
    {
        TOptions options;
        auto bootstrap = CreateBootstrap(options);
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        auto client = bootstrap.GetClientEndpoint();
        auto session = bootstrap.GetTestSession();
        const auto& diskId = options.DiskId;

        {
            ui32 requestCounter = 0;
            session->ReadBlocksHandler =
                [&] (
                    TCallContextPtr ctx,
                    std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
                {
                    Y_UNUSED(ctx);

                    UNIT_ASSERT_VALUES_EQUAL(
                        int(NProto::SOURCE_FD_DATA_CHANNEL),
                        int(request->GetHeaders().GetInternal().GetRequestSource())
                    );

                    ++requestCounter;
                    return MakeFuture<NProto::TReadBlocksLocalResponse>();
                };

            auto request = std::make_shared<NProto::TReadBlocksRequest>();
            request->SetDiskId(diskId);

            auto future = client->ReadBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response.GetError());
            UNIT_ASSERT(requestCounter == 1);
        }

        {
            ui32 requestCounter = 0;
            session->WriteBlocksHandler =
                [&] (
                    TCallContextPtr ctx,
                    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
                {
                    Y_UNUSED(ctx);

                    UNIT_ASSERT_VALUES_EQUAL(
                        int(NProto::SOURCE_FD_DATA_CHANNEL),
                        int(request->GetHeaders().GetInternal().GetRequestSource())
                    );

                    ++requestCounter;
                    return MakeFuture<NProto::TWriteBlocksLocalResponse>();
                };

            auto request = std::make_shared<NProto::TWriteBlocksRequest>();
            request->SetDiskId(diskId);

            auto future = client->WriteBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response.GetError());
            UNIT_ASSERT(requestCounter == 1);
        }

        {
            ui32 requestCounter = 0;
            session->ZeroBlocksHandler =
                [&] (
                    TCallContextPtr ctx,
                    std::shared_ptr<NProto::TZeroBlocksRequest> request)
                {
                    Y_UNUSED(ctx);

                    UNIT_ASSERT_VALUES_EQUAL(
                        int(NProto::SOURCE_FD_DATA_CHANNEL),
                        int(request->GetHeaders().GetInternal().GetRequestSource())
                    );

                    ++requestCounter;
                    return MakeFuture<NProto::TZeroBlocksResponse>();
                };

            auto request = std::make_shared<NProto::TZeroBlocksRequest>();
            request->SetDiskId(diskId);

            auto future = client->ZeroBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response.GetError());
            UNIT_ASSERT(requestCounter == 1);
        }
    }

    Y_UNIT_TEST(ShouldNotHandleControlRequestsForSocketEndpoints)
    {
        TOptions options;
        auto bootstrap = CreateBootstrap(options);
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        auto client = bootstrap.GetClient()->CreateEndpoint();

        {
            auto future = client->ListEndpoints(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TListEndpointsRequest>());

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_GRPC_UNIMPLEMENTED,
                response.GetError().GetCode(),
                response.GetError());
        }
    }

    Y_UNIT_TEST(ShouldHandleOnlyLastSocketEndpointClient)
    {
        TOptions options;
        auto bootstrap = CreateBootstrap(options);
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        auto clientEndpoint = bootstrap.GetClientEndpoint();
        auto session = bootstrap.GetTestSession();
        const auto& diskId = options.DiskId;

        {
            ui32 requestCounter = 0;
            session->ReadBlocksHandler =
                [&] (
                    TCallContextPtr ctx,
                    std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
                {
                    Y_UNUSED(ctx);
                    Y_UNUSED(request);
                    ++requestCounter;
                    return MakeFuture<NProto::TReadBlocksLocalResponse>();
                };

            auto request = std::make_shared<NProto::TReadBlocksRequest>();
            request->SetDiskId(diskId);

            auto future = clientEndpoint->ReadBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response.GetError());
            UNIT_ASSERT(requestCounter == 1);
        }

        auto client = bootstrap.GetClient();
        auto clientEndpoint2 = client->CreateDataEndpoint(options.UnixSocketPath);
        clientEndpoint2->Start();

        {
            ui32 requestCounter = 0;
            session->ReadBlocksHandler =
                [&] (
                    TCallContextPtr ctx,
                    std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
                {
                    Y_UNUSED(ctx);
                    Y_UNUSED(request);
                    ++requestCounter;
                    return MakeFuture<NProto::TReadBlocksLocalResponse>();
                };

            auto request = std::make_shared<NProto::TReadBlocksRequest>();
            request->SetDiskId(diskId);

            auto future = clientEndpoint2->ReadBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response.GetError());
            UNIT_ASSERT(requestCounter == 1);
        }

        {
            ui32 requestCounter = 0;
            session->ReadBlocksHandler =
                [&] (
                    TCallContextPtr ctx,
                    std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
                {
                    Y_UNUSED(ctx);
                    Y_UNUSED(request);
                    ++requestCounter;
                    return MakeFuture<NProto::TReadBlocksLocalResponse>();
                };

            auto request = std::make_shared<NProto::TReadBlocksRequest>();
            request->SetDiskId(diskId);

            auto future = clientEndpoint->ReadBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(HasError(response));
            UNIT_ASSERT(requestCounter == 0);
        }

        clientEndpoint2->Stop();
    }

    Y_UNIT_TEST(ShouldRejectRequestsAfterStopEndpoint)
    {
        TOptions options;
        auto bootstrap = CreateBootstrap(options);
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        auto client = bootstrap.GetClientEndpoint();
        auto session = bootstrap.GetTestSession();

        TManualEvent event;
        auto trigger = NewPromise<NProto::TZeroBlocksResponse>();

        session->ZeroBlocksHandler =
            [&] (
                TCallContextPtr ctx,
                std::shared_ptr<NProto::TZeroBlocksRequest> request)
            {
                Y_UNUSED(ctx);
                Y_UNUSED(request);
                event.Signal();
                return trigger.GetFuture();
            };

        TFuture<NProto::TZeroBlocksResponse> firstFuture;
        {
            auto request = std::make_shared<NProto::TZeroBlocksRequest>();
            request->SetDiskId(options.DiskId);

            firstFuture = bootstrap.GetClientEndpoint()->ZeroBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            event.Wait();
            UNIT_ASSERT(!firstFuture.HasValue());
        }

        {
            auto future = bootstrap.GetEndpointListener()->StopEndpoint(
                options.UnixSocketPath);
            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(error), error);
        }

        {
            auto request = std::make_shared<NProto::TZeroBlocksRequest>();
            request->SetDiskId(options.DiskId);

            auto future = bootstrap.GetClientEndpoint()->ZeroBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            auto response = future.GetValue(TDuration::Seconds(5));
            const auto& error = response.GetError();
            UNIT_ASSERT_C(E_GRPC_UNAVAILABLE == error.GetCode(), error);
        }

        trigger.SetValue(TErrorResponse(E_INVALID_STATE, "Any fatal error"));

        {
            auto response = firstFuture.GetValue(TDuration::Seconds(5));
            const auto& error = response.GetError();
            UNIT_ASSERT_C(E_GRPC_UNAVAILABLE == error.GetCode(), error);
        }
    }

    Y_UNIT_TEST(ShouldReconnectAfterRestartEndpoint)
    {
        auto logging = CreateLoggingService("console");

        TOptions options;
        auto bootstrap = CreateBootstrap(options);
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        NProto::TClientAppConfig clientConfig;
        clientConfig.MutableClientConfig()->SetRetryTimeoutIncrement(100);
        auto config = std::make_shared<TClientAppConfig>(std::move(clientConfig));

        auto clientEndpoint = CreateDurableClient(
            config,
            bootstrap.GetClientEndpoint(),
            CreateRetryPolicy(config, std::nullopt),
            logging,
            bootstrap.GetTimer(),
            bootstrap.GetScheduler(),
            CreateRequestStatsStub(),
            CreateVolumeStatsStub());

        TManualEvent event;
        auto trigger = NewPromise<NProto::TZeroBlocksResponse>();

        auto session = bootstrap.GetTestSession();
        session->ZeroBlocksHandler =
            [&] (
                TCallContextPtr ctx,
                std::shared_ptr<NProto::TZeroBlocksRequest> request)
            {
                Y_UNUSED(ctx);
                Y_UNUSED(request);
                event.Signal();
                return trigger.GetFuture();
            };

        TFuture<NProto::TZeroBlocksResponse> firstFuture;
        {
            auto request = std::make_shared<NProto::TZeroBlocksRequest>();
            request->SetDiskId(options.DiskId);

            firstFuture = clientEndpoint->ZeroBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            event.Wait();
        }

        UNIT_ASSERT(!firstFuture.HasValue());

        {
            auto future = bootstrap.GetEndpointListener()->StopEndpoint(
                options.UnixSocketPath);
            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(error), error);
        }

        UNIT_ASSERT(!firstFuture.HasValue());
        trigger.SetValue(TErrorResponse(E_INVALID_STATE, "Any fatal error"));

        {
            NProto::TStartEndpointRequest request;
            request.SetUnixSocketPath(options.UnixSocketPath);
            request.SetDiskId(options.DiskId);

            NProto::TVolume volume;
            volume.SetDiskId(options.DiskId);
            volume.SetBlockSize(options.BlockSize);
            volume.SetBlocksCount(options.BlocksCount);

            auto newSession = std::make_shared<TTestSession>();

            newSession->EnsureVolumeMountedHandler =
                [=] () {
                    return MakeFuture(CreateMountVolumeResponse(options));
                };

            newSession->ZeroBlocksHandler =
                [&] (
                    TCallContextPtr ctx,
                    std::shared_ptr<NProto::TZeroBlocksRequest> request)
                {
                    Y_UNUSED(ctx);
                    Y_UNUSED(request);
                    return MakeFuture(NProto::TZeroBlocksResponse());
                };

            auto future = bootstrap.GetEndpointListener()->StartEndpoint(
                request,
                volume,
                newSession);
            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(error));
        }

        auto response = firstFuture.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(response), response.GetError());
    }

    Y_UNIT_TEST(ShouldStartEndpointIfSocketAlreadyExists)
    {
        TFsPath unixSocket(CreateGuidAsString() + ".sock");
        unixSocket.Touch();
        Y_DEFER {
            unixSocket.DeleteIfExists();
        };

        TOptions options;
        options.UnixSocketPath = unixSocket.GetPath();

        auto bootstrap = CreateBootstrap(options);
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        auto client = bootstrap.GetClientEndpoint();
        auto session = bootstrap.GetTestSession();
        const auto& diskId = options.DiskId;
        session->ReadBlocksHandler = [&] (
            TCallContextPtr ctx,
            std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            Y_UNUSED(ctx);
            Y_UNUSED(request);
            return MakeFuture<NProto::TReadBlocksLocalResponse>();
        };

        auto error = ReadBlocksLocal(client, diskId);
        UNIT_ASSERT_C(!HasError(error), error);
    }

    Y_UNIT_TEST(ShouldRemoveUnixSocketAfterStopEndpoint)
    {
        TOptions options;
        auto bootstrap = CreateBootstrap(options);
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        auto client = bootstrap.GetClientEndpoint();
        auto session = bootstrap.GetTestSession();
        const auto& diskId = options.DiskId;
        session->ReadBlocksHandler = [&] (
            TCallContextPtr ctx,
            std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            Y_UNUSED(ctx);
            Y_UNUSED(request);
            return MakeFuture<NProto::TReadBlocksLocalResponse>();
        };

        auto error = ReadBlocksLocal(client, diskId);
        UNIT_ASSERT_C(!HasError(error), error);

        bootstrap.GetEndpointListener()->StopEndpoint(options.UnixSocketPath);
        UNIT_ASSERT(!TFsPath(options.UnixSocketPath).Exists());
    }

    Y_UNIT_TEST(ShouldNotRemoveUnixSocketAfterStopServer)
    {
        auto serverCode1 = E_FAIL;
        auto serverCode2 = E_ARGUMENT;

        TOptions options;
        const auto& diskId = options.DiskId;

        auto bootstrap1 = CreateBootstrap(options);
        bootstrap1.Start();
        Y_DEFER {
            bootstrap1.Stop();
        };

        auto client1 = bootstrap1.GetClientEndpoint();
        auto session1 = bootstrap1.GetTestSession();
        session1->ReadBlocksHandler = [&] (
            TCallContextPtr ctx,
            std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            Y_UNUSED(ctx);
            Y_UNUSED(request);
            return MakeFuture<NProto::TReadBlocksLocalResponse>(TErrorResponse(serverCode1));
        };

        UNIT_ASSERT(serverCode1 == ReadBlocksLocal(client1, diskId).GetCode());

        auto bootstrap2 = CreateBootstrap(options);
        bootstrap2.Start();
        Y_DEFER {
            bootstrap2.Stop();
        };

        auto client2 = bootstrap2.GetClientEndpoint();
        auto session2 = bootstrap2.GetTestSession();
        session2->ReadBlocksHandler = [&] (
            TCallContextPtr ctx,
            std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            Y_UNUSED(ctx);
            Y_UNUSED(request);
            return MakeFuture<NProto::TReadBlocksLocalResponse>(TErrorResponse(serverCode2));
        };

        UNIT_ASSERT(serverCode1 == ReadBlocksLocal(client1, diskId).GetCode());
        UNIT_ASSERT(serverCode2 == ReadBlocksLocal(client2, diskId).GetCode());

        if (bootstrap1.GetEndpointListener()) {
            bootstrap1.GetEndpointListener()->Stop();
        }

        if (bootstrap1.GetServer()) {
            bootstrap1.GetServer()->Stop();
        }

        UNIT_ASSERT(TFsPath(options.UnixSocketPath).Exists());

        UNIT_ASSERT(serverCode2 == ReadBlocksLocal(client2, diskId).GetCode());

        size_t attempt = 3;
        for (size_t i = 0; i < attempt; ++i) {
            auto error = ReadBlocksLocal(client1, diskId);
            if (IsConnectionError(error) && i < attempt - 1) {
                continue;
            }
            UNIT_ASSERT_C(serverCode2 == error.GetCode(), error);
        }
    }

    Y_UNIT_TEST(ShouldReturnLastSuccessfulMountVolumeResponse)
    {
        TOptions options;
        auto bootstrap = CreateBootstrap(options);
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        auto client = bootstrap.GetClientEndpoint();
        auto session = bootstrap.GetTestSession();

        auto request = std::make_shared<NProto::TMountVolumeRequest>();
        request->SetDiskId(options.DiskId);

        auto successfulResponse = CreateMountVolumeResponse(options);

        auto failedResponse = successfulResponse;
        failedResponse.MutableError()->SetCode(E_FAIL);
        failedResponse.MutableError()->SetMessage("test error message");

        google::protobuf::util::MessageDifferencer comparator;

        {
            session->EnsureVolumeMountedHandler = [=] () {
                return MakeFuture(failedResponse);
            };

            auto future = client->MountVolume(MakeIntrusive<TCallContext>(), request);
            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(comparator.Equals(failedResponse, response));
        }

        {
            session->EnsureVolumeMountedHandler = [=] () {
                return MakeFuture(successfulResponse);
            };

            auto future = client->MountVolume(MakeIntrusive<TCallContext>(), request);
            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(comparator.Equals(successfulResponse, response));
        }

        {
            session->EnsureVolumeMountedHandler = [=] () {
                return MakeFuture(failedResponse);
            };

            auto future = client->MountVolume(MakeIntrusive<TCallContext>(), request);
            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(comparator.Equals(successfulResponse, response));
        }
    }

    Y_UNIT_TEST(ShouldGetFatalErrorIfEndpointHasInvalidSocketPath)
    {
        auto logging = CreateLoggingService("console");
        TString unixSocket("./invalid/path/to/socket");

        auto clientStorage = std::make_shared<TTestClientStorage>();
        auto listener = CreateSocketEndpointListener(logging, 16, MODE0660);
        listener->SetClientStorageFactory(clientStorage);
        listener->Start();
        Y_DEFER {
            listener->Stop();
        };

        {
            NProto::TStartEndpointRequest request;
            request.SetUnixSocketPath(unixSocket);
            auto future = listener->StartEndpoint(request, {}, {});
            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_VALUES_EQUAL_C(
                EDiagnosticsErrorKind::ErrorFatal,
                GetDiagnosticsErrorKind(error),
                error);
            UNIT_ASSERT_VALUES_EQUAL_C(
                EErrorKind::ErrorFatal,
                GetErrorKind(error),
                error);
        }
    }
}

}   // namespace NCloud::NBlockStore::NServer
