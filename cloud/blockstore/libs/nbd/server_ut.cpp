#include "server.h"

#include "client.h"
#include "client_handler.h"
#include "error_handler.h"
#include "server_handler.h"

#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/durable.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/device_handler.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service/service_test.h>
#include <cloud/blockstore/libs/service/storage_test.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/sglist_test.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/generic/guid.h>
#include <util/generic/scope.h>

namespace NCloud::NBlockStore::NBD {

using namespace NThreading;
using namespace NCloud::NBlockStore::NClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr bool StructuredReply = true;
constexpr bool UseNbsErrors = true;

////////////////////////////////////////////////////////////////////////////////

bool BuffersFilledWithSingleChar(const TSgList& buffers, char sym)
{
    for (const auto& buf: buffers) {
        const char* ptr = buf.Data();

        for (size_t i = 0; i < buf.Size(); ++i) {
            if (*ptr != sym) {
                return false;
            }
            ++ptr;
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
private:
    ILoggingServicePtr Logging;
    ITimerPtr Timer;
    ISchedulerPtr Scheduler;
    IServerPtr Server;
    IClientPtr Client;
    IBlockStorePtr GrpcClientEndpoint;
    IBlockStorePtr ClientEndpoint;
    IServerHandlerFactoryPtr HandlerFactory;
    TNetworkAddress ConnectAddress;
    TString DiskId;

public:
    TBootstrap(
            ILoggingServicePtr logging,
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            IServerPtr server,
            IClientPtr client,
            IBlockStorePtr grpcClientEndpoint,
            IBlockStorePtr clientEndpoint,
            IServerHandlerFactoryPtr handlerFactory,
            TNetworkAddress connectAddress,
            TString diskId)
        : Logging(std::move(logging))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Server(std::move(server))
        , Client(std::move(client))
        , GrpcClientEndpoint(std::move(grpcClientEndpoint))
        , ClientEndpoint(std::move(clientEndpoint))
        , HandlerFactory(std::move(handlerFactory))
        , ConnectAddress(std::move(connectAddress))
        , DiskId(std::move(diskId))
    {}

    NProto::TError Start()
    {
        if (Logging) {
            Logging->Start();
        }

        if (Scheduler) {
            Scheduler->Start();
        }

        if (Server) {
            auto error = StartServerAndEndpoint();
            if (HasError(error)) {
                return error;
            }
        }

        if (Client) {
            Client->Start();
        }

        if (ClientEndpoint) {
            ClientEndpoint->Start();

            auto request = std::make_shared<NProto::TMountVolumeRequest>();
            request->SetDiskId(DiskId);

            auto future = ClientEndpoint->MountVolume(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            auto response = future.GetValue(TDuration::Seconds(5));
            if (HasError(response)) {
                return response.GetError();
            }
        }

        return {};
    }

    void Stop(bool forgetStopEndpoint = false)
    {
        if (ClientEndpoint) {
            ClientEndpoint->Stop();
        }

        if (Client) {
            Client->Stop();
        }

        if (Server) {
            if (!forgetStopEndpoint) {
                auto future = Server->StopEndpoint(ConnectAddress);
                auto error = future.GetValue(TDuration::Seconds(3));
                UNIT_ASSERT_C(!HasError(error), error);
            }

            Server->Stop();
        }

        if (Scheduler) {
            Scheduler->Stop();
        }

        if (Logging) {
            Logging->Stop();
        }
    }

    NProto::TError RestartServer()
    {
        if (Server) {
            Server->Stop();
        }

        Server = CreateServer(Logging, {});
        return StartServerAndEndpoint();
    }

    void StopServer()
    {
        if (Server) {
            Server->Stop();
            Server = nullptr;
        }
    }

    void StopEndpoint()
    {
        if (Server) {
            auto future = Server->StopEndpoint(ConnectAddress);
            auto error = future.GetValue(TDuration::Seconds(3));
            UNIT_ASSERT_C(!HasError(error), error);
        }
    }

    ILoggingServicePtr GetLogging()
    {
        return Logging;
    }

    ITimerPtr GetTimer()
    {
        return Timer;
    }

    ISchedulerPtr GetScheduler()
    {
        return Scheduler;
    }

    IClientPtr GetClient()
    {
        return Client;
    }

    IBlockStorePtr GetClientEndpoint()
    {
        return ClientEndpoint;
    }

    IServerPtr GetServer()
    {
        return Server;
    }

    IBlockStorePtr GetGrpcClientEndpoint()
    {
        return GrpcClientEndpoint;
    }

private:
    NProto::TError StartServerAndEndpoint()
    {
        Server->Start();

        auto future = Server->StartEndpoint(
            ConnectAddress,
            HandlerFactory);

        return future.GetValue(TDuration::Seconds(3));
    }
};

////////////////////////////////////////////////////////////////////////////////

static const TStorageOptions DefaultStorageOptions = {
    .DiskId = "TestDiskId",
    .BlockSize = DefaultBlockSize,
    .BlocksCount = 1024,
    .CheckpointId = "",
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TBootstrap> CreateBootstrap(
    TNetworkAddress connectAddress,
    IStoragePtr storage,
    const TStorageOptions& options = DefaultStorageOptions,
    TServerConfig serverConfig = Default<TServerConfig>(),
    IBlockStorePtr grpcClientEndpoint = nullptr)
{
    const ui32 clientThreadsCount = 1;

    auto logging = CreateLoggingService("console", { TLOG_DEBUG });

    auto timer = CreateWallClockTimer();
    auto scheduler = CreateScheduler();

    auto server = CreateServer(logging, serverConfig);

    auto handlerFactory = CreateServerHandlerFactory(
        CreateDefaultDeviceHandlerFactory(),
        logging,
        std::move(storage),
        CreateServerStatsStub(),
        CreateErrorHandlerStub(),
        options);

    auto client = CreateClient(
        logging,
        clientThreadsCount);

    auto clientHandler = CreateClientHandler(
        logging,
        StructuredReply,
        UseNbsErrors);

    if (!grpcClientEndpoint) {
        auto testGrpcService = std::make_shared<TTestService>();
        testGrpcService->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                UNIT_ASSERT_VALUES_EQUAL(options.DiskId, request->GetDiskId());

                NProto::TMountVolumeResponse response;
                response.SetInactiveClientsTimeout(100);

                auto& volume = *response.MutableVolume();
                volume.SetDiskId(options.DiskId);
                volume.SetBlocksCount(options.BlocksCount);
                volume.SetBlockSize(options.BlockSize);
                return MakeFuture(response);
            };
        testGrpcService->UnmountVolumeHandler =
            [&] (std::shared_ptr<NProto::TUnmountVolumeRequest> request) {
                Y_UNUSED(request);
                return MakeFuture(NProto::TUnmountVolumeResponse());
            };
        grpcClientEndpoint = testGrpcService;
    }

    auto clientEndpoint = client->CreateEndpoint(
        connectAddress,
        std::move(clientHandler),
        grpcClientEndpoint);

    return std::make_unique<TBootstrap>(
        std::move(logging),
        std::move(timer),
        std::move(scheduler),
        std::move(server),
        std::move(client),
        std::move(grpcClientEndpoint),
        std::move(clientEndpoint),
        std::move(handlerFactory),
        std::move(connectAddress),
        options.DiskId);
}

////////////////////////////////////////////////////////////////////////////////

NProto::TError ReadBlocksLocal(const IBlockStorePtr& clientEndpoint)
{
    const ui64 blocksCount = 42;

    TVector<TString> blocks;
    auto sglist = ResizeBlocks(
        blocks,
        blocksCount,
        TString::TUninitialized(DefaultBlockSize));

    auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
    request->SetStartIndex(0);
    request->SetBlocksCount(blocksCount);
    request->BlockSize = DefaultBlockSize;
    request->Sglist = TGuardedSgList(sglist);

    auto future = clientEndpoint->ReadBlocksLocal(
        MakeIntrusive<TCallContext>(),
        std::move(request));

    auto response = future.GetValue(TDuration::Seconds(5));
    return response.GetError();
}

NProto::TError WriteBlocksLocal(const IBlockStorePtr& clientEndpoint)
{
    const ui64 blocksCount = 42;

    TVector<TString> blocks;
    auto sglist = ResizeBlocks(
        blocks,
        blocksCount,
        TString(DefaultBlockSize, 'X'));

    auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
    request->SetStartIndex(0);
    request->BlocksCount = blocksCount;
    request->BlockSize = DefaultBlockSize;
    request->Sglist = TGuardedSgList(sglist);

    auto future = clientEndpoint->WriteBlocksLocal(
        MakeIntrusive<TCallContext>(),
        std::move(request));

    auto response = future.GetValue(TDuration::Seconds(5));
    return response.GetError();
}

NProto::TError ZeroBlocks(const IBlockStorePtr& clientEndpoint)
{
    auto request = std::make_shared<NProto::TZeroBlocksRequest>();
    request->SetStartIndex(0);
    request->SetBlocksCount(42);

    auto future = clientEndpoint->ZeroBlocks(
        MakeIntrusive<TCallContext>(),
        std::move(request));

    auto response = future.GetValue(TDuration::Seconds(5));
    return response.GetError();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServerTest)
{
    Y_UNIT_TEST(ShouldHandleLocalRequests)
    {
        const ui32 startIndex = 0;
        const ui64 blocksCount = 42;
        const char writeSym = 'w';
        const char readSym = 'r';

        auto storage = std::make_shared<TTestStorage>();

        storage->ReadBlocksLocalHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            Y_UNUSED(callContext);

            UNIT_ASSERT(startIndex == request->GetStartIndex());
            UNIT_ASSERT(blocksCount == request->GetBlocksCount());

            auto guard = request->Sglist.Acquire();
            auto sglist = guard.Get();

            for (const auto& buf: sglist) {
                memset((void*)buf.Data(), readSym, buf.Size());
            }

            return MakeFuture<NProto::TReadBlocksLocalResponse>();
        };

        storage->WriteBlocksLocalHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
        {
            Y_UNUSED(callContext);

            UNIT_ASSERT(startIndex == request->GetStartIndex());
            UNIT_ASSERT(blocksCount == request->BlocksCount);

            auto guard = request->Sglist.Acquire();
            auto sglist = guard.Get();

            UNIT_ASSERT(BuffersFilledWithSingleChar(sglist, writeSym));

            return MakeFuture<NProto::TWriteBlocksLocalResponse>();
        };

        storage->ZeroBlocksHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TZeroBlocksRequest> request)
        {
            Y_UNUSED(callContext);

            UNIT_ASSERT(startIndex == request->GetStartIndex());
            UNIT_ASSERT(blocksCount == request->GetBlocksCount());

            return MakeFuture<NProto::TZeroBlocksResponse>();
        };

        TPortManager portManager;
        auto port = portManager.GetPort(9001);
        TNetworkAddress connectAddress(port);

        auto bootstrap = CreateBootstrap(connectAddress, storage);

        auto error = bootstrap->Start();
        UNIT_ASSERT_C(!HasError(error), error);

        TVector<TString> blocks;
        auto sglist = ResizeBlocks(
            blocks,
            blocksCount,
            TString(DefaultBlockSize, writeSym));

        {
            auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
            request->SetStartIndex(startIndex);
            request->BlocksCount = blocksCount;
            request->BlockSize = DefaultBlockSize;
            request->Sglist = TGuardedSgList(sglist);

            auto future = bootstrap->GetClientEndpoint()->WriteBlocksLocal(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response.GetError());
        }

        {
            auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
            request->SetStartIndex(startIndex);
            request->SetBlocksCount(blocksCount);
            request->BlockSize = DefaultBlockSize;
            request->Sglist = TGuardedSgList(sglist);

            auto future = bootstrap->GetClientEndpoint()->ReadBlocksLocal(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response.GetError());

            UNIT_ASSERT(BuffersFilledWithSingleChar(sglist, readSym));
        }

        {
            auto request = std::make_shared<NProto::TZeroBlocksRequest>();
            request->SetStartIndex(startIndex);
            request->SetBlocksCount(blocksCount);

            auto future = bootstrap->GetClientEndpoint()->ZeroBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response.GetError());
        }

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldHandleRequestsWhenCheckpointIdExists)
    {
        const TString checkpointId = "TestCheckpointId";

        auto storage = std::make_shared<TTestStorage>();

        storage->ReadBlocksLocalHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            Y_UNUSED(callContext);

            UNIT_ASSERT(checkpointId == request->GetCheckpointId());

            auto guard = request->Sglist.Acquire();
            for (const auto& buf: guard.Get()) {
                memset((void*)buf.Data(), 'Y', buf.Size());
            }
            return MakeFuture<NProto::TReadBlocksLocalResponse>();
        };

        storage->WriteBlocksLocalHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
        {
            Y_UNUSED(callContext);
            Y_UNUSED(request);

            return MakeFuture<NProto::TWriteBlocksLocalResponse>();
        };

        storage->ZeroBlocksHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TZeroBlocksRequest> request)
        {
            Y_UNUSED(callContext);
            Y_UNUSED(request);

            return MakeFuture<NProto::TZeroBlocksResponse>();
        };

        TPortManager portManager;
        auto port = portManager.GetPort(9001);
        TNetworkAddress connectAddress(port);

        TStorageOptions options = DefaultStorageOptions;
        options.CheckpointId = checkpointId;

        auto bootstrap = CreateBootstrap(connectAddress, storage, options);

        auto error = bootstrap->Start();
        UNIT_ASSERT_C(!HasError(error), error);

        {
            auto error = ReadBlocksLocal(bootstrap->GetClientEndpoint());
            UNIT_ASSERT_C(!HasError(error), error);
        }

        {
            auto error = WriteBlocksLocal(bootstrap->GetClientEndpoint());
            UNIT_ASSERT(HasError(error));
        }

        {
            auto error = ZeroBlocks(bootstrap->GetClientEndpoint());
            UNIT_ASSERT(HasError(error));
        }

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldRemoveUnixSocketAfterStopEndpoint)
    {
        TFsPath unixSocket(CreateGuidAsString() + ".sock");
        TNetworkAddress connectAddress(TUnixSocketPath(unixSocket.GetPath()));

        auto storage = std::make_shared<TTestStorage>();

        storage->ReadBlocksLocalHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            Y_UNUSED(callContext);

            auto guard = request->Sglist.Acquire();
            for (const auto& buf: guard.Get()) {
                memset((void*)buf.Data(), 'Y', buf.Size());
            }
            return MakeFuture<NProto::TReadBlocksLocalResponse>();
        };

        auto bootstrap = CreateBootstrap(connectAddress, storage);

        auto error = bootstrap->Start();
        UNIT_ASSERT_C(!HasError(error), error);

        {
            auto error = ReadBlocksLocal(bootstrap->GetClientEndpoint());
            UNIT_ASSERT_C(!HasError(error), error);
        }

        bootstrap->StopEndpoint();
        UNIT_ASSERT(!unixSocket.Exists());

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldNotRemoveUnixSocketAfterStopServer)
    {
        auto serverCode1 = E_FAIL;
        auto serverCode2 = E_ARGUMENT;

        TFsPath unixSocket(CreateGuidAsString() + ".sock");
        TNetworkAddress connectAddress(TUnixSocketPath(unixSocket.GetPath()));

        auto storage1 = std::make_shared<TTestStorage>();
        storage1->ReadBlocksLocalHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            Y_UNUSED(callContext);
            Y_UNUSED(request);

            return MakeFuture<NProto::TReadBlocksLocalResponse>(TErrorResponse(serverCode1));
        };

        auto storage2 = std::make_shared<TTestStorage>();
        storage2->ReadBlocksLocalHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            Y_UNUSED(callContext);
            Y_UNUSED(request);

            return MakeFuture<NProto::TReadBlocksLocalResponse>(TErrorResponse(serverCode2));
        };

        auto bootstrap1 = CreateBootstrap(connectAddress, storage1);
        {
            auto error = bootstrap1->Start();
            UNIT_ASSERT_C(!HasError(error), error);
        }
        auto client1 = bootstrap1->GetClientEndpoint();

        UNIT_ASSERT(serverCode1 == ReadBlocksLocal(client1).GetCode());

        auto bootstrap2 = CreateBootstrap(connectAddress, storage2);
        {
            auto error = bootstrap2->Start();
            UNIT_ASSERT_C(!HasError(error), error);
        }
        auto client2 = bootstrap2->GetClientEndpoint();

        UNIT_ASSERT(serverCode1 == ReadBlocksLocal(client1).GetCode());
        UNIT_ASSERT(serverCode2 == ReadBlocksLocal(client2).GetCode());

        bootstrap1->StopServer();
        UNIT_ASSERT(unixSocket.Exists());

        UNIT_ASSERT(serverCode2 == ReadBlocksLocal(client2).GetCode());

        size_t attempt = 2;
        for (size_t i = 0; i < attempt; ++i) {
            auto error = ReadBlocksLocal(client1);
            if (IsConnectionError(error) && i < attempt - 1) {
                continue;
            }
            UNIT_ASSERT_C(serverCode2 == error.GetCode(), error);
        }

        bootstrap2->Stop();
        bootstrap1->Stop();
    }

    Y_UNIT_TEST(ShouldReturnNotFoundErrorIfEndpointHasInvalidSocketPath)
    {
        TUnixSocketPath socketPath("./invalid/path/to/socket");
        TNetworkAddress connectAddress(socketPath);

        auto storage = std::make_shared<TTestStorage>();
        auto bootstrap = CreateBootstrap(connectAddress, storage);

        auto error = bootstrap->Start();
        UNIT_ASSERT_VALUES_EQUAL_C(
            EDiagnosticsErrorKind::ErrorFatal,
            GetDiagnosticsErrorKind(error),
            error);
        UNIT_ASSERT_VALUES_EQUAL_C(
            EErrorKind::ErrorFatal,
            GetErrorKind(error),
            error);
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_NOT_FOUND,
            error.GetCode(),
            error);

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldStartEndpointIfSocketAlreadyExists)
    {
        TFsPath unixSocket(CreateGuidAsString() + ".sock");
        unixSocket.Touch();
        Y_DEFER {
            unixSocket.DeleteIfExists();
        };

        TNetworkAddress connectAddress(TUnixSocketPath(unixSocket.GetPath()));

        auto storage = std::make_shared<TTestStorage>();

        storage->ReadBlocksLocalHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            Y_UNUSED(callContext);

            auto guard = request->Sglist.Acquire();
            for (const auto& buf: guard.Get()) {
                memset((void*)buf.Data(), 'Y', buf.Size());
            }
            return MakeFuture<NProto::TReadBlocksLocalResponse>();
        };

        auto bootstrap = CreateBootstrap(connectAddress, storage);

        auto error = bootstrap->Start();
        UNIT_ASSERT_C(!HasError(error), error);

        {
            auto error = ReadBlocksLocal(bootstrap->GetClientEndpoint());
            UNIT_ASSERT_C(!HasError(error), error);
        }

        bootstrap->Stop();
    }

    Y_UNIT_TEST(MountResponseShouldContainBlocksCountAndSize)
    {
        const TString diskId = "TestDiskId";
        const ui32 blockSize = 1234;
        const ui64 blocksCount = 42;

        TUnixSocketPath socketPath("./TestUnixSocket");
        TNetworkAddress connectAddress(socketPath);

        auto logging = CreateLoggingService("console", { TLOG_DEBUG });

        TStorageOptions options;
        options.DiskId = diskId;
        options.BlockSize = blockSize;
        options.BlocksCount = blocksCount;

        auto bootstrap = CreateBootstrap(connectAddress, nullptr, options);

        auto error = bootstrap->Start();
        UNIT_ASSERT_C(!HasError(error), error);

        auto request = std::make_shared<NProto::TMountVolumeRequest>();
        request->SetDiskId(diskId);

        auto future = bootstrap->GetClientEndpoint()->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::move(request));

        auto response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(response), response);

        const auto& volume = response.GetVolume();
        UNIT_ASSERT_VALUES_EQUAL(diskId, volume.GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(blockSize, volume.GetBlockSize());
        UNIT_ASSERT_VALUES_EQUAL(blocksCount, volume.GetBlocksCount());

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldReconnectAfterServerRestart)
    {
        const ui32 startIndex = 0;
        const ui64 blocksCount = 42;

        auto promise = NewPromise<NProto::TZeroBlocksResponse>();

        auto storage = std::make_shared<TTestStorage>();

        storage->ZeroBlocksHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TZeroBlocksRequest> request)
        {
            Y_UNUSED(callContext);

            UNIT_ASSERT(startIndex == request->GetStartIndex());
            UNIT_ASSERT(blocksCount == request->GetBlocksCount());

            return promise.GetFuture();
        };

        TPortManager portManager;
        auto port = portManager.GetPort(9001);
        TNetworkAddress connectAddress(port);

        auto bootstrap = CreateBootstrap(connectAddress, storage);

        NProto::TClientAppConfig clientConfig;
        clientConfig.MutableClientConfig()->SetRetryTimeoutIncrement(100);
        auto config = std::make_shared<TClientAppConfig>(std::move(clientConfig));

        auto clientEndpoint = CreateDurableClient(
            config,
            bootstrap->GetClientEndpoint(),
            CreateRetryPolicy(config, std::nullopt),
            bootstrap->GetLogging(),
            bootstrap->GetTimer(),
            bootstrap->GetScheduler(),
            CreateRequestStatsStub(),
            CreateVolumeStatsStub());

        auto error = bootstrap->Start();
        UNIT_ASSERT_C(!HasError(error), error);

        // unfreeze service
        promise.SetValue({});

        {
            auto request = std::make_shared<NProto::TZeroBlocksRequest>();
            request->SetStartIndex(startIndex);
            request->SetBlocksCount(blocksCount);

            auto future = clientEndpoint->ZeroBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response);
        }

        // freeze service
        promise = NewPromise<NProto::TZeroBlocksResponse>();

        auto inFlightRequest = std::make_shared<NProto::TZeroBlocksRequest>();
        inFlightRequest->SetStartIndex(startIndex);
        inFlightRequest->SetBlocksCount(blocksCount);

        auto inFlightFuture = clientEndpoint->ZeroBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(inFlightRequest));

        Sleep(TDuration::MilliSeconds(100));
        UNIT_ASSERT(!inFlightFuture.HasValue());

        {
            auto error = bootstrap->RestartServer();
            UNIT_ASSERT_C(!HasError(error), error);
        }

        // unfreeze service
        promise.SetValue({});

        auto inFlightResponse = inFlightFuture.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(inFlightResponse), inFlightResponse);

        {
            auto request = std::make_shared<NProto::TZeroBlocksRequest>();
            request->SetStartIndex(startIndex);
            request->SetBlocksCount(blocksCount);

            auto future = clientEndpoint->ZeroBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response);
        }

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldRejectRequestsAfterStopEndpoint)
    {
        const ui32 startIndex = 0;
        const ui64 blocksCount = 42;

        auto storage = std::make_shared<TTestStorage>();

        TManualEvent event;
        auto trigger = NewPromise<NProto::TZeroBlocksResponse>();

        storage->ZeroBlocksHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TZeroBlocksRequest> request)
        {
            Y_UNUSED(callContext);

            UNIT_ASSERT(startIndex == request->GetStartIndex());
            UNIT_ASSERT(blocksCount == request->GetBlocksCount());

            event.Signal();
            return trigger.GetFuture();
        };

        TPortManager portManager;
        auto port = portManager.GetPort(9001);
        TNetworkAddress connectAddress(port);

        auto bootstrap = CreateBootstrap(connectAddress, storage);

        auto error = bootstrap->Start();
        UNIT_ASSERT_C(!HasError(error), error);

        TFuture<NProto::TZeroBlocksResponse> firstFuture;
        {
            auto request = std::make_shared<NProto::TZeroBlocksRequest>();
            request->SetStartIndex(startIndex);
            request->SetBlocksCount(blocksCount);

            firstFuture = bootstrap->GetClientEndpoint()->ZeroBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            event.Wait();
            UNIT_ASSERT(!firstFuture.HasValue());
        }

        bootstrap->StopEndpoint();

        {
            auto request = std::make_shared<NProto::TZeroBlocksRequest>();
            request->SetStartIndex(startIndex);
            request->SetBlocksCount(blocksCount);

            auto future = bootstrap->GetClientEndpoint()->ZeroBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            auto response = future.GetValue(TDuration::Seconds(5));
            const auto& error = response.GetError();
            UNIT_ASSERT_VALUES_EQUAL_C(
                EDiagnosticsErrorKind::ErrorRetriable,
                GetDiagnosticsErrorKind(error),
                error);
            UNIT_ASSERT_VALUES_EQUAL_C(
                EErrorKind::ErrorRetriable,
                GetErrorKind(error),
                error);
            UNIT_ASSERT_C(IsConnectionError(error), error);
        }

        trigger.SetValue(TErrorResponse(E_INVALID_STATE, "Any fatal error"));

        {
            auto response = firstFuture.GetValue(TDuration::Seconds(5));
            const auto& error = response.GetError();
            UNIT_ASSERT_VALUES_EQUAL_C(
                EDiagnosticsErrorKind::ErrorRetriable,
                GetDiagnosticsErrorKind(error),
                error);
            UNIT_ASSERT_VALUES_EQUAL_C(
                EErrorKind::ErrorRetriable,
                GetErrorKind(error),
                error);
            UNIT_ASSERT_C(IsConnectionError(error), error);
        }

        bootstrap->Stop();
    }

    // NBS-2078
    Y_UNIT_TEST(ShouldNotFreezeSocketReadingDueToLimiter)
    {
        const ui32 startIndex = 13;

        auto storage = std::make_shared<TTestStorage>();

        auto trigger = NewPromise<NProto::TZeroBlocksResponse>();

        storage->ZeroBlocksHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TZeroBlocksRequest> request)
        {
            Y_UNUSED(callContext);

            UNIT_ASSERT(startIndex == request->GetStartIndex());

            return trigger.GetFuture();
        };

        TPortManager portManager;
        auto port = portManager.GetPort(9001);
        TNetworkAddress connectAddress(port);

        TServerConfig serverConfig;
        serverConfig.ThreadsCount = 1;
        serverConfig.LimiterEnabled = true;
        serverConfig.MaxInFlightBytesPerThread = 10 * DefaultBlockSize;

        const auto& options = DefaultStorageOptions;

        auto bootstrap = CreateBootstrap(
            connectAddress,
            storage,
            options,
            serverConfig);

        auto error = bootstrap->Start();
        UNIT_ASSERT_C(!HasError(error), error);

        auto port2 = portManager.GetPort(9001);
        TNetworkAddress connectAddress2(port2);

        IBlockStorePtr clientEndpoint2;

        {
            auto handlerFactory = CreateServerHandlerFactory(
                CreateDefaultDeviceHandlerFactory(),
                bootstrap->GetLogging(),
                storage,
                CreateServerStatsStub(),
                CreateErrorHandlerStub(),
                options);

            auto future = bootstrap->GetServer()->StartEndpoint(
                connectAddress2,
                handlerFactory);
            future.GetValue(TDuration::Seconds(3));

            auto clientHandler = CreateClientHandler(
                bootstrap->GetLogging(),
                StructuredReply,
                UseNbsErrors);

            clientEndpoint2 = bootstrap->GetClient()->CreateEndpoint(
                connectAddress2,
                std::move(clientHandler),
                bootstrap->GetGrpcClientEndpoint());

            clientEndpoint2->Start();
        }

        TFuture<NProto::TZeroBlocksResponse> future1;
        TFuture<NProto::TZeroBlocksResponse> future2;
        TFuture<NProto::TZeroBlocksResponse> future3;

        {
            auto request = std::make_shared<NProto::TZeroBlocksRequest>();
            request->SetStartIndex(startIndex);
            request->SetBlocksCount(11);

            future1 = bootstrap->GetClientEndpoint()->ZeroBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            UNIT_ASSERT(!future1.HasValue());
        }

        {
            auto request = std::make_shared<NProto::TZeroBlocksRequest>();
            request->SetStartIndex(startIndex);
            request->SetBlocksCount(1);

            future2 = bootstrap->GetClientEndpoint()->ZeroBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            UNIT_ASSERT(!future2.HasValue());
        }

        {
            auto request = std::make_shared<NProto::TZeroBlocksRequest>();
            request->SetStartIndex(startIndex);
            request->SetBlocksCount(1);

            future3 = clientEndpoint2->ZeroBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            UNIT_ASSERT(!future3.HasValue());
        }

        Sleep(TDuration::Seconds(1));
        trigger.SetValue({});

        UNIT_ASSERT(!HasError(future1.GetValue(TDuration::Seconds(3))));
        UNIT_ASSERT(!HasError(future2.GetValue(TDuration::Seconds(3))));
        UNIT_ASSERT(!HasError(future3.GetValue(TDuration::Seconds(3))));

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldCheckThatVolumeBlockSizeIsEqualDeviceBlockSize)
    {
        TPortManager portManager;
        auto port = portManager.GetPort(9001);
        TNetworkAddress connectAddress(port);

        auto options = DefaultStorageOptions;
        auto mountBlockSize = options.BlockSize / 4;

        auto testGrpcService = std::make_shared<TTestService>();
        testGrpcService->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                UNIT_ASSERT_VALUES_EQUAL(options.DiskId, request->GetDiskId());

                NProto::TMountVolumeResponse response;
                response.SetInactiveClientsTimeout(100);

                auto& volume = *response.MutableVolume();
                volume.SetDiskId(options.DiskId);
                volume.SetBlocksCount(options.BlocksCount);
                volume.SetBlockSize(mountBlockSize);
                return MakeFuture(response);
            };

        auto bootstrap = CreateBootstrap(
            connectAddress,
            nullptr,
            options,
            TServerConfig(),
            testGrpcService);

        auto error = bootstrap->Start();
        UNIT_ASSERT_VALUES_EQUAL_C(E_INVALID_STATE, error.GetCode(), error);

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldHandleNbsSpecificErrorCodes)
    {
        NProto::TError storageError;

        TUnixSocketPath socketPath("./TestUnixSocket");
        TNetworkAddress connectAddress(socketPath);

        auto storage = std::make_shared<TTestStorage>();

        storage->ReadBlocksLocalHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            Y_UNUSED(callContext);

            auto guard = request->Sglist.Acquire();
            for (const auto& buf: guard.Get()) {
                memset((void*)buf.Data(), 'Y', buf.Size());
            }
            return MakeFuture<NProto::TReadBlocksLocalResponse>(
                TErrorResponse(storageError));
        };

        auto bootstrap = CreateBootstrap(connectAddress, storage);

        auto error = bootstrap->Start();
        UNIT_ASSERT_C(!HasError(error), error);

        TVector<EWellKnownResultCodes> nbsErrorCodes = {
            S_OK,
            S_FALSE,
            S_ALREADY,
            E_FAIL,
            E_ARGUMENT,
            E_REJECTED,
            E_INVALID_STATE,
            E_TIMEOUT,
            E_NOT_FOUND,
            E_UNAUTHORIZED,
            E_NOT_IMPLEMENTED,
            E_ABORTED,
            E_TRY_AGAIN,
            E_IO,
            E_CANCELLED,
            E_IO_SILENT,
            E_RETRY_TIMEOUT,
        };

        for (auto nbsErrorCode: nbsErrorCodes) {
            storageError = MakeError(nbsErrorCode, "error message");

            auto error = ReadBlocksLocal(bootstrap->GetClientEndpoint());

            ui32 expectedCode = FAILED(error.GetCode()) ? nbsErrorCode : 0;
            UNIT_ASSERT_VALUES_EQUAL(expectedCode, error.GetCode());
        }

        bootstrap->Stop(true);
    }
}

}   // namespace NCloud::NBlockStore::NBD
