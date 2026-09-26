#include "server.h"

#include "client.h"
#include "client_handler.h"
#include "error_handler.h"
#include "protocol.h"
#include "server_handler.h"
#include "utils.h"

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

#include <library/cpp/coroutine/engine/impl.h>

#include <util/generic/guid.h>
#include <util/generic/scope.h>
#include <util/network/sock.h>

#include <atomic>

namespace NCloud::NBlockStore::NBD {

using namespace NThreading;
using namespace NCloud::NBlockStore::NClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr bool StructuredReply = true;
constexpr bool UseNbsErrors = true;

////////////////////////////////////////////////////////////////////////////////

class TBlockingErrorHandler final
    : public IErrorHandler
{
public:
    TManualEvent SendStopping;
    TManualEvent ContinueSend;

    void ProcessException(std::exception_ptr exception) override
    {
        try {
            std::rethrow_exception(std::move(exception));
        } catch (const TSystemError& e) {
            if (e.Status() == -ESHUTDOWN) {
                SendStopping.Signal();
                ContinueSend.Wait();
            }
        } catch (...) {
            UNIT_FAIL(CurrentExceptionMessage());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestErrorHandler final
    : IErrorHandler
{
    TManualEvent ErrorReported;

    void ProcessException(std::exception_ptr) override
    {
        ErrorReported.Signal();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRawNbdClient
{
private:
    TInetStreamSocket Socket4;
    TInet6StreamSocket Socket6;
    TStreamSocket* Socket = nullptr;
    TStreamSocketInput Input{nullptr};
    TStreamSocketOutput Output{nullptr};
    TRequestReader Reader{Input};
    TRequestWriter Writer{Output};

public:
    explicit TRawNbdClient(ui16 port, bool ipv6 = true)
    {
        int error;
        if (ipv6) {
            TSockAddrInet6 address("::1", port);
            Socket = &Socket6;
            error = Socket->Connect(&address);
        } else {
            TSockAddrInet address("127.0.0.1", port);
            Socket = &Socket4;
            error = Socket->Connect(&address);
        }

        UNIT_ASSERT_VALUES_EQUAL_C(0, error, "failed to connect raw NBD client");
        Input.SetSocket(Socket);
        Output.SetSocket(Socket);
        SetSocketTimeout(*Socket, 5);
    }

    void ReadServerHello()
    {
        TServerHello hello;
        UNIT_ASSERT(Reader.ReadServerHello(hello));
        UNIT_ASSERT_VALUES_EQUAL(NBD_MAGIC, hello.Passwd);
        UNIT_ASSERT_VALUES_EQUAL(NBD_OPTS_MAGIC, hello.Magic);
    }

    void WriteClientHello()
    {
        Writer.WriteClientHello(
            NBD_FLAG_C_FIXED_NEWSTYLE | NBD_FLAG_C_NO_ZEROES);
    }

    void WriteGo()
    {
        TExportInfoRequest request;
        request.InfoTypes = {NBD_INFO_BLOCK_SIZE};

        TBufferRequestWriter requestOut;
        requestOut.WriteExportInfoRequest(request);
        Writer.WriteOption(NBD_OPT_GO, AsStringBuf(requestOut.Buffer()));
    }

    void Disconnect(bool reset = false)
    {
        if (reset) {
            SetZeroLinger(*Socket);
        }
        Socket->Close();
    }

    void WaitForDisconnect()
    {
        char c;
        UNIT_ASSERT_VALUES_EQUAL_C(
            0,
            Socket->Recv(&c, sizeof(c)),
            "raw NBD client was not disconnected");
    }
};

////////////////////////////////////////////////////////////////////////////////

using TNegotiateHandler = std::function<bool(
    IServerHandler&,
    IInputStream&,
    IOutputStream&,
    const std::function<bool()>&)>;

class TServerHandlerDecorator final
    : public IServerHandler
{
private:
    const IServerHandlerPtr Inner;
    const TNegotiateHandler NegotiateHandler;

public:
    TServerHandlerDecorator(
            IServerHandlerPtr inner,
            TNegotiateHandler negotiateHandler)
        : Inner(std::move(inner))
        , NegotiateHandler(std::move(negotiateHandler))
    {}

    bool NegotiateClient(
        IInputStream& in,
        IOutputStream& out,
        const std::function<bool()>& connectionReadyHandler) override
    {
        return NegotiateHandler(
            *Inner,
            in,
            out,
            connectionReadyHandler);
    }

    void SendResponse(
        IOutputStream& out,
        TServerResponse& response) override
    {
        Inner->SendResponse(out, response);
    }

    void ProcessRequests(
        IServerContextPtr ctx,
        IInputStream& in,
        IOutputStream& out,
        TCont* cont) override
    {
        Inner->ProcessRequests(std::move(ctx), in, out, cont);
    }

    void ProcessException(std::exception_ptr e) override
    {
        Inner->ProcessException(std::move(e));
    }

    size_t CollectRequests(
        const TIncompleteRequestsCollector& collector) override
    {
        return Inner->CollectRequests(collector);
    }
};

using THandlerDecorator =
    std::function<IServerHandlerPtr(size_t, IServerHandlerPtr)>;

class TServerHandlerFactoryDecorator final
    : public IServerHandlerFactory
{
private:
    const IServerHandlerFactoryPtr Inner;
    const THandlerDecorator Decorator;
    std::atomic<size_t> HandlerCount = 0;

public:
    TServerHandlerFactoryDecorator(
            IServerHandlerFactoryPtr inner,
            THandlerDecorator decorator)
        : Inner(std::move(inner))
        , Decorator(std::move(decorator))
    {}

    IServerHandlerPtr CreateHandler() override
    {
        return Decorator(
            HandlerCount.fetch_add(1),
            Inner->CreateHandler());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFailingOutput final
    : public IOutputStream
{
private:
    IOutputStream& Inner;
    TManualEvent& WriteAttempted;
    bool Failing = false;

public:
    TFailingOutput(IOutputStream& inner, TManualEvent& writeAttempted)
        : Inner(inner)
        , WriteAttempted(writeAttempted)
    {}

    void FailWrites()
    {
        Failing = true;
    }

private:
    void DoWrite(const void* buf, size_t len) override
    {
        if (Failing) {
            WriteAttempted.Signal();
            ythrow TSystemError(EPIPE) << "injected GO response write failure";
        }

        Inner.Write(buf, len);
    }
};

////////////////////////////////////////////////////////////////////////////////

void ConnectInvalidClient(ui16 port)
{
    TInet6StreamSocket socket;
    TSockAddrInet6 address("::1", port);

    UNIT_ASSERT_VALUES_EQUAL_C(
        0,
        socket.Connect(&address),
        "failed to connect invalid client");

    TStreamSocketInput input(&socket);
    TRequestReader reader(input);

    TServerHello hello;
    UNIT_ASSERT(reader.ReadServerHello(hello));

    TStreamSocketOutput output(&socket);
    TRequestWriter writer(output);
    writer.WriteClientHello(0);

    SetSocketTimeout(socket, 3);
    char c;
    UNIT_ASSERT_VALUES_EQUAL_C(
        0,
        socket.Recv(&c, sizeof(c)),
        "invalid client connection was not closed");
}

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
            auto future = StopEndpointAsync();
            auto error = future.GetValue(TDuration::Seconds(3));
            UNIT_ASSERT_C(!HasError(error), error);
        }
    }

    TFuture<NProto::TError> StopEndpointAsync()
    {
        Y_ABORT_UNLESS(Server);
        return Server->StopEndpoint(ConnectAddress);
    }

    NProto::TError StartEndpoint()
    {
        auto future = Server->StartEndpoint(
            ConnectAddress,
            HandlerFactory);

        return future.GetValue(TDuration::Seconds(3));
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
        return StartEndpoint();
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
    IBlockStorePtr grpcClientEndpoint = nullptr,
    IErrorHandlerPtr errorHandler = nullptr,
    THandlerDecorator handlerDecorator = {})
{
    const ui32 clientThreadsCount = 1;

    auto logging = CreateLoggingService("console", { TLOG_DEBUG });

    auto timer = CreateWallClockTimer();
    auto scheduler = CreateScheduler();

    auto server = CreateServer(logging, serverConfig);

    if (!errorHandler) {
        errorHandler = CreateErrorHandlerStub();
    }

    auto handlerFactory = CreateServerHandlerFactory(
        CreateDefaultDeviceHandlerFactory(),
        logging,
        std::move(storage),
        CreateServerStatsStub(),
        std::move(errorHandler),
        options);

    if (handlerDecorator) {
        handlerFactory = std::make_shared<TServerHandlerFactoryDecorator>(
            std::move(handlerFactory),
            std::move(handlerDecorator));
    }

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
    request->SetBlockSize(DefaultBlockSize);
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
    request->SetBlockSize(DefaultBlockSize);
    request->BlocksCount = blocksCount;
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
            request->SetBlockSize(DefaultBlockSize);
            request->BlocksCount = blocksCount;
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
            request->SetBlockSize(DefaultBlockSize);
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

        auto stopFuture = bootstrap->StopEndpointAsync();
        UNIT_ASSERT(!stopFuture.Wait(TDuration::MilliSeconds(100)));

        trigger.SetValue(TErrorResponse(E_INVALID_STATE, "Any fatal error"));

        error = stopFuture.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(error), error);

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

    Y_UNIT_TEST(ShouldDrainRequestsBeforeEndpointRestart)
    {
        const ui32 startIndex = 0;
        const ui64 blocksCount = 42;

        TManualEvent firstRequestStarted;
        TManualEvent secondRequestStarted;
        auto firstRequestCompleted =
            NewPromise<NProto::TZeroBlocksResponse>();
        std::atomic<size_t> requestCount = 0;

        auto storage = std::make_shared<TTestStorage>();
        storage->ZeroBlocksHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TZeroBlocksRequest> request)
        {
            Y_UNUSED(callContext);

            UNIT_ASSERT_VALUES_EQUAL(startIndex, request->GetStartIndex());
            UNIT_ASSERT_VALUES_EQUAL(blocksCount, request->GetBlocksCount());

            if (requestCount.fetch_add(1) == 0) {
                firstRequestStarted.Signal();
                return firstRequestCompleted.GetFuture();
            }

            secondRequestStarted.Signal();
            return MakeFuture<NProto::TZeroBlocksResponse>();
        };

        TPortManager portManager;
        auto port = portManager.GetPort(9001);
        TNetworkAddress connectAddress(port);

        auto bootstrap = CreateBootstrap(connectAddress, storage);

        auto error = bootstrap->Start();
        UNIT_ASSERT_C(!HasError(error), error);

        auto firstRequest = std::make_shared<NProto::TZeroBlocksRequest>();
        firstRequest->SetStartIndex(startIndex);
        firstRequest->SetBlocksCount(blocksCount);

        auto firstRequestFuture = bootstrap->GetClientEndpoint()->ZeroBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(firstRequest));

        UNIT_ASSERT(firstRequestStarted.WaitT(TDuration::Seconds(5)));

        auto stopFuture = bootstrap->StopEndpointAsync();
        UNIT_ASSERT(!stopFuture.Wait(TDuration::MilliSeconds(100)));

        firstRequestCompleted.SetValue({});

        error = stopFuture.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(error), error);
        UNIT_ASSERT(firstRequestFuture.Wait(TDuration::Seconds(5)));

        error = bootstrap->StartEndpoint();
        UNIT_ASSERT_C(!HasError(error), error);

        auto secondClientEndpoint = bootstrap->GetClient()->CreateEndpoint(
            connectAddress,
            CreateClientHandler(
                bootstrap->GetLogging(),
                StructuredReply,
                UseNbsErrors),
            bootstrap->GetGrpcClientEndpoint());
        secondClientEndpoint->Start();

        auto mountRequest = std::make_shared<NProto::TMountVolumeRequest>();
        mountRequest->SetDiskId(DefaultStorageOptions.DiskId);
        auto mountResponse = secondClientEndpoint->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::move(mountRequest)).GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(mountResponse), mountResponse);

        auto secondRequest = std::make_shared<NProto::TZeroBlocksRequest>();
        secondRequest->SetStartIndex(startIndex);
        secondRequest->SetBlocksCount(blocksCount);

        auto secondRequestFuture = secondClientEndpoint->ZeroBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(secondRequest));

        UNIT_ASSERT(secondRequestStarted.WaitT(TDuration::Seconds(5)));
        auto secondResponse =
            secondRequestFuture.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(secondResponse), secondResponse);

        secondClientEndpoint->Stop();
        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldCompleteDrainAfterClientDisconnect)
    {
        auto storage = std::make_shared<TTestStorage>();
        storage->ZeroBlocksHandler = [] (
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

        auto bootstrap = CreateBootstrap(connectAddress, storage);

        auto error = bootstrap->Start();
        UNIT_ASSERT_C(!HasError(error), error);

        bootstrap->GetClientEndpoint()->Stop();

        // Let the server observe the peer disconnect before accepting the next
        // connection. Its drain must be completed by the send loop shutdown.
        Sleep(TDuration::MilliSeconds(100));

        auto secondClientEndpoint = bootstrap->GetClient()->CreateEndpoint(
            connectAddress,
            CreateClientHandler(
                bootstrap->GetLogging(),
                StructuredReply,
                UseNbsErrors),
            bootstrap->GetGrpcClientEndpoint());
        secondClientEndpoint->Start();

        auto mountRequest = std::make_shared<NProto::TMountVolumeRequest>();
        mountRequest->SetDiskId(DefaultStorageOptions.DiskId);
        auto mountResponse = secondClientEndpoint->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::move(mountRequest)).GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(mountResponse), mountResponse);

        error = ZeroBlocks(secondClientEndpoint);
        UNIT_ASSERT_C(!HasError(error), error);

        secondClientEndpoint->Stop();
        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldCompleteDrainIfRequestFinishesBeforeSendShutdown)
    {
        TManualEvent requestStarted;
        auto requestCompleted = NewPromise<NProto::TZeroBlocksResponse>();
        std::atomic<size_t> requestCount = 0;

        auto storage = std::make_shared<TTestStorage>();
        storage->ZeroBlocksHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TZeroBlocksRequest> request)
        {
            Y_UNUSED(callContext);
            Y_UNUSED(request);

            if (requestCount.fetch_add(1) == 0) {
                requestStarted.Signal();
                return requestCompleted.GetFuture();
            }

            return MakeFuture<NProto::TZeroBlocksResponse>();
        };

        auto errorHandler = std::make_shared<TBlockingErrorHandler>();
        Y_DEFER {
            errorHandler->ContinueSend.Signal();
        };

        TPortManager portManager;
        auto port = portManager.GetPort(9001);
        TNetworkAddress connectAddress(port);

        TServerConfig serverConfig;
        serverConfig.ThreadsCount = 2;

        auto bootstrap = CreateBootstrap(
            connectAddress,
            storage,
            DefaultStorageOptions,
            serverConfig,
            nullptr,
            errorHandler);

        auto error = bootstrap->Start();
        UNIT_ASSERT_C(!HasError(error), error);

        auto request = std::make_shared<NProto::TZeroBlocksRequest>();
        request->SetStartIndex(0);
        request->SetBlocksCount(42);
        auto requestFuture = bootstrap->GetClientEndpoint()->ZeroBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(request));

        UNIT_ASSERT(requestStarted.WaitT(TDuration::Seconds(5)));

        bootstrap->GetClientEndpoint()->Stop();
        UNIT_ASSERT(
            errorHandler->SendStopping.WaitT(TDuration::Seconds(5)));

        requestCompleted.SetValue({});

        // The backend runs on another executor. Keep Send blocked until it has
        // enqueued the response and decremented ActiveRequests.
        Sleep(TDuration::MilliSeconds(100));
        errorHandler->ContinueSend.Signal();

        auto secondClientEndpoint = bootstrap->GetClient()->CreateEndpoint(
            connectAddress,
            CreateClientHandler(
                bootstrap->GetLogging(),
                StructuredReply,
                UseNbsErrors),
            bootstrap->GetGrpcClientEndpoint());
        secondClientEndpoint->Start();

        auto mountRequest = std::make_shared<NProto::TMountVolumeRequest>();
        mountRequest->SetDiskId(DefaultStorageOptions.DiskId);
        auto mountResponse = secondClientEndpoint->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::move(mountRequest)).GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(mountResponse), mountResponse);

        error = ZeroBlocks(secondClientEndpoint);
        UNIT_ASSERT_C(!HasError(error), error);
        UNIT_ASSERT(requestFuture.Wait(TDuration::Seconds(5)));

        secondClientEndpoint->Stop();
        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldDrainRequestsBeforeMountingNewConnection)
    {
        TManualEvent firstRequestStarted;
        auto firstRequestCompleted =
            NewPromise<NProto::TZeroBlocksResponse>();
        std::atomic<size_t> requestCount = 0;

        auto storage = std::make_shared<TTestStorage>();
        storage->ZeroBlocksHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TZeroBlocksRequest> request)
        {
            Y_UNUSED(callContext);
            Y_UNUSED(request);

            if (requestCount.fetch_add(1) == 0) {
                firstRequestStarted.Signal();
                return firstRequestCompleted.GetFuture();
            }

            return MakeFuture<NProto::TZeroBlocksResponse>();
        };

        TFsPath unixSocket(CreateGuidAsString() + ".sock");
        TNetworkAddress connectAddress(TUnixSocketPath(unixSocket.GetPath()));
        auto bootstrap = CreateBootstrap(connectAddress, storage);
        Y_DEFER {
            firstRequestCompleted.TrySetValue({});
        };

        auto error = bootstrap->Start();
        UNIT_ASSERT_C(!HasError(error), error);

        auto firstRequest = std::make_shared<NProto::TZeroBlocksRequest>();
        firstRequest->SetStartIndex(0);
        firstRequest->SetBlocksCount(42);
        auto firstRequestFuture = bootstrap->GetClientEndpoint()->ZeroBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(firstRequest));

        UNIT_ASSERT(firstRequestStarted.WaitT(TDuration::Seconds(5)));

        auto secondClientEndpoint = bootstrap->GetClient()->CreateEndpoint(
            connectAddress,
            CreateClientHandler(
                bootstrap->GetLogging(),
                StructuredReply,
                UseNbsErrors),
            bootstrap->GetGrpcClientEndpoint());
        secondClientEndpoint->Start();

        auto mountRequest = std::make_shared<NProto::TMountVolumeRequest>();
        mountRequest->SetDiskId(DefaultStorageOptions.DiskId);
        auto mountFuture = secondClientEndpoint->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::move(mountRequest));

        // Accepting the new connection closes the old client's socket, but
        // its backend request remains pending until we complete the promise.
        UNIT_ASSERT(firstRequestFuture.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(!mountFuture.Wait(TDuration::MilliSeconds(100)));

        firstRequestCompleted.SetValue({});

        auto mountResponse = mountFuture.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(mountResponse), mountResponse);

        error = ZeroBlocks(secondClientEndpoint);
        UNIT_ASSERT_C(!HasError(error), error);
        UNIT_ASSERT_VALUES_EQUAL(2, requestCount.load());

        secondClientEndpoint->Stop();
        bootstrap->Stop();
    }

    // A client stalled before NBD_OPT_GO must not block a later valid client.
    Y_UNIT_TEST(ShouldAcceptAnotherCandidateWhileOneStallsBeforeGo)
    {
        auto storage = std::make_shared<TTestStorage>();

        TPortManager portManager;
        auto port = portManager.GetPort(9001);
        TNetworkAddress connectAddress(port);

        auto bootstrap = CreateBootstrap(connectAddress, storage);
        auto error = bootstrap->Start();
        UNIT_ASSERT_C(!HasError(error), error);

        TRawNbdClient candidate(port, false);
        candidate.ReadServerHello();
        candidate.WriteClientHello();

        auto nextClient = bootstrap->GetClient()->CreateEndpoint(
            connectAddress,
            CreateClientHandler(
                bootstrap->GetLogging(),
                StructuredReply,
                UseNbsErrors),
            bootstrap->GetGrpcClientEndpoint());
        nextClient->Start();

        auto mountRequest = std::make_shared<NProto::TMountVolumeRequest>();
        mountRequest->SetDiskId(DefaultStorageOptions.DiskId);
        auto mountFuture = nextClient->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::move(mountRequest));

        auto mountResponse = mountFuture.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(mountResponse), mountResponse);

        candidate.WaitForDisconnect();
        nextClient->Stop();
        bootstrap->Stop();
    }

    // A candidate disconnected during drain must not prevent a later handoff.
    Y_UNIT_TEST(ShouldRejectCandidateDisconnectedWhileActiveConnectionDrains)
    {
        TManualEvent firstRequestStarted;
        auto firstRequestCompleted =
            NewPromise<NProto::TZeroBlocksResponse>();

        auto storage = std::make_shared<TTestStorage>();
        storage->ZeroBlocksHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TZeroBlocksRequest> request)
        {
            Y_UNUSED(callContext);
            Y_UNUSED(request);

            firstRequestStarted.Signal();
            return firstRequestCompleted.GetFuture();
        };

        TPortManager portManager;
        auto port = portManager.GetPort(9001);
        TNetworkAddress connectAddress(port);

        auto bootstrap = CreateBootstrap(connectAddress, storage);
        Y_DEFER {
            firstRequestCompleted.TrySetValue({});
        };

        auto error = bootstrap->Start();
        UNIT_ASSERT_C(!HasError(error), error);

        auto firstRequest = std::make_shared<NProto::TZeroBlocksRequest>();
        firstRequest->SetStartIndex(0);
        firstRequest->SetBlocksCount(1);
        auto firstRequestFuture = bootstrap->GetClientEndpoint()->ZeroBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(firstRequest));
        UNIT_ASSERT(firstRequestStarted.WaitT(TDuration::Seconds(5)));

        TRawNbdClient candidate(port, false);
        candidate.ReadServerHello();
        candidate.WriteClientHello();
        candidate.WriteGo();

        // A closed socket means the server reached the drain phase.
        UNIT_ASSERT(firstRequestFuture.Wait(TDuration::Seconds(5)));
        candidate.Disconnect(true);

        auto nextClient = bootstrap->GetClient()->CreateEndpoint(
            connectAddress,
            CreateClientHandler(
                bootstrap->GetLogging(),
                StructuredReply,
                UseNbsErrors),
            bootstrap->GetGrpcClientEndpoint());
        nextClient->Start();

        auto mountRequest = std::make_shared<NProto::TMountVolumeRequest>();
        mountRequest->SetDiskId(DefaultStorageOptions.DiskId);
        auto mountFuture = nextClient->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::move(mountRequest));
        UNIT_ASSERT(!mountFuture.Wait(TDuration::MilliSeconds(100)));

        firstRequestCompleted.SetValue({});

        auto mountResponse = mountFuture.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(mountResponse), mountResponse);

        nextClient->Stop();
        bootstrap->Stop();
    }

    // Handoff and endpoint shutdown must wait for active backend I/O to drain.
    Y_UNIT_TEST(ShouldKeepHandoffAndEndpointShutdownPendingWhileDraining)
    {
        TManualEvent firstRequestStarted;
        auto firstRequestCompleted =
            NewPromise<NProto::TZeroBlocksResponse>();

        auto storage = std::make_shared<TTestStorage>();
        storage->ZeroBlocksHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TZeroBlocksRequest> request)
        {
            Y_UNUSED(callContext);
            Y_UNUSED(request);

            firstRequestStarted.Signal();
            return firstRequestCompleted.GetFuture();
        };

        TPortManager portManager;
        auto port = portManager.GetPort(9001);
        TNetworkAddress connectAddress(port);

        auto bootstrap = CreateBootstrap(connectAddress, storage);
        Y_DEFER {
            firstRequestCompleted.TrySetValue({});
        };

        auto error = bootstrap->Start();
        UNIT_ASSERT_C(!HasError(error), error);

        auto firstRequest = std::make_shared<NProto::TZeroBlocksRequest>();
        firstRequest->SetStartIndex(0);
        firstRequest->SetBlocksCount(1);
        auto firstRequestFuture = bootstrap->GetClientEndpoint()->ZeroBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(firstRequest));
        UNIT_ASSERT(firstRequestStarted.WaitT(TDuration::Seconds(5)));

        auto candidate = bootstrap->GetClient()->CreateEndpoint(
            connectAddress,
            CreateClientHandler(
                bootstrap->GetLogging(),
                StructuredReply,
                UseNbsErrors),
            bootstrap->GetGrpcClientEndpoint());
        candidate->Start();

        auto mountRequest = std::make_shared<NProto::TMountVolumeRequest>();
        mountRequest->SetDiskId(DefaultStorageOptions.DiskId);
        auto mountFuture = candidate->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::move(mountRequest));

        UNIT_ASSERT(firstRequestFuture.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(!mountFuture.Wait(TDuration::MilliSeconds(100)));

        auto stopFuture = bootstrap->StopEndpointAsync();
        UNIT_ASSERT(!stopFuture.Wait(TDuration::MilliSeconds(100)));

        firstRequestCompleted.SetValue({});

        auto stopError = stopFuture.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(stopError), stopError);
        UNIT_ASSERT(mountFuture.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(HasError(mountFuture.GetValue()));

        candidate->Stop();
        bootstrap->Stop(true);
    }

    // Endpoint shutdown must cancel a client stalled before NBD_OPT_GO.
    Y_UNIT_TEST(ShouldCompleteEndpointShutdownWhileWaitingForNegotiationReady)
    {
        auto storage = std::make_shared<TTestStorage>();

        TPortManager portManager;
        auto port = portManager.GetPort(9001);
        TNetworkAddress connectAddress(port);

        auto bootstrap = CreateBootstrap(connectAddress, storage);
        auto error = bootstrap->Start();
        UNIT_ASSERT_C(!HasError(error), error);

        TRawNbdClient candidate(port);
        candidate.ReadServerHello();
        candidate.WriteClientHello();

        auto stopError = bootstrap->StopEndpointAsync().GetValue(
            TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(stopError), stopError);

        candidate.WaitForDisconnect();
        bootstrap->Stop(true);
    }

    // Endpoint shutdown must not wait for negotiation after candidate selection.
    Y_UNIT_TEST(ShouldCompleteEndpointShutdownWhileWaitingForNegotiationResult)
    {
        TManualEvent activationCompleted;
        TManualEvent negotiationCompleted;
        std::atomic<bool> continueNegotiation = false;

        THandlerDecorator decorator = [&] (
            size_t handlerIndex,
            IServerHandlerPtr handler) -> IServerHandlerPtr
        {
            if (handlerIndex != 1) {
                return handler;
            }

            TNegotiateHandler negotiateHandler = [&] (
                IServerHandler& inner,
                IInputStream& in,
                IOutputStream& out,
                const std::function<bool()>& connectionReadyHandler)
            {
                auto wrappedReadyHandler = [&] {
                    const bool result = connectionReadyHandler();
                    activationCompleted.Signal();

                    while (!continueNegotiation.load()) {
                        RunningCont()->SleepT(TDuration::MilliSeconds(10));
                    }

                    return result;
                };

                Y_DEFER {
                    negotiationCompleted.Signal();
                };
                return inner.NegotiateClient(
                    in,
                    out,
                    wrappedReadyHandler);
            };

            return std::make_shared<TServerHandlerDecorator>(
                std::move(handler),
                std::move(negotiateHandler));
        };

        auto storage = std::make_shared<TTestStorage>();

        TPortManager portManager;
        auto port = portManager.GetPort(9001);
        TNetworkAddress connectAddress(port);

        auto bootstrap = CreateBootstrap(
            connectAddress,
            storage,
            DefaultStorageOptions,
            Default<TServerConfig>(),
            nullptr,
            nullptr,
            std::move(decorator));
        Y_DEFER {
            continueNegotiation.store(true);
        };

        auto error = bootstrap->Start();
        UNIT_ASSERT_C(!HasError(error), error);

        TRawNbdClient candidate(port);
        candidate.ReadServerHello();
        candidate.WriteClientHello();
        candidate.WriteGo();
        UNIT_ASSERT(activationCompleted.WaitT(TDuration::Seconds(5)));

        auto stopError = bootstrap->StopEndpointAsync().GetValue(
            TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(stopError), stopError);

        continueNegotiation.store(true);
        UNIT_ASSERT(negotiationCompleted.WaitT(TDuration::Seconds(5)));
        candidate.WaitForDisconnect();

        bootstrap->Stop(true);
    }

    // A failed GO response must reject the candidate without restarting the endpoint.
    Y_UNIT_TEST(ShouldRejectCandidateWhenWritingGoResponseFails)
    {
        TManualEvent writeAttempted;
        THandlerDecorator decorator = [&] (
            size_t handlerIndex,
            IServerHandlerPtr handler) -> IServerHandlerPtr
        {
            if (handlerIndex != 1) {
                return handler;
            }

            TNegotiateHandler negotiateHandler = [&] (
                IServerHandler& inner,
                IInputStream& in,
                IOutputStream& out,
                const std::function<bool()>& connectionReadyHandler)
            {
                TFailingOutput failingOutput(out, writeAttempted);
                auto wrappedReadyHandler = [&] {
                    const bool result = connectionReadyHandler();
                    if (result) {
                        failingOutput.FailWrites();
                    }
                    return result;
                };

                return inner.NegotiateClient(
                    in,
                    failingOutput,
                    wrappedReadyHandler);
            };

            return std::make_shared<TServerHandlerDecorator>(
                std::move(handler),
                std::move(negotiateHandler));
        };

        auto storage = std::make_shared<TTestStorage>();
        auto errorHandler = std::make_shared<TTestErrorHandler>();

        TPortManager portManager;
        auto port = portManager.GetPort(9001);
        TNetworkAddress connectAddress(port);

        auto bootstrap = CreateBootstrap(
            connectAddress,
            storage,
            DefaultStorageOptions,
            Default<TServerConfig>(),
            nullptr,
            errorHandler,
            std::move(decorator));
        auto error = bootstrap->Start();
        UNIT_ASSERT_C(!HasError(error), error);

        TRawNbdClient candidate(port);
        candidate.ReadServerHello();
        candidate.WriteClientHello();
        candidate.WriteGo();

        UNIT_ASSERT(writeAttempted.WaitT(TDuration::Seconds(5)));
        candidate.WaitForDisconnect();
        UNIT_ASSERT_C(
            !errorHandler->ErrorReported.WaitT(TDuration::Zero()),
            "candidate write failure was reported to the endpoint");

        auto nextClient = bootstrap->GetClient()->CreateEndpoint(
            connectAddress,
            CreateClientHandler(
                bootstrap->GetLogging(),
                StructuredReply,
                UseNbsErrors),
            bootstrap->GetGrpcClientEndpoint());
        nextClient->Start();

        auto mountRequest = std::make_shared<NProto::TMountVolumeRequest>();
        mountRequest->SetDiskId(DefaultStorageOptions.DiskId);
        auto mountResponse = nextClient->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::move(mountRequest)).GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(mountResponse), mountResponse);

        nextClient->Stop();
        bootstrap->Stop();
    }

    // If candidates share a drain, only the latest candidate may become active.
    Y_UNIT_TEST(ShouldActivateOnlyLatestCandidateWaitingForSameDrain)
    {
        TManualEvent firstRequestStarted;
        auto firstRequestCompleted =
            NewPromise<NProto::TZeroBlocksResponse>();
        std::atomic<size_t> requestCount = 0;

        auto storage = std::make_shared<TTestStorage>();
        storage->ZeroBlocksHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TZeroBlocksRequest> request)
        {
            Y_UNUSED(callContext);
            Y_UNUSED(request);

            if (requestCount.fetch_add(1) == 0) {
                firstRequestStarted.Signal();
                return firstRequestCompleted.GetFuture();
            }

            return MakeFuture<NProto::TZeroBlocksResponse>();
        };

        TPortManager portManager;
        auto port = portManager.GetPort(9001);
        TNetworkAddress connectAddress(port);

        auto bootstrap = CreateBootstrap(connectAddress, storage);
        Y_DEFER {
            firstRequestCompleted.TrySetValue({});
        };

        auto error = bootstrap->Start();
        UNIT_ASSERT_C(!HasError(error), error);

        auto firstRequest = std::make_shared<NProto::TZeroBlocksRequest>();
        firstRequest->SetStartIndex(0);
        firstRequest->SetBlocksCount(1);
        auto firstRequestFuture = bootstrap->GetClientEndpoint()->ZeroBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(firstRequest));
        UNIT_ASSERT(firstRequestStarted.WaitT(TDuration::Seconds(5)));

        TRawNbdClient firstCandidate(port, false);
        firstCandidate.ReadServerHello();
        firstCandidate.WriteClientHello();
        firstCandidate.WriteGo();

        UNIT_ASSERT(firstRequestFuture.Wait(TDuration::Seconds(5)));

        auto secondCandidate = bootstrap->GetClient()->CreateEndpoint(
            connectAddress,
            CreateClientHandler(
                bootstrap->GetLogging(),
                StructuredReply,
                UseNbsErrors),
            bootstrap->GetGrpcClientEndpoint());
        secondCandidate->Start();

        auto secondMountRequest =
            std::make_shared<NProto::TMountVolumeRequest>();
        secondMountRequest->SetDiskId(DefaultStorageOptions.DiskId);
        auto secondMountFuture = secondCandidate->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::move(secondMountRequest));
        firstCandidate.WaitForDisconnect();
        UNIT_ASSERT(!secondMountFuture.Wait(TDuration::MilliSeconds(100)));

        firstRequestCompleted.SetValue({});

        auto secondMountResponse =
            secondMountFuture.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(secondMountResponse), secondMountResponse);

        error = ZeroBlocks(secondCandidate);
        UNIT_ASSERT_C(!HasError(error), error);
        UNIT_ASSERT_VALUES_EQUAL(2, requestCount.load());

        secondCandidate->Stop();
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

    Y_UNIT_TEST(ShouldNotDropActiveConnectionOnInvalidConnection)
    {
        const ui32 startIndex = 13;
        const ui32 blocksCount = 1;

        TManualEvent requestReceived;
        auto trigger = NewPromise<NProto::TZeroBlocksResponse>();

        auto storage = std::make_shared<TTestStorage>();
        storage->ZeroBlocksHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TZeroBlocksRequest> request)
        {
            Y_UNUSED(callContext);

            UNIT_ASSERT_VALUES_EQUAL(startIndex, request->GetStartIndex());
            UNIT_ASSERT_VALUES_EQUAL(blocksCount, request->GetBlocksCount());

            requestReceived.Signal();
            return trigger.GetFuture();
        };

        TPortManager portManager;
        auto port = portManager.GetPort(9001);
        TNetworkAddress connectAddress(port);

        auto errorHandler = std::make_shared<TTestErrorHandler>();
        auto bootstrap = CreateBootstrap(
            connectAddress,
            storage,
            DefaultStorageOptions,
            Default<TServerConfig>(),
            nullptr,
            errorHandler);

        auto error = bootstrap->Start();
        UNIT_ASSERT_C(!HasError(error), error);

        auto request = std::make_shared<NProto::TZeroBlocksRequest>();
        request->SetStartIndex(startIndex);
        request->SetBlocksCount(blocksCount);

        auto future = bootstrap->GetClientEndpoint()->ZeroBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(request));

        requestReceived.Wait();
        UNIT_ASSERT(!future.HasValue());

        ConnectInvalidClient(port);
        UNIT_ASSERT_C(
            !errorHandler->ErrorReported.WaitT(TDuration::Zero()),
            "invalid client error was reported to the endpoint");

        trigger.SetValue({});

        auto response = future.GetValue(TDuration::Seconds(3));
        UNIT_ASSERT_C(!HasError(response), response);

        bootstrap->GetClientEndpoint()->Stop();
        UNIT_ASSERT_C(
            errorHandler->ErrorReported.WaitT(TDuration::Seconds(3)),
            "active client shutdown was not reported to the endpoint");

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
