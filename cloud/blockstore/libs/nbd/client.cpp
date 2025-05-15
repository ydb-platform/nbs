#include "client.h"

#include "client_handler.h"
#include "protocol.h"
#include "utils.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/common/random.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/coroutine/queue.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/coroutine/engine/network.h>
#include <library/cpp/coroutine/engine/sockpool.h>
#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/datetime/cputimer.h>
#include <util/network/address.h>
#include <util/network/socket.h>
#include <util/system/thread.h>

#include <atomic>

namespace NCloud::NBlockStore::NBD {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

static constexpr ui32 UnavailableError = E_GRPC_UNAVAILABLE;

////////////////////////////////////////////////////////////////////////////////

class TEndpointBase
    : public IBlockStore
{
public:
    TEndpointBase() = default;

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        Y_UNUSED(callContext);                                                 \
        Y_UNUSED(request);                                                     \
        const auto& type = GetBlockStoreRequestName(EBlockStoreRequest::name); \
        return MakeFuture<NProto::T##name##Response>(TErrorResponse(           \
            E_NOT_IMPLEMENTED,                                                 \
            TStringBuilder() << "Unsupported request " << type.Quote()));      \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD
};

////////////////////////////////////////////////////////////////////////////////

template <typename TResponse>
struct TClientRequestImpl final
    : public TClientRequest
{
    TPromise<TResponse> Response = NewPromise<TResponse>();

    using TClientRequest::TClientRequest;

    void Complete(const NProto::TError& error) override
    {
        Response.SetValue(TErrorResponse(error));
    }
};

using TClientReadRequest = TClientRequestImpl<NProto::TReadBlocksLocalResponse>;
using TClientWriteRequest = TClientRequestImpl<NProto::TWriteBlocksLocalResponse>;
using TClientZeroRequest = TClientRequestImpl<NProto::TZeroBlocksResponse>;
using TClientMountRequest = TClientRequestImpl<NProto::TError>;

////////////////////////////////////////////////////////////////////////////////

ui64 RequestConnectionId()
{
    static std::atomic<ui64> id = 0;
    return id.fetch_add(1, std::memory_order_relaxed);
}

////////////////////////////////////////////////////////////////////////////////

class TConnection
    : public TAtomicRefCount<TConnection>
{
private:
    TLog Log;
    const IClientHandlerPtr Handler;
    const TNetworkAddress ConnectAddress;
    const TString LogTag;

    TSocketHolder Socket;
    TContLockFreeQueue<TClientRequestPtr> RequestQueue;
    bool NeedConnect = true;

    TPromise<TExportInfo> ExportInfo = NewPromise<TExportInfo>();

public:
    TConnection(
            const TLog& log,
            TContExecutor* e,
            IClientHandlerPtr handler,
            const TNetworkAddress& connectAddress)
        : Log(log)
        , Handler(std::move(handler))
        , ConnectAddress(connectAddress)
        , LogTag(TStringBuilder() << "Conn#" << RequestConnectionId()
            << " " << PrintHostAndPort(ConnectAddress))
        , RequestQueue(e)
    {}

    void Start(TContExecutor* e)
    {
        e->Create<TConnection, &TConnection::Send>(this, "send");
    }

    void Stop()
    {
        RequestQueue.Enqueue(nullptr);
    }

    void EnqueueRequest(TClientRequestPtr request)
    {
        RequestQueue.Enqueue(std::move(request));
    }

    TFuture<TExportInfo> GetExportInfo()
    {
        return ExportInfo.GetFuture();
    }

private:
    void DoConnect(TCont* c)
    {
        if (int ret = NCoro::ConnectI(c, Socket, ConnectAddress)) {
            ythrow TSystemError(ret) << "could not connect";
        }

        if (IsTcpAddress(ConnectAddress)) {
            SetNoDelay(Socket, true);
        }

        TContIO io(Socket, c);

        if (!Handler->NegotiateClient(io, io)) {
            ythrow TServiceError(UnavailableError) << "failed to negotiate";
        }

        if (!ExportInfo.HasValue()) {
            ExportInfo.SetValue(Handler->GetExportInfo());
        }

        STORAGE_INFO(LogTag << " - connected client endpoint");

        // start coroutine to read responses
        c->Executor()->Create<TConnection, &TConnection::Receive>(this, "recv");
        NeedConnect = false;
    }

    void Send(TCont* c)
    {
        TIntrusivePtr<TConnection> holder(this);

        STORAGE_INFO(LogTag << " - started Send loop");

        TClientRequestPtr request;
        while (RequestQueue.Dequeue(&request)) {
            if (!request) {
                // stop signal received
                break;
            }

            try {
                DoSendRequest(c, request);
            } catch (...) {
                auto exceptionMessage = CurrentExceptionMessage();
                STORAGE_WARN(LogTag << " - unhandled error in Send: "
                    << exceptionMessage);

                request->Complete(
                    TErrorResponse(UnavailableError, exceptionMessage));
                NeedConnect = true;
            }

            request.Reset();
        }

        Socket.ShutDown(SHUT_RDWR);
    }

    void DoSendRequest(TCont* c, TClientRequestPtr request)
    {
        if (NeedConnect) {
            DoConnect(c);
        }

        TContIO io(Socket, c);
        Handler->SendRequest(io, std::move(request));
    }

    void Receive(TCont* c)
    {
        TIntrusivePtr<TConnection> holder(this);

        STORAGE_INFO(LogTag << " - started Receive loop");

        try {
            DoReceive(c);
        } catch (...) {
            if (!c->Cancelled()) {
                STORAGE_WARN(LogTag << " - unhandled error in Receive: "
                    << CurrentExceptionMessage());
            }
        }

        Handler->CancelAllRequests(
            TErrorResponse(UnavailableError, "server is unavailable"));

        NeedConnect = true;
    }

    void DoReceive(TCont* c)
    {
        TContIO io(Socket, c);

        Handler->ProcessRequests(io);
    }
};

using TConnectionPtr = TIntrusivePtr<TConnection>;

////////////////////////////////////////////////////////////////////////////////

class TEndpoint final
    : public TEndpointBase
{
private:
    const TExecutorPtr Executor;
    const TNetworkAddress ConnectAddress;
    const IClientHandlerPtr Handler;
    const IBlockStorePtr VolumeClient;

    TLog Log;

    TConnectionPtr Connection;

public:
    TEndpoint(
            TLog& log,
            TExecutorPtr executor,
            const TNetworkAddress& connectAddress,
            IClientHandlerPtr handler,
            IBlockStorePtr volumeClient)
        : Executor(std::move(executor))
        , ConnectAddress(connectAddress)
        , Handler(std::move(handler))
        , VolumeClient(std::move(volumeClient))
        , Log(log)
    {}

    ~TEndpoint()
    {
        Stop();
    }

    void Start() override
    {
        Connection = MakeIntrusive<TConnection>(
            Log,
            Executor->GetContExecutor(),
            Handler,
            ConnectAddress);

        Executor->Execute([&] {
            Connection->Start(Executor->GetContExecutor());
        }).GetValueSync();
    }

    void Stop() override
    {
        if (Connection) {
            Connection->Stop();
        }
    }

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> zeroRequest) override
    {
        auto request = MakeIntrusive<TClientZeroRequest>(
            EClientRequestType::ZeroBlocks,
            callContext->RequestId,
            zeroRequest->GetStartIndex(),
            zeroRequest->GetBlocksCount());

        Y_ABORT_UNLESS(Connection);
        Connection->EnqueueRequest(request);
        return request->Response;
    }

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> readRequest) override
    {
        auto request = MakeIntrusive<TClientReadRequest>(
            EClientRequestType::ReadBlocks,
            callContext->RequestId,
            readRequest->GetStartIndex(),
            readRequest->GetBlocksCount(),
            readRequest->Sglist);

        Y_ABORT_UNLESS(Connection);
        Connection->EnqueueRequest(request);
        return request->Response;
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> writeRequest) override
    {
        auto request = MakeIntrusive<TClientWriteRequest>(
            EClientRequestType::WriteBlocks,
            callContext->RequestId,
            writeRequest->GetStartIndex(),
            writeRequest->BlocksCount,
            writeRequest->Sglist);

        Y_ABORT_UNLESS(Connection);
        Connection->EnqueueRequest(request);
        return request->Response;
    }

    TFuture<NProto::TMountVolumeResponse> MountVolume(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TMountVolumeRequest> request) override
    {
        auto future = EnsureClientConnected(*callContext);

        const auto& volumeClient = VolumeClient;
        const auto& connection = Connection;

        return future.Apply([=, this, request = std::move(request)] (auto f) mutable {
            auto error = f.ExtractValue();

            if (HasError(error)) {
                NProto::TMountVolumeResponse response;
                response.MutableError()->CopyFrom(std::move(error));
                return MakeFuture(response);
            }

            auto future = volumeClient->MountVolume(
                std::move(callContext),
                std::move(request));

            return future.Apply([=, this] (const auto& f) {
                return HandleMountVolumeResponse(
                    f.GetValue(),
                    connection->GetExportInfo().GetValue());
            });
        });
    }

    TFuture<NProto::TUnmountVolumeResponse> UnmountVolume(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TUnmountVolumeRequest> request) override
    {
        return VolumeClient->UnmountVolume(
            std::move(callContext),
            std::move(request));
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

private:
    TFuture<NProto::TError> EnsureClientConnected(TCallContext& callContext)
    {
        Y_ABORT_UNLESS(Connection);

        if (Connection->GetExportInfo().HasValue()) {
            return MakeFuture(NProto::TError());
        }

        auto request = MakeIntrusive<TClientMountRequest>(
            EClientRequestType::MountVolume,
            callContext.RequestId,
            0ull,
            0u);

        Connection->EnqueueRequest(request);
        return request->Response.GetFuture();
    }

    NProto::TMountVolumeResponse HandleMountVolumeResponse(
        const NProto::TMountVolumeResponse& response,
        const TExportInfo& exportInfo)
    {
        if (!HasError(response) &&
            response.GetVolume().GetBlockSize() != GetBlockSize(exportInfo))
        {
            return TErrorResponse(E_INVALID_STATE, TStringBuilder()
                << "volume BlockSize is not equal to nbd-device BlockSize: "
                << response.GetVolume().GetBlockSize()
                << " != " << GetBlockSize(exportInfo));
        }

        return response;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TEndpointExecutor
{
private:
    const TExecutorPtr Executor;

    TVector<std::weak_ptr<TEndpoint>> Endpoints;
    TAdaptiveLock Lock;

public:
    TEndpointExecutor(TString name)
        : Executor(TExecutor::Create(std::move(name)))
    {}

    TExecutorPtr GetExecutor()
    {
        return Executor;
    }

    void Start()
    {
        Executor->Start();
    }

    void Stop()
    {
        Executor->Stop();
    }

    void AddEndpoint(std::shared_ptr<TEndpoint> endpoint)
    {
        with_lock (Lock) {
            CleanEndpoints();
            Endpoints.emplace_back(endpoint);
        }
    }

    ui32 GetEndpointsCount()
    {
        with_lock (Lock) {
            CleanEndpoints();
            return Endpoints.size();
        }
    }

private:
    void CleanEndpoints()
    {
        for (auto it = Endpoints.begin(); it != Endpoints.end(); ) {
            if (it->lock()) {
                ++it;
            } else {
                it = Endpoints.erase(it);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TClient final
    : public IClient
{
private:
    const ui32 ThreadsCount;

    TVector<std::unique_ptr<TEndpointExecutor>> Executors;

    TLog Log;

    TAtomic ShouldStop = 0;

public:
    TClient(ILoggingServicePtr logging, ui32 threadsCount)
        : ThreadsCount(threadsCount)
    {
        Log = logging->CreateLog("BLOCKSTORE_NBD");

        InitExecutors();
    }

    ~TClient()
    {
        Stop();
    }

    void Start() override
    {
        STORAGE_INFO("Starting");

        for (const auto& executor: Executors) {
            executor->Start();
        }
    }

    void Stop() override
    {
        if (AtomicSwap(&ShouldStop, 1) == 1) {
            return;
        }

        STORAGE_INFO("Shutting down");

        for (const auto& executor: Executors) {
            executor->Stop();
        }
    }

    IBlockStorePtr CreateEndpoint(
        const TNetworkAddress& connectAddress,
        IClientHandlerPtr handler,
        IBlockStorePtr volumeClient) override
    {
        auto executor = PickExecutor();
        Y_ABORT_UNLESS(executor);

        auto endpoint = std::make_shared<TEndpoint>(
            Log,
            executor->GetExecutor(),
            connectAddress,
            std::move(handler),
            std::move(volumeClient));

        executor->AddEndpoint(endpoint);
        return endpoint;
    }

private:
    void InitExecutors()
    {
        for (size_t i = 1; i <= ThreadsCount; ++i) {
            auto executor = std::make_unique<TEndpointExecutor>(
                TStringBuilder() << "NBD" << i);

            Executors.push_back(std::move(executor));
        }
    }

    TEndpointExecutor* PickExecutor() const
    {
        TEndpointExecutor* result = nullptr;

        for (const auto& executor: Executors) {
            if (result == nullptr ||
                executor->GetEndpointsCount() < result->GetEndpointsCount())
            {
                result = executor.get();
            }
        }

        return result;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(ILoggingServicePtr logging, ui32 threadsCount)
{
    return std::make_shared<TClient>(std::move(logging), threadsCount);
}

}   // namespace NCloud::NBlockStore::NBD
