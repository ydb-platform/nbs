#include "server.h"

#include "request.h"
#include "service.h"

#include <cloud/fastshard/libs/sn/iface/storage_node.h>

#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/coroutine/queue.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/coroutine/engine/network.h>
#include <library/cpp/coroutine/engine/sockpool.h>
#include <library/cpp/coroutine/listener/listen.h>
#include <library/cpp/threading/future/wait/wait.h>

#include <util/generic/scope.h>
#include <util/network/address.h>

#include <mutex>
#include <optional>
#include <type_traits>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <
    typename TProtoRequest,
    typename TProtoResponse,
    auto Method,
    TProtoRequest* (NProto::TDeviceProtocolRequest::*mutableRequest)(),
    TProtoResponse* (NProto::TDeviceProtocolResponse::*mutableResponse)()>
struct TServerMethod
{
    using TRequest = TProtoRequest;
    using TResponse = TProtoResponse;

    // The response holds pages: move it out of the future, do not copy.
    constexpr static bool MovesResponse =
        std::is_same_v<TProtoResponse, NProto::TReadPagesResponse> ||
        std::is_same_v<TProtoResponse, NProto::TReadJournalTailResponse>;

    static TFuture<TResponse> Execute(
        IServerBackend& backend,
        TRequest&& request)
    {
        return (backend.*Method)(std::move(request));
    }

    static TRequest& MutableProto(NProto::TDeviceProtocolRequest& request)
    {
        return *(request.*mutableRequest)();
    }

    static TResponse& MutableProto(NProto::TDeviceProtocolResponse& response)
    {
        return *(response.*mutableResponse)();
    }
};

#define STORAGE_DECLARE_METHOD(name, ...)                                      \
    struct T##name##Method                                                     \
        : TServerMethod<                                                       \
              NProto::T##name##Request,                                        \
              NProto::T##name##Response,                                       \
              &IServerBackend::name,                                           \
              &NProto::TDeviceProtocolRequest::Mutable##name,                  \
              &NProto::TDeviceProtocolResponse::Mutable##name>                 \
    {                                                                          \
        constexpr static TStringBuf Name = #name;                              \
    };                                                                         \
    // STORAGE_DECLARE_METHOD

SN_METHODS(STORAGE_DECLARE_METHOD)

////////////////////////////////////////////////////////////////////////////////

struct TConnection
{
    TSocketHolder Socket;
    const TExecutorPtr Executor;

    TContLockFreeQueue<NProto::TDeviceProtocolResponse> ResponseQueue;

    TCont* Send = nullptr;
    TCont* Recv = nullptr;

    TConnection(TSocketHolder socket, TExecutorPtr executor)
        : Socket(std::move(socket))
        , Executor(std::move(executor))
        , ResponseQueue(Executor->GetContExecutor())
    {}

    void OnReceiveStopped()
    {
        Recv = nullptr;

        // Empty response is used as a sentinel to stop response dequeuing
        ResponseQueue.Enqueue({});
    }

    void OnSendStopped()
    {
        Send = nullptr;
    }

    bool DequeueResponse(NProto::TDeviceProtocolResponse* response)
    {
        if (!ResponseQueue.Dequeue(response)) {
            return false;
        }

        return !IsShutdownSentinel(*response);
    }

    bool IsAlive() const
    {
        return Recv || Send;
    }

    static bool IsShutdownSentinel(
        const NProto::TDeviceProtocolResponse& response)
    {
        return response.GetResponseCase() ==
               NProto::TDeviceProtocolResponse::ResponseCase::RESPONSE_NOT_SET;
    }
};

using TConnectionPtr = std::shared_ptr<TConnection>;

////////////////////////////////////////////////////////////////////////////////

class TServer final
    : public std::enable_shared_from_this<TServer>
    , public IStartable
    , public TContListener::ICallBack
{
private:
    const TNetworkAddress ListenAddress;
    const TExecutorPtr Executor;
    const IServerBackendPtr Backend;
    const TVector<TExecutorPtr> ConnectionExecutors;
    const TLog Log;

    std::optional<TContListener> Listener;
    size_t NextConnectionExecutor = 0;

    std::mutex ConnectionsLock;
    TVector<TConnectionPtr> Connections;

public:
    TServer(
        const TNetworkAddress& listenAddress,
        ILoggingServicePtr logging,
        TExecutorPtr executor,
        IServerBackendPtr backend,
        TVector<TExecutorPtr> connectionExecutors);

    // IStartable

    void Start() final;
    void Stop() final;

    // TContListener::ICallBack

    void OnError() final;
    void OnAcceptFull(const TAcceptFull& accept) final;

private:
    void StartListen();
    void StopImpl();

    // Run in the executor of the connection.
    void StartConnection(TConnectionPtr conn);
    void StopConnection(const TConnectionPtr& conn);

    bool HasConnections();

    void Receive(TConnectionPtr conn);
    void Send(TConnectionPtr conn);
    void OnExit(TConnectionPtr conn);

    auto ReadDeviceProtocolRequest(TContIO& io)
        -> std::optional<NProto::TDeviceProtocolRequest>;

    void HandleRequest(
        NProto::TDeviceProtocolRequest& request,
        TConnectionPtr conn);

    template <typename TMethod>
    void ProcessRequest(
        TConnectionPtr conn,
        NProto::TDeviceProtocolRequest&& request)
    {
        using TResponse = typename TMethod::TResponse;

        TFuture<TResponse> future;

        const ui64 requestId = request.GetRequestId();

        try {
            auto& proto = TMethod::MutableProto(request);

            STORAGE_TRACE(
                TMethod::Name << " #" << requestId
                              << " received: " << DescribeRequest(proto));

            future = TMethod::Execute(*Backend, std::move(proto));
        } catch (...) {
            STORAGE_ERROR(
                TMethod::Name << " failed: " << CurrentExceptionMessage());

            future = MakeErrorFuture<TResponse>(std::current_exception());
        }

        future.Subscribe(
            [Log = Log, conn, requestId](const auto& future)
            {
                NProto::TDeviceProtocolResponse response;
                response.SetRequestId(requestId);

                auto& proto = TMethod::MutableProto(response);

                if constexpr (TMethod::MovesResponse) {
                    // Move the value: nobody else reads this future
                    proto = ExtractResponse(future);
                } else {
                    // Copy the value: MakeFuture<T>() returns a shared future
                    proto.CopyFrom(SafeExecute<TResponse>(
                        [&] { return future.GetValue(); }));
                }

                const bool failed = HasError(proto.GetError());

                STORAGE_LOG(
                    failed ? TLOG_ERR : TLOG_RESOURCES,
                    TMethod::Name << " #" << requestId
                                  << (failed ? " failed: " : " completed: ")
                                  << FormatError(proto.GetError()));

                conn->ResponseQueue.Enqueue(std::move(response));
            });
    }
};

////////////////////////////////////////////////////////////////////////////////

TServer::TServer(
    const TNetworkAddress& listenAddress,
    ILoggingServicePtr logging,
    TExecutorPtr executor,
    IServerBackendPtr backend,
    TVector<TExecutorPtr> connectionExecutors)
    : ListenAddress(listenAddress)
    , Executor(std::move(executor))
    , Backend(std::move(backend))
    , ConnectionExecutors(
          connectionExecutors.empty() ? TVector<TExecutorPtr>{Executor}
                                      : std::move(connectionExecutors))
    , Log(logging->CreateLog("BLOCKSTORE_JOURNALLED_DEVICE"))
{}

void TServer::Start()
{
    Backend->Start();

    auto future = Executor->Execute([this] { StartListen(); });
    future.GetValueSync();
}

void TServer::Stop()
{
    auto future = Executor->Execute([this] { StopImpl(); });
    future.Wait();

    Backend->Stop();
}

// TContListener::ICallBack

void TServer::OnError()
{
    STORAGE_ERROR("unhandled error in Accept: " << CurrentExceptionMessage());
}

void TServer::OnAcceptFull(const TAcceptFull& accept)
{
    TSocketHolder socket(accept.S->Release());

    auto address = NAddr::GetPeerAddr(socket);
    STORAGE_DEBUG("new connection from " << PrintHostAndPort(*address));

    SetNoDelay(socket, true);

    //
    // Add the connection to the list first, so that a stop finds it
    //

    auto executor = ConnectionExecutors[
        NextConnectionExecutor++ % ConnectionExecutors.size()];

    auto conn = std::make_shared<TConnection>(std::move(socket), executor);

    {
        std::lock_guard lock(ConnectionsLock);
        Connections.push_back(conn);
    }

    //
    // Create the coroutines in the thread that runs them
    //

    executor->ExecuteSimple(
        [weakSelf = weak_from_this(), conn = std::move(conn)]() mutable
        {
            if (auto self = weakSelf.lock()) {
                self->StartConnection(std::move(conn));
            }
        });
}

void TServer::StartConnection(TConnectionPtr conn)
{
    TContExecutor* e = conn->Executor->GetContExecutor();

    conn->Send = e->CreateOwned(
        [weakSelf = weak_from_this(), conn](TCont*)
        {
            if (auto self = weakSelf.lock()) {
                self->Send(conn);
            }
        },
        "send");

    conn->Recv = e->CreateOwned(
        [weakSelf = weak_from_this(), conn](TCont*)
        {
            if (auto self = weakSelf.lock()) {
                self->Receive(conn);
            }
        },
        "receive");
}

void TServer::StopConnection(const TConnectionPtr& conn)
{
    if (conn->Send) {
        conn->Send->Cancel();
    }

    if (conn->Recv) {
        conn->Recv->Cancel();
    }
}

bool TServer::HasConnections()
{
    std::lock_guard lock(ConnectionsLock);
    return !Connections.empty();
}

void TServer::StartListen()
{
    Listener.emplace(this, Executor->GetContExecutor());
    Listener->Bind(ListenAddress);
    Listener->Listen();
}

void TServer::StopImpl()
{
    STORAGE_DEBUG("Stopping");

    if (Listener) {
        Listener->Stop();
    }

    //
    // Cancel the coroutines of every connection in its own executor
    //

    TVector<TConnectionPtr> connections;
    {
        std::lock_guard lock(ConnectionsLock);
        connections = Connections;
    }

    TVector<TFuture<void>> futures;
    futures.reserve(connections.size());

    for (const auto& conn: connections) {
        futures.push_back(conn->Executor->Execute(
            [this, conn] { StopConnection(conn); }));
    }

    Executor->WaitFor(WaitAll(futures));

    connections.clear();

    Listener.reset();

    while (HasConnections()) {
        RunningCont()->Yield();
    }

    STORAGE_DEBUG("Stopped");
}

void TServer::Receive(TConnectionPtr conn)
{
    try {
        TContIO io(conn->Socket, RunningCont());

        while (auto request = ReadDeviceProtocolRequest(io)) {
            HandleRequest(*request, conn);
        }

        STORAGE_DEBUG(
            "connection closed by peer "
            << PrintHostAndPort(*NAddr::GetPeerAddr(conn->Socket)));
    } catch (...) {
        STORAGE_ERROR("Receive: " << CurrentExceptionMessage());
    }

    conn->OnReceiveStopped();
    OnExit(conn);
}

void TServer::Send(TConnectionPtr conn)
{
    try {
        TContIO io(conn->Socket, RunningCont());

        NProto::TDeviceProtocolResponse response;
        while (conn->DequeueResponse(&response)) {
            TString payload;

            const bool ok = response.SerializeToString(&payload);
            Y_ABORT_UNLESS(ok);

            const ui32 wireSize = HostToInet(static_cast<ui32>(payload.size()));

            io.Write(&wireSize, sizeof(wireSize));
            if (!payload.empty()) {
                io.Write(payload.data(), payload.size());
            }

            io.Flush();
        }
    } catch (...) {
        STORAGE_ERROR("Send: " << CurrentExceptionMessage());
    }

    conn->OnSendStopped();
    OnExit(conn);
}

void TServer::OnExit(TConnectionPtr conn)
{
    if (conn->IsAlive()) {
        return;
    }

    STORAGE_DEBUG(
        "remove connection "
        << PrintHostAndPort(*NAddr::GetPeerAddr(conn->Socket)));

    std::lock_guard lock(ConnectionsLock);
    std::erase(Connections, conn);
}

auto TServer::ReadDeviceProtocolRequest(TContIO& io)
    -> std::optional<NProto::TDeviceProtocolRequest>
{
    ui32 wireSize = 0;
    const size_t headerSize = io.Load(&wireSize, sizeof(wireSize));
    if (headerSize == 0) {
        // The peer closed the connection
        return std::nullopt;
    }

    Y_ENSURE(
        headerSize == sizeof(wireSize),
        "truncated request header: " << headerSize << " bytes");

    const ui32 size = InetToHost(wireSize);

    // TODO(sharpeye): define and enforce the maximum request payload size

    TString payload;
    payload.resize(size);
    io.LoadOrFail(payload.Detach(), size);

    NProto::TDeviceProtocolRequest request;
    Y_ENSURE(request.ParseFromString(payload));

    return request;
}

void TServer::HandleRequest(
    NProto::TDeviceProtocolRequest& request,
    TConnectionPtr conn)
{
    using ERequestCase = NProto::TDeviceProtocolRequest::RequestCase;

    const ui64 requestId = request.GetRequestId();

    switch (request.GetRequestCase()) {
        case ERequestCase::kAcquireDevices: {
            ProcessRequest<TAcquireDevicesMethod>(conn, std::move(request));
            break;
        }
        case ERequestCase::kReleaseDevices: {
            ProcessRequest<TReleaseDevicesMethod>(conn, std::move(request));
            break;
        }
        case ERequestCase::kFormatDevice: {
            ProcessRequest<TFormatDeviceMethod>(conn, std::move(request));
            break;
        }
        case ERequestCase::kListDevices: {
            ProcessRequest<TListDevicesMethod>(conn, std::move(request));
            break;
        }
        case ERequestCase::kReadPages: {
            ProcessRequest<TReadPagesMethod>(conn, std::move(request));
            break;
        }
        case ERequestCase::kWriteLogRecord: {
            ProcessRequest<TWriteLogRecordMethod>(conn, std::move(request));
            break;
        }
        case ERequestCase::kReadJournalTail: {
            ProcessRequest<TReadJournalTailMethod>(conn, std::move(request));
            break;
        }
        case ERequestCase::kAdvanceLsnLowWatermark: {
            ProcessRequest<TAdvanceLsnLowWatermarkMethod>(
                conn,
                std::move(request));
            break;
        }
        case ERequestCase::kWritePages: {
            ProcessRequest<TWritePagesMethod>(conn, std::move(request));
            break;
        }
        default: {
            NProto::TDeviceProtocolResponse response;
            response.SetRequestId(requestId);
            *response.MutableProtocolError() =
                MakeError(E_ARGUMENT, "unknown request");
            conn->ResponseQueue.Enqueue(std::move(response));
        }
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<IStartable> CreateServer(
    const TNetworkAddress& listenAddress,
    ILoggingServicePtr logging,
    TExecutorPtr executor,
    IServerBackendPtr backend,
    TVector<TExecutorPtr> connectionExecutors)
{
    return std::make_shared<TServer>(
        listenAddress,
        std::move(logging),
        std::move(executor),
        std::move(backend),
        std::move(connectionExecutors));
}

}   // namespace NCloud::NJournalled
