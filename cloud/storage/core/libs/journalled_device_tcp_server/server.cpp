#include "server.h"

#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/coroutine/queue.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/coroutine/engine/network.h>
#include <library/cpp/coroutine/engine/sockpool.h>
#include <library/cpp/coroutine/listener/listen.h>

#include <util/generic/scope.h>
#include <util/network/address.h>

#include <optional>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TConnection: TThrRefBase
{
    TSocketHolder Socket;
    TContLockFreeQueue<NProto::TDeviceProtocolResponse> ResponseQueue;

    TCont* Send = nullptr;
    TCont* Recv = nullptr;

    TConnection(TSocketHolder socket, TContExecutor* e)
        : Socket(std::move(socket))
        , ResponseQueue(e)
    {}
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
    const ILoggingServicePtr Logging;
    const TExecutorPtr Executor;
    const IServerBackendPtr Backend;

    TLog Log;
    std::optional<TContListener> Listener;

    TVector<TConnectionPtr> Connections;

public:
    TServer(
        const TNetworkAddress& listenAddress,
        ILoggingServicePtr logging,
        TExecutorPtr executor,
        IServerBackendPtr backend);

    // IStartable

    void Start() final;
    void Stop() final;

    // TContListener::ICallBack

    void OnError() final;
    void OnAcceptFull(const TAcceptFull& accept) final;

private:
    void StartListen();
    void StopImpl();

    void Receive(TConnectionPtr conn);
    void Send(TConnectionPtr conn);
    void OnExit(TConnectionPtr conn);

    auto ReadDeviceProtocolRequest(TContIO& io)
        -> NProto::TDeviceProtocolRequest;

    void HandleRequest(
        NProto::TDeviceProtocolRequest& request,
        TConnectionPtr conn);
};

////////////////////////////////////////////////////////////////////////////////

TServer::TServer(
    const TNetworkAddress& listenAddress,
    ILoggingServicePtr logging,
    TExecutorPtr executor,
    IServerBackendPtr backend)
    : ListenAddress(listenAddress)
    , Logging(std::move(logging))
    , Executor(std::move(executor))
    , Backend(std::move(backend))
{}

void TServer::Start()
{
    Log = Logging->CreateLog("DEVICE_SERVER");

    auto future = Executor->Execute([this] { StartListen(); });
    future.Wait();
}

void TServer::Stop()
{
    auto future = Executor->Execute([this] { StopImpl(); });
    future.Wait();
}

// TContListener::ICallBack

void TServer::OnError()
{
    STORAGE_ERROR("unhandled error in Accept: " << CurrentExceptionMessage());
}

void TServer::OnAcceptFull(const TAcceptFull& accept)
{
    TSocketHolder socket(accept.S->Release());

    auto address = NAddr::GetSockAddr(socket);
    STORAGE_DEBUG("new connection from " << PrintHostAndPort(*address));

    SetNoDelay(socket, true);

    TContExecutor* e = Executor->GetContExecutor();

    auto conn = std::make_shared<TConnection>(std::move(socket), e);

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

    Connections.push_back(conn);
}

void TServer::StartListen()
{
    Listener.emplace(this, Executor->GetContExecutor());
    Listener->Bind(ListenAddress);
    Listener->Listen();
}

void TServer::StopImpl()
{
    if (Listener) {
        Listener->Stop();
    }

    for (auto& conn: Connections) {
        if (conn->Send) {
            conn->Send->Cancel();
        }

        if (conn->Recv) {
            conn->Recv->Cancel();
        }
    }
}

void TServer::Receive(TConnectionPtr conn)
{
    try {
        TContIO io(conn->Socket, RunningCont());

        for (;;) {
            auto request = ReadDeviceProtocolRequest(io);

            HandleRequest(request, conn);
        }
    } catch (...) {
        STORAGE_ERROR("Receive: " << CurrentExceptionMessage());
    }

    conn->Recv = nullptr;
    OnExit(conn);
}

void TServer::Send(TConnectionPtr conn)
{
    try {
        TContIO io(conn->Socket, RunningCont());

        NProto::TDeviceProtocolResponse response;
        while (conn->ResponseQueue.Dequeue(&response)) {
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

    conn->Send = nullptr;
    OnExit(conn);
}

void TServer::OnExit(TConnectionPtr conn)
{
    if (conn->Send != nullptr || conn->Recv != nullptr) {
        return;
    }

    STORAGE_DEBUG(
        "remove connection "
        << PrintHostAndPort(*NAddr::GetSockAddr(conn->Socket)));

    std::erase(Connections, conn);
}

auto TServer::ReadDeviceProtocolRequest(TContIO& io)
    -> NProto::TDeviceProtocolRequest
{
    ui32 wireSize = 0;
    io.LoadOrFail(&wireSize, sizeof(wireSize));

    ui32 size = InetToHost(wireSize);

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
            STORAGE_DEBUG("Acquire: " << request.GetAcquireDevices());

            auto future = Backend->AcquireDevices(
                Now(),
                std::move(*request.MutableAcquireDevices()));

            future.Subscribe(
                [conn, requestId](
                    const TFuture<NProto::TAcquireDevicesResponse>& future)
                {
                    NProto::TDeviceProtocolResponse response;
                    response.SetRequestId(requestId);
                    response.MutableAcquireDevices()->CopyFrom(
                        future.GetValue());

                    conn->ResponseQueue.Enqueue(std::move(response));
                });

            break;
        }
        case ERequestCase::kReleaseDevices: {
            STORAGE_DEBUG("Release: " << request.GetReleaseDevices());

            auto future = Backend->ReleaseDevices(
                Now(),
                std::move(*request.MutableReleaseDevices()));

            future.Subscribe(
                [conn, requestId](
                    const TFuture<NProto::TReleaseDevicesResponse>& future)
                {
                    NProto::TDeviceProtocolResponse response;
                    response.SetRequestId(requestId);
                    response.MutableReleaseDevices()->CopyFrom(
                        future.GetValue());

                    conn->ResponseQueue.Enqueue(std::move(response));
                });
            break;
        }
        case ERequestCase::kReadPages: {
            auto future = Backend->ReadPages(
                Now(),
                std::move(*request.MutableReadPages()));

            future.Subscribe(
                [conn,
                 requestId](const TFuture<NProto::TReadPagesResponse>& future)
                {
                    NProto::TDeviceProtocolResponse response;
                    response.SetRequestId(requestId);
                    response.MutableReadPages()->CopyFrom(future.GetValue());

                    conn->ResponseQueue.Enqueue(std::move(response));
                });
            break;
        }
        case ERequestCase::kWriteLogRecord: {
            auto future = Backend->WriteLogRecord(
                Now(),
                std::move(*request.MutableWriteLogRecord()));

            future.Subscribe(
                [conn, requestId](
                    const TFuture<NProto::TWriteLogRecordResponse>& future)
                {
                    NProto::TDeviceProtocolResponse response;
                    response.SetRequestId(requestId);
                    response.MutableWriteLogRecord()->CopyFrom(
                        future.GetValue());

                    conn->ResponseQueue.Enqueue(std::move(response));
                });
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
    IServerBackendPtr backend)
{
    return std::make_shared<TServer>(
        listenAddress,
        std::move(logging),
        std::move(executor),
        std::move(backend));
}

}   // namespace NCloud::NJournalled
