#include "server.h"

#include <cloud/blockstore/libs/storage/device_tcp_server/protos/protocol.pb.h>

#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/coroutine/queue.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/coroutine/engine/network.h>
#include <library/cpp/coroutine/engine/sockpool.h>
#include <library/cpp/coroutine/listener/listen.h>

#include <util/generic/scope.h>
#include <util/network/address.h>

#include <optional>

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TConnection: TThrRefBase
{
    TSocketHolder Socket;
    TContLockFreeQueue<NProto::TDeviceProtocolResponse> ResponseQueue;

    TCont* Send = nullptr;
    TCont* Recv = nullptr;

    explicit TConnection(TSocketHolder socket, TContExecutor* e)
        : Socket(std::move(socket))
        , ResponseQueue(e)
    {}
};

using TConnectionPtr = std::shared_ptr<TConnection>;

////////////////////////////////////////////////////////////////////////////////

class TDeviceTCPServer final
    : public std::enable_shared_from_this<TDeviceTCPServer>
    , public IStartable
    , public TContListener::ICallBack
{
private:
    const TNetworkAddress ListenAddress;
    const ILoggingServicePtr Logging;
    const TExecutorPtr Executor;
    const IDeviceServerBackendPtr Backend;

    TLog Log;
    std::optional<TContListener> Listener;

    TVector<TConnectionPtr> Connections;

public:
    TDeviceTCPServer(
        const TNetworkAddress& listenAddress,
        ILoggingServicePtr logging,
        TExecutorPtr executor,
        IDeviceServerBackendPtr backend)
        : ListenAddress(listenAddress)
        , Logging(std::move(logging))
        , Executor(std::move(executor))
        , Backend(std::move(backend))
    {}

    // IStartable

    void Start() final
    {
        Log = Logging->CreateLog("DEVICE_SERVER");

        auto future = Executor->Execute([this] { StartListen(); });
        future.Wait();
    }

    void Stop() final
    {
        auto future = Executor->Execute([this] { StopImpl(); });
        future.Wait();
    }

    // TContListener::ICallBack

    void OnError() final
    {
        STORAGE_ERROR(
            "unhandled error in Accept: " << CurrentExceptionMessage());
    }

    void OnAcceptFull(const TAcceptFull& accept) final
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

private:
    void StartListen()
    {
        Listener.emplace(this, Executor->GetContExecutor());
        Listener->Bind(ListenAddress);
        Listener->Listen();
    }

    void StopImpl()
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

    void Receive(TConnectionPtr conn)
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

    void Send(TConnectionPtr conn)
    {
        try {
            TContIO io(conn->Socket, RunningCont());

            NProto::TDeviceProtocolResponse response;
            while (conn->ResponseQueue.Dequeue(&response)) {
                TString payload;

                const bool ok = response.SerializeToString(&payload);
                Y_ABORT_UNLESS(ok);

                const ui32 wireSize =
                    HostToInet(static_cast<ui32>(payload.size()));

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

    void OnExit(TConnectionPtr conn)
    {
        if (conn->Send != nullptr || conn->Recv != nullptr) {
            return;
        }

        std::erase(Connections, conn);
    }

    auto ReadDeviceProtocolRequest(TContIO& io)
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

    void HandleRequest(
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
            case ERequestCase::kReadDeviceBlocks: {
                auto future = Backend->ReadDeviceBlocks(
                    Now(),
                    std::move(*request.MutableReadDeviceBlocks()));

                future.Subscribe(
                    [conn, requestId](
                        const TFuture<NProto::TReadDeviceBlocksResponse>&
                            future)
                    {
                        NProto::TDeviceProtocolResponse response;
                        response.SetRequestId(requestId);
                        response.MutableReadDeviceBlocks()->CopyFrom(
                            future.GetValue());

                        conn->ResponseQueue.Enqueue(std::move(response));
                    });
                break;
            }
            case ERequestCase::kWriteDeviceBlocks: {
                auto future = Backend->WriteDeviceBlocks(
                    Now(),
                    std::move(*request.MutableWriteDeviceBlocks()));

                future.Subscribe(
                    [conn, requestId](
                        const TFuture<NProto::TWriteDeviceBlocksResponse>&
                            future)
                    {
                        NProto::TDeviceProtocolResponse response;
                        response.SetRequestId(requestId);
                        response.MutableWriteDeviceBlocks()->CopyFrom(
                            future.GetValue());

                        conn->ResponseQueue.Enqueue(std::move(response));
                    });
                break;
            }
            case ERequestCase::kZeroDeviceBlocks: {
                auto future = Backend->ZeroDeviceBlocks(
                    Now(),
                    std::move(*request.MutableZeroDeviceBlocks()));

                future.Subscribe(
                    [conn, requestId](
                        const TFuture<NProto::TZeroDeviceBlocksResponse>&
                            future)
                    {
                        NProto::TDeviceProtocolResponse response;
                        response.SetRequestId(requestId);
                        response.MutableZeroDeviceBlocks()->CopyFrom(
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
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<IStartable> CreateDeviceTCPServer(
    const TNetworkAddress& listenAddress,
    ILoggingServicePtr logging,
    TExecutorPtr executor,
    IDeviceServerBackendPtr backend)
{
    return std::make_shared<TDeviceTCPServer>(
        listenAddress,
        std::move(logging),
        std::move(executor),
        std::move(backend));
}

}   // namespace NCloud::NBlockStore::NStorage
