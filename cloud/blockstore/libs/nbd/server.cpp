#include "server.h"

#include "limiter.h"
#include "server_handler.h"
#include "utils.h"

#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/coroutine/queue.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/coroutine/engine/network.h>
#include <library/cpp/coroutine/engine/sockpool.h>
#include <library/cpp/coroutine/listener/listen.h>

#include <util/folder/path.h>
#include <util/generic/map.h>
#include <util/network/address.h>
#include <util/network/socket.h>
#include <util/random/random.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/system/thread.h>
#include <util/thread/singleton.h>

#include <atomic>

namespace NCloud::NBlockStore::NBD {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TThreadContext
{
    TExecutor* Executor = nullptr;
};

TThreadContext& CurrentThread()
{
    return *FastTlsSingleton<TThreadContext>();
}

////////////////////////////////////////////////////////////////////////////////

struct TAppContext
{
    TLog Log;

    TVector<TExecutor*> Executors;

    TAtomic ShouldStop = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TConnection final
    : public IServerContext
{
private:
    TAppContext& AppCtx;

    TLog Log;
    TContExecutor* Executor;
    ILimiterPtr Limiter;
    IServerHandlerPtr Handler;
    TSocketHolder Socket;
    const TFuture<void> Ready;

    TContLockFreeQueue<TServerResponsePtr> ResponseQueue;

    size_t InFlightBytes = 0;

    // Requests read from this connection that may still reach or be executing
    // in the backend. Includes requests waiting in Limiter::Acquire.
    std::atomic<size_t> ActiveRequests = 0;

    // Set after the receive loop exits, so ActiveRequests can no longer grow.
    // Drain completes when this is set and ActiveRequests reaches zero.
    std::atomic<bool> ReceiveFinished = false;

    std::atomic_flag ShuttingDown = false;
    TPromise<void> DrainResult = NewPromise<void>();

public:
    TConnection(
            TAppContext& appCtx,
            TContExecutor* e,
            ILimiterPtr limiter,
            IServerHandlerPtr handler,
            TSocketHolder socket,
            TFuture<void> ready)
        : AppCtx(appCtx)
        , Log(appCtx.Log)
        , Executor(e)
        , Limiter(std::move(limiter))
        , Handler(std::move(handler))
        , Socket(std::move(socket))
        , Ready(std::move(ready))
        , ResponseQueue(e)
    {}

    ~TConnection()
    {
        Y_ABORT_UNLESS(InFlightBytes == 0);
    }

    void Start() override
    {
        Executor->Create<TConnection, &TConnection::Receive>(this, "recv");
        Executor->Create<TConnection, &TConnection::Send>(this, "send");
    }

    void Stop() override
    {
        ShutDown();
    }

    TFuture<void> GetDrainResult() const
    {
        return DrainResult.GetFuture();
    }

    void Enqueue(ITaskPtr task) override
    {
        size_t index = 0;

        if (AppCtx.Executors.size() > 1) {
            index = RandomNumber(AppCtx.Executors.size() - 1);

            if (AppCtx.Executors[index] == CurrentThread().Executor) {
                index = AppCtx.Executors.size() - 1;
            }
        }

        AppCtx.Executors[index]->Enqueue(std::move(task));
    }

    const NProto::TReadBlocksLocalResponse& WaitFor(
        const TFuture<NProto::TReadBlocksLocalResponse>& future) override
    {
        return CurrentThread().Executor->WaitFor(future);
    }

    const NProto::TWriteBlocksLocalResponse& WaitFor(
        const TFuture<NProto::TWriteBlocksLocalResponse>& future) override
    {
        return CurrentThread().Executor->WaitFor(future);
    }

    const NProto::TZeroBlocksResponse& WaitFor(
        const TFuture<NProto::TZeroBlocksResponse>& future) override
    {
        return CurrentThread().Executor->WaitFor(future);
    }

    void SendResponse(TServerResponsePtr response) override
    {
        ResponseQueue.Enqueue(std::move(response));
        CompleteRequest();
    }

    bool AcquireRequest(size_t requestBytes) override
    {
        if (IsShuttingDown()) {
            return false;
        }

        ActiveRequests.fetch_add(1, std::memory_order_acq_rel);

        if (Limiter) {
            if (!Limiter->Acquire(requestBytes)) {
                CompleteRequest();
                return false;
            }

            InFlightBytes += requestBytes;
        }

        // Limiter::Acquire may yield, allowing the connection to start
        // shutting down while this request is waiting for capacity.
        if (IsShuttingDown()) {
            ReleaseRequest(requestBytes);
            CompleteRequest();
            return false;
        }

        return true;
    }

    size_t CollectRequests(const TIncompleteRequestsCollector& collector)
    {
        if (IsShuttingDown()) {
            return 0;
        }

        return Handler->CollectRequests(collector);
    }

private:
    void ReleaseRequest(size_t requestBytes)
    {
        if (Limiter) {
            Limiter->Release(requestBytes);
            InFlightBytes -= requestBytes;
        }
    }

    void Receive(TCont* c)
    {
        TIntrusivePtr<TConnection> holder(this);
        try {
            DoReceive(c);
        } catch (...) {
            if (!IsShuttingDown() && !c->Cancelled()) {
                STORAGE_INFO("lost connection with client, failed to receive: "
                    << CurrentExceptionMessage());
                Handler->ProcessException(std::current_exception());
            }
        }

        ReceiveFinished.store(true, std::memory_order_release);
        TryCompleteDrain();
        ResponseQueue.Enqueue(nullptr);
    }

    void DoReceive(TCont* c)
    {
        TContIO io(Socket, c);

        if (!Handler->NegotiateClient(io, io)) {
            return;
        }

        CurrentThread().Executor->WaitFor(Ready);

        if (!c->Cancelled() && !IsShuttingDown()) {
            Handler->ProcessRequests(this, io, io, c);
        }
    }

    void Send(TCont* c)
    {
        TIntrusivePtr<TConnection> holder(this);

        TServerResponsePtr response;
        while (ResponseQueue.Dequeue(&response)) {
            if (!response) {
                // stop signal received
                Handler->ProcessException(
                    std::make_exception_ptr(TSystemError(-ESHUTDOWN)));
                break;
            }

            try {
                DoSendResponse(c, *response);
            } catch (...) {
                STORAGE_INFO("lost connection with client, failed to send: "
                    << CurrentExceptionMessage());
                Handler->ProcessException(std::current_exception());
            }

            ReleaseRequest(response->RequestBytes);
            response.Reset();
        }

        ShutDown();
        ReleaseRequest(InFlightBytes);
    }

    void DoSendResponse(TCont* c, TServerResponse& response)
    {
        if (IsShutdownError(response.Error)) {
            return;
        }

        TContIO io(Socket, c);
        Handler->SendResponse(io, response);
    }

    bool IsShutdownError(const NProto::TError& error)
    {
        if (AtomicGet(AppCtx.ShouldStop) == 0) {
            return false;
        }

        if (error.GetCode() == E_CANCELLED) {
            return true;
        }

        if (GetErrorKind(error) == EErrorKind::ErrorRetriable) {
            return true;
        }

        return false;
    }

    void ShutDown()
    {
        if (!ShuttingDown.test_and_set(std::memory_order_acq_rel)) {
            Socket.ShutDown(SHUT_RDWR);
        }
    }

    bool IsShuttingDown() const
    {
        return ShuttingDown.test(std::memory_order_acquire);
    }

    void CompleteRequest()
    {
        const auto previous =
            ActiveRequests.fetch_sub(1, std::memory_order_acq_rel);
        Y_ABORT_UNLESS(previous != 0);

        if (previous == 1) {
            TryCompleteDrain();
        }
    }

    void TryCompleteDrain()
    {
        if (ReceiveFinished.load(std::memory_order_acquire) &&
            ActiveRequests.load(std::memory_order_acquire) == 0)
        {
            DrainResult.TrySetValue();
        }
    }
};

using TConnectionPtr = TIntrusivePtr<TConnection>;

////////////////////////////////////////////////////////////////////////////////

struct TEndpointStopResult
{
    NProto::TError Error;
    TFuture<void> DrainResult;
};

////////////////////////////////////////////////////////////////////////////////

class TEndpoint final
    : public TContListener::ICallBack
{
private:
    TAppContext& AppCtx;
    TLog Log;
    TContExecutor* Executor;

    const ILimiterPtr Limiter;
    const IServerHandlerFactoryPtr HandlerFactory;
    const TNetworkAddress ListenAddress;
    const ui32 SocketAccessMode;

    // Completion tail for all connections accepted by this endpoint, including
    // the tail inherited from a previous instance of the same endpoint.
    TFuture<void> DrainResult;
    std::unique_ptr<TContListener> Listener;
    TConnectionPtr Connection;

public:
    TEndpoint(
            TAppContext& appCtx,
            TContExecutor* executor,
            ILimiterPtr limiter,
            IServerHandlerFactoryPtr handlerFactory,
            const TNetworkAddress& listenAddress,
            const ui32 socketAccessMode,
            TFuture<void> drainResult)
        : AppCtx(appCtx)
        , Log(appCtx.Log)
        , Executor(executor)
        , Limiter(std::move(limiter))
        , HandlerFactory(std::move(handlerFactory))
        , ListenAddress(listenAddress)
        , SocketAccessMode(socketAccessMode)
        , DrainResult(std::move(drainResult))
    {}

    ~TEndpoint()
    {
        Stop(false);
    }

    NProto::TError Start()
    {
        auto localHost = PrintHostAndPort(ListenAddress);
        STORAGE_DEBUG("listen on " << localHost);

        return SafeExecute<NProto::TError>([&] {
            ValidateSocketPath(ListenAddress);
            DeleteSocketIfExists(ListenAddress);

            Listener = std::make_unique<TContListener>(this, Executor);
            Listener->Bind(ListenAddress);
            Listener->Listen();

            if (IsUnixAddress(ListenAddress)) {
                ChmodSocket(
                    ListenAddress,
                    SocketAccessMode);
            }

            return NProto::TError();
        });
    }

    TEndpointStopResult Stop(bool deleteSocket)
    {
        auto error = SafeExecute<NProto::TError>([&] {
            if (Connection) {
                Connection->Stop();
            };

            if (Listener) {
                Listener->Stop();
            }

            if (deleteSocket) {
                DeleteSocketIfExists(ListenAddress);
            }

            return NProto::TError();
        });

        return {
            .Error = std::move(error),
            .DrainResult = DrainResult,
        };
    }

    size_t CollectRequests(const TIncompleteRequestsCollector& collector)
    {
        if (!Connection) {
            return 0;
        }

        return Connection->CollectRequests(collector);
    }

private:
    void OnAcceptFull(const TAcceptFull& accept) override
    {
        TSocketHolder socket(accept.S->Release());

        auto address = NAddr::GetPeerAddr(socket);
        STORAGE_DEBUG("new connection from " << PrintHostAndPort(*address));

        if (IsTcpAddress(*address)) {
            SetNoDelay(socket, true);
        }

        auto ready = DrainResult;
        if (Connection) {
            Connection->Stop();
        }

        Connection = MakeIntrusive<TConnection>(
            AppCtx,
            Executor,
            Limiter,
            HandlerFactory->CreateHandler(),
            std::move(socket),
            ready);

        // Keep the whole chain even if this connection closes before it starts
        // processing requests.
        DrainResult = WaitAll(ready, Connection->GetDrainResult());

        Connection->Start();
    }

    void OnError() override
    {
        STORAGE_ERROR("unhandled error in Accept: "
            << CurrentExceptionMessage());
    }

    static char* GetSocketPath(const TNetworkAddress& addr)
    {
        auto it = addr.Begin();
        Y_ABORT_UNLESS(it->ai_family == AF_UNIX);

        auto socketPath = it->ai_addr->sa_data;

        ++it;
        Y_ABORT_UNLESS(it == addr.End());

        return socketPath;
    }

    static void ChmodSocket(const TNetworkAddress& addr, int mode)
    {
        auto socketPath = GetSocketPath(addr);
        auto err = Chmod(socketPath, mode);

        if (err != 0) {
            STORAGE_THROW_SERVICE_ERROR(MAKE_SYSTEM_ERROR(err))
                << "failed to chmod socket " << socketPath;
        }
    }

    static void DeleteSocketIfExists(const TNetworkAddress& addr)
    {
        if (IsUnixAddress(addr)) {
            TFsPath(GetSocketPath(addr)).DeleteIfExists();
        }
    }

    static void ValidateSocketPath(const TNetworkAddress& addr)
    {
        if (IsUnixAddress(addr)) {
            const auto& socketPath = TFsPath(GetSocketPath(addr));
            if (!socketPath.Parent().Exists()) {
                STORAGE_THROW_SERVICE_ERROR(E_NOT_FOUND)
                    << "Invalid socket path " << socketPath;
            }
        }
    }
};

using TEndpointPtr = std::shared_ptr<TEndpoint>;

////////////////////////////////////////////////////////////////////////////////

class TExecutorThread final
{
private:
    TAppContext& AppCtx;

    TLog Log;
    const TExecutorPtr Executor;
    const bool LimiterEnabled;
    const size_t MaxInFlightBytesPerThread;
    const ui32 SocketAccessMode;

    ILimiterPtr Limiter;

    TAdaptiveLock Lock;
    TMap<TString, TEndpointPtr> Endpoints;

public:
    TExecutorThread(
            TAppContext& appCtx,
            TExecutorPtr executor,
            bool limiterEnabled,
            size_t maxInFlightBytesPerThread,
            ui32 socketAccessMode)
        : AppCtx(appCtx)
        , Log(appCtx.Log)
        , Executor(std::move(executor))
        , LimiterEnabled(limiterEnabled)
        , MaxInFlightBytesPerThread(maxInFlightBytesPerThread)
        , SocketAccessMode(socketAccessMode)
    {}

    void Start()
    {
        Executor->Start();

        if (LimiterEnabled) {
            Limiter = CreateLimiter(
                Log,
                Executor->GetContExecutor(),
                MaxInFlightBytesPerThread);
        }

        Executor->Execute([&] {
            CurrentThread().Executor = Executor.get();
        });
    }

    void Stop()
    {
        Executor->Stop();
    }

    TEndpointPtr CreateEndpoint(
        TNetworkAddress listenAddress,
        IServerHandlerFactoryPtr handlerFactory,
        TFuture<void> drainResult)
    {
        return std::make_shared<TEndpoint>(
            AppCtx,
            Executor->GetContExecutor(),
            Limiter,
            std::move(handlerFactory),
            std::move(listenAddress),
            SocketAccessMode,
            std::move(drainResult));
    }

    TFuture<NProto::TError> StartEndpoint(TEndpointPtr endpoint)
    {
        return Executor->Execute([endpoint = std::move(endpoint)] {
            return endpoint->Start();
        });
    }

    TFuture<TEndpointStopResult> StopEndpoint(TEndpointPtr endpoint)
    {
        return Executor->Execute([endpoint = std::move(endpoint)] {
            return endpoint->Stop(true);
        });
    }

    void AddEndpoint(TString address, TEndpointPtr endpoint)
    {
        with_lock (Lock) {
            auto [it, inserted] = Endpoints.emplace(
                std::move(address),
                std::move(endpoint));
            Y_ABORT_UNLESS(inserted);
        }
    }

    TEndpointPtr RemoveEndpoint(const TString& address)
    {
        with_lock (Lock) {
            auto it = Endpoints.find(address);
            Y_ABORT_UNLESS(it != Endpoints.end());

            auto endpoint = std::move(it->second);
            Endpoints.erase(it);

            return endpoint;
        }
    }

    size_t GetEndpointsCount() const
    {
        with_lock (Lock) {
            return Endpoints.size();
        }
    }

    TFuture<size_t> CollectRequests(
        const TIncompleteRequestsCollector& collector)
    {
        return Executor->Execute([&] {
            size_t count = 0;
            with_lock (Lock) {
                for (auto& it: Endpoints) {
                    count += it.second->CollectRequests(collector);
                }
            }
            return count;
        });
    }
};

using TExecutorThreadPtr = std::unique_ptr<TExecutorThread>;

////////////////////////////////////////////////////////////////////////////////

class TServer final
    : public TAppContext
    , public IServer
    , public std::enable_shared_from_this<TServer>
{
private:
    // A completion barrier retained after an endpoint is stopped and consumed
    // when an endpoint with the same listen address is started again.
    struct TPendingDrain
    {
        // Identifies this map entry so its completion callback cannot erase a
        // newer barrier stored for the same address.
        ui64 Id = 0;

        // Becomes ready when all requests accepted by previous connections
        // have finished and is passed to the next endpoint instance.
        TFuture<void> Future;
    };

    TVector<TExecutorThreadPtr> ExecutorThreads;

    TAdaptiveLock Lock;
    TMap<TString, TExecutorThread*> EndpointMap;

    // Preserves the connection completion tail across StopEndpoint/StartEndpoint
    // for the same listen address.
    TMap<TString, TPendingDrain> PendingDrains;
    ui64 LastPendingDrainId = 0;

public:
    TServer(
        ILoggingServicePtr logging,
        const TServerConfig& config)
    {
        Log = logging->CreateLog("BLOCKSTORE_NBD");

        InitExecutors(config);
    }

    ~TServer()
    {
        Stop();
    }

    void Start() override
    {
        STORAGE_INFO("Starting");

        for (auto& executorThread: ExecutorThreads) {
            executorThread->Start();
        }
    }

    void Stop() override
    {
        if (AtomicSwap(&ShouldStop, 1) == 1) {
            return;
        }

        STORAGE_INFO("Shutting down");

        for (auto& executorThread: ExecutorThreads) {
            executorThread->Stop();
        }

        with_lock (Lock) {
            ExecutorThreads.clear();
            EndpointMap.clear();
            PendingDrains.clear();
        }
    }

    TFuture<NProto::TError> StartEndpoint(
        TNetworkAddress listenAddress,
        IServerHandlerFactoryPtr handlerFactory) override
    {
        if (AtomicGet(ShouldStop) == 1) {
            NProto::TError error;
            error.SetCode(E_REJECTED);
            error.SetMessage("NBD server is stopped");
            return MakeFuture(error);
        }

        auto address = PrintHostAndPort(listenAddress);

        TFuture<void> drainResult = MakeFuture();
        with_lock (Lock) {
            auto it = EndpointMap.find(address);
            if (it != EndpointMap.end()) {
                NProto::TError error;
                error.SetCode(S_ALREADY);
                error.SetMessage(TStringBuilder()
                    << "endpoint " << address.Quote()
                    << " has already been started");
                return MakeFuture(error);
            }

            auto drainIt = PendingDrains.find(address);
            if (drainIt != PendingDrains.end()) {
                drainResult = drainIt->second.Future;
                PendingDrains.erase(drainIt);
            }
        }

        auto* executorThread = PickExecutor();
        Y_ABORT_UNLESS(executorThread);

        auto endpoint = executorThread->CreateEndpoint(
            listenAddress,
            std::move(handlerFactory),
            drainResult);

        auto future = executorThread->StartEndpoint(endpoint);

        auto weak_ptr = weak_from_this();

        return future.Apply([=, weak_ptr = std::move(weak_ptr)] (const auto& f) mutable {
            const auto& error = f.GetValue();
            auto ptr = weak_ptr.lock();
            if (HasError(error)) {
                if (ptr) {
                    ptr->StorePendingDrain(address, std::move(drainResult));
                }
                return error;
            }

            if (!ptr) {
                NProto::TError error;
                error.SetCode(E_REJECTED);
                error.SetMessage("NBD server is destroyed");
                return error;
            }

            ptr->AddEndpoint(
                executorThread,
                address,
                std::move(endpoint));

            return NProto::TError();
        });
    }

    TFuture<NProto::TError> StopEndpoint(TNetworkAddress listenAddress) override
    {
        if (AtomicGet(ShouldStop) == 1) {
            NProto::TError error;
            error.SetCode(E_REJECTED);
            error.SetMessage("NBD server is stopped");
            return MakeFuture(error);
        }

        auto address = PrintHostAndPort(listenAddress);
        TExecutorThread* executorThread;

        with_lock (Lock) {
            auto it = EndpointMap.find(address);
            if (it == EndpointMap.end()) {
                NProto::TError error;
                error.SetCode(S_ALREADY);
                error.SetMessage(TStringBuilder()
                    << "endpoint " << address.Quote()
                    << " has already been stopped");
                return MakeFuture(error);
            }

            executorThread = it->second;
            EndpointMap.erase(it);
        }

        auto endpoint = executorThread->RemoveEndpoint(address);
        auto future = executorThread->StopEndpoint(std::move(endpoint));

        return future.Apply(
            [weakPtr = weak_from_this(),
             address](const TFuture<TEndpointStopResult>& future)
            {
                const auto& [error, drainResult] = future.GetValue();
                if (auto ptr = weakPtr.lock()) {
                    ptr->StorePendingDrain(address, drainResult);
                }
                return error;
            });
    }

    size_t CollectRequests(
        const TIncompleteRequestsCollector& collector) override
    {
        TVector<TFuture<size_t>> futures;
        for (auto& executorThread: ExecutorThreads) {
            futures.push_back(executorThread->CollectRequests(collector));
        }
        WaitAll(futures).Wait();

        size_t count = 0;
        for (auto& future: futures) {
            count += future.GetValue();
        }
        return count;
    }

private:
    void RemovePendingDrain(const TString& address, ui64 id)
    {
        with_lock (Lock) {
            auto it = PendingDrains.find(address);
            if (it != PendingDrains.end() && it->second.Id == id) {
                PendingDrains.erase(it);
            }
        }
    }

    void StorePendingDrain(TString address, TFuture<void> future)
    {
        ui64 id = 0;
        with_lock (Lock) {
            auto it = PendingDrains.find(address);
            if (it != PendingDrains.end()) {
                future = WaitAll(it->second.Future, future);
            }

            id = ++LastPendingDrainId;
            PendingDrains.insert_or_assign(
                address,
                TPendingDrain{
                    .Id = id,
                    .Future = future,
                });
        }

        future.Subscribe(
            [weakPtr = weak_from_this(), address = std::move(address), id](
                const auto&)
            {
                if (auto ptr = weakPtr.lock()) {
                    ptr->RemovePendingDrain(address, id);
                }
            });
    }

    void InitExecutors(const TServerConfig& config)
    {
        for (size_t i = 1; i <= config.ThreadsCount; ++i) {
            auto executor = TExecutor::Create(
                TStringBuilder() << "NBD" << i,
                config.Affinity);

            Executors.push_back(executor.get());

            auto executorThread = std::make_unique<TExecutorThread>(
                *this,
                std::move(executor),
                config.LimiterEnabled,
                config.MaxInFlightBytesPerThread,
                config.SocketAccessMode);

            ExecutorThreads.push_back(std::move(executorThread));
        }
    }

    TExecutorThread* PickExecutor()
    {
        TExecutorThread* result = nullptr;

        for (auto& executor: ExecutorThreads) {
            if (result == nullptr ||
                executor->GetEndpointsCount() < result->GetEndpointsCount())
            {
                result = executor.get();
            }
        }

        return result;
    }

    void AddEndpoint(
        TExecutorThread* executorThread,
        TString address,
        TEndpointPtr endpoint)
    {
        with_lock (Lock) {
            auto [it, inserted] = EndpointMap.emplace(
                address,
                executorThread);
            Y_ABORT_UNLESS(inserted);
        }

        executorThread->AddEndpoint(
            std::move(address),
            std::move(endpoint));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    ILoggingServicePtr logging,
    const TServerConfig& config)
{
    return std::make_shared<TServer>(
        std::move(logging),
        config);
}

}   // namespace NCloud::NBlockStore::NBD
