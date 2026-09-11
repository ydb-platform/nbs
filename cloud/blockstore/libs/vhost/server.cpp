#include "server.h"

#include "app_context.h"
#include "endpoint.h"
#include "executor.h"
#include "vhost.h"

#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/service/device_handler.h>
#include <cloud/blockstore/libs/service/storage.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <cloud/contrib/vhost/include/vhost/server.h>

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/utility.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/system/mutex.h>

namespace NCloud::NBlockStore::NVhost {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

TVector<TExecutor*> NormalizeExecutors(
    const TVector<TExecutor*>& executors,
    ui32 vhostQueuesCount)
{
    TVector<TExecutor*> normalized;
    normalized.reserve(vhostQueuesCount);
    for (ui32 i = 0; i < vhostQueuesCount; ++i) {
        normalized.push_back(executors[i % executors.size()]);
    }
    return normalized;
}

void RequestEndpointStop(
    const TEndpointPtr& endpoint,
    bool deleteSocket,
    TPromise<NProto::TError> stopPromise)
{
    const auto error = SafeExecute<NProto::TError>(
        [&]
        {
            endpoint->Stop(deleteSocket).Subscribe(
                [stopPromise](const auto& future) mutable
                {
                    stopPromise.SetValue(
                        SafeExecute<NProto::TError>(
                            [&] { return future.GetValue(); }));
                });
            return NProto::TError();
        });
    if (HasError(error)) {
        stopPromise.SetValue(error);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TServer final
    : public TAppContext
    , public IServer
    , public std::enable_shared_from_this<TServer>
{
private:
    const IVhostQueueFactoryPtr VhostQueueFactory;
    const IDeviceHandlerFactoryPtr DeviceHandlerFactory;
    const TServerConfig Config;
    const TVhostCallbacks Callbacks;

    TMutex Lock;

    TVector<TExecutorPtr> Executors;

    THashMap<TString, TEndpointPtr> Endpoints;
    struct TStoppingEndpoint
    {
        TEndpointPtr Endpoint;
        TFuture<NProto::TError> Future;
    };
    THashMap<TString, TStoppingEndpoint> StoppingEndpoints;

public:
    TServer(
        const ILoggingServicePtr& logging,
        IServerStatsPtr serverStats,
        IVhostQueueFactoryPtr vhostQueueFactory,
        IDeviceHandlerFactoryPtr deviceHandlerFactory,
        TServerConfig config,
        TVhostCallbacks callbacks);

    ~TServer() override;

    void Start() override;
    void Stop() override;

    size_t CollectRequests(
        const TIncompleteRequestsCollector& collector) override;

    TFuture<NProto::TError> StartEndpoint(
        TString socketPath,
        IStoragePtr storage,
        const TStorageOptions& options) override;

    TFuture<NProto::TError> StopEndpoint(const TString& socketPath) override;

    NProto::TError UpdateEndpoint(
        const TString& socketPath,
        ui64 blocksCount) override;

private:
    void InitExecutors();

    // Picks |count| distinct executors with the lowest number of assigned
    // vhost queues. Must be called under Lock.
    TVector<TExecutor*> PickExecutors(ui32 count);

    struct TStopRequest
    {
        // Has to be completed by RequestEndpointStop().
        TPromise<NProto::TError> Promise;
        // Resolved once HandleStoppedEndpoint() has dropped the endpoint.
        TFuture<NProto::TError> Future;
    };

    // Must be called under Lock.
    TStopRequest RegisterStoppingEndpoint(
        const TString& socketPath,
        TEndpointPtr endpoint);

    void StopAllEndpoints();

    void HandleStoppedEndpoint(
        const TString& socketPath,
        const NProto::TError& error);

    IDeviceHandlerPtr CreateDeviceHandler(
        const TStorageOptions& options,
        IStoragePtr storage);
};

////////////////////////////////////////////////////////////////////////////////

TServer::TServer(
    const ILoggingServicePtr& logging,
    IServerStatsPtr serverStats,
    IVhostQueueFactoryPtr vhostQueueFactory,
    IDeviceHandlerFactoryPtr deviceHandlerFactory,
    TServerConfig config,
    TVhostCallbacks callbacks)
    : VhostQueueFactory(std::move(vhostQueueFactory))
    , DeviceHandlerFactory(std::move(deviceHandlerFactory))
    , Config(std::move(config))
    , Callbacks(std::move(callbacks))
{
    Log = logging->CreateLog("BLOCKSTORE_VHOST");
    ServerStats = std::move(serverStats);

    InitExecutors();
}

TServer::~TServer()
{
    Stop();
}

void TServer::Start()
{
    STORAGE_INFO("Start");

    for (auto& executor: Executors) {
        executor->Start();
    }
}

void TServer::Stop()
{
    if (ShouldStop.test_and_set()) {
        return;
    }

    STORAGE_INFO("Shutting down");

    StopAllEndpoints();

    for (auto& executor: Executors) {
        executor->Shutdown();
    }
}

size_t TServer::CollectRequests(const TIncompleteRequestsCollector& collector)
{
    size_t count = 0;
    with_lock (Lock) {
        for (auto& it: Endpoints) {
            count += it.second->CollectRequests(collector);
        }
        for (auto& it: StoppingEndpoints) {
            count += it.second.Endpoint->CollectRequests(collector);
        }
    }
    return count;
}

TFuture<NProto::TError> TServer::StartEndpoint(
    TString socketPath,
    IStoragePtr storage,
    const TStorageOptions& options)
{
    if (options.VhostQueuesCount == 0 ||
        options.VhostQueuesCount > VHD_MAX_REQUEST_QUEUES)
    {
        return MakeFuture(MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "Vhost queues count must be in range [1, "
                << VHD_MAX_REQUEST_QUEUES << "]"));
    }

    if (ShouldStop.test()) {
        return MakeFuture(MakeError(E_FAIL, "Vhost server is stopped"));
    }

    // There is no point in taking more executors than there are
    // virtqueues (and libvhost forbids it).
    const ui32 maxExecutorsCount =
        Min<ui32>(options.VhostQueuesCount, Executors.size());
    const ui32 executorsCount =
        std::clamp<ui32>(options.ThreadCount, 1, maxExecutorsCount);

    if (options.ThreadCount > maxExecutorsCount) {
        STORAGE_WARN(
            "Endpoint " << socketPath.Quote() << " requested "
                        << options.ThreadCount << " threads, but only "
                        << maxExecutorsCount << " can be used (vhost queues: "
                        << options.VhostQueuesCount
                        << ", thread pool size: " << Executors.size() << ")");
    }

    auto deviceHandler = CreateDeviceHandler(options, std::move(storage));
    TEndpointPtr endpoint;
    TVector<IVhostQueuePtr> queues;

    with_lock (Lock) {
        // Check again under the lock to avoid races with Stop().
        if (ShouldStop.test()) {
            return MakeFuture(MakeError(E_FAIL, "Vhost server is stopped"));
        }

        if (StoppingEndpoints.contains(socketPath)) {
            return MakeFuture(MakeError(E_REJECTED, "endpoint is stopping"));
        }

        auto it = Endpoints.find(socketPath);
        if (it != Endpoints.end()) {
            NProto::TError error;
            error.SetCode(S_ALREADY);
            error.SetMessage(
                TStringBuilder() << "endpoint " << socketPath.Quote()
                                 << " has already been started");
            return MakeFuture(error);
        }

        TVector<TExecutor*> executors = PickExecutors(executorsCount);
        Y_ABORT_UNLESS(executors.size() == executorsCount);

        executors = NormalizeExecutors(executors, options.VhostQueuesCount);
        Y_ABORT_UNLESS(executors.size() == options.VhostQueuesCount);

        queues.reserve(executors.size());
        for (auto* executor: executors) {
            queues.push_back(executor->GetQueue());
        }

        // The ctor bumps the assignment counters of the picked executors, so
        // it has to run under Lock together with PickExecutors.
        endpoint = std::make_shared<TEndpoint>(
            *this,
            std::move(deviceHandler),
            socketPath,
            options,
            Config.SocketAccessMode,
            std::move(executors));
    }

    STORAGE_INFO(
        "Start endpoint " << socketPath.Quote() << " with "
                          << options.VhostQueuesCount << " vhost queues"
                          << " served by " << executorsCount << " executors"
                          << " (" << options.ThreadCount << " requested)");

    auto vhostDevice = VhostQueueFactory->CreateDevice(
        socketPath,
        options.DeviceName.empty() ? options.DiskId : options.DeviceName,
        options.BlockSize,
        options.BlocksCount,
        options.VhostQueuesCount,
        options.DiscardEnabled,
        options.WriteZeroesEnabled,
        options.OptimalIoSize,
        std::move(queues),
        endpoint->GetCookie(),
        Callbacks);
    endpoint->SetVhostDevice(std::move(vhostDevice));

    auto error = SafeExecute<NProto::TError>([&] { return endpoint->Start(); });
    if (HasError(error)) {
        return MakeFuture(error);
    }

    with_lock (Lock) {
        auto [it, inserted] =
            Endpoints.emplace(std::move(socketPath), std::move(endpoint));
        Y_ABORT_UNLESS(inserted);
    }

    return MakeFuture<NProto::TError>();
}

TServer::TStopRequest TServer::RegisterStoppingEndpoint(
    const TString& socketPath,
    TEndpointPtr endpoint)
{
    auto promise = NewPromise<NProto::TError>();

    // Keeps the server alive until the stop completes. Empty when called from
    // ~TServer() via Stop().
    auto self = weak_from_this().lock();

    auto stopFuture = promise.GetFuture().Apply(
        [this, self = std::move(self), socketPath](const auto& future)
        {
            Y_UNUSED(self);
            const auto& error = future.GetValue();
            HandleStoppedEndpoint(socketPath, error);
            return error;
        });

    auto [it, inserted] = StoppingEndpoints.emplace(
        socketPath,
        TStoppingEndpoint{std::move(endpoint), stopFuture});
    Y_ABORT_UNLESS(inserted);

    return {std::move(promise), std::move(stopFuture)};
}

TFuture<NProto::TError> TServer::StopEndpoint(const TString& socketPath)
{
    if (ShouldStop.test()) {
        NProto::TError error;
        error.SetCode(E_FAIL);
        error.SetMessage("Vhost server is stopped");
        return MakeFuture(error);
    }

    TEndpointPtr endpoint;
    TStopRequest stopRequest;

    with_lock (Lock) {
        if (auto stoppingIt = StoppingEndpoints.find(socketPath);
            stoppingIt != StoppingEndpoints.end())
        {
            return stoppingIt->second.Future;
        }

        auto it = Endpoints.find(socketPath);
        if (it == Endpoints.end()) {
            NProto::TError error;
            error.SetCode(S_ALREADY);
            error.SetMessage(
                TStringBuilder() << "endpoint " << socketPath.Quote()
                                 << " has already been stopped");
            return MakeFuture(error);
        }

        endpoint = std::move(it->second);
        Endpoints.erase(it);

        stopRequest = RegisterStoppingEndpoint(socketPath, endpoint);
    }

    RequestEndpointStop(endpoint, true, std::move(stopRequest.Promise));
    return stopRequest.Future;
}

NProto::TError TServer::UpdateEndpoint(
    const TString& socketPath,
    ui64 blocksCount)
{
    if (ShouldStop.test()) {
        NProto::TError error;
        error.SetCode(E_FAIL);
        error.SetMessage("Vhost server is stopped");
        return error;
    }

    TEndpointPtr endpoint;

    with_lock (Lock) {
        auto it = Endpoints.find(socketPath);
        if (it == Endpoints.end()) {
            NProto::TError error;
            error.SetCode(S_FALSE);
            error.SetMessage(
                TStringBuilder()
                << "endpoint " << socketPath.Quote() << " not started");
            return error;
        }

        endpoint = it->second;
    }

    if (endpoint) {
        endpoint->Update(blocksCount);
    }
    return NProto::TError{};
}

void TServer::StopAllEndpoints()
{
    TVector<TFuture<NProto::TError>> futures;
    TVector<std::pair<TEndpointPtr, TPromise<NProto::TError>>> endpointsToStop;

    with_lock (Lock) {
        for (const auto& entry: StoppingEndpoints) {
            futures.push_back(entry.second.Future);
        }

        for (auto& [socketPath, endpoint]: Endpoints) {
            auto stopRequest = RegisterStoppingEndpoint(socketPath, endpoint);

            futures.push_back(std::move(stopRequest.Future));

            endpointsToStop.emplace_back(
                std::move(endpoint),
                std::move(stopRequest.Promise));
        }

        Endpoints.clear();
    }

    for (auto& [endpoint, stopPromise]: endpointsToStop) {
        RequestEndpointStop(endpoint, false, std::move(stopPromise));
    }

    WaitAll(futures).Wait();
}

void TServer::HandleStoppedEndpoint(
    const TString& socketPath,
    const NProto::TError& error)
{
    // remove endpoint outside of the lock to don't make extra work inside the
    // lock
    TStoppingEndpoint stoppedEndpoint;
    bool erased = false;
    with_lock (Lock) {
        auto it = StoppingEndpoints.find(socketPath);
        if (it != StoppingEndpoints.end()) {
            stoppedEndpoint = std::move(it->second);
            StoppingEndpoints.erase(it);
            erased = true;
        }
    }

    if (erased && HasError(error)) {
        STORAGE_ERROR(
            "Failed to stop endpoint: " << socketPath.Quote()
                                        << ". Error: " << error);
    }
}

void TServer::InitExecutors()
{
    for (size_t i = 1; i <= Config.ThreadsCount; ++i) {
        auto vhostQueue = VhostQueueFactory->CreateQueue();

        auto executor = std::make_unique<TExecutor>(
            TStringBuilder() << "VHOST" << i,
            *ServerStats,
            std::move(vhostQueue),
            Config.Affinity);

        Executors.push_back(std::move(executor));
    }
}

TVector<TExecutor*> TServer::PickExecutors(ui32 count)
{
    Y_ABORT_UNLESS(count > 0);
    Y_ABORT_UNLESS(count <= Executors.size());

    TMultiMap<ui32, TExecutor*> byLoad;
    for (const auto& executor: Executors) {
        byLoad.emplace(
            executor->GetAssignedVhostQueuesCount(),
            executor.get());
    }

    TVector<TExecutor*> picked;
    picked.reserve(count);
    // NOTE: The order can be significant: libvhost assigns any remainder from
    // the round-robin distribution to the first queues.
    for (const auto& [_, executor]: byLoad) {
        if (picked.size() == count) {
            break;
        }
        picked.push_back(executor);
    }

    Y_ABORT_UNLESS(picked.size() == count);
    return picked;
}

IDeviceHandlerPtr TServer::CreateDeviceHandler(
    const TStorageOptions& options,
    IStoragePtr storage)
{
    TDeviceHandlerParams params{
        .Storage = std::move(storage),
        .DiskId = options.DiskId,
        .CloudId = options.CloudId,
        .FolderId = options.FolderId,
        .ClientId = options.ClientId,
        .BlockSize = options.BlockSize,
        .MaxZeroBlocksSubRequestSize = options.MaxZeroBlocksSubRequestSize,
        .UnalignedRequestsDisabled = options.UnalignedRequestsDisabled,
        .StorageMediaKind = options.StorageMediaKind};

    return DeviceHandlerFactory->CreateDeviceHandler(std::move(params));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    const ILoggingServicePtr& logging,
    IServerStatsPtr serverStats,
    IVhostQueueFactoryPtr vhostQueueFactory,
    IDeviceHandlerFactoryPtr deviceHandlerFactory,
    TServerConfig config,
    TVhostCallbacks callbacks)
{
    return std::make_shared<TServer>(
        logging,
        std::move(serverStats),
        std::move(vhostQueueFactory),
        std::move(deviceHandlerFactory),
        std::move(config),
        std::move(callbacks));
}

}   // namespace NCloud::NBlockStore::NVhost
