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
    Y_ABORT_UNLESS(options.VhostQueuesCount > 0);

    if (ShouldStop.test()) {
        NProto::TError error;
        error.SetCode(E_FAIL);
        error.SetMessage("Vhost server is stopped");
        return MakeFuture(error);
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
                        << options.ThreadCount << " threads"
                        << ", but only " << maxExecutorsCount << " can be used"
                        << " (vhost queues: " << options.VhostQueuesCount
                        << ", thread pool size: " << Executors.size() << ")");
    }

    auto deviceHandler = CreateDeviceHandler(options, std::move(storage));
    TEndpointPtr endpoint;
    TVector<IVhostQueuePtr> queues;

    with_lock (Lock) {
        auto it = Endpoints.find(socketPath);
        if (it != Endpoints.end()) {
            NProto::TError error;
            error.SetCode(S_ALREADY);
            error.SetMessage(
                TStringBuilder() << "endpoint " << socketPath.Quote()
                                 << " has already been started");
            return MakeFuture(error);
        }

        auto executors = PickExecutors(executorsCount);
        Y_ABORT_UNLESS(executors.size() == executorsCount);

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
        Callbacks,
        options.ReadOnly);
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

TFuture<NProto::TError> TServer::StopEndpoint(const TString& socketPath)
{
    if (ShouldStop.test()) {
        NProto::TError error;
        error.SetCode(E_FAIL);
        error.SetMessage("Vhost server is stopped");
        return MakeFuture(error);
    }

    TEndpointPtr endpoint;
    TFuture<NProto::TError> stopFuture;

    with_lock (Lock) {
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

        stopFuture = endpoint->Stop(true);
        StoppingEndpoints.emplace(
            socketPath,
            TStoppingEndpoint{endpoint, stopFuture});
    }

    auto ptr = shared_from_this();
    return stopFuture.Apply(
        [ptr = std::move(ptr), socketPath](const auto& future)
        {
            const auto& error = future.GetValue();
            ptr->HandleStoppedEndpoint(socketPath, error);
            return error;
        });
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
    TVector<TString> sockets;
    TVector<TFuture<NProto::TError>> futures;

    with_lock (Lock) {
        for (const auto& [socketPath, stoppingEndpoint]: StoppingEndpoints) {
            sockets.push_back(socketPath);
            futures.push_back(stoppingEndpoint.Future);
        }

        for (auto& it: Endpoints) {
            const auto& socketPath = it.first;
            auto endpoint = std::move(it.second);
            auto future = endpoint->Stop(false);

            StoppingEndpoints.emplace(
                socketPath,
                TStoppingEndpoint{endpoint, future});

            sockets.push_back(socketPath);
            futures.push_back(std::move(future));
        }

        Endpoints.clear();
    }

    WaitAll(futures).Wait();

    for (size_t i = 0; i < sockets.size(); ++i) {
        const auto& socketPath = sockets[i];
        const auto& future = futures[i];
        HandleStoppedEndpoint(socketPath, future.GetValue());
    }
}

void TServer::HandleStoppedEndpoint(
    const TString& socketPath,
    const NProto::TError& error)
{
    bool erased = false;
    with_lock (Lock) {
        auto it = StoppingEndpoints.find(socketPath);
        if (it != StoppingEndpoints.end()) {
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
