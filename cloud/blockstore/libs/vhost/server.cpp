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
    TMutex Lock;

    TVector<TExecutorPtr> Executors;

    THashMap<TString, TEndpointPtr> Endpoints;
    THashMap<TString, TEndpointPtr> StoppingEndpoints;

public:
    TServer(
        ILoggingServicePtr logging,
        IServerStatsPtr serverStats,
        IVhostQueueFactoryPtr vhostQueueFactory,
        IDeviceHandlerFactoryPtr deviceHandlerFactory,
        TServerConfig config,
        TVhostCallbacks callbacks);

    ~TServer();

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
    // endpoints. Must be called under Lock.
    TVector<TExecutor*> PickExecutors(ui32 count);

    void StopAllEndpoints();

    void HandleStoppedEndpoint(
        const TString& socketPath,
        const NProto::TError& error);

    // Creates a single device handler shared by all executors of the endpoint.
    // The whole storage-wrapper chain (aligned device handler, unaligned
    // read-modify-write guard, etc.) is built once per endpoint and is safe to
    // use from several threads at once.
    IDeviceHandlerPtr CreateDeviceHandler(
        const TStorageOptions& options,
        IStoragePtr storage);
};

////////////////////////////////////////////////////////////////////////////////

TServer::TServer(
    ILoggingServicePtr logging,
    IServerStatsPtr serverStats,
    IVhostQueueFactoryPtr vhostQueueFactory,
    IDeviceHandlerFactoryPtr deviceHandlerFactory,
    TServerConfig config,
    TVhostCallbacks callbacks)
{
    Log = logging->CreateLog("BLOCKSTORE_VHOST");
    ServerStats = std::move(serverStats);
    VhostQueueFactory = std::move(vhostQueueFactory);
    DeviceHandlerFactory = std::move(deviceHandlerFactory);
    Config = std::move(config);
    Callbacks = std::move(callbacks);

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
            count += it.second->CollectRequests(collector);
        }
    }
    return count;
}

TFuture<NProto::TError> TServer::StartEndpoint(
    TString socketPath,
    IStoragePtr storage,
    const TStorageOptions& options)
{
    if (ShouldStop.test()) {
        NProto::TError error;
        error.SetCode(E_FAIL);
        error.SetMessage("Vhost server is stopped");
        return MakeFuture(error);
    }

    // Number of virtqueues exposed to the guest.
    const ui32 vhostQueuesCount = Max<ui32>(1, options.VhostQueuesCount);
    // Number of executors that will serve the endpoint. libvhost spreads the
    // guest's virtqueues over their request queues, so the endpoint is not
    // limited by a single thread anymore. There is no point in taking more
    // executors than there are virtqueues (and libvhost forbids it).
    const ui32 executorsCount = Min<ui32>(vhostQueuesCount, Executors.size());

    // Single device handler shared by all executors of this endpoint. The
    // whole storage-wrapper chain is built once per endpoint.
    auto deviceHandler = CreateDeviceHandler(options, std::move(storage));

    TEndpointPtr endpoint;
    TVector<IVhostQueuePtr> queues;

    with_lock (Lock) {
        auto it = Endpoints.find(socketPath);
        if (it != Endpoints.end()) {
            NProto::TError error;
            error.SetCode(S_ALREADY);
            error.SetMessage(TStringBuilder()
                << "endpoint " << socketPath.Quote()
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

    STORAGE_INFO("Start endpoint " << socketPath.Quote()
        << " with " << vhostQueuesCount << " vhost queues"
        << " served by " << executorsCount << " executors");

    auto vhostDevice = VhostQueueFactory->CreateDevice(
        socketPath,
        options.DeviceName.empty() ? options.DiskId : options.DeviceName,
        options.BlockSize,
        options.BlocksCount,
        vhostQueuesCount,
        options.DiscardEnabled,
        options.WriteZeroesEnabled,
        options.OptimalIoSize,
        std::move(queues),
        endpoint->GetCookie(),
        Callbacks);
    endpoint->SetVhostDevice(std::move(vhostDevice));

    auto error = SafeExecute<NProto::TError>([&] {
        return endpoint->Start();
    });
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

    with_lock (Lock) {
        auto it = Endpoints.find(socketPath);
        if (it == Endpoints.end()) {
            NProto::TError error;
            error.SetCode(S_ALREADY);
            error.SetMessage(TStringBuilder()
                << "endpoint " << socketPath.Quote()
                << " has already been stopped");
            return MakeFuture(error);
        }

        endpoint = std::move(it->second);
        Endpoints.erase(it);

        StoppingEndpoints.emplace(socketPath, endpoint);
    }

    auto ptr = shared_from_this();
    return endpoint->Stop(true).Apply(
        [ptr = std::move(ptr), socketPath] (const auto& future) {
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
            error.SetMessage(TStringBuilder()
                << "endpoint " << socketPath.Quote()
                << " not started");
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
        for (auto& it: Endpoints) {
            const auto& socketPath = it.first;
            auto endpoint = std::move(it.second);

            StoppingEndpoints.emplace(socketPath, endpoint);

            sockets.push_back(socketPath);
            futures.push_back(endpoint->Stop(false));
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
    if (HasError(error)) {
        STORAGE_ERROR("Failed to stop endpoint: "
            << socketPath.Quote()
            << ". Error: " << error);
    }

    with_lock (Lock) {
        auto it = StoppingEndpoints.find(socketPath);
        if (it != StoppingEndpoints.end()) {
            StoppingEndpoints.erase(it);
        }
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

    // Snapshot the assignment counters into a multimap so that the ordering is
    // computed against a stable set of values. The multimap sorts by key
    // ascending, so the first |count| entries are the least loaded executors.
    TMultiMap<ui32, TExecutor*> byLoad;
    for (const auto& executor: Executors) {
        byLoad.emplace(executor->GetAssignedEndpointsCount(), executor.get());
    }

    TVector<TExecutor*> picked;
    picked.reserve(count);
    for (const auto& [load, executor]: byLoad) {
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
    ILoggingServicePtr logging,
    IServerStatsPtr serverStats,
    IVhostQueueFactoryPtr vhostQueueFactory,
    IDeviceHandlerFactoryPtr deviceHandlerFactory,
    TServerConfig config,
    TVhostCallbacks callbacks)
{
    return std::make_shared<TServer>(
        std::move(logging),
        std::move(serverStats),
        std::move(vhostQueueFactory),
        std::move(deviceHandlerFactory),
        std::move(config),
        std::move(callbacks));
}

}   // namespace NCloud::NBlockStore::NVhost
