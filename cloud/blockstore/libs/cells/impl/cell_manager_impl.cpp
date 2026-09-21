#include "cell_manager_impl.h"

#include "describe_volume.h"
#include "connection.h"
#include "endpoint_bootstrap.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>
#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/server/config.h>
#include <cloud/blockstore/libs/service/context.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/grpc/tls_certificate_provider.h>
#include <cloud/storage/core/libs/rdma/impl/client.h>
#include <cloud/storage/core/libs/rdma/impl/verbs.h>


#include <util/generic/hash_set.h>
#include <util/random/random.h>
#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

TCellManager::TCellManager(TCellsConfigPtr config, TBootstrap bootstrap)
    : ICellManager(std::move(config))
    , Bootstrap(std::move(bootstrap))
{
    for (const auto& cell: Config->GetCells()) {
        Pools.emplace(
            cell.first,
            std::make_shared<TCellHostPool>(cell.second, Bootstrap));
    }

    InboundActivity = std::make_shared<TCellInboundActivity>();
}

std::shared_ptr<TCellInboundActivity> TCellManager::GetInboundActivity()
{
    return InboundActivity;
}

void TCellManager::Start()
{
    Bootstrap.CertProvider->Start();
    Bootstrap.GrpcClient->Start();

    for (auto& pool: Pools) {
        pool.second->Start();
    }
}

void TCellManager::Stop()
{
    // before the client goes down: a sweep that outlived it would find
    // every host unreachable and migrate every connection on the way out
    for (auto& pool: Pools) {
        pool.second->Stop();
    }

    Bootstrap.GrpcClient->Stop();
    Bootstrap.CertProvider->Stop();
}

TCellsSnapshot TCellManager::GetSnapshot()
{
    TCellsSnapshot snapshot;
    for (const auto& [cellId, pool]: Pools) {
        auto& statuses = snapshot.HostStatuses[cellId];
        for (const auto& status: pool->GetHostStatuses()) {
            statuses.push_back({
                .Fqdn = status.Fqdn,
                .Alive = status.Alive,
                .Warm = status.Warm,
                .Connections = static_cast<ui32>(status.Connections)});
        }
    }
    snapshot.InboundActivity = InboundActivity->Snapshot(Bootstrap.Timer->Now());
    return snapshot;
}

NThreading::TFuture<TVector<TCellDescribeResult>> TCellManager::SearchVolume(
    TString diskId,
    IBlockStorePtr localService,
    TDuration timeout)
{
    NProto::TClientAppConfig clientAppConfig;
    auto& clientConfig = *clientAppConfig.MutableClientConfig();
    clientConfig = Config->GetGrpcClientConfig().GetClientConfig();
    clientConfig.SetClientId(FQDNHostName());
    auto appConfig =
        std::make_shared<NClient::TClientAppConfig>(clientAppConfig);

    NProto::TDescribeVolumeRequest request;
    request.SetDiskId(diskId);
    request.MutableHeaders()->SetClientId(FQDNHostName());

    TVector<TString> cellIds;
    cellIds.reserve(Config->GetCells().size());
    for (const auto& [cellId, cellConfig]: Config->GetCells()) {
        Y_UNUSED(cellConfig);
        cellIds.push_back(cellId);
    }

    return SearchVolumeAcrossCells(
        std::move(request),
        cellIds,
        GetCellsEndpoints(appConfig),
        std::move(localService),
        timeout,
        Bootstrap.Scheduler);
}

TCellConnectionFuture TCellManager::CreateConnection(
    const TString& cellId,
    const TString& fqdn,
    const NClient::TClientAppConfigPtr& clientConfig,
    ICellConnectionObserverPtr observer)
{
    auto* pool = Pools.FindPtr(cellId);
    if (!pool) {
        return MakeFuture(TResultOrError<ICellConnectionPtr>(MakeError(
            E_INVALID_STATE,
            TStringBuilder() << "Cell " << cellId << " is not configured")));
    }

    auto hostConfig = TCellHostConfig();
    if (fqdn) {
        hostConfig = (*pool)->MakeHostConfig(fqdn);
    } else {
        auto picked = (*pool)->PickHost();
        if (HasError(picked)) {
            return MakeFuture(
                TResultOrError<ICellConnectionPtr>(picked.GetError()));
        }
        hostConfig = picked.ExtractResult();
    }

    return CreateCellConnection(
        *pool,
        std::move(hostConfig),
        Bootstrap,
        clientConfig,
        std::move(observer));
}

TCellHostEndpointsByCellId TCellManager::GetCellsEndpoints(
    const NClient::TClientAppConfigPtr& clientConfig)
{
    TCellHostEndpointsByCellId res;
    for (auto& [cellId, pool]: Pools) {
        auto endpoints = pool->GetDescribeEndpoints(clientConfig);
        if (endpoints.empty()) {
            continue;
        }
        res.emplace(cellId, std::move(endpoints));
    }
    return res;
}

[[nodiscard]] TDescribeVolumeFuture TCellManager::DescribeVolume(
    TCallContextPtr callContext,
    const TString& diskId,
    const NProto::THeaders& headers,
    IBlockStorePtr service,
    const NProto::TClientConfig& clientConfig)
{
    NProto::TDescribeVolumeRequest request;
    request.MutableHeaders()->CopyFrom(headers);
    request.SetDiskId(diskId);

    auto configuredCellCount = Config->GetCells().size();
    if (configuredCellCount == 0) {
        return service->DescribeVolume(
            std::move(callContext),
            std::make_shared<NProto::TDescribeVolumeRequest>(
                std::move(request)));
    }

    NProto::TClientAppConfig clientAppConfig;
    auto& config = *clientAppConfig.MutableClientConfig();
    config = clientConfig;
    config.SetClientId(FQDNHostName());
    auto appConfig =
        std::make_shared<NClient::TClientAppConfig>(clientAppConfig);

    auto cellHostEndpoints = GetCellsEndpoints(appConfig);

    bool hasUnavailableCells = cellHostEndpoints.size() < configuredCellCount;

    return NCloud::NBlockStore::NCells::DescribeVolume(
        *Config,
        std::move(request),
        std::move(service),
        cellHostEndpoints,
        hasUnavailableCells,
        Bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

ICellManagerPtr CreateCellManager(
    TCellsConfigPtr config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    ITraceSerializerPtr traceSerializer,
    IServerStatsPtr serverStats,
    ICertificateProviderPtr certificateProvider,
    NCloud::NStorage::NRdma::IClientPtr rdmaClient)
{
    auto appConfig = std::make_shared<NClient::TClientAppConfig>(
        config->GetGrpcClientConfig());

    auto result = NClient::CreateMultiHostClient(
        std::move(appConfig),
        timer,
        scheduler,
        logging,
        monitoring,
        std::move(serverStats),
        certificateProvider);

    if (HasError(result)) {
        STORAGE_THROW_SERVICE_ERROR(E_FAIL) << "unable to create gRPC client";
    }

    auto rdmaTaskQueue =
        config->GetRdmaTransportWorkers()
            ? CreateThreadPool("CELLS", config->GetRdmaTransportWorkers())
            : CreateTaskQueueStub();

    rdmaTaskQueue->Start();

    TBootstrap bootstrap{
        .Timer = std::move(timer),
        .Scheduler = std::move(scheduler),
        .Logging = std::move(logging),
        .Monitoring = std::move(monitoring),
        .TraceSerializer = std::move(traceSerializer),
        .CertProvider = std::move(certificateProvider),
        .GrpcClient = std::move(result.ExtractResult()),
        .RdmaClient = std::move(rdmaClient),
        .RdmaTaskQueue = std::move(rdmaTaskQueue),
        .EndpointsSetup = CreateCellHostEndpointBootstrap()};

    return std::make_shared<TCellManager>(std::move(config), bootstrap);
}

}   // namespace NCloud::NBlockStore::NCells
