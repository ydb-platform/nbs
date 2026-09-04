#include "cell_manager_impl.h"

#include "describe_volume.h"
#include "connection.h"
#include "endpoint_bootstrap.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>
#include <cloud/blockstore/libs/server/config.h>
#include <cloud/blockstore/libs/service/context.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/grpc/tls_certificate_provider.h>
#include <cloud/storage/core/libs/rdma/impl/client.h>
#include <cloud/storage/core/libs/rdma/impl/verbs.h>

#include <library/cpp/monlib/service/pages/html_mon_page.h>
#include <library/cpp/monlib/service/pages/index_mon_page.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/hash_set.h>
#include <util/random/random.h>
#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NCells {

using namespace NMonitoring;

////////////////////////////////////////////////////////////////////////////////

class TCellsMonPage final: public THtmlMonPage
{
private:
    TCellManager& Manager;

public:
    TCellsMonPage(TCellManager& manager, const TString& componentName)
        : THtmlMonPage(componentName, componentName, true)
        , Manager(manager)
    {}

    void OutputContent(IMonHttpRequest& request) override
    {
        Manager.OutputHtml(request.Output(), request);
    }
};

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

    if (Bootstrap.Monitoring) {
        auto rootPage =
            Bootstrap.Monitoring->RegisterIndexPage("blockstore", "BlockStore");
        static_cast<TIndexMonPage&>(*rootPage).Register(
            new TCellsMonPage(*this, "Cells"));
    }
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
    Bootstrap.GrpcClient->Stop();
    Bootstrap.CertProvider->Stop();
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
        auto picked = (*pool)->PickConfiguredHost();
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

void TCellManager::OutputHtml(
    IOutputStream& out,
    const IMonHttpRequest& request)
{
    Y_UNUSED(out);
    Y_UNUSED(request);
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
