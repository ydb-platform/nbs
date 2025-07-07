#pragma once

#include "public.h"
#include "host_endpoint.h"

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/blockstore/libs/rdma/iface/client.h>

#include <cloud/blockstore/libs/diagnostics/public.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

using TDescribeFuture = NThreading::TFuture<NProto::TDescribeVolumeResponse>;

struct ICellsManager
    : public IStartable
{
    TCellsConfigPtr Config;

    explicit ICellsManager(TCellsConfigPtr config)
        : Config(std::move(config))
    {}

    [[nodiscard]] virtual TResultOrError<THostEndpoint> GetCellEndpoint(
        const TString& cellId,
        const NClient::TClientAppConfigPtr& clientConfig) = 0;

    [[nodiscard]] virtual std::optional<TDescribeFuture> DescribeVolume(
        const TString& diskId,
        const NProto::THeaders& headers,
        const IBlockStorePtr& localService,
        const NProto::TClientConfig& clientConfig) = 0;
};

////////////////////////////////////////////////////////////////////////////////

ICellsManagerPtr CreateCellsManager(
    TCellsConfigPtr config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    ITraceSerializerPtr traceSerializer,
    IServerStatsPtr serverStats,
    NRdma::IClientPtr rdmaClient);

ICellsManagerPtr CreateCellsManagerStub();

}   // namespace NCloud::NBlockStore::NCells
