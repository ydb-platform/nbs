#pragma once

#include <cloud/blockstore/libs/cells/iface/bootstrap.h>
#include <cloud/blockstore/libs/cells/iface/cell.h>
#include <cloud/blockstore/libs/cells/iface/cells.h>
#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/cells/iface/endpoint_bootstrap.h>
#include <cloud/blockstore/libs/cells/iface/host_endpoint.h>
#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>
#include <cloud/blockstore/libs/server/config.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/service/pages/html_mon_page.h>
#include <library/cpp/monlib/service/pages/index_mon_page.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/hash_set.h>
#include <util/random/random.h>
#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NCells {

using namespace NMonitoring;

////////////////////////////////////////////////////////////////////////////////

struct TCellManager
    : public ICellManager
{
    const TBootstrap Bootstrap;

    THashMap<TString, ICellPtr> Cells;

    TCellManager(TCellsConfigPtr config, TBootstrap bootstrap);

    void Start() override;
    void Stop() override;

    TResultOrError<TCellHostEndpoint> GetCellEndpoint(
        const TString& cellId,
        const NClient::TClientAppConfigPtr& clientConfig) override;

    [[nodiscard]] std::optional<TDescribeVolumeFuture> DescribeVolume(
        const TString& diskId,
        const NProto::THeaders& headers,
        const IBlockStorePtr& localService,
        const NProto::TClientConfig& clientConfig) override;

    void OutputHtml(IOutputStream& out, const IMonHttpRequest& request);

private:
    [[nodiscard]] TCellHostEndpointsByCellId GetCellsEndpoints(
        const NClient::TClientAppConfigPtr& clientConfig);
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NCells
