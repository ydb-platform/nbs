#pragma once

#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/cells/iface/endpoints_setup.h>
#include <cloud/blockstore/libs/cells/iface/host_endpoint.h>
#include <cloud/blockstore/libs/cells/iface/arguments.h>
#include <cloud/blockstore/libs/cells/iface/cell.h>
#include <cloud/blockstore/libs/cells/iface/cells.h>
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

struct TCellsManager
    : public ICellsManager
{
    const TArguments Args;

    THashMap<TString, ICellManagerPtr> Cells;

    TCellsManager(
        TCellsConfigPtr config,
        TArguments args);

    void Start() override;
    void Stop() override;

    TResultOrError<THostEndpoint> GetCellEndpoint(
        const TString& cellId,
        const NClient::TClientAppConfigPtr& clientConfig) override;

    [[nodiscard]] std::optional<TDescribeFuture> DescribeVolume(
        const TString& diskId,
        const NProto::THeaders& headers,
        const IBlockStorePtr& localService,
        const NProto::TClientConfig& clientConfig) override;

    void OutputHtml(IOutputStream& out, const IMonHttpRequest& request);

private:
    [[nodiscard]] TCellsEndpoints GetCellsEndpoints(
        const NClient::TClientAppConfigPtr& clientConfig);
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NCells
