#pragma once

#include "config.h"
#include "host_endpoint.h"
#include "bootstrap.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>
#include <cloud/blockstore/libs/rdma/iface/client.h>
#include <cloud/blockstore/libs/server/config.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <util/generic/hash_set.h>
#include <util/random/random.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

struct ICell
    : public IStartable
{
    [[nodiscard]] virtual TResultOrError<TCellHostEndpoint> GetCellClient(
        const NClient::TClientAppConfigPtr& clientConfig) = 0;

    [[nodiscard]] virtual TCellHostEndpoints GetCellClients(
        const NClient::TClientAppConfigPtr& clientConfig) = 0;
};

using ICellPtr = std::shared_ptr<ICell>;

////////////////////////////////////////////////////////////////////////////////

ICellPtr CreateCell(TBootstrap boorstrap, TCellConfig config);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NCells
