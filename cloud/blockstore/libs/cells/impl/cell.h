#pragma once

#include "bootstrap.h"

#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/cells/iface/host_endpoint.h>
#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

struct ICell: public IStartable
{
    [[nodiscard]] virtual TResultOrError<TCellHostEndpoint> GetCellClient(
        const NClient::TClientAppConfigPtr& clientConfig) = 0;

    [[nodiscard]] virtual TCellHostEndpoints GetCellClients(
        const NClient::TClientAppConfigPtr& clientConfig) = 0;
};

using ICellPtr = std::shared_ptr<ICell>;

////////////////////////////////////////////////////////////////////////////////

ICellPtr CreateCell(TBootstrap bootstrap, TCellConfig config);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NCells
