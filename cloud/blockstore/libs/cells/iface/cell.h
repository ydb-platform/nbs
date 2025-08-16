#pragma once

#include "host_endpoint.h"

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

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NCells
