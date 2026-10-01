#pragma once

#include "bootstrap.h"
#include "host_pool.h"

#include <cloud/blockstore/libs/cells/iface/cell_manager.h>
#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/cells/iface/connection.h>
#include <cloud/blockstore/libs/client/public.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

TCellConnectionFuture CreateCellConnection(
    TCellHostPoolPtr pool,
    TCellHostConfig hostConfig,
    TBootstrap bootstrap,
    NClient::TClientAppConfigPtr clientConfig,
    ICellConnectionObserverPtr observer);

TCellConnectionRegistryPtr CreateCellConnectionRegistry();

// What the live connections in the registry serve; forgets the ones that are
// gone - a connection lives as long as the mount using it, and the registry
// only looks at it.
TVector<TCellMountStatus> GetCellMounts(TCellConnectionRegistry& registry);

}   // namespace NCloud::NBlockStore::NCells
