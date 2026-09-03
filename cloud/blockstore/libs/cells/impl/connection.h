#pragma once

#include "bootstrap.h"
#include "host_pool.h"

#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/cells/iface/connection.h>
#include <cloud/blockstore/libs/client/public.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

TCellConnectionFuture CreateCellConnection(
    TCellHostPoolPtr pool,
    TCellHostConfig hostConfig,
    TBootstrap bootstrap,
    NClient::TClientAppConfigPtr clientConfig,
    ICellConnectionObserverPtr observer);

}   // namespace NCloud::NBlockStore::NCells
