#pragma once

#include "bootstrap.h"

#include <cloud/blockstore/libs/cells/iface/cell_host.h>
#include <cloud/blockstore/libs/cells/iface/host_endpoint.h>
#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

ICellHostPtr CreateHost(TCellHostConfig config, TBootstrap bootstrap);

}   // namespace NCloud::NBlockStore::NCells
