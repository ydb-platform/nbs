#pragma once

#include "endpoint_bootstrap.h"

#include <cloud/blockstore/libs/cells/iface/host.h>
#include <cloud/blockstore/libs/cells/iface/host_endpoint.h>
#include <cloud/blockstore/libs/cells/iface/public.h>
#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

ICellHostPtr CreateHost(TCellHostConfig config, TBootstrap boorstrap);

}   // namespace NCloud::NBlockStore::NCells
