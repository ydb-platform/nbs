#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/public.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

enum class ERdmaTaskQueueOpt : bool
{
    DontUse = false,
    Use = true
};

IStorageProviderPtr CreateRdmaStorageProvider(
    IServerStatsPtr serverStats,
    NRdma::IClientPtr client,
    ERdmaTaskQueueOpt taskQueueOpt);

}   // namespace NCloud::NBlockStore::NStorage
