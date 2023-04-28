#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/rdma/public.h>
#include <cloud/blockstore/libs/service/public.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

IStorageProviderPtr CreateRdmaStorageProvider(
    IServerStatsPtr serverStats,
    NRdma::IClientPtr client);

}   // namespace NCloud::NBlockStore::NStorage
