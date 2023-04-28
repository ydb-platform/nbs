#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/spdk/public.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

IStorageProviderPtr CreateSpdkStorageProvider(
    NSpdk::ISpdkEnvPtr spdk,
    ICachingAllocatorPtr allocator,
    IServerStatsPtr serverStats);

IStoragePtr CreateSpdkStorage(
    NSpdk::ISpdkDevicePtr device,
    ICachingAllocatorPtr allocator,
    ui32 blockSize);

IStoragePtr CreateReadOnlySpdkStorage(
    NSpdk::ISpdkDevicePtr device,
    ICachingAllocatorPtr allocator,
    ui32 blockSize);

}   // namespace NCloud::NBlockStore::NServer
