#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/blockstore/libs/nvme/public.h>
#include <cloud/blockstore/libs/rdma/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/spdk/public.h>
#include <cloud/blockstore/libs/storage/core/public.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateDiskAgent(
    TStorageConfigPtr config,
    TDiskAgentConfigPtr agentConfig,
    NSpdk::ISpdkEnvPtr spdk,
    ICachingAllocatorPtr allocator,
    IStorageProviderPtr storageProvider,
    IProfileLogPtr profileLog,
    IBlockDigestGeneratorPtr blockDigestGenerator,
    ILoggingServicePtr logging,
    NRdma::IServerPtr rdmaServer,
    NNvme::INvmeManagerPtr nvmeManager);

}   // namespace NCloud::NBlockStore::NStorage
