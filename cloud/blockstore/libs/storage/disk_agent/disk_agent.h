#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/blockstore/libs/nvme/public.h>
#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/spdk/iface/public.h>
#include <cloud/blockstore/libs/storage/core/public.h>

#include <cloud/storage/core/libs/common/public.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateDiskAgent(
    TStorageConfigPtr config,
    TDiskAgentConfigPtr agentConfig,
    NRdma::TRdmaConfigPtr rdmaConfig,
    NSpdk::ISpdkEnvPtr spdk,
    ICachingAllocatorPtr allocator,
    IStorageProviderPtr storageProvider,
    IProfileLogPtr profileLog,
    IBlockDigestGeneratorPtr blockDigestGenerator,
    ITaskQueuePtr backgroundTaskQueue,
    ILoggingServicePtr logging,
    NRdma::IServerPtr rdmaServer,
    NNvme::INvmeManagerPtr nvmeManager);

}   // namespace NCloud::NBlockStore::NStorage
