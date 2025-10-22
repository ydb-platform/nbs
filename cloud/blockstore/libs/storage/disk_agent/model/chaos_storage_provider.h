#pragma once

#include <cloud/blockstore/libs/nvme/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

// Attention! this provider adds errors into the responses and spoils the
// written data in accordance with the ChaosConfig section of the DiskAgent
// config. It is highly discouraged to add this section for production. Use with
// caution.

IStorageProviderPtr CreateChaosStorageProvider(
    IStorageProviderPtr storageProvider,
    const NProto::TChaosConfig& chaosConfig);

}   // namespace NCloud::NBlockStore::NServer
