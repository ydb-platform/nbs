#pragma once

#include "public.h"

#include <cloud/blockstore/libs/nvme/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/service_local/public.h>

#include <cloud/storage/core/libs/common/public.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TCreateDiskAgentBackendComponentsResult
{
    NServer::IFileIOServiceProviderPtr FileIOServiceProvider;
    NNvme::INvmeManagerPtr NvmeManager;
    IStorageProviderPtr StorageProvider;
};

TCreateDiskAgentBackendComponentsResult CreateDiskAgentBackendComponents(
    const TDiskAgentConfig& config);

}   // namespace NCloud::NBlockStore::NStorage
