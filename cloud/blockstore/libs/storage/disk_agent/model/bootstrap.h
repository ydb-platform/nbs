#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/nvme/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/service_local/public.h>

#include <cloud/storage/core/libs/common/public.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TCreateDiskAgentBackendComponentsResult
{
    NNvme::INvmeManagerPtr NvmeManager;
    NServer::IFileIOServiceProviderPtr FileIOServiceProvider;
    IStorageProviderPtr StorageProvider;
};

TCreateDiskAgentBackendComponentsResult CreateDiskAgentBackendComponents(
    ILoggingServicePtr logging,
    const TDiskAgentConfig& config);

}   // namespace NCloud::NBlockStore::NStorage
