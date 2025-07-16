#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/blockstore/libs/service_local/public.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

IFileIOServiceFactoryPtr CreateAIOServiceFactory(
    const TDiskAgentConfig& config);

IFileIOServiceFactoryPtr CreateIoUringServiceFactory(
    const TDiskAgentConfig& config);

NServer::IFileIOServiceProviderPtr CreateFileIOServiceProvider(
    const TDiskAgentConfig& config,
    IFileIOServiceFactoryPtr factory);

}   // namespace NCloud::NBlockStore::NStorage
