#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/blockstore/libs/service_local/public.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

std::function<IFileIOServicePtr()> CreateAIOServiceFactory(
    const TDiskAgentConfig& config);

std::function<IFileIOServicePtr()> CreateIoUringServiceFactory(
    const TDiskAgentConfig& config);

NServer::IFileIOServiceProviderPtr CreateFileIOServiceProvider(
    const TDiskAgentConfig& config,
    std::function<IFileIOServicePtr()> factory);

}   // namespace NCloud::NBlockStore::NStorage
