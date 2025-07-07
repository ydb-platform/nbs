#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/public.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

std::function<IFileIOServicePtr()> CreateAIOServiceFactory(
    const TDiskAgentConfig& config);

std::function<IFileIOServicePtr()> CreateIoUringServiceFactory(
    const TDiskAgentConfig& config);

}   // namespace NCloud::NBlockStore::NStorage
