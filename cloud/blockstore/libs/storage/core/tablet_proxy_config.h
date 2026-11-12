#pragma once

#include "public.h"

#include <cloud/storage/core/libs/tablet_proxy/tablet_proxy.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NCloud::NStorage::TTabletProxyConfig GetTabletProxyConfig(
    const TStorageConfig& config);

}   // namespace NCloud::NBlockStore::NStorage
