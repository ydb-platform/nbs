#include "tablet_proxy_config.h"

#include "config.h"

#include <cloud/blockstore/libs/kikimr/components.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NCloud::NStorage::TTabletProxyConfig GetTabletProxyConfig(
    const TStorageConfig& config)
{
    return {
        .LogComponent = TBlockStoreComponents::TABLET_PROXY_SERVICE,
        .PipeClientRetryCount = config.GetPipeClientRetryCount(),
        .PipeClientMinRetryTime = config.GetPipeClientMinRetryTime(),
        .PipeClientMaxRetryTime = config.GetPipeClientMaxRetryTime(),
        .PipeInactivityTimeout = config.GetVolumeProxyPipeInactivityTimeout()};
}

}   // namespace NCloud::NBlockStore::NStorage
