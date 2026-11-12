#include "tablet_proxy_config.h"

#include "config.h"

#include <cloud/blockstore/libs/kikimr/components.h>

#include <util/datetime/base.h>
#include <util/generic/ymath.h>

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
        // Zero closes an idle pipe at once; the tablet proxy reads zero as
        // "never close".
        .PipeInactivityTimeout =
            Max(config.GetVolumeProxyPipeInactivityTimeout(),
                TDuration::MicroSeconds(1))};
}

}   // namespace NCloud::NBlockStore::NStorage
