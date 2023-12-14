#pragma once

#include "options.h"
#include "stats.h"

#include <cloud/contrib/vhost/include/vhost/blockdev.h>
#include <cloud/storage/core/libs/common/startable.h>

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

struct IBackend: public IStartable
{
    virtual ~IBackend() = default;

    virtual void Init(const TOptions& options, vhd_bdev_info& devInfo) = 0;
    virtual void ProcessQueue(
        vhd_request_queue* queue,
        TSimpleStats& queueStats) = 0;
    virtual std::optional<TSimpleStats> GetCompletionStats(
        TDuration timeout) = 0;
};

}   // namespace NCloud::NBlockStore::NVHostServer
