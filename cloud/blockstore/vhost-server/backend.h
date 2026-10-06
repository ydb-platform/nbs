#pragma once

#include "options.h"
#include "stats.h"

#include <cloud/contrib/vhost/include/vhost/blockdev.h>
#include <cloud/storage/core/libs/common/startable.h>

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

struct IBackend: public IStartable
{
    virtual vhd_bdev_info Init(const TOptions& options) = 0;
    virtual void ProcessQueue(
        ui32 queueIndex,
        vhd_request_queue* queue,
        TSimpleStats& queueStats) = 0;
    virtual std::optional<TSimpleStats> GetCompletionStats(
        TDuration timeout) = 0;
    // A fresh absolute snapshot; it must not wait for an I/O completion.
    virtual std::optional<TIoDepthSnapshot> GetIoDepthStats() = 0;
};

}   // namespace NCloud::NBlockStore::NVHostServer
