#pragma once

#include <cloud/storage/core/libs/common/timer.h>

#include <util/datetime/base.h>
#include <util/system/types.h>

#include <atomic>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

class TFastShardContext
{
public:
    const ui64 RequestId;
    ITimer& Timer;
    const std::atomic<bool>& Stopped;
    const TInstant Started;
    const TInstant Deadline;

    TFastShardContext(
        ui64 requestId,
        ITimer& timer,
        const std::atomic<bool>& stopped,
        TDuration timeout);

    TFastShardContext(const TFastShardContext&) = delete;
    TFastShardContext& operator=(const TFastShardContext&) = delete;

    bool IsStopped() const;

    TDuration GetRequestTime() const;

    TDuration GetBackoffTime() const;
    void AddBackoffTime(TDuration duration);

private:
    std::atomic<ui64> BackoffMicroSeconds = 0;
};

}   // namespace NCloud::NFileStore::NStorage::NFastShard
