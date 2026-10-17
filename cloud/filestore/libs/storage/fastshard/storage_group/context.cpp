#include "context.h"

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

TFastShardContext::TFastShardContext(
        ui64 requestId,
        ITimer& timer,
        const std::atomic<bool>& stopped,
        TDuration timeout)
    : RequestId(requestId)
    , Timer(timer)
    , Stopped(stopped)
    , Started(Timer.Now())
    , Deadline(Started + timeout)
{}

bool TFastShardContext::IsStopped() const
{
    return Stopped.load(std::memory_order_acquire);
}

TDuration TFastShardContext::GetRequestTime() const
{
    return Timer.Now() - Started;
}

TDuration TFastShardContext::GetBackoffTime() const
{
    return TDuration::MicroSeconds(BackoffMicroSeconds.load());
}

void TFastShardContext::AddBackoffTime(TDuration duration)
{
    BackoffMicroSeconds += duration.MicroSeconds();
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
