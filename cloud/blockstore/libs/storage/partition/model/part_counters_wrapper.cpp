#include "part_counters_wrapper.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

TThreadSafePartCounters::TThreadSafePartCounters(TPartitionDiskCountersPtr counters)
    : Counters(std::move(counters))
{}

TPartitionDiskCountersPtr TThreadSafePartCounters::Swap(
    TPartitionDiskCountersPtr counters)
{
    TGuard guard(Lock);
    auto retCounters = std::move(Counters);
    Counters = std::move(counters);
    return retCounters;
}

NProto::TPartitionStats TThreadSafePartStats::Swap(
    NProto::TPartitionStats stats)
{
    TGuard guard(Lock);
    auto retStats = std::move(Stats);
    Stats = std::move(stats);
    return retStats;
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
