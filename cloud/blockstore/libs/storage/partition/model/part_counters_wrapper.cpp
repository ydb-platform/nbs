#include "part_counters_wrapper.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

TThreadSafePartCounters::TThreadSafePartCounters(TPartitionDiskCountersPtr counters)
    : Counters(std::move(counters))
{}

TPartitionDiskCountersPtr TThreadSafePartCounters::Swap(
    TPartitionDiskCountersPtr counters)
{
    TGuard<TMutex> guard(Mutex);
    auto retCounters = std::move(Counters);
    Counters = std::move(counters);
    return retCounters;
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
