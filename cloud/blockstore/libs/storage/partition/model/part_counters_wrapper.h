
#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/core/disk_counters.h>

#include <util/system/mutex.h>

#include <memory>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

class TThreadSafePartCounters
{
private:
    TAdaptiveLock Lock;
    TPartitionDiskCountersPtr Counters;

public:
    TThreadSafePartCounters() = default;

    explicit TThreadSafePartCounters(TPartitionDiskCountersPtr counters);

    template <typename TFunc>
    auto Access(TFunc&& func) const
    {
        TGuard guard(Lock);
        return func(Counters);
    }

    template <typename TFunc>
    auto Access(TFunc&& func)
    {
        TGuard guard(Lock);
        return func(Counters);
    }

    TPartitionDiskCountersPtr Swap(TPartitionDiskCountersPtr counters);
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
