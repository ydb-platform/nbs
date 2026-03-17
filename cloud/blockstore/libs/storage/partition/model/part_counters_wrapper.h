
#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/core/disk_counters.h>
#include <cloud/blockstore/libs/storage/protos/part.pb.h>

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

using TThreadSafePartCountersPtr = std::shared_ptr<TThreadSafePartCounters>;

class TThreadSafePartStats
{
private:
    TAdaptiveLock Lock;
    NProto::TPartitionStats Stats;

public:
    TThreadSafePartStats() = default;

    template <typename TFunc>
    auto Access(TFunc&& func) const
    {
        TGuard guard(Lock);
        return func(Stats);
    }

    template <typename TFunc>
    auto Access(TFunc&& func)
    {
        TGuard guard(Lock);
        return func(Stats);
    }

    NProto::TPartitionStats Swap(NProto::TPartitionStats stats);
};

using TThreadSafePartStatsPtr = std::shared_ptr<TThreadSafePartStats>;

}   // namespace NCloud::NBlockStore::NStorage::NPartition
