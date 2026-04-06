#pragma once

#include <cloud/blockstore/libs/storage/partition/model/checkpoint.h>
#include <cloud/blockstore/libs/storage/partition/model/commit_queue.h>
#include <cloud/blockstore/libs/storage/partition/model/group_downtimes.h>
#include <cloud/blockstore/libs/storage/partition/model/part_counters_wrapper.h>
#include <cloud/blockstore/libs/storage/partition/model/resource_metrics_updates_queue.h>

#include <util/system/spinlock.h>

#include <atomic>
#include <memory>

namespace NCloud::NBlockStore::NStorage {

template <typename T, typename TLock>
struct TGuarg
{
    TGuard<TLock> Guard;
    T& Value;

    TGuarg(TLock& lock, T& value)
        : Guard(lock)
        , Value(value)
    {}

    T& operator*()
    {
        return Value;
    }

    T* operator->()
    {
        return &Value;
    }
};

template <typename T, typename TLock>
struct TConstGuarg
{
    TGuard<TLock> Guard;
    const T& Value;

    TConstGuarg(TLock& lock, const T& value)
        : Guard(lock)
        , Value(value)
    {}

    const T& operator*()
    {
        return Value;
    }

    const T* operator->()
    {
        return &Value;
    }
};

class TPartitionThreadSafeState
    : public std::enable_shared_from_this<TPartitionThreadSafeState>
{
    using TTxPtr = std::unique_ptr<ITransactionBase>;

public:
    NPartition::TResourceMetricsQueue ResourceMetricsQueue;
    NPartition::TThreadSafePartCounters PartCounters;
    NPartition::TThreadSafePartStats PartStats;
    NPartition::TGroupDowntimes GroupDowntimes;

    std::atomic<ui64> UnflushedFreshBlobByteCount = 0;

private:
    TAdaptiveLock StateLock;

    ui32 Generation = 0;
    ui32 LastCommitId = 0;

    NPartition::TBarriers TrimFreshLogBarriers;
    NPartition::TCommitQueue CommitQueue;

    NPartition::TCheckpointsInFlight CheckpointsInFlight;

public:
    TPartitionThreadSafeState() = default;

    TPartitionThreadSafeState(ui32 generation, ui32 lastCommitId);

    void Init(ui32 generation, ui32 lastCommitId);

    NPartition::TResourceMetricsQueuePtr GetResourceMetricsQueue()
    {
        return {shared_from_this(), &ResourceMetricsQueue};
    }

    NPartition::TThreadSafePartCountersPtr GetPartCounters()
    {
        return {shared_from_this(), &PartCounters};
    }

    NPartition::TGroupDowntimesPtr GetGroupDowntimes()
    {
        return {shared_from_this(), &GroupDowntimes};
    }

    std::shared_ptr<std::atomic<ui64>> GetUnflushedFreshBlobByteCount()
    {
        return {shared_from_this(), &UnflushedFreshBlobByteCount};
    }

    ui64 GenerateCommitId();
    ui64 GetLastCommitId() const;

    auto GetTrimFreshLogBarriers()
    {
        return TConstGuarg<NPartition::TBarriers, TAdaptiveLock>(
            StateLock,
            TrimFreshLogBarriers);
    }

    auto AccessTrimFreshLogBarriers()
    {
        return TGuarg<NPartition::TBarriers, TAdaptiveLock>(
            StateLock,
            TrimFreshLogBarriers);
    }

    ui64 GetTrimFreshLogToCommitId() const;

    auto GetCommitQueue()
    {
        return TConstGuarg<NPartition::TCommitQueue, TAdaptiveLock>(
            StateLock,
            CommitQueue);
    }

    auto AccessCommitQueue()
    {
        return TGuarg<NPartition::TCommitQueue, TAdaptiveLock>(
            StateLock,
            CommitQueue);
    }

    auto GetCheckpointsInFlight()
    {
        return TConstGuarg<NPartition::TCheckpointsInFlight, TAdaptiveLock>(
            StateLock,
            CheckpointsInFlight);
    }

    auto AccessCheckpointsInFlight()
    {
        return TGuarg<NPartition::TCheckpointsInFlight, TAdaptiveLock>(
            StateLock,
            CheckpointsInFlight);
    }

private:
    ui64 GenerateCommitIdImpl();
    ui64 GetLastCommitIdImpl() const;
};

using TPartitionThreadSafeStatePtr = std::shared_ptr<TPartitionThreadSafeState>;

}   // namespace NCloud::NBlockStore::NStorage
