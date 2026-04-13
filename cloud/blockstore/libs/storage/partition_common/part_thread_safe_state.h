#pragma once

#include "cloud/blockstore/libs/storage/core/bs_group_operation_tracker.h"
#include <cloud/blockstore/libs/storage/partition/model/checkpoint.h>
#include <cloud/blockstore/libs/storage/partition/model/commit_queue.h>
#include <cloud/blockstore/libs/storage/partition/model/group_downtimes.h>
#include <cloud/blockstore/libs/storage/partition/model/part_counters_wrapper.h>
#include <cloud/blockstore/libs/storage/partition/model/resource_metrics_updates_queue.h>

#include <util/system/spinlock.h>

#include <atomic>
#include <memory>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

template <typename T, typename TLock>
struct TObjectGuard
{
    TGuard<TLock> Guard;
    T& Value;

    TObjectGuard(TLock& lock, T& value)
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
struct TConstObjectGuard
{
    TGuard<TLock> Guard;
    const T& Value;

    TConstObjectGuard(TLock& lock, const T& value)
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

////////////////////////////////////////////////////////////////////////////////

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
    std::atomic<ui64> BSGroupOperationId = 0;

private:
    TAdaptiveLock StateLock;

    ui32 Generation = 0;
    ui32 LastCommitId = 0;

    NPartition::TBarriers TrimFreshLogBarriers;
    NPartition::TCommitQueue CommitQueue;

    NPartition::TCheckpointsInFlight CheckpointsInFlight;

    TAdaptiveLock BSGroupOperationTimeTrackerLock;
    TBSGroupOperationTimeTracker BSGroupOperationTimeTracker;

public:
    TPartitionThreadSafeState() = default;

    TPartitionThreadSafeState(ui32 generation, ui32 lastCommitId);

    void Init(ui32 generation, ui32 lastCommitId);

    ui64 GenerateCommitId();
    ui64 GetLastCommitId() const;

    ui64 StartFreshWrite(ui64 blockCount);
    void FinishFreshWrite(ui64 commitId, ui64 blockCount, bool isError);

    ui64 GetTrimFreshLogToCommitId() const;

#define DEFINE_THREAD_SAFE_STATE_ACCESSORS(field, lock)                        \
    auto Get##field()                                                          \
    {                                                                          \
        return TConstObjectGuard<decltype(field), TAdaptiveLock>(lock, field); \
    }                                                                          \
    auto Access##field()                                                       \
    {                                                                          \
        return TObjectGuard<decltype(field), TAdaptiveLock>(lock, field);      \
    }                                                                          \
    // DEFINE_THREAD_SAFE_STATE_ACCESSORS

    DEFINE_THREAD_SAFE_STATE_ACCESSORS(TrimFreshLogBarriers, StateLock)
    DEFINE_THREAD_SAFE_STATE_ACCESSORS(CommitQueue, StateLock)
    DEFINE_THREAD_SAFE_STATE_ACCESSORS(CheckpointsInFlight, StateLock)
    DEFINE_THREAD_SAFE_STATE_ACCESSORS(
        BSGroupOperationTimeTracker,
        BSGroupOperationTimeTrackerLock)

#undef DEFINE_THREAD_SAFE_STATE_ACCESSORS

private:
    ui64 GenerateCommitIdImpl();
    ui64 GetLastCommitIdImpl() const;
};

using TPartitionThreadSafeStatePtr = std::shared_ptr<TPartitionThreadSafeState>;

}   // namespace NCloud::NBlockStore::NStorage
