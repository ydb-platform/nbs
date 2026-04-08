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

private:
    TAdaptiveLock StateLock;

    NActors::TActorId PartitionActorId;

    ui32 Generation = 0;
    ui32 LastCommitId = 0;

    NPartition::TBarriers TrimFreshLogBarriers;
    NPartition::TCommitQueue CommitQueue;

    NPartition::TCheckpointsInFlight CheckpointsInFlight;

    std::atomic<ui64> FreshBlocksInFlight = 0;

public:
    TPartitionThreadSafeState() = default;

    TPartitionThreadSafeState(
        NActors::TActorId partitionActorId,
        ui32 generation,
        ui32 lastCommitId);

    void Init(
        NActors::TActorId partitionActorId,
        ui32 generation,
        ui32 lastCommitId);

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

    ui64 StartFreshWrite(ui64 blockCount);
    void FinishFreshWrite(
        const NActors::TActorContext& ctx,
        ui64 commitId,
        ui64 blockCount,
        bool isError);

    auto GetTrimFreshLogBarriers()
    {
        return TConstObjectGuard<NPartition::TBarriers, TAdaptiveLock>(
            StateLock,
            TrimFreshLogBarriers);
    }

    auto AccessTrimFreshLogBarriers()
    {
        return TObjectGuard<NPartition::TBarriers, TAdaptiveLock>(
            StateLock,
            TrimFreshLogBarriers);
    }

    ui64 GetTrimFreshLogToCommitId() const;

    auto GetCommitQueue()
    {
        return TConstObjectGuard<NPartition::TCommitQueue, TAdaptiveLock>(
            StateLock,
            CommitQueue);
    }

    auto AccessCommitQueue()
    {
        return TObjectGuard<NPartition::TCommitQueue, TAdaptiveLock>(
            StateLock,
            CommitQueue);
    }

    auto GetCheckpointsInFlight()
    {
        return TConstObjectGuard<
            NPartition::TCheckpointsInFlight,
            TAdaptiveLock>(StateLock, CheckpointsInFlight);
    }

    auto AccessCheckpointsInFlight()
    {
        return TObjectGuard<NPartition::TCheckpointsInFlight, TAdaptiveLock>(
            StateLock,
            CheckpointsInFlight);
    }

    void WaitCommitForCompaction(
        const NActors::TActorContext& ctx,
        std::unique_ptr<ITransactionBase> tx,
        ui64 commitId);

    void WaitCommitForCheckpoint(
        const NActors::TActorContext& ctx,
        std::unique_ptr<ITransactionBase> tx,
        const TString& checkpointId,
        ui64 commitId);

    void ProcessCommitQueue(const NActors::TActorContext& ctx);

    void ProcessCheckpointQueue(const NActors::TActorContext& ctx);

    bool ProcessNextCheckpointRequest(
        const NActors::TActorContext& ctx,
        const TString& checkpointId);

    void IncrementFreshBlocksInFlight(size_t value);
    void DecrementFreshBlocksInFlight(size_t value);

    size_t GetFreshBlocksInFlight() const;

private:
    ui64 GenerateCommitIdImpl();
    ui64 GetLastCommitIdImpl() const;

    void ExecuteTxs(
        const NActors::TActorContext& ctx,
        TVector<std::unique_ptr<ITransactionBase>> txs);

    void ProcessCommitQueueImpl(
        TVector<std::unique_ptr<ITransactionBase>>& txs);

    void ProcessCheckpointQueueImpl(
        TVector<std::unique_ptr<ITransactionBase>>& txs);

    bool ProcessNextCheckpointRequestImpl(
        const TString& checkpointId,
        TVector<std::unique_ptr<ITransactionBase>>& txs);
};

using TPartitionThreadSafeStatePtr = std::shared_ptr<TPartitionThreadSafeState>;

}   // namespace NCloud::NBlockStore::NStorage
