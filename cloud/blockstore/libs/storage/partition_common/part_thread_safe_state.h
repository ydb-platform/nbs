#pragma once

#include <cloud/blockstore/libs/storage/partition_common/model/checkpoint.h>
#include <cloud/blockstore/libs/storage/partition_common/model/commit_queue.h>
#include <cloud/blockstore/libs/storage/partition_common/model/group_downtimes.h>
#include <cloud/blockstore/libs/storage/partition_common/model/part_counters_wrapper.h>
#include <cloud/blockstore/libs/storage/partition_common/model/resource_metrics_updates_queue.h>
#include <cloud/blockstore/libs/storage/model/requests_in_progress.h>
#include <cloud/blockstore/libs/storage/partition_common/drain_actor_companion.h>

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
    , private IRequestsInProgress
{
    using TTxPtr = std::unique_ptr<ITransactionBase>;

public:
    TResourceMetricsQueue ResourceMetricsQueue;
    TThreadSafePartCounters PartCounters;
    TThreadSafePartStats PartStats;
    TGroupDowntimes GroupDowntimes;

    std::atomic<ui64> UnflushedFreshBlobByteCount = 0;

    std::atomic<ui64> WriteAndZeroRequestsInProgress = 0;

private:
    const TString DiskId;
    const ui64 TabletId = 0;

    TAdaptiveLock StateLock;

    NActors::TActorId PartitionActorId;

    ui32 Generation = 0;
    ui32 LastCommitId = 0;

    TBarriers TrimFreshLogBarriers;
    TCommitQueue CommitQueue;
    TCommitQueueWithCallback FreshWritesCommitQueue;

    TCheckpointsInFlight CheckpointsInFlight;

    std::atomic<ui64> FreshBlocksInFlight = 0;

    TAdaptiveLock DrainLock;
    TDrainActorCompanion DrainActorCompanion{
        *this,
        DiskId};

public:
    TPartitionThreadSafeState() = default;

    TPartitionThreadSafeState(TString diskId, ui64 tabletId)
        : DiskId(std::move(diskId))
        , TabletId(tabletId)
    {}

    ui64 GetTabletId() const
    {
        return TabletId;
    }

    void Init(
        NActors::TActorId partitionActorId,
        ui32 generation,
        ui32 lastCommitId);

    TResourceMetricsQueuePtr GetResourceMetricsQueue()
    {
        return {shared_from_this(), &ResourceMetricsQueue};
    }

    TThreadSafePartCountersPtr GetPartCounters()
    {
        return {shared_from_this(), &PartCounters};
    }

    TGroupDowntimesPtr GetGroupDowntimes()
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
        return TConstObjectGuard<TBarriers, TAdaptiveLock>(
            StateLock,
            TrimFreshLogBarriers);
    }

    auto AccessTrimFreshLogBarriers()
    {
        return TObjectGuard<TBarriers, TAdaptiveLock>(
            StateLock,
            TrimFreshLogBarriers);
    }

    ui64 GetTrimFreshLogToCommitId() const;

    auto GetCommitQueue()
    {
        return TConstObjectGuard<TCommitQueue, TAdaptiveLock>(
            StateLock,
            CommitQueue);
    }

    auto AccessCommitQueue()
    {
        return TObjectGuard<TCommitQueue, TAdaptiveLock>(
            StateLock,
            CommitQueue);
    }

    auto GetCheckpointsInFlight()
    {
        return TConstObjectGuard<TCheckpointsInFlight, TAdaptiveLock>(
            StateLock,
            CheckpointsInFlight);
    }

    auto AccessCheckpointsInFlight()
    {
        return TObjectGuard<TCheckpointsInFlight, TAdaptiveLock>(
            StateLock,
            CheckpointsInFlight);
    }

    void WaitCommitForCompaction(
        const NActors::TActorContext& ctx,
        std::unique_ptr<ITransactionBase> tx,
        ui64 commitId);

    void WaitFreshWritesToComplete(
        TCommitQueueCallback callback,
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

    auto AccessDrainActorCompanion()
    {
        return TObjectGuard<TDrainActorCompanion, TAdaptiveLock>(
            DrainLock,
            DrainActorCompanion);
    }

    auto GetDrainActorCompanion()
    {
        return TConstObjectGuard<TDrainActorCompanion, TAdaptiveLock>(
            DrainLock,
            DrainActorCompanion);
    }

    // IRequestsInProgress
    bool WriteRequestInProgress() const override;
    bool OverlapsWithWrites(TBlockRange64 range) const override;
    void WaitForInFlightWrites() override;
    bool IsWaitingForInFlightWrites() const override;

private:
    ui64 GenerateCommitIdImpl();
    ui64 GetLastCommitIdImpl() const;

    void ExecuteTxs(
        const NActors::TActorContext& ctx,
        TVector<std::unique_ptr<ITransactionBase>> txs);

    void ProcessCommitQueueImpl(
        TVector<std::unique_ptr<ITransactionBase>>& txs,
        TVector<TCommitQueueCallback>& callbacks);

    void CollectCheckpointQueueTransactions(
        TVector<std::unique_ptr<ITransactionBase>>& txs);

    bool CollectNextCheckpointTx(
        const TString& checkpointId,
        TVector<std::unique_ptr<ITransactionBase>>& txs);
};

using TPartitionThreadSafeStatePtr = std::shared_ptr<TPartitionThreadSafeState>;

}   // namespace NCloud::NBlockStore::NStorage
