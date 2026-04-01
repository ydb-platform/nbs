#pragma once

#include <cloud/blockstore/libs/storage/partition/model/commit_queue.h>
#include <cloud/blockstore/libs/storage/partition/model/group_downtimes.h>
#include <cloud/blockstore/libs/storage/partition/model/part_counters_wrapper.h>
#include <cloud/blockstore/libs/storage/partition/model/resource_metrics_updates_queue.h>

#include <util/system/spinlock.h>

#include <atomic>
#include <memory>

namespace NActors {

struct TActorContext;

}   // namespace NActors

namespace NCloud::NBlockStore::NStorage {

class TPartitionThreadSafeState
    : public std::enable_shared_from_this<TPartitionThreadSafeState>
{
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

    ui64 StartFreshWrite(ui64 blockCount);
    void FinishFreshWrite(ui64 commitId, ui64 blockCount, bool isError);

    void ProcessCommitQueue(const NActors::TActorContext& ctx);
    void WaitForCommitQueueBarrier(
        const NActors::TActorContext& ctx,
        ui64 commitId,
        ui64 cookie);

    void AcquireTrimFreshLogBarrier(ui64 commitId, ui64 blockCount);
    void ReleaseTrimFreshLogBarrier(ui64 commitId, ui64 blockCount);

    ui64 GetTrimFreshLogToCommitId() const;

    void AcquireCommitQueueBarrier(ui64 commitId);
    void ReleaseCommitQueueBarrier(ui64 commitId);
    ui64 GetCommitQueueMinCommitId() const;

private:
    ui64 GenerateCommitIdImpl();
    ui64 GetLastCommitIdImpl() const;
};

using TPartitionThreadSafeStatePtr = std::shared_ptr<TPartitionThreadSafeState>;

}   // namespace NCloud::NBlockStore::NStorage
