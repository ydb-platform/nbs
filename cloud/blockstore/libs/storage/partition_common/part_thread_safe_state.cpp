#include "part_thread_safe_state.h"

#include "events_private.h"

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/tablet/model/commit.h>

#include <contrib/ydb/library/actors/core/actor.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NPartition;

////////////////////////////////////////////////////////////////////////////////

TPartitionThreadSafeState::TPartitionThreadSafeState(
    ui32 generation,
    ui32 lastCommitId)
{
    Init(generation, lastCommitId);
}

void TPartitionThreadSafeState::Init(ui32 generation, ui32 lastCommitId)
{
    Generation = generation;
    LastCommitId = lastCommitId;
}

ui64 TPartitionThreadSafeState::GenerateCommitId()
{
    TGuard guard(StateLock);
    return GenerateCommitIdImpl();
}

ui64 TPartitionThreadSafeState::GetLastCommitId() const
{
    TGuard guard(StateLock);
    return GetLastCommitIdImpl();
}

ui64 TPartitionThreadSafeState::StartFreshWrite(ui64 blockCount)
{
    TGuard guard(StateLock);

    auto commitId = GenerateCommitIdImpl();

    TrimFreshLogBarriers.AcquireBarrierN(commitId, blockCount);
    CommitQueue.AcquireBarrier(commitId);
    return commitId;
}

void TPartitionThreadSafeState::FinishFreshWrite(
    ui64 commitId,
    ui64 blockCount,
    bool isError)
{
    TGuard guard(StateLock);
    CommitQueue.ReleaseBarrier(commitId);

    if (isError) {
        TrimFreshLogBarriers.ReleaseBarrierN(commitId, blockCount);
    }
}

void TPartitionThreadSafeState::ProcessCommitQueue(const TActorContext& ctx)
{
    TGuard guard(StateLock);

    ui64 minCommitId = CommitQueue.GetMinCommitId();

    while (!CommitQueue.Empty()) {
        ui64 commitId = CommitQueue.Peek();

        if (minCommitId >= commitId) {
            // start execution
            auto callback = CommitQueue.Dequeue();
            callback(ctx);
        } else {
            // delay execution until all previous commits completed
            break;
        }
    }
}

void TPartitionThreadSafeState::WaitForCommitQueueBarrier(
    const TActorContext& ctx,
    ui64 commitId,
    ui64 cookie)
{
    TGuard guard(StateLock);

    auto waiter = ctx.SelfID;
    auto callback = [waiter, cookie](const TActorContext& ctx)
    {
        NCloud::Send(
            ctx,
            waiter,
            std::make_unique<TEvPartitionCommonPrivate::TEvCommitsCompleted>(),
            cookie);
    };

    ui64 minCommitId = CommitQueue.GetMinCommitId();

    if (minCommitId >= commitId) {
        callback(ctx);
    } else {
        CommitQueue.Enqueue(std::move(callback), commitId);
    }
}

void TPartitionThreadSafeState::AcquireTrimFreshLogBarrier(
    ui64 commitId,
    ui64 blockCount)
{
    TGuard guard(StateLock);
    TrimFreshLogBarriers.AcquireBarrierN(commitId, blockCount);
}

void TPartitionThreadSafeState::ReleaseTrimFreshLogBarrier(
    ui64 commitId,
    ui64 blockCount)
{
    TGuard guard(StateLock);
    TrimFreshLogBarriers.ReleaseBarrierN(commitId, blockCount);
}

ui64 TPartitionThreadSafeState::GetTrimFreshLogToCommitId() const
{
    TGuard guard(StateLock);

    return Min(
        GetLastCommitIdImpl(),
        // if there are fresh writes in-flight, trim only up to
        // the smallest in-flight commit id minus one
        TrimFreshLogBarriers.GetMinCommitId() - 1);
}

void TPartitionThreadSafeState::AcquireCommitQueueBarrier(ui64 commitId)
{
    TGuard guard(StateLock);
    CommitQueue.AcquireBarrier(commitId);
}

void TPartitionThreadSafeState::ReleaseCommitQueueBarrier(ui64 commitId)
{
    TGuard guard(StateLock);
    CommitQueue.ReleaseBarrier(commitId);
}

ui64 TPartitionThreadSafeState::GetCommitQueueMinCommitId() const
{
    TGuard guard(StateLock);
    return CommitQueue.GetMinCommitId();
}

ui64 TPartitionThreadSafeState::GenerateCommitIdImpl()
{
    ++LastCommitId;
    return MakeCommitId(Generation, LastCommitId);
}

ui64 TPartitionThreadSafeState::GetLastCommitIdImpl() const
{
    return MakeCommitId(Generation, LastCommitId);
}

}   // namespace NCloud::NBlockStore::NStorage
