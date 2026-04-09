#include "part_thread_safe_state.h"

#include "events_private.h"

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/tablet/model/commit.h>

#include <contrib/ydb/library/actors/core/actor.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NPartition;

////////////////////////////////////////////////////////////////////////////////

TPartitionThreadSafeState::TPartitionThreadSafeState(
        ui64 tabletId,
        NActors::TActorId partitionActorId,
        ui32 generation,
        ui32 lastCommitId)
    : TabletId(tabletId)
    , PartitionActorId(partitionActorId)
{
    Init(partitionActorId, generation, lastCommitId);
}

void TPartitionThreadSafeState::Init(
    NActors::TActorId partitionActorId,
    ui32 generation,
    ui32 lastCommitId)
{
    PartitionActorId = partitionActorId;
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
    const NActors::TActorContext& ctx,
    ui64 commitId,
    ui64 blockCount,
    bool isError)
{
    TVector<std::unique_ptr<ITransactionBase>> txs;

    with_lock(StateLock) {
        CommitQueue.ReleaseBarrier(commitId);
        if (isError) {
            TrimFreshLogBarriers.ReleaseBarrierN(commitId, blockCount);
        }
        ProcessCommitQueueImpl(txs);
    }

    ExecuteTxs(ctx, std::move(txs));
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

void TPartitionThreadSafeState::WaitCommitForCompaction(
    const NActors::TActorContext& ctx,
    std::unique_ptr<ITransactionBase> tx,
    ui64 commitId)
{
    with_lock (StateLock) {
        ui64 minCommitId = CommitQueue.GetMinCommitId();
        Y_ABORT_UNLESS(minCommitId <= commitId);

        if (minCommitId != commitId) {
            // delay execution until all previous commits completed
            CommitQueue.Enqueue(std::move(tx), commitId);
            return;
        }
    }

    TVector<std::unique_ptr<ITransactionBase>> txs;
    txs.push_back(std::move(tx));

    ExecuteTxs(ctx, std::move(txs));
}

void TPartitionThreadSafeState::WaitCommitForCheckpoint(
    const NActors::TActorContext& ctx,
    std::unique_ptr<ITransactionBase> tx,
    const TString& checkpointId,
    ui64 commitId)
{
    TVector<std::unique_ptr<ITransactionBase>> txs;
    with_lock (StateLock) {
        ui64 minCommitId = CommitQueue.GetMinCommitId();

        auto added =
            CheckpointsInFlight.AddTx(checkpointId, std::move(tx), commitId);
        STORAGE_VERIFY(added, TWellKnownEntityTypes::TABLET, TabletId);

        auto nextTx = CheckpointsInFlight.GetTx(checkpointId, minCommitId);
        if (nextTx) {
            txs.push_back(std::move(nextTx));
        }
    }

    ExecuteTxs(ctx, std::move(txs));
}

void TPartitionThreadSafeState::ProcessCommitQueue(
    const NActors::TActorContext& ctx)
{
    TVector<std::unique_ptr<ITransactionBase>> txs;

    with_lock (StateLock) {
        ProcessCommitQueueImpl(txs);
    }

    ExecuteTxs(ctx, std::move(txs));
}

void TPartitionThreadSafeState::ProcessCheckpointQueue(const NActors::TActorContext& ctx)
{
    TVector<std::unique_ptr<ITransactionBase>> txs;

    with_lock (StateLock) {
        CollectCheckpointQueueTransactions(txs);
    }

    ExecuteTxs(ctx, std::move(txs));
}

bool TPartitionThreadSafeState::ProcessNextCheckpointRequest(
    const NActors::TActorContext& ctx,
    const TString& checkpointId)
{
    TVector<std::unique_ptr<ITransactionBase>> txs;

    bool res = false;
    with_lock (StateLock) {
        res = CollectNextCheckpointTx(checkpointId, txs);
    }
    ExecuteTxs(ctx, std::move(txs));

    return res;
}

void TPartitionThreadSafeState::IncrementFreshBlocksInFlight(size_t value)
{
    FreshBlocksInFlight.fetch_add(value);
}

void TPartitionThreadSafeState::DecrementFreshBlocksInFlight(size_t value)
{
    FreshBlocksInFlight.fetch_sub(value);
}

size_t TPartitionThreadSafeState::GetFreshBlocksInFlight() const
{
    return FreshBlocksInFlight.load();
}

ui64 TPartitionThreadSafeState::GenerateCommitIdImpl()
{
    if (LastCommitId == Max<ui32>()) {
        return InvalidCommitId;
    }

    ++LastCommitId;
    return MakeCommitId(Generation, LastCommitId);
}

ui64 TPartitionThreadSafeState::GetLastCommitIdImpl() const
{
    return MakeCommitId(Generation, LastCommitId);
}

void TPartitionThreadSafeState::ExecuteTxs(
    const NActors::TActorContext& ctx,
    TVector<std::unique_ptr<ITransactionBase>> txs)
{
    if (txs.empty()) {
        return;
    }

    Y_ABORT_UNLESS(PartitionActorId);

    auto executeTxRequest =
        std::make_unique<TEvPartitionCommonPrivate::TEvExecuteTransactions>();
    executeTxRequest->Transactions = std::move(txs);
    NCloud::Send(ctx, PartitionActorId, std::move(executeTxRequest));
}

void TPartitionThreadSafeState::ProcessCommitQueueImpl(
    TVector<std::unique_ptr<ITransactionBase>>& txs)
{
    ui64 minCommitId = CommitQueue.GetMinCommitId();

    while (!CommitQueue.Empty()) {
        ui64 commitId = CommitQueue.Peek();
        Y_ABORT_UNLESS(minCommitId <= commitId);

        if (minCommitId == commitId) {
            // start execution
            txs.push_back(CommitQueue.Dequeue());
        } else {
            // delay execution until all previous commits completed
            break;
        }
    }

    // Since create checkpoint operation waits for the last commit to
    // complete here we force checkpoints queue to try to proceed to the
    // next create checkpoint request
    CollectCheckpointQueueTransactions(txs);
}

void TPartitionThreadSafeState::CollectCheckpointQueueTransactions(
    TVector<std::unique_ptr<ITransactionBase>>& txs)
{
    ui64 minCommitId = CommitQueue.GetMinCommitId();

    while (auto tx = CheckpointsInFlight.GetTx(minCommitId)) {
        txs.push_back(std::move(tx));
    }
}

bool TPartitionThreadSafeState::CollectNextCheckpointTx(
    const TString& checkpointId,
    TVector<std::unique_ptr<ITransactionBase>>& txs)
{
    ui64 minCommitId = CommitQueue.GetMinCommitId();

    CheckpointsInFlight.PopTx(checkpointId);

    auto tx = CheckpointsInFlight.GetTx(checkpointId, minCommitId);
    if (tx) {
        txs.push_back(std::move(tx));
        return true;
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

bool TPartitionThreadSafeState::WriteRequestInProgress() const
{
    return WriteAndZeroRequestsInProgress.load() > 0;
}

bool TPartitionThreadSafeState::OverlapsWithWrites(TBlockRange64 range) const
{
    Y_UNUSED(range);
    Y_ABORT("Unimplemented");
}

void TPartitionThreadSafeState::WaitForInFlightWrites()
{
    Y_ABORT("Unimplemented");
}

bool TPartitionThreadSafeState::IsWaitingForInFlightWrites() const
{
    Y_ABORT("Unimplemented");
}

}   // namespace NCloud::NBlockStore::NStorage
