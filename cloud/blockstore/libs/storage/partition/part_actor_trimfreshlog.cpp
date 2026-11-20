#include "part_actor.h"

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/partition_common/actor_trimfreshlog.h>

#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/tablet/model/commit.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::ShouldStartOrdinaryTrim(ui64 ordinaryTrimToCommitId)
{
    if (ordinaryTrimToCommitId == State->GetLastTrimFreshLogToCommitId()) {
        return false;
    }

    const ui64 threshold = GetFreshDeadBlobByteCountTrimThreshold();
    return !threshold || State->IsOrdinaryTrimRequested() ||
           State->GetAndUpdateOrdinaryTrimmableFreshBlobByteCount(
               ordinaryTrimToCommitId) >= threshold;
}

bool TPartitionActor::ShouldStartSelectiveTrim(
    const TActorContext& ctx,
    const TPartitionThreadSafeState::TFreshTrimSnapshot& snapshot)
{
    if (!IsSelectiveTrimEnabled()) {
        return false;
    }

    // Normally, the SelectiveTrimToCommitId is above the
    // OrdinaryTrimToCommitId. But the opposite is a technically possible
    // situation, since the OrdinaryTrimToCommitId can be advanced a little
    // earlier than the SelectiveTrimToCommitId. No point in doing the selective
    // trim in this case.
    if (snapshot.SelectiveTrimToCommitId <= snapshot.OrdinaryTrimToCommitId) {
        return false;
    }

    const ui64 ordinaryTrimToCommitId = snapshot.OrdinaryTrimToCommitId;
    const ui64 trimmableByOrdinaryTrimByteCount =
        State->GetAndUpdateOrdinaryTrimmableFreshBlobByteCount(
            ordinaryTrimToCommitId);
    const ui64 threshold = GetFreshDeadBlobByteCountTrimThreshold();
    // A selective trim is worth doing only if it can trim at least threshold
    // bytes more than an ordinary trim.
    if (State->GetDeadUntrimmedFreshBlobByteCount() <
            trimmableByOrdinaryTrimByteCount + threshold ||
        !State->IsSelectiveScanAllowed(
            State->GetTotalKilledFreshBlobByteCount()))
    {
        return false;
    }

    // No need to start a selective trim since OrdinaryTrimToCommitId is about
    // to be increased.
    if (State->GetFlushState().GetOperationState().Status !=
            EOperationStatus::Idle ||
        ShouldFlush(ctx))
    {
        return false;
    }

    // Selective trims are disabled until the ordinary trims reach tablet's
    // current generation. This is done for simplicity. Otherwise, we would have
    // to restore the state of keep/doNotKeep flags on restart. Plus, this way
    // we guarantee that we won't place the barrier below already placed one or
    // with the same key but different value.
    return ParseCommitId(ordinaryTrimToCommitId).first ==
           Executor()->Generation();
}

bool TPartitionActor::CheckDeadFreshBlobByteCount() const
{
    const ui64 addedToCommitId =
        SharedState->GetFreshTrimSnapshot().SelectiveTrimToCommitId;
    TVector<ui64> liveCommitIds;
    State->GetTrimFreshLogBarriers()->GetCommitIds(liveCommitIds);
    return State->CheckDeadFreshBlobByteCount(liveCommitIds, addedToCommitId);
}

TPartitionActor::ETrimFreshLogKind TPartitionActor::ChooseTrimFreshLogKind(
    const TActorContext& ctx,
    const TPartitionThreadSafeState::TFreshTrimSnapshot& snapshot)
{
    // An ordinary trim wins when both are due. It is cheaper, its hard barrier
    // also deletes dead blobs with Keep flags, and the selective trim stays due
    // after it.
    if (ShouldStartOrdinaryTrim(snapshot.OrdinaryTrimToCommitId)) {
        return ETrimFreshLogKind::Ordinary;
    }

    if (ShouldStartSelectiveTrim(ctx, snapshot)) {
        return ETrimFreshLogKind::Selective;
    }

    return ETrimFreshLogKind::None;
}

void TPartitionActor::EnqueueTrimFreshLogIfNeeded(const TActorContext& ctx)
{
    if (State->GetTrimFreshLogState().Status != EOperationStatus::Idle) {
        // already enqueued
        return;
    }

    if (ChooseTrimFreshLogKind(ctx, SharedState->GetFreshTrimSnapshot()) ==
        ETrimFreshLogKind::None)
    {
        // not ready
        return;
    }

    State->AccessTrimFreshLogState().SetStatus(
        EOperationStatus::Enqueued,
        ctx.Now());

    using TRequest = TEvPartitionCommonPrivate::TEvTrimFreshLogRequest;
    auto request = std::make_unique<TRequest>(
        MakeIntrusive<TCallContext>(CreateRequestId())
    );

    if (State->GetTrimFreshLogBackoffDelay()) {
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s TrimFreshLog request scheduled: %lu, %s",
            LogTitle.GetWithTime().c_str(),
            request->CallContext->RequestId,
            FormatDuration(State->GetTrimFreshLogBackoffDelay()).c_str());

        ctx.Schedule(State->GetTrimFreshLogBackoffDelay(), request.release());
    } else {
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s TrimFreshLog request sent: %lu",
            LogTitle.GetWithTime().c_str(),
            request->CallContext->RequestId);

        NCloud::Send(
            ctx,
            SelfId(),
            std::move(request));
    }
}

void TPartitionActor::HandleTrimFreshLog(
    const TEvPartitionCommonPrivate::TEvTrimFreshLogRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    using TMethod = TEvPartitionCommonPrivate::TTrimFreshLogMethod;
    auto requestInfo = CreateRequestInfo<TMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        BackgroundTaskStarted_Partition,
        requestInfo->CallContext->LWOrbit,
        "TrimFreshLog",
        static_cast<ui32>(PartitionConfig.GetStorageMediaKind()),
        requestInfo->CallContext->RequestId,
        PartitionConfig.GetDiskId());

    auto replyError = [=] (
        const TActorContext& ctx,
        TRequestInfo& requestInfo,
        ui32 errorCode,
        TString errorReason)
    {
        using TResponse = TEvPartitionCommonPrivate::TEvTrimFreshLogResponse;
        auto response = std::make_unique<TResponse>(
            MakeError(errorCode, std::move(errorReason))
        );

        LWTRACK(
            ResponseSent_Partition,
            requestInfo.CallContext->LWOrbit,
            "TrimFreshLog",
            requestInfo.CallContext->RequestId);

        NCloud::Reply(ctx, requestInfo, std::move(response));
    };

    if (State->GetTrimFreshLogState().Status == EOperationStatus::Started) {
        replyError(ctx, *requestInfo, E_TRY_AGAIN, "trim already started");
        return;
    }

    Y_DEBUG_ABORT_UNLESS(
        CheckDeadFreshBlobByteCount(),
        "Dead fresh blob byte count: %lu",
        State->GetDeadUntrimmedFreshBlobByteCount());

    const auto snapshot = SharedState->GetFreshTrimSnapshot();
    auto kind = ChooseTrimFreshLogKind(ctx, snapshot);

    std::optional<TSelectiveTrimPlan> plan;
    if (kind == ETrimFreshLogKind::Selective) {
        plan = State->BuildSelectiveTrimPlan({
            .OrdinaryTrimToCommitId = snapshot.OrdinaryTrimToCommitId,
            .SelectiveTrimToCommitId = snapshot.SelectiveTrimToCommitId,
            .KeptToCommitId = State->GetKeptToCommitId(),
            .LastSentSelectiveTrimToCommitId =
                State->GetLastSentSelectiveTrimToCommitId(),
            .CurrentGeneration = Executor()->Generation(),
        });

        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s Selective TrimFreshLog scan ordinary trim to: @%lu, "
            "selective trim to: @%lu, soft barrier: @%lu, keep: %lu, "
            "do not keep: %lu, collecting: %lu",
            LogTitle.GetWithTime().c_str(),
            snapshot.OrdinaryTrimToCommitId,
            snapshot.SelectiveTrimToCommitId,
            plan ? plan->TrimToCommitId : 0,
            plan ? plan->Keep.size() : 0,
            plan ? plan->DoNotKeep.size() : 0,
            plan ? plan->CollectingCommitIds.size() : 0);

        State->DeferSelectiveScan(
            State->GetTotalKilledFreshBlobByteCount(),
            GetFreshDeadBlobByteCountTrimThreshold());

        if (!plan) {
            // Nothing to collect behind live blobs. No ordinary trim is due
            // either, or it would have been chosen.
            kind = ETrimFreshLogKind::None;
        }
    }

    if (kind == ETrimFreshLogKind::None) {
        State->AccessTrimFreshLogState().SetStatus(
            EOperationStatus::Idle,
            ctx.Now());
        replyError(ctx, *requestInfo, S_ALREADY, "nothing to trim");
        return;
    }

    ui32 nextPerGenerationCounter = State->NextCollectPerGenerationCounter();
    if (nextPerGenerationCounter == InvalidCollectPerGenerationCounter) {
        RebootPartitionOnCollectCounterOverflow(ctx, "TrimFreshLog");
        return;
    }

    const bool selective = plan.has_value();
    ui64 trimFreshLogToCommitId = snapshot.OrdinaryTrimToCommitId;
    TVector<TPartialBlobId> keep;
    TVector<TPartialBlobId> doNotKeep;
    TVector<ui64> collectingCommitIds;
    if (selective) {
        trimFreshLogToCommitId = plan->TrimToCommitId;
        keep = std::move(plan->Keep);
        doNotKeep = std::move(plan->DoNotKeep);
        collectingCommitIds = std::move(plan->CollectingCommitIds);
        State->StartSelectiveTrim(trimFreshLogToCommitId);
    } else {
        State->SetOrdinaryTrimRequested(false);
    }

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Start %s TrimFreshLog @%lu @%lu",
        LogTitle.GetWithTime().c_str(),
        selective ? "selective" : "ordinary",
        trimFreshLogToCommitId,
        nextPerGenerationCounter);

    State->AccessTrimFreshLogState().SetStatus(
        EOperationStatus::Started,
        ctx.Now());

    TVector<ui32> freshChannels = State->GetChannelsByKind([](auto kind) {
        return kind == EChannelDataKind::Fresh;
    });

    auto actor = NCloud::Register<TTrimFreshLogActor>(
        ctx,
        requestInfo,
        SelfId(),
        Info(),
        trimFreshLogToCommitId,
        Executor()->Generation(),
        nextPerGenerationCounter,
        !selective,   // hard
        std::move(keep),
        std::move(doNotKeep),
        selective,
        std::move(collectingCommitIds),
        std::move(freshChannels),
        VolumeLabels,
        Config->GetTrimFreshLogTimeout());

    Actors.Insert(actor);
}

void TPartitionActor::HandleTrimFreshLogCompleted(
    const TEvPartitionCommonPrivate::TEvTrimFreshLogCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const bool failed = FAILED(msg->GetStatus());

    if (failed) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s TrimFreshLog failed: %u reason: %s",
            LogTitle.GetWithTime().c_str(),
            msg->GetStatus(),
            FormatError(msg->GetError()).c_str());

        State->RegisterTrimFreshLogError();
    } else {
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s TrimFreshLog completed",
            LogTitle.GetWithTime().c_str());

        State->RegisterTrimFreshLogSuccess();
    }

    ui64 unflushedByteCount = 0;
    if (msg->SelectiveTrim) {
        if (!failed) {
            State->CompleteSelectiveTrim();
            unflushedByteCount =
                State->TrimCollectedFreshBlobs(msg->CollectedCommitIds)
                    .UnflushedBytesRemoved;
        } else {
            State->FailSelectiveTrim();
        }
    } else if (!failed) {
        State->SetLastTrimFreshLogToCommitId(msg->CommitId);
        unflushedByteCount =
            State->TrimFreshBlobs(msg->CommitId).UnflushedBytesRemoved;
    } else {
        State->SetOrdinaryTrimRequested(true);
    }

    if (IsFreshBlocksWriterEnabled()) {
        SharedState->UnflushedFreshBlobByteCount.fetch_sub(unflushedByteCount);
    }

    Y_DEBUG_ABORT_UNLESS(
        CheckDeadFreshBlobByteCount(),
        "Dead fresh blob byte count: %lu",
        State->GetDeadUntrimmedFreshBlobByteCount());

    State->AccessTrimFreshLogState().SetStatus(
        EOperationStatus::Idle,
        ctx.Now());

    Actors.Erase(ev->Sender);

    UpdateCPUUsageStat(ctx.Now(), msg->ExecCycles);

    EnqueueTrimFreshLogIfNeeded(ctx);

    ui64 time = CyclesToDurationSafe(msg->TotalCycles).MicroSeconds();
    PartCounters->RequestCounters.TrimFreshLog.AddRequest(time);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
