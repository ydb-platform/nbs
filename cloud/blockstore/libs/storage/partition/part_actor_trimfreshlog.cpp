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

    // With a zero threshold, every move of OrdinaryTrimToCommitId is trimmed.
    // Otherwise, the dead blobs at or below it must reach the threshold, unless
    // a flush or a failure requested the trim.
    const ui64 threshold = GetFreshDeadBlobByteCountTrimThreshold();
    return !threshold || State->IsOrdinaryTrimRequested() ||
           State->GetOrdinaryTrimDeadFreshBlobByteCount(
               ordinaryTrimToCommitId) >= threshold;
}

bool TPartitionActor::ShouldStartSelectiveTrim(
    const TActorContext& ctx,
    ui64 ordinaryTrimToCommitId)
{
    if (!IsSelectiveTrimEnabled()) {
        return false;
    }

    // The dead blobs above OrdinaryTrimToCommitId must reach the threshold.
    // After a scan that found nothing to collect, more blobs must die first.
    const ui64 threshold = GetFreshDeadBlobByteCountTrimThreshold();
    if (State->GetDeadFreshBlobByteCount() <
            State->GetOrdinaryTrimDeadFreshBlobByteCount(
                ordinaryTrimToCommitId) +
                threshold ||
        !State->IsSelectiveScanAllowed(State->GetKilledFreshBlobByteCount()))
    {
        return false;
    }

    // A flush moves OrdinaryTrimToCommitId past these blobs without Keep flags.
    // Its completion enqueues a trim, and the dead blobs above it are still
    // counted then.
    if (State->GetFlushState().GetOperationState().Status !=
            EOperationStatus::Idle ||
        ShouldFlush(ctx))
    {
        return false;
    }

    // Before OrdinaryTrimToCommitId reaches the current generation, a soft
    // barrier from an earlier generation may be above it, and VDisks reject a
    // decreasing barrier.
    return ParseCommitId(ordinaryTrimToCommitId).first ==
           Executor()->Generation();
}

bool TPartitionActor::CheckDeadFreshBlobByteCount() const
{
    TVector<ui64> liveCommitIds;
    State->GetTrimFreshLogBarriers()->GetCommitIds(liveCommitIds);
    return State->CheckDeadFreshBlobByteCount(liveCommitIds);
}

TPartitionActor::ETrimFreshLogKind TPartitionActor::ChooseTrimFreshLogKind(
    const TActorContext& ctx)
{
    const ui64 ordinaryTrimToCommitId = State->GetTrimFreshLogToCommitId();
    const bool ordinary = ShouldStartOrdinaryTrim(ordinaryTrimToCommitId);
    const bool selective =
        ShouldStartSelectiveTrim(ctx, ordinaryTrimToCommitId);

    if (ordinary && selective) {
        // Alternate, so that frequent requests cannot starve either kind.
        return State->GetLastTrimWasOrdinary() ? ETrimFreshLogKind::Selective
                                               : ETrimFreshLogKind::Ordinary;
    }
    if (ordinary) {
        return ETrimFreshLogKind::Ordinary;
    }
    if (selective) {
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

    if (ChooseTrimFreshLogKind(ctx) == ETrimFreshLogKind::None) {
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
        State->GetDeadFreshBlobByteCount());

    auto kind = ChooseTrimFreshLogKind(ctx);

    std::optional<TSelectiveTrimPlan> plan;
    if (kind == ETrimFreshLogKind::Selective) {
        const auto snapshot = SharedState->GetFreshTrimSnapshot();
        plan = State->BuildSelectiveTrimPlan(
            snapshot.OrdinaryTrimToCommitId,
            snapshot.SafeCommitId,
            snapshot.LiveCommitIds,
            State->GetKeptToCommitId(),
            State->GetSoftBarrierCommitId(),
            MaxCollectGarbageFlagsPerMessage);

        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s Selective TrimFreshLog scan ordinary trim to: @%lu, "
            "safe: @%lu, soft barrier: @%lu, keep: %lu, "
            "do not keep: %lu, collecting: %lu",
            LogTitle.GetWithTime().c_str(),
            snapshot.OrdinaryTrimToCommitId,
            snapshot.SafeCommitId,
            plan ? plan->SoftBarrierCommitId : 0,
            plan ? plan->Keep.size() : 0,
            plan ? plan->DoNotKeep.size() : 0,
            plan ? plan->CollectingCommitIds.size() : 0);

        if (!plan) {
            // Nothing to collect behind live blobs, but OrdinaryTrimToCommitId
            // may have moved.
            State->DeferSelectiveScan(
                State->GetKilledFreshBlobByteCount(),
                GetFreshDeadBlobByteCountTrimThreshold());
            kind = ShouldStartOrdinaryTrim(State->GetTrimFreshLogToCommitId())
                       ? ETrimFreshLogKind::Ordinary
                       : ETrimFreshLogKind::None;
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

    // Ordinary trims send hard barriers, so selective trims send the only soft
    // barriers on fresh channels.
    ui64 trimFreshLogToCommitId = 0;
    TVector<TPartialBlobId> keep;
    TVector<TPartialBlobId> doNotKeep;
    if (plan) {
        trimFreshLogToCommitId = plan->SoftBarrierCommitId;
        keep = std::move(plan->Keep);
        doNotKeep = std::move(plan->DoNotKeep);
        State->StartSelectiveTrim(
            trimFreshLogToCommitId,
            std::move(plan->CollectingCommitIds));
    } else {
        State->SetOrdinaryTrimRequested(false);
        trimFreshLogToCommitId = State->GetTrimFreshLogToCommitId();
    }
    State->SetLastTrimWasOrdinary(!plan);

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Start %s TrimFreshLog @%lu @%lu",
        LogTitle.GetWithTime().c_str(),
        plan ? "selective" : "ordinary",
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
        !plan,   // hard
        std::move(keep),
        std::move(doNotKeep),
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

    // msg->CommitId is used only for ordinary trims.
    ui64 trimmedByteCount = 0;
    if (State->IsSelectiveTrimInFlight()) {
        if (!failed) {
            trimmedByteCount =
                State->TrimCollectedFreshBlobs(State->CompleteSelectiveTrim());
        } else {
            State->FailSelectiveTrim();
        }
    } else if (!failed) {
        State->SetLastTrimFreshLogToCommitId(msg->CommitId);
        trimmedByteCount = State->TrimFreshBlobs(msg->CommitId);
    } else {
        State->SetOrdinaryTrimRequested(true);
    }

    if (IsFreshBlocksWriterEnabled()) {
        SharedState->UnflushedFreshBlobByteCount.fetch_sub(trimmedByteCount);
    }

    Y_DEBUG_ABORT_UNLESS(
        CheckDeadFreshBlobByteCount(),
        "Dead fresh blob byte count: %lu",
        State->GetDeadFreshBlobByteCount());

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
