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

bool TPartitionActor::ShouldStartPrefixTrim() const
{
    if (State->GetTrimFreshLogToCommitId() ==
        State->GetLastTrimFreshLogToCommitId())
    {
        return false;
    }

    // With a zero threshold, every move of P is trimmed.
    return !GetFreshDeadBlobByteCountTrimThreshold() ||
           State->IsPrefixTrimRequested();
}

bool TPartitionActor::ShouldStartPartialTrim(
    const TActorContext& ctx,
    ui64 prefixCommitId)
{
    if (!IsPartialTrimEnabled() || !State->IsPartialTrimRequested()) {
        return false;
    }

    // Flush completion moves P past these blobs without Keep flags. The
    // request stays set until a partial trim starts.
    if (State->GetFlushState().GetOperationState().Status !=
            EOperationStatus::Idle ||
        ShouldFlush(ctx))
    {
        return false;
    }

    // Before P reaches the current generation, a soft barrier from an earlier
    // generation may be above P, and VDisks reject a decreasing barrier.
    return ParseCommitId(prefixCommitId).first == Executor()->Generation();
}

TPartitionActor::ETrimFreshLogKind TPartitionActor::ChooseTrimFreshLogKind(
    const TActorContext& ctx)
{
    const bool prefix = ShouldStartPrefixTrim();
    const bool partial =
        ShouldStartPartialTrim(ctx, State->GetTrimFreshLogToCommitId());

    if (prefix && partial) {
        // Alternate, so that frequent requests cannot starve either kind.
        return State->GetLastTrimWasPrefix() ? ETrimFreshLogKind::Partial
                                             : ETrimFreshLogKind::Prefix;
    }
    if (prefix) {
        return ETrimFreshLogKind::Prefix;
    }
    if (partial) {
        return ETrimFreshLogKind::Partial;
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

    auto kind = ChooseTrimFreshLogKind(ctx);

    std::optional<TPartialTrimPlan> plan;
    if (kind == ETrimFreshLogKind::Partial) {
        State->SetPartialTrimRequested(false);

        const auto snapshot = SharedState->GetFreshTrimSnapshot();
        plan = State->BuildPartialTrimPlan(
            snapshot.PrefixCommitId,
            snapshot.SafeCommitId,
            snapshot.LiveCommitIds,
            State->GetKeptToCommitId(),
            State->GetSoftBarrierCommitId(),
            MaxCollectGarbageFlagsPerMessage);

        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s Partial TrimFreshLog scan P: @%lu, S: @%lu, C: @%lu, "
            "keep: %lu, do not keep: %lu, collecting: %lu",
            LogTitle.GetWithTime().c_str(),
            snapshot.PrefixCommitId,
            snapshot.SafeCommitId,
            plan ? plan->SoftBarrierCommitId : 0,
            plan ? plan->Keep.size() : 0,
            plan ? plan->DoNotKeep.size() : 0,
            plan ? plan->CollectingCommitIds.size() : 0);

        if (!plan) {
            // Nothing to collect behind live blobs, but P may have moved.
            kind = ShouldStartPrefixTrim() ? ETrimFreshLogKind::Prefix
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

    // Prefix trims send hard barriers, so partial trims send the only soft
    // barriers on fresh channels.
    ui64 trimFreshLogToCommitId = 0;
    TVector<TPartialBlobId> keep;
    TVector<TPartialBlobId> doNotKeep;
    if (plan) {
        trimFreshLogToCommitId = plan->SoftBarrierCommitId;
        keep = std::move(plan->Keep);
        doNotKeep = std::move(plan->DoNotKeep);
        State->StartPartialTrim(
            trimFreshLogToCommitId,
            std::move(plan->CollectingCommitIds));
    } else {
        State->SetPrefixTrimRequested(false);
        trimFreshLogToCommitId = State->GetTrimFreshLogToCommitId();
    }
    State->SetLastTrimWasPrefix(!plan);

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Start %s TrimFreshLog @%lu @%lu",
        LogTitle.GetWithTime().c_str(),
        plan ? "partial" : "prefix",
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

    // msg->CommitId is used only for prefix trims.
    ui64 trimmedByteCount = 0;
    if (State->IsPartialTrimInFlight()) {
        if (!failed) {
            trimmedByteCount =
                State->TrimCollectedFreshBlobs(State->CompletePartialTrim());
        } else {
            State->FailPartialTrim();
        }
    } else if (!failed) {
        State->SetLastTrimFreshLogToCommitId(msg->CommitId);
        trimmedByteCount = State->TrimFreshBlobs(msg->CommitId);
    } else {
        State->SetPrefixTrimRequested(true);
    }

    if (IsFreshBlocksWriterEnabled()) {
        SharedState->UnflushedFreshBlobByteCount.fetch_sub(trimmedByteCount);
    }

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
