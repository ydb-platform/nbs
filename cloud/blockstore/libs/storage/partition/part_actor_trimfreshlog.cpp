#include "part_actor.h"

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/partition_common/actor_trimfreshlog.h>

#include <cloud/storage/core/libs/common/format.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::EnqueueTrimFreshLogIfNeeded(const TActorContext& ctx)
{
    if (State->GetTrimFreshLogState().Status != EOperationStatus::Idle) {
        // already enqueued
        return;
    }

    ui64 trimFreshLogToCommitId = State->GetTrimFreshLogToCommitId();

    if (trimFreshLogToCommitId == State->GetLastTrimFreshLogToCommitId()) {
        // not ready
        return;
    }

    State->GetTrimFreshLogState().SetStatus(EOperationStatus::Enqueued);

    using TRequest = TEvPartitionCommonPrivate::TEvTrimFreshLogRequest;
    auto request = std::make_unique<TRequest>(
        MakeIntrusive<TCallContext>(CreateRequestId())
    );

    if (State->GetTrimFreshLogTimeout()) {
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s TrimFreshLog request scheduled: %lu, %s",
            LogTitle.GetWithTime().c_str(),
            request->CallContext->RequestId,
            FormatDuration(State->GetTrimFreshLogTimeout()).c_str());

        ctx.Schedule(State->GetTrimFreshLogTimeout(), request.release());
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

    ui64 trimFreshLogToCommitId = State->GetTrimFreshLogToCommitId();

    auto nextPerGenerationCounter = State->NextCollectPerGenerationCounter();
    if (nextPerGenerationCounter == InvalidCollectPerGenerationCounter) {
        RebootPartitionOnCollectCounterOverflow(ctx, "TrimFreshLog");
        return;
    }

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Start TrimFreshLog @%lu @%lu",
        LogTitle.GetWithTime().c_str(),
        trimFreshLogToCommitId,
        nextPerGenerationCounter);

    State->GetTrimFreshLogState().SetStatus(EOperationStatus::Started);

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
        std::move(freshChannels),
        PartitionConfig.GetDiskId(),
        Config->GetTrimFreshLogTimeout());

    Actors.Insert(actor);
}

void TPartitionActor::HandleTrimFreshLogCompleted(
    const TEvPartitionCommonPrivate::TEvTrimFreshLogCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (FAILED(msg->GetStatus())) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s TrimFreshLog failed: %u reason: %s",
            LogTitle.GetWithTime().c_str(),
            msg->GetStatus(),
            FormatError(msg->GetError()).c_str());

        if (msg->GetStatus() == E_TIMEOUT) {
            Suicide(ctx);
            return;
        }

        State->RegisterTrimFreshLogError();
    } else {
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s TrimFreshLog completed",
            LogTitle.GetWithTime().c_str());

        State->RegisterTrimFreshLogSuccess();
        State->SetLastTrimFreshLogToCommitId(msg->CommitId);
        State->TrimFreshBlobs(msg->CommitId);
    }

    State->GetTrimFreshLogState().SetStatus(EOperationStatus::Idle);

    Actors.Erase(ev->Sender);

    UpdateCPUUsageStat(ctx.Now(), msg->ExecCycles);

    EnqueueTrimFreshLogIfNeeded(ctx);

    auto time = CyclesToDurationSafe(msg->TotalCycles).MicroSeconds();
    PartCounters->RequestCounters.TrimFreshLog.AddRequest(time);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
