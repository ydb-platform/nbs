#include "part_actor.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/generic/guid.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/xrange.h>
#include <util/stream/str.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration CompactOpHistoryDuration = TDuration::Days(1);

////////////////////////////////////////////////////////////////////////////////

class TForcedCompactionActor final
    : public TActorBootstrapped<TForcedCompactionActor>
{
private:
    const TActorId Tablet;
    const TVector<ui32> RangesToCompact;
    const TDuration RetryTimeout;
    const ui32 RangeCountPerRun;

    size_t CurrentRangeIndex = 0;

public:
    TForcedCompactionActor(
        const TActorId& tablet,
        TVector<ui32> rangesToCompact,
        TDuration retryTimeout,
        ui32 rangeCountPerRun);

    void Bootstrap(const TActorContext& ctx);

private:
    void SendCompactionRequest(const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error = {});

private:
    STFUNC(StateWork);

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

    void HandleCompactionResponse(
        const TEvPartitionPrivate::TEvCompactionResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TForcedCompactionActor::TForcedCompactionActor(
        const TActorId& tablet,
        TVector<ui32> rangesToCompact,
        TDuration retryTimeout,
        ui32 rangeCountPerRun)
    : Tablet(tablet)
    , RangesToCompact(std::move(rangesToCompact))
    , RetryTimeout(retryTimeout)
    , RangeCountPerRun(rangeCountPerRun)
{}

void TForcedCompactionActor::Bootstrap(const TActorContext& ctx)
{
    SendCompactionRequest(ctx);
    Become(&TThis::StateWork);
}

void TForcedCompactionActor::SendCompactionRequest(const TActorContext& ctx)
{
    TVector<ui32> ranges(Reserve(RangeCountPerRun));
    ranges.assign(
        RangesToCompact.begin() + CurrentRangeIndex,
        RangesToCompact.begin() +
            Min(CurrentRangeIndex + RangeCountPerRun, RangesToCompact.size()));

    auto request = std::make_unique<TEvPartitionPrivate::TEvCompactionRequest>(
        MakeIntrusive<TCallContext>(),
        std::move(ranges),
        TCompactionOptions().
            set(ToBit(ECompactionOption::Forced)).
            set(ToBit(ECompactionOption::Full)));

    NCloud::Send(ctx, Tablet, std::move(request));
}

void TForcedCompactionActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    {
        auto response = std::make_unique<TEvPartitionPrivate::TEvForcedCompactionCompleted>(error);
        NCloud::Send(ctx, Tablet, std::move(response));
    }

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TForcedCompactionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvents::TEvWakeup, HandleWakeup);

        HFunc(TEvPartitionPrivate::TEvCompactionResponse, HandleCompactionResponse);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

void TForcedCompactionActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    SendCompactionRequest(ctx);
}

void TForcedCompactionActor::HandleCompactionResponse(
    const TEvPartitionPrivate::TEvCompactionResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    ui32 errorCode = msg->GetStatus();
    if (FAILED(errorCode)) {
        if (errorCode == E_TRY_AGAIN) {
            ctx.Schedule(RetryTimeout, new TEvents::TEvWakeup());
            return;
        }
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    CurrentRangeIndex += RangeCountPerRun;
    if (CurrentRangeIndex < RangesToCompact.size()) {
        SendCompactionRequest(ctx);
    } else {
        ReplyAndDie(ctx);
    }
}

void TForcedCompactionActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto error = MakeError(E_REJECTED, "tablet is shutting down");

    ReplyAndDie(ctx, error);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleGetCompactionStatus(
    const TEvVolume::TEvGetCompactionStatusRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    ui32 progress = 0;
    ui32 total = 0;
    bool isCompleted = false;

    NProto::TError result;

    const auto& operationId = msg->Record.GetOperationId();
    const auto& stateRunning = State->GetForcedCompactionState();

    if (operationId == stateRunning.OperationId) {
        progress = stateRunning.Progress;
        total = stateRunning.RangesCount;
    }  else if (GetCompletedForcedCompactionRanges(operationId, ctx.Now(), total)) {
        progress = total;
        isCompleted = true;
    } else if (IsCompactRangePending(operationId, total)) {
        isCompleted = false;
    } else {
        result = MakeError(E_NOT_FOUND, "Compact operation not found");
    }

    auto response = std::make_unique<TEvVolume::TEvGetCompactionStatusResponse>(result);
    response->Record.SetProgress(progress);
    response->Record.SetTotal(total);
    response->Record.SetIsCompleted(isCompleted);
    NCloud::Send(ctx, ev->Sender, std::move(response), ev->Cookie);
}

void TPartitionActor::HandleForcedCompactionCompleted(
    const TEvPartitionPrivate::TEvForcedCompactionCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    Y_UNUSED(ctx);

    const auto& state = State->GetForcedCompactionState();
    CompletedForcedCompactionRequests.emplace(
        state.OperationId,
        TForcedCompactionResult(state.RangesCount, ctx.Now()));
    State->ResetForcedCompaction();
    Actors.Erase(ev->Sender);
    EnqueueForcedCompaction(ctx);
}

void TPartitionActor::HandleCompactRange(
    const TEvVolume::TEvCompactRangeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo<TEvVolume::TCompactRangeMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    auto replyError = [=] (
        const TActorContext& ctx,
        TRequestInfo& requestInfo,
        ui32 errorCode,
        TString errorReason)
    {
        auto response = std::make_unique<TEvVolume::TEvCompactRangeResponse>(
            MakeError(errorCode, std::move(errorReason)));
        NCloud::Reply(ctx, requestInfo, std::move(response));
    };

    auto& compactionMap = State->GetCompactionMap();

    TVector<ui32> rangesToCompact;
    if (msg->Record.GetStartIndex() || msg->Record.GetBlocksCount()) {
        auto startIndex = Min(
            State->GetBlocksCount(),
            msg->Record.GetStartIndex());

        auto endIndex = Min(
            State->GetBlocksCount(),
            msg->Record.GetStartIndex() + msg->Record.GetBlocksCount());

        rangesToCompact = TVector<ui32>(
            ::xrange(
                compactionMap.GetRangeStart(startIndex),
                compactionMap.GetRangeStart(endIndex) + compactionMap.GetRangeSize(),
                compactionMap.GetRangeSize())
        );
    } else {
        rangesToCompact = compactionMap.GetNonEmptyRanges();
    }

    if (!rangesToCompact) {
        replyError(ctx, *requestInfo, S_FALSE, "nothing to compact");
        return;
    }

    auto operationId = std::move(*msg->Record.MutableOperationId());
    if (!operationId) {
        operationId = CreateGuidAsString();
    }

    auto response = std::make_unique<TEvVolume::TEvCompactRangeResponse>();
    response->Record.SetOperationId(operationId);

    NCloud::Reply(ctx, *requestInfo, std::move(response));

    AddForcedCompaction(ctx, std::move(rangesToCompact), operationId);
    EnqueueForcedCompaction(ctx);
}

void TPartitionActor::AddForcedCompaction(
    const TActorContext& ctx,
    TVector<ui32> rangesToCompact,
    TString operationId)
{
    PendingForcedCompactionRequests.emplace_back(
        std::move(rangesToCompact),
        std::move(operationId));

    for (auto i = CompletedForcedCompactionRequests.begin();
        i != CompletedForcedCompactionRequests.end();)
    {
        auto prev = i++;
        if (prev->second.CompleteTs < ctx.Now() - CompactOpHistoryDuration) {
            CompletedForcedCompactionRequests.erase(prev);
        }
    }
}

void TPartitionActor::EnqueueForcedCompaction(const TActorContext& ctx)
{
    if (State && State->IsLoadStateFinished()) {
        if (State->GetForcedCompactionState().IsRunning ||
            PendingForcedCompactionRequests.empty())
        {
            return;
        }

        auto& compactInfo = PendingForcedCompactionRequests.front();

        State->StartForcedCompaction(
            compactInfo.OperationId,
            compactInfo.RangesToCompact.size());

        const bool batchCompactionEnabledForCloud =
            Config->IsBatchCompactionFeatureEnabled(
                PartitionConfig.GetCloudId(),
                PartitionConfig.GetFolderId(),
                PartitionConfig.GetDiskId());
        const bool batchCompactionEnabled =
            Config->GetBatchCompactionEnabled() ||
            batchCompactionEnabledForCloud;

        auto actorId = NCloud::Register<TForcedCompactionActor>(
            ctx,
            SelfId(),
            std::move(compactInfo.RangesToCompact),
            Config->GetCompactionRetryTimeout(),
            batchCompactionEnabled
                ? Config->GetForcedCompactionRangeCountPerRun()
                : 1);

        PendingForcedCompactionRequests.pop_front();

        Actors.Insert(actorId);
    }
}

bool TPartitionActor::GetCompletedForcedCompactionRanges(
    const TString& operationId,
    TInstant now,
    ui32& ranges)
{
    auto it = CompletedForcedCompactionRequests.find(operationId);
    if (it == CompletedForcedCompactionRequests.end()) {
        return false;
    }

    if (it->second.CompleteTs < now - CompactOpHistoryDuration) {
        CompletedForcedCompactionRequests.erase(it);
        return false;
    }

    ranges = it->second.NumRanges;

    return true;
}

bool TPartitionActor::IsCompactRangePending(
        const TString& operationId,
        ui32& ranges) const
{
    for (const auto& r : PendingForcedCompactionRequests) {
        if (r.OperationId == operationId) {
            ranges = r.RangesToCompact.size();
            return true;
        }
    }
    return false;
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
