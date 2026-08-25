#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log.h>
#include <cloud/filestore/libs/storage/model/block_buffer.h>
#include <cloud/filestore/libs/storage/tablet/model/blob_builder.h>
#include <cloud/filestore/libs/storage/tablet/tablet_state.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

/**
 * @brief An actor that performs a forced operation for a sequence of ranges.
 * It is implemented as a template class to avoid code duplication.
 *
 * @tparam TRequestConstructor A functor that constructs a unique_ptr to a
 * request that is necessary to be performed to passed range.
 */
template <typename TResponseType, typename TRequestConstructor>
class TForcedRangeOperationActor final
    : public TActorBootstrapped<
          TForcedRangeOperationActor<TResponseType, TRequestConstructor>>
{
private:
    using TBase = NActors::TActorBootstrapped<
        TForcedRangeOperationActor<TResponseType, TRequestConstructor>>;

    const TActorId Tablet;
    const TString LogTag;
    const TDuration RetryTimeout;

    const TString OperationId;
    const TVector<ui32> Ranges;
    ui32 CurrentRange = 0;
    const TRequestInfoPtr RequestInfo;

public:
    TForcedRangeOperationActor(
        TActorId tablet,
        TString logTag,
        TDuration retry,
        TString operationId,
        TVector<ui32> ranges,
        TRequestInfoPtr requestInfo);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void SendCurrentRangeRequest(const TActorContext& ctx);

    void HandleRangeOperationResponse(
        const TResponseType::TPtr& ev,
        const TActorContext& ctx);

    void HandleWakeUp(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error);

    void ReportProgress(
        const TActorContext& ctx,
        ui32 rangeIdForRestart);
};

////////////////////////////////////////////////////////////////////////////////

template <typename TResponseType, typename TRequestConstructor>
TForcedRangeOperationActor<TResponseType, TRequestConstructor>::
    TForcedRangeOperationActor(
        TActorId tablet,
        TString logTag,
        TDuration retry,
        TString operationId,
        TVector<ui32> ranges,
        TRequestInfoPtr requestInfo)
    : Tablet(tablet)
    , LogTag(std::move(logTag))
    , RetryTimeout(retry)
    , OperationId(std::move(operationId))
    , Ranges(std::move(ranges))
    , RequestInfo(std::move(requestInfo))
{}

template <typename TResponseType, typename TRequestConstructor>
void TForcedRangeOperationActor<TResponseType, TRequestConstructor>::Bootstrap(
    const TActorContext& ctx)
{
    TBase::Become(&TBase::TThis::StateWork);

    FILESTORE_TRACK(
        RequestReceived_TabletWorker,
        RequestInfo->CallContext,
        "ForcedOperation");

    if (Ranges.empty()) {
        ReplyAndDie(ctx, {});
        return;
    }

    SendCurrentRangeRequest(ctx);
}

template <typename TResponseType, typename TRequestConstructor>
void TForcedRangeOperationActor<TResponseType, TRequestConstructor>::
    SendCurrentRangeRequest(const TActorContext& ctx)
{
    TABLET_VERIFY(CurrentRange < Ranges.size());
    auto request = TRequestConstructor()(Ranges[CurrentRange]);

    ctx.Send(Tablet, request.release());
}

template <typename TResponseType, typename TRequestConstructor>
STFUNC(
    (TForcedRangeOperationActor<TResponseType, TRequestConstructor>::StateWork))
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleWakeUp);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TResponseType, HandleRangeOperationResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::TABLET_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

template <typename TResponseType, typename TRequestConstructor>
void TForcedRangeOperationActor<TResponseType, TRequestConstructor>::
    HandleRangeOperationResponse(
        const TResponseType::TPtr& ev,
        const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->Error)) {
        if (msg->Error.GetCode() == E_TRY_AGAIN) {
            ctx.Schedule(RetryTimeout, new TEvents::TEvWakeup());
            return;
        }

        return ReplyAndDie(ctx, msg->Error);
    }

    ++CurrentRange;

    if (CurrentRange == Ranges.size()) {
        return ReplyAndDie(ctx, {});
    }

    ReportProgress(ctx, Ranges[CurrentRange]);
    SendCurrentRangeRequest(ctx);
}

template <typename TResponseType, typename TRequestConstructor>
void TForcedRangeOperationActor<TResponseType, TRequestConstructor>::
    HandleWakeUp(const TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx)
{
    Y_UNUSED(ev);
    SendCurrentRangeRequest(ctx);
}

template <typename TResponseType, typename TRequestConstructor>
void TForcedRangeOperationActor<TResponseType, TRequestConstructor>::
    HandlePoisonPill(
        const TEvents::TEvPoison::TPtr& ev,
        const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_FAIL, "actor killed"));
}

template <typename TResponseType, typename TRequestConstructor>
void TForcedRangeOperationActor<TResponseType, TRequestConstructor>::
    ReplyAndDie(const TActorContext& ctx, const NProto::TError& error)
{
    {
        // notify tablet
        auto response = std::make_unique<
            TEvIndexTabletPrivate::TEvForcedOperationTaskCompleted>(
                error,
                OperationId);
        NCloud::Send(ctx, Tablet, std::move(response));
    }

    FILESTORE_TRACK(
        ResponseSent_TabletWorker,
        RequestInfo->CallContext,
        "ForcedOperation");

    if (RequestInfo->Sender != Tablet) {
        // reply to caller
        auto response = std::make_unique<
            TEvIndexTabletPrivate::TEvForcedOperationTaskResponse>(error);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
    }

    TBase::Die(ctx);
}

template <typename TResponseType, typename TRequestConstructor>
void TForcedRangeOperationActor<TResponseType, TRequestConstructor>::
    ReportProgress(
        const TActorContext& ctx,
        ui32 rangeIdForRestart)
{
    using TEvent = TEvIndexTabletPrivate::TEvForcedOperationProgress;
    NCloud::Send(
        ctx,
        Tablet,
        std::make_unique<TEvent>(
            OperationId,
            CurrentRange,
            rangeIdForRestart));
}

////////////////////////////////////////////////////////////////////////////////

struct TCompactionRequestConstructor
{
    std::unique_ptr<TEvIndexTabletPrivate::TEvCompactionRequest> operator()(
        const ui32 rangeId) const
    {
        return std::make_unique<TEvIndexTabletPrivate::TEvCompactionRequest>(
            rangeId,
            true);
    }
};

struct TCleanupRequestConstructor
{
    std::unique_ptr<TEvIndexTabletPrivate::TEvCleanupRequest> operator()(
        const ui32 range) const
    {
        return std::make_unique<TEvIndexTabletPrivate::TEvCleanupRequest>(
            range);
    }
};

struct TDeleteZeroCompactionRangesRequestConstructor
{
    std::unique_ptr<TEvIndexTabletPrivate::TEvDeleteZeroCompactionRangesRequest>
    operator()(const ui32 range) const
    {
        return std::make_unique<
            TEvIndexTabletPrivate::TEvDeleteZeroCompactionRangesRequest>(range);
    }
};

using TForcedCompactionActor = TForcedRangeOperationActor<
    TEvIndexTabletPrivate::TEvCompactionResponse,
    TCompactionRequestConstructor>;

using TForcedCleanupActor = TForcedRangeOperationActor<
    TEvIndexTabletPrivate::TEvCleanupResponse,
    TCleanupRequestConstructor>;

using TDeleteRangesWithEmptyScoreActor = TForcedRangeOperationActor<
    TEvIndexTabletPrivate::TEvDeleteZeroCompactionRangesResponse,
    TDeleteZeroCompactionRangesRequestConstructor>;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::EnqueueForcedOperationIfNeeded(
    const TActorContext& ctx)
{
    if (IsForcedOperationRunning()) {
        return;
    }

    auto pendingRequest = DequeueForcedOperation();
    if (!pendingRequest) {
        return;
    }

    auto request =
        std::make_unique<TEvIndexTabletPrivate::TEvForcedOperationTaskRequest>(
            std::move(pendingRequest->Args),
            pendingRequest->Mode,
            std::move(pendingRequest->OperationId));
    ctx.Send(ctx.SelfID, request.release());
}

void TIndexTabletActor::HandleForcedOperationTask(
    const TEvIndexTabletPrivate::TEvForcedOperationTaskRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto* rangeArgs =
        std::get_if<TEvIndexTabletPrivate::TForcedRangeOperationArgs>(
            &msg->Args);

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s ForcedOperation request for %lu ranges",
        LogTag.c_str(),
        rangeArgs ? rangeArgs->Ranges.size() : 0);

    auto replyError = [&] (
        const NProto::TError& error)
    {
        if (ev->Sender == ctx.SelfID) {
            return;
        }

        auto response = std::make_unique<
            TEvIndexTabletPrivate::TEvForcedOperationTaskResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
    };

    if (!rangeArgs ||
        rangeArgs->Ranges.empty() ||
        rangeArgs->Ranges.size() > Max<ui32>())
    {
        replyError(ErrorInvalidArgument());
        return;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    // will lose original request info in case of enqueueing external request
    if (IsForcedOperationRunning()) {
        EnqueueForcedOperation(msg->Mode, std::move(msg->Args));
        return;
    }

    const auto operationId = msg->OperationId;
    auto ranges = std::move(rangeArgs->Ranges);
    StartForcedOperation(
        msg->Mode,
        operationId,
        TIndexTabletState::TForcedRangeOperationDetails(ranges.size()));

    std::unique_ptr<IActor> actor;

    switch (msg->Mode) {
        case TEvIndexTabletPrivate::EForcedOperationMode::Compaction:
            actor = std::make_unique<TForcedCompactionActor>(
                ctx.SelfID,
                LogTag,
                Config->GetCompactionRetryTimeout(),
                operationId,
                std::move(ranges),
                std::move(requestInfo));
            break;

        case TEvIndexTabletPrivate::EForcedOperationMode::Cleanup:
            actor = std::make_unique<TForcedCleanupActor>(
                ctx.SelfID,
                LogTag,
                Config->GetCompactionRetryTimeout(),
                operationId,
                std::move(ranges),
                std::move(requestInfo));
            break;
        case TEvIndexTabletPrivate::EForcedOperationMode::DeleteZeroCompactionRanges:
            actor = std::make_unique<TDeleteRangesWithEmptyScoreActor>(
                ctx.SelfID,
                LogTag,
                Config->GetCompactionRetryTimeout(),
                operationId,
                std::move(ranges),
                std::move(requestInfo));
            break;

        default:
            TABLET_VERIFY_C(false, "unexpected forced compaction mode");
    }

    auto actorId = ctx.Register(actor.release());
    WorkerActors.insert(actorId);
}

void TIndexTabletActor::HandleForcedOperationTaskCompleted(
    const TEvIndexTabletPrivate::TEvForcedOperationTaskCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s ForcedOperation completed (%s)",
        LogTag.c_str(),
        FormatError(msg->GetError()).c_str());

    TABLET_VERIFY(IsForcedOperationRunning());
    TABLET_VERIFY(GetForcedOperation()->State.OperationId == msg->OperationId);
    WorkerActors.erase(ev->Sender);

    CompleteForcedOperation(msg->GetError());
    EnqueueForcedOperationIfNeeded(ctx);
}

void TIndexTabletActor::HandleForcedOperationProgress(
    const TEvIndexTabletPrivate::TEvForcedOperationProgress::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    const auto* msg = ev->Get();
    UpdateForcedRangeOperationProgress(
        msg->OperationId,
        msg->ProcessedRangeCount,
        msg->RangeIdForRestart);
}

}   // namespace NCloud::NFileStore::NStorage
