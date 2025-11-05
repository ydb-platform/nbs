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
 * @brief An actor that performs forced compaction or forced cleanup. It is
 * implemented as a template class to avoid code duplication.
 *
 * @tparam TRequestConstructor A functor that constructs a unique_ptr to a
 * request that is necessary to be performed to passed range.
 */
template <typename TResponseType, typename TRequestConstructor>
class TForcedOperationActor final
    : public TActorBootstrapped<
          TForcedOperationActor<TResponseType, TRequestConstructor>>
{
private:
    using TBase = NActors::TActorBootstrapped<
        TForcedOperationActor<TResponseType, TRequestConstructor>>;

    const TActorId Tablet;
    const TString LogTag;
    const TDuration RetryTimeout;

    TIndexTabletState::TForcedRangeOperationState State;
    const TRequestInfoPtr RequestInfo;

public:
    TForcedOperationActor(
        TActorId tablet,
        TString logTag,
        TDuration retry,
        TIndexTabletState::TForcedRangeOperationState state,
        TRequestInfoPtr requestInfo);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void SendRangeOperationRequest(const TActorContext& ctx);

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

    void ReportProgress(const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

template <typename TResponseType, typename TRequestConstructor>
TForcedOperationActor<TResponseType, TRequestConstructor>::
    TForcedOperationActor(
        TActorId tablet,
        TString logTag,
        TDuration retry,
        TIndexTabletState::TForcedRangeOperationState state,
        TRequestInfoPtr requestInfo)
    : Tablet(tablet)
    , LogTag(std::move(logTag))
    , RetryTimeout(retry)
    , State(std::move(state))
    , RequestInfo(std::move(requestInfo))
{}

template <typename TResponseType, typename TRequestConstructor>
void TForcedOperationActor<TResponseType, TRequestConstructor>::Bootstrap(
    const TActorContext& ctx)
{
    TBase::Become(&TBase::TThis::StateWork);

    FILESTORE_TRACK(
        RequestReceived_TabletWorker,
        RequestInfo->CallContext,
        "ForcedRangeOperation");

    SendRangeOperationRequest(ctx);
}

template <typename TResponseType, typename TRequestConstructor>
void TForcedOperationActor<TResponseType, TRequestConstructor>::
    SendRangeOperationRequest(const TActorContext& ctx)
{
    auto request = TRequestConstructor()(State.GetCurrentRange());

    ctx.Send(Tablet, request.release());
}

template <typename TResponseType, typename TRequestConstructor>
STFUNC(
    (TForcedOperationActor<TResponseType, TRequestConstructor>::StateWork))
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
void TForcedOperationActor<TResponseType, TRequestConstructor>::
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

    if (!State.Progress()) {
        return ReplyAndDie(ctx, {});
    }

    ReportProgress(ctx);
    SendRangeOperationRequest(ctx);
}

template <typename TResponseType, typename TRequestConstructor>
void TForcedOperationActor<TResponseType, TRequestConstructor>::
    HandleWakeUp(const TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx)
{
    Y_UNUSED(ev);
    SendRangeOperationRequest(ctx);
}

template <typename TResponseType, typename TRequestConstructor>
void TForcedOperationActor<TResponseType, TRequestConstructor>::
    HandlePoisonPill(
        const TEvents::TEvPoison::TPtr& ev,
        const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_FAIL, "actor killed"));
}

template <typename TResponseType, typename TRequestConstructor>
void TForcedOperationActor<TResponseType, TRequestConstructor>::
    ReplyAndDie(const TActorContext& ctx, const NProto::TError& error)
{
    {
        // notify tablet
        auto response = std::make_unique<
            TEvIndexTabletPrivate::TEvForcedRangeOperationCompleted>(error);
        NCloud::Send(ctx, Tablet, std::move(response));
    }

    FILESTORE_TRACK(
        ResponseSent_TabletWorker,
        RequestInfo->CallContext,
        "ForcedRangeOperation");

    if (RequestInfo->Sender != Tablet) {
        // reply to caller
        auto response = std::make_unique<
            TEvIndexTabletPrivate::TEvForcedRangeOperationResponse>(error);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
    }

    TBase::Die(ctx);
}

template <typename TResponseType, typename TRequestConstructor>
void TForcedOperationActor<TResponseType, TRequestConstructor>::
    ReportProgress(const TActorContext& ctx)
{
    using TEvent = TEvIndexTabletPrivate::TEvForcedRangeOperationProgress;
    NCloud::Send(ctx, Tablet, std::make_unique<TEvent>(State.Current));
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

using TForcedCompactionActor = TForcedOperationActor<
    TEvIndexTabletPrivate::TEvCompactionResponse,
    TCompactionRequestConstructor>;

using TForcedCleanupActor = TForcedOperationActor<
    TEvIndexTabletPrivate::TEvCleanupResponse,
    TCleanupRequestConstructor>;

using TDeleteRangesWithEmptyScoreActor = TForcedOperationActor<
    TEvIndexTabletPrivate::TEvDeleteZeroCompactionRangesResponse,
    TDeleteZeroCompactionRangesRequestConstructor>;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::EnqueueForcedRangeOperationIfNeeded(
    const TActorContext& ctx)
{
    if (IsForcedRangeOperationRunning()) {
        return;
    }

    auto pendingRequest = DequeueForcedRangeOperation();
    if (pendingRequest.Ranges.empty()) {
        return;
    }

    auto request =
        std::make_unique<TEvIndexTabletPrivate::TEvForcedRangeOperationRequest>(
            std::move(pendingRequest.Ranges),
            pendingRequest.Mode,
            std::move(pendingRequest.OperationId));
    ctx.Send(ctx.SelfID, request.release());
}

void TIndexTabletActor::HandleForcedRangeOperation(
    const TEvIndexTabletPrivate::TEvForcedRangeOperationRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s ForcedRangeOperation request for %lu ranges",
        LogTag.c_str(),
        msg->Ranges.size());

    auto replyError = [&] (
        const NProto::TError& error)
    {
        if (ev->Sender == ctx.SelfID) {
            return;
        }

        auto response = std::make_unique<
            TEvIndexTabletPrivate::TEvForcedRangeOperationResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
    };

    if (msg->Ranges.empty() || msg->Ranges.size() > Max<ui32>()) {
        replyError(ErrorInvalidArgument());
        return;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    // will lose original request info in case of enqueueing external request
    if (IsForcedRangeOperationRunning()) {
        EnqueueForcedRangeOperation(msg->Mode, std::move(msg->Ranges));
        return;
    }

    StartForcedRangeOperation(
        msg->Mode,
        std::move(msg->Ranges),
        std::move(msg->OperationId));

    std::unique_ptr<IActor> actor;

    switch (msg->Mode) {
        case TEvIndexTabletPrivate::EForcedRangeOperationMode::Compaction:
            actor = std::make_unique<TForcedCompactionActor>(
                ctx.SelfID,
                LogTag,
                Config->GetCompactionRetryTimeout(),
                *GetForcedRangeOperationState(),
                std::move(requestInfo));
            break;

        case TEvIndexTabletPrivate::EForcedRangeOperationMode::Cleanup:
            actor = std::make_unique<TForcedCleanupActor>(
                ctx.SelfID,
                LogTag,
                Config->GetCompactionRetryTimeout(),
                *GetForcedRangeOperationState(),
                std::move(requestInfo));
            break;
        case TEvIndexTabletPrivate::EForcedRangeOperationMode::DeleteZeroCompactionRanges:
            actor = std::make_unique<TDeleteRangesWithEmptyScoreActor>(
                ctx.SelfID,
                LogTag,
                Config->GetCompactionRetryTimeout(),
                *GetForcedRangeOperationState(),
                std::move(requestInfo));
            break;

        default:
            TABLET_VERIFY_C(false, "unexpected forced compaction mode");
    }

    auto actorId = ctx.Register(actor.release());
    WorkerActors.insert(actorId);
}

void TIndexTabletActor::HandleForcedRangeOperationCompleted(
    const TEvIndexTabletPrivate::TEvForcedRangeOperationCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s ForcedRangeOperation completed (%s)",
        LogTag.c_str(),
        FormatError(msg->GetError()).c_str());

    TABLET_VERIFY(IsForcedRangeOperationRunning());
    WorkerActors.erase(ev->Sender);

    CompleteForcedRangeOperation();
    EnqueueForcedRangeOperationIfNeeded(ctx);
}

void TIndexTabletActor::HandleForcedRangeOperationProgress(
    const TEvIndexTabletPrivate::TEvForcedRangeOperationProgress::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    if (IsForcedRangeOperationRunning()) {
        UpdateForcedRangeOperationProgress(ev->Get()->Current);
    }
}

}   // namespace NCloud::NFileStore::NStorage
