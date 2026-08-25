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
 * @brief An actor that performs various forced bookkeeping operations. It is
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

    TIndexTabletState::TForcedOperationState State;
    const TRequestInfoPtr RequestInfo;

public:
    TForcedOperationActor(
        TActorId tablet,
        TString logTag,
        TDuration retry,
        TIndexTabletState::TForcedOperationState state,
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
        TIndexTabletState::TForcedOperationState state,
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
        "ForcedOperation");

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
            TEvIndexTabletPrivate::TEvForcedOperationCompleted>(error);
        NCloud::Send(ctx, Tablet, std::move(response));
    }

    FILESTORE_TRACK(
        ResponseSent_TabletWorker,
        RequestInfo->CallContext,
        "ForcedOperation");

    if (RequestInfo->Sender != Tablet) {
        // reply to caller
        auto response = std::make_unique<
            TEvIndexTabletPrivate::TEvForcedOperationResponse>(error);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
    }

    TBase::Die(ctx);
}

template <typename TResponseType, typename TRequestConstructor>
void TForcedOperationActor<TResponseType, TRequestConstructor>::
    ReportProgress(const TActorContext& ctx)
{
    using TEvent = TEvIndexTabletPrivate::TEvForcedOperationProgress;
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

void TIndexTabletActor::EnqueueForcedOperationIfNeeded(
    const TActorContext& ctx)
{
    if (IsForcedOperationRunning()) {
        return;
    }

    auto pendingRequest = DequeueForcedOperation();
    if (pendingRequest.Ranges.empty()) {
        return;
    }

    auto request =
        std::make_unique<TEvIndexTabletPrivate::TEvForcedOperationRequest>(
            std::move(pendingRequest.Ranges),
            pendingRequest.Mode,
            std::move(pendingRequest.OperationId));
    ctx.Send(ctx.SelfID, request.release());
}

void TIndexTabletActor::HandleForcedOperation(
    const TEvIndexTabletPrivate::TEvForcedOperationRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s ForcedOperation request for %lu ranges",
        LogTag.c_str(),
        msg->Ranges.size());

    auto replyError = [&] (
        const NProto::TError& error)
    {
        if (ev->Sender == ctx.SelfID) {
            return;
        }

        auto response = std::make_unique<
            TEvIndexTabletPrivate::TEvForcedOperationResponse>(error);
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
    if (IsForcedOperationRunning()) {
        EnqueueForcedOperation(msg->Mode, std::move(msg->Ranges));
        return;
    }

    StartForcedOperation(
        msg->Mode,
        std::move(msg->Ranges),
        std::move(msg->OperationId));

    std::unique_ptr<IActor> actor;

    switch (msg->Mode) {
        case TEvIndexTabletPrivate::EForcedOperationMode::Compaction:
            actor = std::make_unique<TForcedCompactionActor>(
                ctx.SelfID,
                LogTag,
                Config->GetCompactionRetryTimeout(),
                *GetForcedOperationState(),
                std::move(requestInfo));
            break;

        case TEvIndexTabletPrivate::EForcedOperationMode::Cleanup:
            actor = std::make_unique<TForcedCleanupActor>(
                ctx.SelfID,
                LogTag,
                Config->GetCompactionRetryTimeout(),
                *GetForcedOperationState(),
                std::move(requestInfo));
            break;
        case TEvIndexTabletPrivate::EForcedOperationMode::DeleteZeroCompactionRanges:
            actor = std::make_unique<TDeleteRangesWithEmptyScoreActor>(
                ctx.SelfID,
                LogTag,
                Config->GetCompactionRetryTimeout(),
                *GetForcedOperationState(),
                std::move(requestInfo));
            break;

        default:
            TABLET_VERIFY_C(false, "unexpected forced compaction mode");
    }

    auto actorId = ctx.Register(actor.release());
    WorkerActors.insert(actorId);
}

void TIndexTabletActor::HandleForcedOperationCompleted(
    const TEvIndexTabletPrivate::TEvForcedOperationCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s ForcedOperation completed (%s)",
        LogTag.c_str(),
        FormatError(msg->GetError()).c_str());

    TABLET_VERIFY(IsForcedOperationRunning());
    WorkerActors.erase(ev->Sender);

    CompleteForcedOperation();
    EnqueueForcedOperationIfNeeded(ctx);
}

void TIndexTabletActor::HandleForcedOperationProgress(
    const TEvIndexTabletPrivate::TEvForcedOperationProgress::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    if (IsForcedOperationRunning()) {
        UpdateForcedOperationProgress(ev->Get()->Current);
    }
}

}   // namespace NCloud::NFileStore::NStorage
