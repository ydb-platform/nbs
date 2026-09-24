#include "tablet_actor.h"

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TDefaultCompletionPolicy
{
    static bool IsCompleted(const NProto::TError& error)
    {
        Y_UNUSED(error);
        return true;
    }
};

struct TFlushBytesCompletionPolicy
{
    static bool IsCompleted(const NProto::TError& error)
    {
        // A successful flush processes only one chunk and may reply before
        // trimming it. Only an empty flush confirms that all chunks are
        // drained.
        return error.GetCode() == S_FALSE;
    }
};

////////////////////////////////////////////////////////////////////////////////

/**
 * @brief An actor that performs forced flush, flush_bytes, collect_garbage ops.
 * It is implemented as a template class to avoid code duplication.
 *
 * @tparam TCompletionPolicy Determines whether a successful response completes
 * the forced operation. Errors use the same retry policy for all operations.
 */
template <
    typename TResponseType,
    typename TRequestType,
    typename TCompletionPolicy = TDefaultCompletionPolicy>
class TForcedOperationActor final
    : public TActorBootstrapped<
          TForcedOperationActor<TResponseType, TRequestType, TCompletionPolicy>>
{
private:
    using TBase = NActors::TActorBootstrapped<
        TForcedOperationActor<TResponseType, TRequestType, TCompletionPolicy>>;

    const TActorId Tablet;
    const TString LogTag;
    const TDuration RetryTimeout;

    const TRequestInfoPtr RequestInfo;

public:
    TForcedOperationActor(
        TActorId tablet,
        TString logTag,
        TDuration retry,
        TRequestInfoPtr requestInfo);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void SendOperationRequest(const TActorContext& ctx);

    void HandleOperationResponse(
        const TResponseType::TPtr& ev,
        const TActorContext& ctx);

    void HandleWakeUp(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(const TActorContext& ctx, const NProto::TError& error);
};

////////////////////////////////////////////////////////////////////////////////

template <
    typename TResponseType,
    typename TRequestType,
    typename TCompletionPolicy>
TForcedOperationActor<TResponseType, TRequestType, TCompletionPolicy>::
    TForcedOperationActor(
        TActorId tablet,
        TString logTag,
        TDuration retry,
        TRequestInfoPtr requestInfo)
    : Tablet(tablet)
    , LogTag(std::move(logTag))
    , RetryTimeout(retry)
    , RequestInfo(std::move(requestInfo))
{}

template <
    typename TResponseType,
    typename TRequestType,
    typename TCompletionPolicy>
void TForcedOperationActor<TResponseType, TRequestType, TCompletionPolicy>::
    Bootstrap(const TActorContext& ctx)
{
    TBase::Become(&TBase::TThis::StateWork);

    FILESTORE_TRACK(
        RequestReceived_TabletWorker,
        RequestInfo->CallContext,
        "ForcedTabletOperation");

    SendOperationRequest(ctx);
}

template <
    typename TResponseType,
    typename TRequestType,
    typename TCompletionPolicy>
void TForcedOperationActor<TResponseType, TRequestType, TCompletionPolicy>::
    SendOperationRequest(const TActorContext& ctx)
{
    auto request = std::make_unique<TRequestType>();
    ctx.Send(Tablet, request.release());
}

template <
    typename TResponseType,
    typename TRequestType,
    typename TCompletionPolicy>
STFUNC((TForcedOperationActor<TResponseType, TRequestType, TCompletionPolicy>::
            StateWork))
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleWakeUp);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TResponseType, HandleOperationResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::TABLET_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

template <
    typename TResponseType,
    typename TRequestType,
    typename TCompletionPolicy>
void TForcedOperationActor<TResponseType, TRequestType, TCompletionPolicy>::
    HandleOperationResponse(
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

    if (TCompletionPolicy::IsCompleted(msg->Error)) {
        return ReplyAndDie(ctx, {});
    }

    SendOperationRequest(ctx);
}

template <
    typename TResponseType,
    typename TRequestType,
    typename TCompletionPolicy>
void TForcedOperationActor<TResponseType, TRequestType, TCompletionPolicy>::
    HandleWakeUp(const TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx)
{
    Y_UNUSED(ev);
    SendOperationRequest(ctx);
}

template <
    typename TResponseType,
    typename TRequestType,
    typename TCompletionPolicy>
void TForcedOperationActor<TResponseType, TRequestType, TCompletionPolicy>::
    HandlePoisonPill(
        const TEvents::TEvPoison::TPtr& ev,
        const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_FAIL, "actor killed"));
}

template <
    typename TResponseType,
    typename TRequestType,
    typename TCompletionPolicy>
void TForcedOperationActor<TResponseType, TRequestType, TCompletionPolicy>::
    ReplyAndDie(const TActorContext& ctx, const NProto::TError& error)
{
    {
        // notify tablet
        auto response = std::make_unique<
            TEvIndexTabletPrivate::TEvForcedTabletOperationCompleted>(error);
        NCloud::Send(ctx, Tablet, std::move(response));
    }

    FILESTORE_TRACK(
        ResponseSent_TabletWorker,
        RequestInfo->CallContext,
        "ForcedTabletOperation");

    if (RequestInfo->Sender != Tablet) {
        // reply to caller
        auto response = std::make_unique<
            TEvIndexTabletPrivate::TEvForcedTabletOperationResponse>(error);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
    }

    TBase::Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

using TForcedFlushActor = TForcedOperationActor<
    TEvIndexTabletPrivate::TEvFlushResponse,
    TEvIndexTabletPrivate::TEvFlushRequest>;

using TForcedFlushBytesActor = TForcedOperationActor<
    TEvIndexTabletPrivate::TEvFlushBytesResponse,
    TEvIndexTabletPrivate::TEvFlushBytesRequest,
    TFlushBytesCompletionPolicy>;

using TForcedCollectGarbageActor = TForcedOperationActor<
    TEvIndexTabletPrivate::TEvCollectGarbageResponse,
    TEvIndexTabletPrivate::TEvCollectGarbageRequest>;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleForcedTabletOperation(
    const TEvIndexTabletPrivate::TEvForcedTabletOperationRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s ForcedTabletOperation mode=%u request",
        LogTag.c_str(),
        msg->Mode);

    auto replyError = [&](const NProto::TError& error)
    {
        AbortForcedTabletOperation(
            msg->Mode,
            std::move(msg->OperationId),
            error);

        if (ev->Sender == ctx.SelfID) {
            return;
        }

        auto response = std::make_unique<
            TEvIndexTabletPrivate::TEvForcedTabletOperationResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
    };

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    if (IsForcedOperationRunning()) {
        EnqueueForcedTabletOperation(msg->Mode, std::move(msg->OperationId));
        return;
    }

    const auto* state =
        StartForcedTabletOperation(msg->Mode, std::move(msg->OperationId));
    if (!state) {
        replyError(MakeError(E_INVALID_STATE, "could not start the operation"));
        return;
    }

    std::unique_ptr<IActor> actor;

    switch (msg->Mode) {
        case TEvIndexTabletPrivate::EForcedTabletOperationMode::Flush:
            actor = std::make_unique<TForcedFlushActor>(
                ctx.SelfID,
                LogTag,
                Config->GetCompactionRetryTimeout(),
                std::move(requestInfo));
            break;
        case TEvIndexTabletPrivate::EForcedTabletOperationMode::FlushBytes:
            actor = std::make_unique<TForcedFlushBytesActor>(
                ctx.SelfID,
                LogTag,
                Config->GetCompactionRetryTimeout(),
                std::move(requestInfo));
            break;
        case TEvIndexTabletPrivate::EForcedTabletOperationMode::CollectGarbage:
            actor = std::make_unique<TForcedCollectGarbageActor>(
                ctx.SelfID,
                LogTag,
                Config->GetCompactionRetryTimeout(),
                std::move(requestInfo));
            break;
    }

    auto actorId = ctx.Register(actor.release());
    WorkerActors.insert(actorId);
}

void TIndexTabletActor::HandleForcedTabletOperationCompleted(
    const TEvIndexTabletPrivate::TEvForcedTabletOperationCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    if (!IsForcedOperationRunning()) {
        ReportForcedOperationUnexpectedState(
            "got ForcedTabletOperationCompleted but no current op");
        return;
    }

    auto* msg = ev->Get();
    const auto* state =
        std::get_if<TForcedTabletOperationState>(GetForcedOperationState());
    if (!state) {
        ReportForcedOperationUnexpectedState(
            "got ForcedTabletOperationCompleted but current op is a range op");
        return;
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s ForcedTabletOperation mode=%u completed (%s)",
        LogTag.c_str(),
        state->Mode,
        FormatError(msg->GetError()).c_str());

    WorkerActors.erase(ev->Sender);

    CompleteForcedOperation(msg->GetError());
    EnqueueForcedOperationIfNeeded(ctx);
}

}   // namespace NCloud::NFileStore::NStorage
