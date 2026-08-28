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
 * @brief An actor that performs forced flush, flush_bytes, collect_garbage ops.
 * It is implemented as a template class to avoid code duplication.
 */
template <typename TResponseType, typename TRequestType>
class TForcedOperationActor final
    : public TActorBootstrapped<
          TForcedOperationActor<TResponseType, TRequestType>>
{
private:
    using TBase = NActors::TActorBootstrapped<
        TForcedOperationActor<TResponseType, TRequestType>>;

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

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error);
};

////////////////////////////////////////////////////////////////////////////////

template <typename TResponseType, typename TRequestType>
TForcedOperationActor<TResponseType, TRequestType>::
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

template <typename TResponseType, typename TRequestType>
void TForcedOperationActor<TResponseType, TRequestType>::Bootstrap(
    const TActorContext& ctx)
{
    TBase::Become(&TBase::TThis::StateWork);

    FILESTORE_TRACK(
        RequestReceived_TabletWorker,
        RequestInfo->CallContext,
        "ForcedTabletOperation");

    SendOperationRequest(ctx);
}

template <typename TResponseType, typename TRequestType>
void TForcedOperationActor<TResponseType, TRequestType>::
    SendOperationRequest(const TActorContext& ctx)
{
    auto request = std::make_unique<TRequestType>();
    ctx.Send(Tablet, request.release());
}

template <typename TResponseType, typename TRequestType>
STFUNC(
    (TForcedOperationActor<TResponseType, TRequestType>::StateWork))
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

template <typename TResponseType, typename TRequestType>
void TForcedOperationActor<TResponseType, TRequestType>::
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

    return ReplyAndDie(ctx, {});
}

template <typename TResponseType, typename TRequestType>
void TForcedOperationActor<TResponseType, TRequestType>::
    HandleWakeUp(const TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx)
{
    Y_UNUSED(ev);
    SendOperationRequest(ctx);
}

template <typename TResponseType, typename TRequestType>
void TForcedOperationActor<TResponseType, TRequestType>::
    HandlePoisonPill(
        const TEvents::TEvPoison::TPtr& ev,
        const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_FAIL, "actor killed"));
}

template <typename TResponseType, typename TRequestType>
void TForcedOperationActor<TResponseType, TRequestType>::
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
    TEvIndexTabletPrivate::TEvFlushBytesRequest>;

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

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s ForcedTabletOperation mode=%u request",
        LogTag.c_str(),
        msg->Mode);

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    // will lose original request info in case of enqueueing external request
    if (IsForcedOperationRunning()) {
        EnqueueForcedTabletOperation(msg->Mode);
        return;
    }

    StartForcedTabletOperation(
        msg->Mode,
        std::move(msg->OperationId));

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
        default:
            TABLET_VERIFY_C(false, "unexpected forced tablet operation mode");
    }

    auto actorId = ctx.Register(actor.release());
    WorkerActors.insert(actorId);
}

void TIndexTabletActor::HandleForcedTabletOperationCompleted(
    const TEvIndexTabletPrivate::TEvForcedTabletOperationCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    TABLET_VERIFY(IsForcedOperationRunning());
    const auto* state =
        std::get_if<TForcedTabletOperationState>(GetForcedOperationState());
    TABLET_VERIFY(state);

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s ForcedTabletOperation mode=%u completed (%s)",
        LogTag.c_str(),
        state->Mode,
        FormatError(msg->GetError()).c_str());

    WorkerActors.erase(ev->Sender);

    CompleteForcedTabletOperation();
    EnqueueForcedOperationIfNeeded(ctx);
}

}   // namespace NCloud::NFileStore::NStorage
