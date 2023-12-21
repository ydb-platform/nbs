#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log.h>
#include <cloud/filestore/libs/storage/tablet/model/blob_builder.h>
#include <cloud/filestore/libs/storage/tablet/model/block_buffer.h>
#include <cloud/filestore/libs/storage/tablet/tablet_state.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TForcedCompactionActor final
    : public TActorBootstrapped<TForcedCompactionActor>
{
private:
    const TActorId Tablet;
    const TString LogTag;
    const TDuration RetryTimeout;

    const TIndexTabletState::TForcedCompactionStatePtr State;
    const TRequestInfoPtr RequestInfo;

public:
    TForcedCompactionActor(
        TActorId tablet,
        TString logTag,
        TDuration retry,
        TIndexTabletState::TForcedCompactionStatePtr state,
        TRequestInfoPtr requestInfo);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void SendCompactionRequest(const TActorContext& ctx);

    void HandleCompactionResponse(
        const TEvIndexTabletPrivate::TEvCompactionResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleWakeUp(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error = {});
};

////////////////////////////////////////////////////////////////////////////////

TForcedCompactionActor::TForcedCompactionActor(
        TActorId tablet,
        TString logTag,
        TDuration retry,
        TIndexTabletState::TForcedCompactionStatePtr state,
        TRequestInfoPtr requestInfo)
    : Tablet(tablet)
    , LogTag(std::move(logTag))
    , RetryTimeout(retry)
    , State(std::move(state))
    , RequestInfo(std::move(requestInfo))
{
    ActivityType = TFileStoreActivities::TABLET_WORKER;
}

void TForcedCompactionActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    FILESTORE_TRACK(
        RequestReceived_TabletWorker,
        RequestInfo->CallContext,
        "ForcedCompaction");

    SendCompactionRequest(ctx);
}

void TForcedCompactionActor::SendCompactionRequest(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvIndexTabletPrivate::TEvCompactionRequest>(
        State->GetCurrentRange(), true);

    ctx.Send(Tablet, request.release());
}

STFUNC(TForcedCompactionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleWakeUp);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvIndexTabletPrivate::TEvCompactionResponse, HandleCompactionResponse);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::TABLET_WORKER);
            break;
    }
}

void TForcedCompactionActor::HandleCompactionResponse(
    const TEvIndexTabletPrivate::TEvCompactionResponse::TPtr& ev,
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

    if (!State->Progress()) {
        return ReplyAndDie(ctx, {});
    }

    SendCompactionRequest(ctx);
}

void TForcedCompactionActor::HandleWakeUp(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    SendCompactionRequest(ctx);
}

void TForcedCompactionActor::HandlePoisonPill(
    const TEvents::TEvPoison::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_FAIL, "actor killed"));
}

void TForcedCompactionActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    {
        // notify tablet
        auto response = std::make_unique<TEvIndexTabletPrivate::TEvForcedCompactionCompleted>(
            error);
        NCloud::Send(ctx, Tablet, std::move(response));
    }

    FILESTORE_TRACK(
        ResponseSent_TabletWorker,
        RequestInfo->CallContext,
        "ForcedCompaction");

    if (RequestInfo->Sender != Tablet) {
        // reply to caller
        auto response = std::make_unique<TEvIndexTabletPrivate::TEvForcedCompactionResponse>(
            error);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
    }

    Die(ctx);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::EnqueueForcedCompactionIfNeeded(const TActorContext& ctx)
{
    if (IsForcedCompactionRunning()) {
        return;
    }

    auto ranges = DequeueForcedCompaction();
    if (ranges.empty()) {
        return;
    }

    auto request =
        std::make_unique<TEvIndexTabletPrivate::TEvForcedCompactionRequest>(std::move(ranges));
    ctx.Send(ctx.SelfID, request.release());
}

void TIndexTabletActor::HandleForcedCompaction(
    const TEvIndexTabletPrivate::TEvForcedCompactionRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s ForcedCompaction request for %lu ranges",
        LogTag.c_str(),
        msg->Ranges.size());

    auto replyError = [&] (
        const NProto::TError& error)
    {
        if (ev->Sender == ctx.SelfID) {
            return;
        }

        auto response =
            std::make_unique<TEvIndexTabletPrivate::TEvForcedCompactionResponse>(error);
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

    // will loose original request info in case of enqueueing external request
    if (IsForcedCompactionRunning()) {
        EnqueueForcedCompaction(std::move(msg->Ranges));
        return;
    }

    StartForcedCompaction(std::move(msg->Ranges));

    auto actor = std::make_unique<TForcedCompactionActor>(
        ctx.SelfID,
        LogTag,
        Config->GetCompactionRetryTimeout(),
        GetForcedCompactionState(),
        std::move(requestInfo));

    auto actorId = ctx.Register(actor.release());
    WorkerActors.insert(actorId);
}

void TIndexTabletActor::HandleForcedCompactionCompleted(
    const TEvIndexTabletPrivate::TEvForcedCompactionCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s ForcedCompaction completed (%s)",
        LogTag.c_str(),
        FormatError(msg->GetError()).c_str());

    TABLET_VERIFY(IsForcedCompactionRunning());
    WorkerActors.erase(ev->Sender);

    CompleteForcedCompaction();
    EnqueueForcedCompactionIfNeeded(ctx);
}

}   // namespace NCloud::NFileStore::NStorage
