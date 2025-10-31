#include "tablet_actor.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDestroyCheckpointActor final
    : public TActorBootstrapped<TDestroyCheckpointActor>
{
private:
    const TActorId Tablet;
    const TRequestInfoPtr RequestInfo;
    const TString CheckpointId;

public:
    TDestroyCheckpointActor(
        const TActorId& tablet,
        TRequestInfoPtr requestInfo,
        TString checkpointId);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateMarkCheckpointDeleted);
    STFUNC(StateRemoveCheckpointNodes);
    STFUNC(StateRemoveCheckpointBlobs);
    STFUNC(StateRemoveCheckpoint);

    void DeleteCheckpoint(const TActorContext& ctx, EDeleteCheckpointMode mode);

    void MarkCheckpointDeleted_HandleDeleteCheckpointResponse(
        const TEvIndexTabletPrivate::TEvDeleteCheckpointResponse::TPtr& ev,
        const TActorContext& ctx);

    void RemoveCheckpointNodes_HandleDeleteCheckpointResponse(
        const TEvIndexTabletPrivate::TEvDeleteCheckpointResponse::TPtr& ev,
        const TActorContext& ctx);

    void RemoveCheckpointBlobs_HandleDeleteCheckpointResponse(
        const TEvIndexTabletPrivate::TEvDeleteCheckpointResponse::TPtr& ev,
        const TActorContext& ctx);

    void RemoveCheckpoint_HandleDeleteCheckpointResponse(
        const TEvIndexTabletPrivate::TEvDeleteCheckpointResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error = {});
};

////////////////////////////////////////////////////////////////////////////////

TDestroyCheckpointActor::TDestroyCheckpointActor(
        const TActorId& tablet,
        TRequestInfoPtr requestInfo,
        TString checkpointId)
    : Tablet(tablet)
    , RequestInfo(std::move(requestInfo))
    , CheckpointId(std::move(checkpointId))
{}

void TDestroyCheckpointActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateMarkCheckpointDeleted);
    DeleteCheckpoint(ctx, EDeleteCheckpointMode::MarkCheckpointDeleted);
}

void TDestroyCheckpointActor::DeleteCheckpoint(
    const TActorContext& ctx,
    EDeleteCheckpointMode mode)
{
    auto request = std::make_unique<TEvIndexTabletPrivate::TEvDeleteCheckpointRequest>(
        CheckpointId,
        mode);

    NCloud::Send(ctx, Tablet, std::move(request));
}

void TDestroyCheckpointActor::MarkCheckpointDeleted_HandleDeleteCheckpointResponse(
    const TEvIndexTabletPrivate::TEvDeleteCheckpointResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (FAILED(msg->GetStatus())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    Become(&TThis::StateRemoveCheckpointNodes);
    DeleteCheckpoint(ctx, EDeleteCheckpointMode::RemoveCheckpointNodes);
}

void TDestroyCheckpointActor::RemoveCheckpointNodes_HandleDeleteCheckpointResponse(
    const TEvIndexTabletPrivate::TEvDeleteCheckpointResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (FAILED(msg->GetStatus())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    if (msg->GetStatus() == S_OK) {
        DeleteCheckpoint(ctx, EDeleteCheckpointMode::RemoveCheckpointNodes);
        return;
    }

    Become(&TThis::StateRemoveCheckpointBlobs);
    DeleteCheckpoint(ctx, EDeleteCheckpointMode::RemoveCheckpointBlobs);
}

void TDestroyCheckpointActor::RemoveCheckpointBlobs_HandleDeleteCheckpointResponse(
    const TEvIndexTabletPrivate::TEvDeleteCheckpointResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (FAILED(msg->GetStatus())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    if (msg->GetStatus() == S_OK) {
        DeleteCheckpoint(ctx, EDeleteCheckpointMode::RemoveCheckpointBlobs);
        return;
    }

    Become(&TThis::StateRemoveCheckpoint);
    DeleteCheckpoint(ctx, EDeleteCheckpointMode::RemoveCheckpoint);
}

void TDestroyCheckpointActor::RemoveCheckpoint_HandleDeleteCheckpointResponse(
    const TEvIndexTabletPrivate::TEvDeleteCheckpointResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    ReplyAndDie(ctx, msg->GetError());
}

void TDestroyCheckpointActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

void TDestroyCheckpointActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    {
        // notify tablet
        using TCompletion = TEvIndexTabletPrivate::TEvDeleteCheckpointCompleted;
        auto response = std::make_unique<TCompletion>(error);
        NCloud::Send(ctx, Tablet, std::move(response));
    }

    if (RequestInfo->Sender != Tablet) {
        // reply to caller
        auto response =
            std::make_unique<TEvService::TEvDestroyCheckpointResponse>(error);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
    }

    Die(ctx);
}

STFUNC(TDestroyCheckpointActor::StateMarkCheckpointDeleted)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvIndexTabletPrivate::TEvDeleteCheckpointResponse,
            MarkCheckpointDeleted_HandleDeleteCheckpointResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::TABLET_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TDestroyCheckpointActor::StateRemoveCheckpointNodes)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvIndexTabletPrivate::TEvDeleteCheckpointResponse,
            RemoveCheckpointNodes_HandleDeleteCheckpointResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::TABLET_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TDestroyCheckpointActor::StateRemoveCheckpointBlobs)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvIndexTabletPrivate::TEvDeleteCheckpointResponse,
            RemoveCheckpointBlobs_HandleDeleteCheckpointResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::TABLET_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TDestroyCheckpointActor::StateRemoveCheckpoint)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvIndexTabletPrivate::TEvDeleteCheckpointResponse,
            RemoveCheckpoint_HandleDeleteCheckpointResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::TABLET_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::RestartCheckpointDestruction(const TActorContext& ctx)
{
    for (const auto* checkpoint: GetCheckpoints()) {
        if (checkpoint->GetDeleted()) {
            auto request =
                std::make_unique<TEvService::TEvDestroyCheckpointRequest>();
            request->Record.SetCheckpointId(checkpoint->GetCheckpointId());

            NCloud::Send(ctx, ctx.SelfID, std::move(request));
        }
    }
}

void TIndexTabletActor::HandleDestroyCheckpoint(
    const TEvService::TEvDestroyCheckpointRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (auto error = IsDataOperationAllowed(); HasError(error)) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvService::TEvDestroyCheckpointResponse>(
                std::move(error)));

        return;
    }

    auto* msg = ev->Get();

    const auto& checkpointId = msg->Record.GetCheckpointId();

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s DestroyCheckpoint started (checkpointId: %s)",
        LogTag.c_str(),
        checkpointId.c_str());

    auto* checkpoint = FindCheckpoint(checkpointId);
    if (!checkpoint) {
        using TResponse = TEvService::TEvDestroyCheckpointResponse;
        auto response = std::make_unique<TResponse>(
            ErrorInvalidCheckpoint(checkpointId));

        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    auto actor = std::make_unique<TDestroyCheckpointActor>(
        ctx.SelfID,
        std::move(requestInfo),
        checkpointId);

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);
}

void TIndexTabletActor::HandleDeleteCheckpointCompleted(
    const TEvIndexTabletPrivate::TEvDeleteCheckpointCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s DestroyCheckpoint completed (%s)",
        LogTag.c_str(),
        FormatError(msg->GetError()).c_str());

    WorkerActors.erase(ev->Sender);
}

}   // namespace NCloud::NFileStore::NStorage
