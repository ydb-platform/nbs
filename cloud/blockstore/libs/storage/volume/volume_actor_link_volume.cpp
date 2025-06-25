#include "volume_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::PrepareAddFollower(
    const TActorContext& ctx,
    ITransactionBase::TTransactionContext& tx,
    TTxVolume::TAddFollower& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    args.LinkUUID = CreateGuidAsString();
    return true;
}

void TVolumeActor::ExecuteAddFollower(
    const TActorContext& ctx,
    ITransactionBase::TTransactionContext& tx,
    TTxVolume::TAddFollower& args)
{
    Y_DEBUG_ABORT_UNLESS(!args.FollowerDiskId.empty());

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "[%lu] Save follower link %s <- %s",
        TabletID(),
        State->GetDiskId().c_str(),
        args.FollowerDiskId.c_str());

    TVolumeDatabase db(tx.DB);

    auto newFollower = TFollowerDiskInfo{
        .LinkUUID = args.LinkUUID,
        .FollowerDiskId = args.FollowerDiskId,
        .ScaleUnitId = "",
        .MigratedBytes = std::nullopt};

    db.WriteFollower(newFollower);
    State->AddOrUpdateFollower(std::move(newFollower));
}

void TVolumeActor::CompleteAddFollower(
    const TActorContext& ctx,
    TTxVolume::TAddFollower& args)
{
    auto response =
        std::make_unique<TEvVolume::TEvLinkLeaderVolumeToFollowerResponse>(
            MakeError(S_OK));
    response->Record.SetLinkUUID(args.LinkUUID);
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    RestartPartition(ctx, {});
}

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::PrepareRemoveFollower(
    const TActorContext& ctx,
    ITransactionBase::TTransactionContext& tx,
    TTxVolume::TRemoveFollower& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TVolumeActor::ExecuteRemoveFollower(
    const TActorContext& ctx,
    ITransactionBase::TTransactionContext& tx,
    TTxVolume::TRemoveFollower& args)
{
    Y_DEBUG_ABORT_UNLESS(!args.LinkUUID.empty());

    auto follower = State->FindFollowerByUuid(args.LinkUUID);
    Y_DEBUG_ABORT_UNLESS(follower);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "[%lu] Remove follower link %s <- %s",
        TabletID(),
        State->GetDiskId().c_str(),
        follower->FollowerDiskId.c_str());

    TVolumeDatabase db(tx.DB);
    State->RemoveFollower(args.LinkUUID);
    db.DeleteFollower(*follower);
}

void TVolumeActor::CompleteRemoveFollower(
    const TActorContext& ctx,
    TTxVolume::TRemoveFollower& args)
{
    auto response =
        std::make_unique<TEvVolume::TEvUnlinkLeaderVolumeFromFollowerResponse>(
            MakeError(S_OK));
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    RestartPartition(ctx, {});
}

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::PrepareUpdateFollower(
    const TActorContext& ctx,
    ITransactionBase::TTransactionContext& tx,
    TTxVolume::TUpdateFollower& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TVolumeActor::ExecuteUpdateFollower(
    const TActorContext& ctx,
    ITransactionBase::TTransactionContext& tx,
    TTxVolume::TUpdateFollower& args)
{
    auto current = State->FindFollowerByUuid(args.FollowerInfo.LinkUUID);
    if (!current) {
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "[%lu] Update follower %s <- %s: %s -> %s",
        TabletID(),
        State->GetDiskId().Quote().c_str(),
        args.FollowerInfo.GetDiskIdForPrint().Quote().c_str(),
        current->Describe().c_str(),
        args.FollowerInfo.Describe().c_str());

    TVolumeDatabase db(tx.DB);
    State->AddOrUpdateFollower(args.FollowerInfo);
    db.WriteFollower(args.FollowerInfo);
}

void TVolumeActor::CompleteUpdateFollower(
    const TActorContext& ctx,
    TTxVolume::TUpdateFollower& args)
{
    auto response =
        std::make_unique<TEvVolumePrivate::TEvUpdateFollowerStateResponse>(
            MakeError(S_OK));
    response->NewState = args.FollowerInfo.State;
    response->MigratedBytes = args.FollowerInfo.MigratedBytes;
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleLinkLeaderVolumeToFollower(
    const TEvVolume::TEvLinkLeaderVolumeToFollowerRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "[%lu] Add follower link %s <- %s",
        TabletID(),
        State->GetDiskId().Quote().c_str(),
        msg->Record.GetFollowerDiskId().Quote().c_str());

    if (State->FindFollowerByDiskId(msg->Record.GetFollowerDiskId())) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvVolume::TEvLinkLeaderVolumeToFollowerResponse>(
                MakeError(S_ALREADY)));
        return;
    }

    ExecuteTx<TAddFollower>(
        ctx,
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        msg->Record.GetFollowerDiskId());
}

void TVolumeActor::HandleUnlinkLeaderVolumeFromFollower(
    const TEvVolume::TEvUnlinkLeaderVolumeFromFollowerRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "[%lu] Remove follower link %s <- %s",
        TabletID(),
        State->GetDiskId().Quote().c_str(),
        msg->Record.GetFollowerDiskId().Quote().c_str());

    auto follower =
        State->FindFollowerByDiskId(msg->Record.GetFollowerDiskId());
    if (!follower) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<
                TEvVolume::TEvUnlinkLeaderVolumeFromFollowerResponse>(
                MakeError(S_ALREADY)));
        return;
    }

    ExecuteTx<TRemoveFollower>(
        ctx,
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        follower->LinkUUID);
}

void TVolumeActor::HandleUpdateFollowerState(
    const TEvVolumePrivate::TEvUpdateFollowerStateRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    using EReason = TEvVolumePrivate::TUpdateFollowerStateRequest::EReason;
    using EState = TFollowerDiskInfo::EState;

    auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    auto followerInfo = State->FindFollowerByUuid(msg->FollowerUuid);

    auto reply = [&](EState newState)
    {
        auto response = std::make_unique<
            TEvVolumePrivate::TEvUpdateFollowerStateResponse>(
            newState,
            msg->MigratedBytes);
        NCloud::Reply(ctx, *ev, std::move(response));
    };

    if (!followerInfo) {
        reply(EState::Error);
        return;
    }

    auto currentState = followerInfo->State;
    if (currentState == EState::Error) {
        reply(EState::Error);
        return;
    }

    EState newState = EState::None;

    switch (msg->Reason) {
        case EReason::FillProgressUpdate: {
            if (currentState == EState::Ready) {
                reply(EState::Ready);
                return;
            }
            newState = EState::Preparing;
            break;
        }
        case EReason::FillCompleted: {
            newState = EState::Ready;
            break;
        }
        case EReason::FillError: {
            newState = EState::Error;
            break;
        }
    }
    Y_DEBUG_ABORT_UNLESS(newState != EState::None);

    followerInfo->State = newState;
    followerInfo->MigratedBytes = msg->MigratedBytes;

    ExecuteTx<TUpdateFollower>(
        ctx,
        std::move(requestInfo),
        std::move(*followerInfo));
}

}   // namespace NCloud::NBlockStore::NStorage
