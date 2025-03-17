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
        .Uuid = CreateGuidAsString(),
        .FollowerDiskId = args.FollowerDiskId,
        .ScaleUnitId = "",
        .MigrationBlockIndex = std::nullopt};

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
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
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
    Y_DEBUG_ABORT_UNLESS(!args.Id.empty());

    auto follower = State->FindFollowerByUuid(args.Id);
    Y_DEBUG_ABORT_UNLESS(follower);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "[%lu] Remove follower link %s <- %s",
        TabletID(),
        State->GetDiskId().c_str(),
        follower->FollowerDiskId.c_str());

    TVolumeDatabase db(tx.DB);
    State->RemoveFollower(args.Id);
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
        follower->Uuid);
}

}   // namespace NCloud::NBlockStore::NStorage
