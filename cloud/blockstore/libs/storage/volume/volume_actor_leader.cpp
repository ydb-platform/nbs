#include "volume_actor.h"

#include <cloud/blockstore/libs/storage/volume/actors/create_volume_link_actor.h>
#include <cloud/blockstore/libs/storage/volume/actors/propagate_to_follower.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

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
    auto current = State->FindFollower(args.FollowerInfo.Link);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "[%lu] Persist follower %s: %s -> %s",
        TabletID(),
        args.FollowerInfo.Link.Describe().c_str(),
        current ? current->Describe().c_str() : "{none}",
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
    response->Follower = args.FollowerInfo;
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
    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "[%lu] Remove follower %s",
        TabletID(),
        args.Link.Describe().c_str());

    TVolumeDatabase db(tx.DB);
    State->RemoveFollower(args.Link);
    db.DeleteFollower(args.Link);
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

void TVolumeActor::HandleLinkLeaderVolumeToFollower(
    const TEvVolume::TEvLinkLeaderVolumeToFollowerRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    auto link = TLeaderFollowerLink{
        .LinkUUID = {},
        .LeaderDiskId = State->GetDiskId(),
        .LeaderScaleUnitId = {},
        .FollowerDiskId = msg->Record.GetFollowerDiskId(),
        .FollowerScaleUnitId = {}};

    if (auto follower = State->FindFollower(link)) {
        switch (follower->State) {
            case TFollowerDiskInfo::EState::None:
            case TFollowerDiskInfo::EState::Created: {
                // Link creation in progress.
                break;
            }

            case TFollowerDiskInfo::EState::Preparing:
            case TFollowerDiskInfo::EState::DataReady:
            case TFollowerDiskInfo::EState::Error: {
                // Link creation finished.
                LOG_INFO(
                    ctx,
                    TBlockStoreComponents::VOLUME,
                    "[%lu] Link %s (repeated)",
                    TabletID(),
                    follower->Link.Describe().c_str());
                auto response = std::make_unique<
                    TEvVolume::TEvLinkLeaderVolumeToFollowerResponse>(
                    MakeError(S_ALREADY));
                NCloud::Reply(ctx, *requestInfo, std::move(response));
                return;
            }
        }
    }

    auto& createFollowerRequest = State->AccessCreateFollowerRequestInfo(link);
    createFollowerRequest.Requests.push_back(requestInfo);
    if (createFollowerRequest.Link.LinkUUID) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::VOLUME,
            "[%lu] Link %s (repeated)",
            TabletID(),
            createFollowerRequest.Link.Describe().c_str());
        return;
    }

    // Create UUID for new link.
    createFollowerRequest.Link.LinkUUID = CreateGuidAsString();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "[%lu] Link %s started",
        TabletID(),
        createFollowerRequest.Link.Describe().c_str());

    auto actor = NCloud::Register<TCreateVolumeLinkActor>(
        ctx,
        TabletID(),
        SelfId(),
        createFollowerRequest.Link);

    createFollowerRequest.CreateVolumeLinkActor = actor;
}

void TVolumeActor::HandleUnlinkLeaderVolumeFromFollower(
    const TEvVolume::TEvUnlinkLeaderVolumeFromFollowerRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto link = TLeaderFollowerLink{
        .LinkUUID = "",
        .LeaderDiskId = State->GetDiskId(),
        .LeaderScaleUnitId = "",
        .FollowerDiskId = msg->Record.GetFollowerDiskId(),
        .FollowerScaleUnitId = ""};

    auto follower = State->FindFollower(link);
    if (follower) {
        link = follower->Link;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "[%lu] Unlink %s started",
        TabletID(),
        link.Describe().c_str());

    if (follower) {
        // Destroy link on leader side
        ExecuteTx<TRemoveFollower>(
            ctx,
            CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
            link);
    } else {
        // Link on leader side already destroyed.
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<
                TEvVolume::TEvUnlinkLeaderVolumeFromFollowerResponse>(
                MakeError(S_ALREADY)));
    }

    // In any case, we notify the follower side just in case.
    NCloud::Register<TPropagateLinkToFollowerActor>(
        ctx,
        TabletID(),
        CreateRequestInfo(SelfId(), 0, MakeIntrusive<TCallContext>()),
        std::move(link),
        TPropagateLinkToFollowerActor::EReason::Destruction);
}

void TVolumeActor::HandleUpdateFollowerState(
    const TEvVolumePrivate::TEvUpdateFollowerStateRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    using EState = TFollowerDiskInfo::EState;

    auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    auto replyError =
        [&](NProto::TError error, TFollowerDiskInfo updatedFollower)
    {
        auto response =
            std::make_unique<TEvVolumePrivate::TEvUpdateFollowerStateResponse>(
                std::move(error),
                std::move(updatedFollower));
        NCloud::Reply(ctx, *ev, std::move(response));
    };

    if (auto currentFollower = State->FindFollower(msg->Follower.Link)) {
        if (currentFollower->State == EState::Error) {
            replyError(
                MakeError(E_INVALID_STATE, "Can't change \"Error\" state"),
                std::move(*currentFollower));
            return;
        }

        if (currentFollower->State > msg->Follower.State) {
            replyError(
                MakeError(E_INVALID_STATE, "Can't downgrade state."),
                std::move(*currentFollower));
            return;
        }
    }

    ExecuteTx<TUpdateFollower>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Follower));
}

void TVolumeActor::HandleCreateLinkFinished(
    const TEvVolumePrivate::TEvCreateLinkFinished::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const bool hasError = HasError(msg->Error);
    LOG_LOG(
        ctx,
        hasError ? NActors::NLog::PRI_ERROR : NActors::NLog::PRI_INFO,
        TBlockStoreComponents::VOLUME,
        "[%lu] Link %s finished: %s",
        TabletID(),
        msg->Link.Describe().c_str(),
        FormatError(msg->Error).c_str());

    auto& createFollowerRequest =
        State->AccessCreateFollowerRequestInfo(msg->Link);
    for (const auto& requestInfo: createFollowerRequest.Requests) {
        auto response =
            std::make_unique<TEvVolume::TEvLinkLeaderVolumeToFollowerResponse>(
                msg->Error);
        NCloud::Reply(ctx, *requestInfo, std::move(response));
    }

    State->DeleteCreateFollowerRequestInfo(msg->Link);

    if (!hasError) {
        RestartPartition(ctx, {});
    }
}

void TVolumeActor::HandleLinkOnFollowerDestroyed(
    const TEvVolumePrivate::TEvLinkOnFollowerDestroyed::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "[%lu] Link %s on follower destroyed: %s",
        TabletID(),
        msg->Link.Describe().c_str(),
        FormatError(msg->GetError()).c_str());
}

}   // namespace NCloud::NBlockStore::NStorage
