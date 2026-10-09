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
    if (!current) {
        // Only the active create operation may introduce a new UUID.
        // Progress from a cancelled or replaced migration must not resurrect
        // it.
        const auto* pending =
            State->FindCreateFollowerRequestInfo(args.FollowerInfo.Link);
        if (!pending ||
            pending->Link.LinkUUID != args.FollowerInfo.Link.LinkUUID)
        {
            args.Error =
                MakeError(E_INVALID_STATE, "Follower link no longer exists");
            return;
        }
    } else {
        if (current->State == TFollowerDiskInfo::EState::Error ||
            current->State > args.FollowerInfo.State)
        {
            args.Error =
                MakeError(E_INVALID_STATE, "Follower state cannot regress");
            args.FollowerInfo = *current;
            return;
        }
        args.FollowerInfo.Link = current->Link;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Persist follower %s: %s -> %s",
        LogTitle.GetWithTime().c_str(),
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
            args.Error);
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
    const auto follower = State->FindFollower(args.Link);
    if (args.RequireCancellable && follower &&
        (follower->State == TFollowerDiskInfo::EState::DataReady ||
         follower->State == TFollowerDiskInfo::EState::LeadershipTransferred))
    {
        args.Error = MakeError(
            E_INVALID_STATE,
            "Cannot cancel a link after leadership transfer has started");
        return;
    }
    if (follower) {
        args.Link = follower->Link;
    } else {
        args.Error = MakeError(S_ALREADY);
    }
    if (auto* pending = State->FindCreateFollowerRequestInfo(args.Link)) {
        args.PendingCreateRequests = std::move(pending->Requests);
        State->DeleteCreateFollowerRequestInfo(args.Link);
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Remove follower %s",
        LogTitle.GetWithTime().c_str(),
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
            args.Error);
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
    if (HasError(args.Error)) {
        return;
    }

    for (const auto& requestInfo: args.PendingCreateRequests) {
        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<TEvVolume::TEvLinkLeaderVolumeToFollowerResponse>(
                MakeError(E_INVALID_STATE, "Link creation was cancelled")));
    }
    // Propagate only a committed cancellation, with the same cutover fence.
    NCloud::Register<TPropagateLinkToFollowerActor>(
        ctx, LogTitle.GetBrief(),
        CreateRequestInfo(SelfId(), 0, MakeIntrusive<TCallContext>()),
        args.Link, TPropagateLinkToFollowerActor::EReason::Destruction);
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
        .LeaderDiskId = msg->Record.GetDiskId(),
        .LeaderShardId = msg->Record.GetLeaderShardId(),
        .FollowerDiskId = msg->Record.GetFollowerDiskId(),
        .FollowerShardId =  msg->Record.GetFollowerShardId()};

    if (auto follower = State->FindFollower(link)) {
        link = follower->Link;

        switch (follower->State) {
            case TFollowerDiskInfo::EState::None:
            case TFollowerDiskInfo::EState::Created: {
                // Link creation in progress.
                break;
            }

            case TFollowerDiskInfo::EState::Preparing:
            case TFollowerDiskInfo::EState::DataReady:
            case TFollowerDiskInfo::EState::LeadershipTransferred:
            case TFollowerDiskInfo::EState::Error: {
                // Link creation finished.
                LOG_INFO(
                    ctx,
                    TBlockStoreComponents::VOLUME,
                    "%s Link %s already exists",
                    LogTitle.GetWithTime().c_str(),
                    follower->Link.Describe().c_str());
                auto response = std::make_unique<
                    TEvVolume::TEvLinkLeaderVolumeToFollowerResponse>(
                    MakeError(S_ALREADY));
                response->Record.SetLinkUUID(follower->Link.LinkUUID);
                NCloud::Reply(ctx, *requestInfo, std::move(response));
                return;
            }
        }
    }

    if (auto* request = State->FindCreateFollowerRequestInfo(link)) {
        request->Requests.push_back(requestInfo);
        LOG_INFO(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Link %s creation already in progress",
            LogTitle.GetWithTime().c_str(),
            request->Link.Describe().c_str());
        return;
    }

    if (State->IsVolumeOperationRestricted()) {
        auto response =
            std::make_unique<TEvVolume::TEvLinkLeaderVolumeToFollowerResponse>(
                MakeError(
                    E_TRY_AGAIN,
                    "CreateVolumeLink is not allowed while another exclusive "
                    "volume operation is in progress on the leader volume"));
        NCloud::Reply(ctx, *requestInfo, std::move(response));
        return;
    }

    // Save create link request.
    auto& createFollowerRequest = State->AccessCreateFollowerRequestInfo(link);
    createFollowerRequest.Requests.push_back(requestInfo);
    if (createFollowerRequest.Link.LinkUUID) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Link %s creation already in progress",
            LogTitle.GetWithTime().c_str(),
            createFollowerRequest.Link.Describe().c_str());
        return;
    }

    // Create UUID for new link.
    createFollowerRequest.Link.LinkUUID = CreateGuidAsString();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Link %s creation started",
        LogTitle.GetWithTime().c_str(),
        createFollowerRequest.Link.Describe().c_str());

    auto actor = NCloud::Register<TCreateVolumeLinkActor>(
        ctx,
        LogTitle.GetBrief(),
        SelfId(),
        createFollowerRequest.Link,
        Config->GetSchemeShardDirForShard(link.LeaderShardId) ==
            Config->GetSchemeShardDirForShard(link.FollowerShardId));

    createFollowerRequest.CreateVolumeLinkActor = actor;
}

void TVolumeActor::HandleUnlinkLeaderVolumeFromFollower(
    const TEvVolume::TEvUnlinkLeaderVolumeFromFollowerRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto link = TLeaderFollowerLink{
        .LinkUUID = "",
        .LeaderDiskId = msg->Record.GetDiskId(),
        .LeaderShardId = msg->Record.GetLeaderShardId(),
        .FollowerDiskId = msg->Record.GetFollowerDiskId(),
        .FollowerShardId = msg->Record.GetFollowerShardId()};

    if (const auto follower = State->FindFollower(link)) {
        link = follower->Link;
    }
    ExecuteTx<TRemoveFollower>(
        ctx, CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        std::move(link), msg->Record.GetRequireCancellable());
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
        msg->Follower.Link = currentFollower->Link;

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

    if (!msg->Follower.Link.LinkUUID) {
        replyError(
            MakeError(
                E_ARGUMENT,
                "Can't change follower state without LinkUUID"),
            std::move(msg->Follower));
        return;
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
        "%s Link %s finished: %s",
        LogTitle.GetWithTime().c_str(),
        msg->Link.Describe().c_str(),
        FormatError(msg->Error).c_str());

    auto* createFollowerRequest =
        State->FindCreateFollowerRequestInfo(msg->Link);
    if (!createFollowerRequest) {
        // A cancelled create actor may finish after its operation was removed.
        return;
    }
    for (const auto& requestInfo: createFollowerRequest->Requests) {
        auto response =
            std::make_unique<TEvVolume::TEvLinkLeaderVolumeToFollowerResponse>(
                msg->Error);
        response->Record.SetLinkUUID(msg->Link.LinkUUID);
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
        "%s Link %s on follower destroyed: %s",
        LogTitle.GetWithTime().c_str(),
        msg->Link.Describe().c_str(),
        FormatError(msg->GetError()).c_str());
}

}   // namespace NCloud::NBlockStore::NStorage
