#include "volume_actor.h"

#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
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
            args.FollowerInfo = {};
            return;
        }
    } else {
        if (current->CancellationPending) {
            args.Error =
                MakeError(E_INVALID_STATE, "Follower cancellation is pending");
            args.FollowerInfo = {};
            return;
        }
        const auto followerTabletId = args.FollowerInfo.Link.FollowerTabletId;
        if (current->State == TFollowerDiskInfo::EState::Error ||
            current->State > args.FollowerInfo.State)
        {
            args.Error =
                MakeError(E_INVALID_STATE, "Follower state cannot regress");
            args.FollowerInfo = *current;
            return;
        }
        args.FollowerInfo.Link = current->Link;
        if (!args.FollowerInfo.Link.FollowerTabletId) {
            args.FollowerInfo.Link.FollowerTabletId = followerTabletId;
        }
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Persist follower %s: %s -> %s",
        LogTitle.GetWithTime().c_str(),
        args.FollowerInfo.Link.Describe().c_str(),
        current ? current->Describe().c_str() : "{none}",
        args.FollowerInfo.Describe().c_str());

    args.FollowerInfo.Link.LeaderTabletId = TabletID();
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
    // Do not deliver a successful but superseded state after cancellation.
    if (const auto current = State->FindFollower(args.FollowerInfo.Link);
        current && !current->CancellationPending)
    {
        response->Follower = *current;
    }
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
    // Recovery may finish before its Error transaction commits. Release
    // copy-only retention here, after the terminal state is durable.
    if (!HasError(args.Error) &&
        args.FollowerInfo.State == TFollowerDiskInfo::EState::Error &&
        PartitionsStartedReason == EPartitionsStartedReason::STARTED_FOR_COPY &&
        !State->HasActiveFollower())
    {
        if (State->HasActiveClients(ctx.Now())) {
            // A remote mount may still have the copy-only start reason.
            StartPartitionsIfNeeded(ctx);
        } else {
            RestartPartition(ctx, {});
        }
    }
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
        args.Changed = true;
    }
    if (auto* pending = State->FindCreateFollowerRequestInfo(args.Link)) {
        if (!follower) {
            args.Link = pending->Link;
        }
        args.CreateVolumeLinkActor = pending->CreateVolumeLinkActor;
        args.PendingCreateRequests = std::move(pending->Requests);
        State->DeleteCreateFollowerRequestInfo(args.Link);
        args.Changed = true;
    }
    if (!args.Changed) {
        if (const auto cancellation =
                State->FindFollowerCancellation(args.Link))
        {
            args.Link = cancellation->Link;
        }
        args.Error = MakeError(S_ALREADY);
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Remove follower %s",
        LogTitle.GetWithTime().c_str(),
        args.Link.Describe().c_str());

    // The same commit releases copy authority and records the durable
    // obligation to cancel this exact generation on the destination.
    auto cancelled = follower.value_or(TFollowerDiskInfo{});
    cancelled.Link = args.Link;
    cancelled.CreatedAt = ctx.Now();
    cancelled.State = TFollowerDiskInfo::EState::Error;
    cancelled.CancellationPending = true;
    cancelled.CancellationRequireCancellable = args.RequireCancellable;
    TVolumeDatabase db(tx.DB);
    State->AddOrUpdateFollower(cancelled);
    db.WriteFollower(cancelled);
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
    PropagateFollowerCancellations(ctx);
    if (!args.Changed) {
        return;
    }
    if (args.CreateVolumeLinkActor) {
        NCloud::Send(ctx, args.CreateVolumeLinkActor,
                     std::make_unique<TEvents::TEvPoisonPill>());
    }

    for (const auto& requestInfo: args.PendingCreateRequests) {
        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<TEvVolume::TEvLinkLeaderVolumeToFollowerResponse>(
                MakeError(E_INVALID_STATE, "Link creation was cancelled")));
    }
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
        const auto followerTabletId = msg->Follower.Link.FollowerTabletId;
        msg->Follower.Link = currentFollower->Link;
        if (!msg->Follower.Link.FollowerTabletId) {
            msg->Follower.Link.FollowerTabletId = followerTabletId;
        }
        if (currentFollower->CancellationPending) {
            replyError(
                MakeError(E_INVALID_STATE, "Follower cancellation is pending"),
                {});
            return;
        }

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

void TVolumeActor::PropagateFollowerCancellations(
    const NActors::TActorContext& ctx)
{
    for (const auto& follower: State->GetAllFollowers()) {
        if (!follower.CancellationPending ||
            FollowerCancellationPropagators.contains(follower.Link.LinkUUID))
        {
            continue;
        }
        const auto actor = NCloud::Register<TPropagateLinkToFollowerActor>(
            ctx, LogTitle.GetBrief(),
            CreateRequestInfo(SelfId(), 0, MakeIntrusive<TCallContext>()),
            follower.Link, TPropagateLinkToFollowerActor::EReason::Destruction,
            follower.CancellationRequireCancellable);
        FollowerCancellationPropagators.emplace(follower.Link.LinkUUID, actor);
    }
}

void TVolumeActor::HandleRetryFollowerCancellations(
    const TEvVolumePrivate::TEvRetryFollowerCancellations::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);
    PropagateFollowerCancellations(ctx);
}

void TVolumeActor::HandleLinkOnFollowerDestroyed(
    const TEvVolumePrivate::TEvLinkOnFollowerDestroyed::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto it = FollowerCancellationPropagators.find(msg->Link.LinkUUID);
    if (it == FollowerCancellationPropagators.end() || it->second != ev->Sender)
    {
        return;
    }
    if (HasError(msg->GetError()) &&
        !IsNotFoundSchemeShardError(msg->GetError()) &&
        msg->GetError().GetCode() != E_NOT_FOUND)
    {
        // Keep the durable obligation; a repeat request may retry immediately.
        FollowerCancellationPropagators.erase(it);
        ctx.Schedule(TDuration::Seconds(1),
                     new TEvVolumePrivate::TEvRetryFollowerCancellations());
        return;
    }
    ExecuteTx<TFinishFollowerCancellation>(
        ctx, CreateRequestInfo(SelfId(), 0, MakeIntrusive<TCallContext>()),
        msg->Link, ev->Sender);
}

bool TVolumeActor::PrepareFinishFollowerCancellation(
    const TActorContext& ctx, ITransactionBase::TTransactionContext& tx,
    TTxVolume::TFinishFollowerCancellation& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
    return true;
}

void TVolumeActor::ExecuteFinishFollowerCancellation(
    const TActorContext& ctx, ITransactionBase::TTransactionContext& tx,
    TTxVolume::TFinishFollowerCancellation& args)
{
    Y_UNUSED(ctx);
    if (const auto cancellation = State->FindFollowerCancellation(args.Link)) {
        TVolumeDatabase db(tx.DB);
        State->RemoveFollower(cancellation->Link);
        db.DeleteFollower(cancellation->Link);
    }
}

void TVolumeActor::CompleteFinishFollowerCancellation(
    const TActorContext& ctx, TTxVolume::TFinishFollowerCancellation& args)
{
    const auto it = FollowerCancellationPropagators.find(args.Link.LinkUUID);
    if (it != FollowerCancellationPropagators.end() &&
        it->second == args.Propagator)
    {
        FollowerCancellationPropagators.erase(it);
    }
    PropagateFollowerCancellations(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
