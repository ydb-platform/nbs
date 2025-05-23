#include "volume_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::PrepareUpdateLeader(
    const TActorContext& ctx,
    ITransactionBase::TTransactionContext& tx,
    TTxVolume::TUpdateLeader& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TVolumeActor::ExecuteUpdateLeader(
    const TActorContext& ctx,
    ITransactionBase::TTransactionContext& tx,
    TTxVolume::TUpdateLeader& args)
{
    auto current = State->FindLeader(args.Leader.Link);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "[%lu] Persist leader %s %s -> %s",
        TabletID(),
        State->GetDiskId().c_str(),
        args.Leader.Link.Describe().c_str(),
        current ? current->Describe().c_str() : "{}",
        args.Leader.Describe().c_str());

    TVolumeDatabase db(tx.DB);
    State->AddOrUpdateLeader(args.Leader);
    db.WriteLeader(args.Leader);
}

void TVolumeActor::CompleteUpdateLeader(
    const TActorContext& ctx,
    TTxVolume::TUpdateLeader& args)
{
    auto response =
        std::make_unique<TEvVolume::TEvNotifyFollowerVolumeResponse>(
            MakeError(S_OK));
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    RestartPartition(ctx, {});
}

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::PrepareRemoveLeader(
    const TActorContext& ctx,
    ITransactionBase::TTransactionContext& tx,
    TTxVolume::TRemoveLeader& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TVolumeActor::ExecuteRemoveLeader(
    const TActorContext& ctx,
    ITransactionBase::TTransactionContext& tx,
    TTxVolume::TRemoveLeader& args)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "[%lu] Remove leader %s",
        TabletID(),
        args.Link.Describe().c_str());

    TVolumeDatabase db(tx.DB);
    State->RemoveLeader(args.Link);
    db.DeleteLeader(args.Link);
}

void TVolumeActor::CompleteRemoveLeader(
    const TActorContext& ctx,
    TTxVolume::TRemoveLeader& args)
{
    auto response =
        std::make_unique<TEvVolume::TEvNotifyFollowerVolumeResponse>(
            MakeError(S_OK));

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    RestartPartition(ctx, {});
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::CreateLeaderLink(
    TRequestInfoPtr requestInfo,
    TLeaderFollowerLink link,
    const NActors::TActorContext& ctx)
{
    auto currentLeader = State->FindLeader(link);
    if (currentLeader) {
        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<TEvVolume::TEvNotifyFollowerVolumeResponse>(
                MakeError(S_ALREADY)));
        return;
    }

    auto leaderInfo = TLeaderDiskInfo{
        .Link = std::move(link),
        .CreatedAt = TInstant::Now(),
        .State = TLeaderDiskInfo::EState::Follower};

    ExecuteTx<TUpdateLeader>(ctx, std::move(requestInfo), std::move(leaderInfo));
}

void TVolumeActor::DestroyLeaderLink(
    TRequestInfoPtr requestInfo,
    TLeaderFollowerLink link,
    const NActors::TActorContext& ctx)
{
    auto currenLeader = State->FindLeader(link);
    if (!currenLeader) {
        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<TEvVolume::TEvNotifyFollowerVolumeResponse>(
                MakeError(S_ALREADY)));
        return;
    }

    ExecuteTx<TRemoveLeader>(ctx, std::move(requestInfo), std::move(link));
}

void TVolumeActor::HandleNotifyFollowerVolume(
    const TEvVolume::TEvNotifyFollowerVolumeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto link = TLeaderFollowerLink{
        .LinkUUID = msg->Record.GetLinkUUID(),
        .LeaderDiskId = msg->Record.GetLeaderDiskId(),
        .LeaderScaleUnitId = msg->Record.GetLeaderScaleUnitId(),
        .FollowerDiskId = State->GetDiskId(),
        .FollowerScaleUnitId = {}};

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "[%lu] Update link %s on follower %s",
        TabletID(),
        link.Describe().c_str(),
        NProto::EFollowerNotificationReason_Name(msg->Record.GetReason())
            .c_str());

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    switch (msg->Record.GetReason()) {
        case NProto::FOLLOWER_NOTIFICATION_REASON_CREATED: {
            CreateLeaderLink(
                std::move(requestInfo),
                std::move(link),
                ctx);
            break;
        }
        case NProto::FOLLOWER_NOTIFICATION_REASON_DESTROYED: {
            DestroyLeaderLink(
                std::move(requestInfo),
                std::move(link),
                ctx);
            break;
        }
        default: {
            break;
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
