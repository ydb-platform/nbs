#include "volume_actor.h"

#include <cloud/storage/core/libs/common/format.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration OutdatedLeaderDestructionBackoffDelay =
    TDuration::Seconds(30);
constexpr TDuration OutdatedLeaderDestructionMaxBackoffDelay =
    TDuration::Seconds(180);

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
        "%s Persist leader %s %s -> %s",
        LogTitle.GetWithTime().c_str(),
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
        std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerResponse>(
            MakeError(S_OK));
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    DestroyOutdatedLeaderIfNeeded(ctx);
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
        "%s Remove leader %s",
        LogTitle.GetWithTime().c_str(),
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
        std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerResponse>(
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
            std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerResponse>(
                MakeError(S_ALREADY)));
        return;
    }

    auto leaderInfo = TLeaderDiskInfo{
        .Link = std::move(link),
        .CreatedAt = TInstant::Now(),
        .State = TLeaderDiskInfo::EState::Following};

    ExecuteTx<TUpdateLeader>(
        ctx,
        std::move(requestInfo),
        std::move(leaderInfo));
}

void TVolumeActor::DestroyLeaderLink(
    TRequestInfoPtr requestInfo,
    TLeaderFollowerLink link,
    const NActors::TActorContext& ctx)
{
    auto currentLeader = State->FindLeader(link);
    if (!currentLeader) {
        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerResponse>(
                MakeError(S_ALREADY)));
        return;
    }

    ExecuteTx<TRemoveLeader>(ctx, std::move(requestInfo), std::move(link));
}

void TVolumeActor::UpdateLeaderLink(
    TRequestInfoPtr requestInfo,
    TLeaderFollowerLink link,
    TLeaderDiskInfo::EState state,
    const NActors::TActorContext& ctx)
{
    auto currentLeader = State->FindLeader(link);

    if (!currentLeader) {
        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerResponse>(
                MakeError(S_FALSE, "Leader not found")));
        return;
    }

    if (currentLeader->State > state) {
        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerResponse>(
                MakeError(
                    S_FALSE,
                    TStringBuilder() << "Leader state already "
                                     << ToString(currentLeader->State))));
        return;
    }

    if (currentLeader->State == state) {
        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerResponse>(
                MakeError(S_ALREADY)));
        return;
    }

    auto leaderInfo = TLeaderDiskInfo{
        .Link = std::move(link),
        .CreatedAt = TInstant::Now(),
        .State = state};

    ExecuteTx<TUpdateLeader>(
        ctx,
        std::move(requestInfo),
        std::move(leaderInfo));
}

void TVolumeActor::DestroyOutdatedLeaderIfNeeded(
    const NActors::TActorContext& ctx)
{
    for (const auto& leader: State->GetAllLeaders()) {
        if (leader.State != TLeaderDiskInfo::EState::Leader) {
            continue;
        }

        if (!OutdatedLeaderDestruction.has_value()) {
            OutdatedLeaderDestruction.emplace(
                TOutdatedLeaderDestruction{
                    .TryCount = 0,
                    .DelayProvider = TBackoffDelayProvider(
                        OutdatedLeaderDestructionBackoffDelay,
                        OutdatedLeaderDestructionMaxBackoffDelay)});
        }

        ++OutdatedLeaderDestruction->TryCount;

        auto request = std::make_unique<TEvService::TEvDestroyVolumeRequest>();
        request->Record.MutableHeaders()->SetExactDiskIdMatch(true);
        request->Record.SetDiskId(leader.Link.LeaderDiskId);

        auto event = std::make_unique<IEventHandle>(
            MakeStorageServiceId(),
            SelfId(),
            request.release(),
            0,                      // flags
            leader.Link.GetHash()   // cookie
        );

        LOG_INFO(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Schedule destroying old leader DiskId=%s, try# %lu after "
            "%s",
            LogTitle.GetWithTime().c_str(),
            leader.Link.LeaderDiskId.Quote().c_str(),
            OutdatedLeaderDestruction->TryCount,
            FormatDuration(OutdatedLeaderDestruction->DelayProvider.GetDelay())
                .c_str());

        ctx.Schedule(
            OutdatedLeaderDestruction->DelayProvider.GetDelay(),
            std::move(event));
    }
}

void TVolumeActor::HandleDestroyOutdatedLeaderVolumeResponse(
    const TEvService::TEvDestroyVolumeResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto leader = State->FindLeaderByHash(ev->Cookie);
    TString diskId = leader ? leader->Link.LeaderDiskId : "unknown";

    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Destroy leader volume %s error: %s",
            LogTitle.GetWithTime().c_str(),
            diskId.Quote().c_str(),
            FormatError(msg->GetError()).c_str());

        if (leader) {
            OutdatedLeaderDestruction->DelayProvider.IncreaseDelay();
            DestroyOutdatedLeaderIfNeeded(ctx);
        }
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Destroy leader volume %s success",
        LogTitle.GetWithTime().c_str(),
        diskId.Quote().c_str());

    if (leader) {
        UpdateLeaderLink(
            CreateRequestInfo({}, 0, MakeIntrusive<TCallContext>()),
            leader->Link,
            TLeaderDiskInfo::EState::Principal,
            ctx);
    }
}

void TVolumeActor::HandleUpdateLinkOnFollower(
    const TEvVolume::TEvUpdateLinkOnFollowerRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto link = TLeaderFollowerLink{
        .LinkUUID = msg->Record.GetLinkUUID(),
        .LeaderDiskId = msg->Record.GetLeaderDiskId(),
        .LeaderShardId = msg->Record.GetLeaderShardId(),
        .FollowerDiskId = msg->Record.GetDiskId(),
        .FollowerShardId = msg->Record.GetFollowerShardId()};

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Handle update link %s on follower %s",
        LogTitle.GetWithTime().c_str(),
        link.Describe().c_str(),
        NProto::ELinkAction_Name(msg->Record.GetAction()).c_str());

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    if (link.FollowerDiskId != State->GetDiskId()) {
        TString message = TStringBuilder()
                          << "Delivered to " << State->GetDiskId().Quote()
                          << " instead of " << link.FollowerDiskId.Quote();
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Handle update link %s on follower error: %s",
            LogTitle.GetWithTime().c_str(),
            link.Describe().c_str(),
            message.c_str());

        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerResponse>(
                MakeError(E_ARGUMENT, std::move(message))));
        return;
    }

    switch (msg->Record.GetAction()) {
        case NProto::LINK_ACTION_CREATE: {
            CreateLeaderLink(std::move(requestInfo), std::move(link), ctx);
            break;
        }
        case NProto::LINK_ACTION_DESTROY: {
            DestroyLeaderLink(std::move(requestInfo), std::move(link), ctx);
            break;
        }
        case NProto::LINK_ACTION_COMPLETED: {
            UpdateLeaderLink(
                std::move(requestInfo),
                std::move(link),
                TLeaderDiskInfo::EState::Leader,
                ctx);
            break;
        }
        default: {
            break;
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
