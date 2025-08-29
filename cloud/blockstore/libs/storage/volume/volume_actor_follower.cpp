#include "volume_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {
////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TEvService::TEvAddTagsRequest> MakeAddOutdatedRequest(
    const TLeaderFollowerLink& link)
{
    TString tag = TString(OutdatedVolumeTagName) + "=" + link.FollowerDiskId;
    TVector<TString> tags({std::move(tag)});
    auto result = std::make_unique<TEvService::TEvAddTagsRequest>(
        link.LeaderDiskId,
        std::move(tags));
    return result;
}

bool IsPathDoesNotExistError(const NProto::TError& error)
{
    if (HasError(error) &&
        FACILITY_FROM_CODE(error.GetCode()) == FACILITY_SCHEMESHARD)
    {
        const auto status =
            static_cast<NKikimrScheme::EStatus>(STATUS_FROM_CODE(error.GetCode()));
        return status == NKikimrScheme::StatusPathDoesNotExist;
    }
    return false;
}

}   // namespace

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
    auto currenLeader = State->FindLeader(link);
    if (!currenLeader) {
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
    auto currenLeader = State->FindLeader(link);
    if (!currenLeader) {
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

void TVolumeActor::AddOutdatedTagToLeader(
    TRequestInfoPtr requestInfo,
    TLeaderFollowerLink link,
    const NActors::TActorContext& ctx)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Add outdated tag to leader",
        LogTitle.GetWithTime().c_str());

    auto currenLeader = State->FindLeader(link);
    if (!currenLeader) {
        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerResponse>(
                MakeError(S_ALREADY)));
        return;
    }

    auto& requests = State->AccessLinkCompletedRequests(currenLeader->Link);
    if (requests.RequestInfo) {
        NCloud::Reply(
            ctx,
            *requests.RequestInfo,
            std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerResponse>(
                MakeError(E_REJECTED, "Another request is in progress")));
        return;
    };
    requests.RequestInfo = std::move(requestInfo);

    const ui64 cookie = currenLeader->Link.GetHash();
    ctx.Send(
        MakeStorageServiceId(),
        MakeAddOutdatedRequest(currenLeader->Link),
        0,   // flags
        cookie);
}

void TVolumeActor::HandleAddOutdatedTagResponse(
    const TEvService::TEvAddTagsResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto currenLeader = State->FindLeaderByHash(ev->Cookie);
    if (!currenLeader) {
        return;
    }

    auto& requests = State->AccessLinkCompletedRequests(currenLeader->Link);
    TRequestInfoPtr requestInfo = std::move(requests.RequestInfo);

    if (HasError(msg->GetError())) {
        if (IsPathDoesNotExistError(msg->GetError())) {
            LOG_WARN(
                ctx,
                TBlockStoreComponents::VOLUME,
                "%s %s Leader volume outdated tag set error: %s. Leader volume "
                "does not exist.",
                LogTitle.GetWithTime().c_str(),
                currenLeader->Link.Describe().c_str(),
                FormatError(msg->GetError()).c_str());

            UpdateLeaderLink(
                std::move(requestInfo),
                currenLeader->Link,
                TLeaderDiskInfo::EState::Leader,
                ctx);
            return;
        }

        if (GetErrorKind(msg->GetError()) == EErrorKind::ErrorRetriable) {
            LOG_WARN(
                ctx,
                TBlockStoreComponents::VOLUME,
                "%s %s Leader volume outdated tag set retriable error: %s",
                LogTitle.GetWithTime().c_str(),
                currenLeader->Link.Describe().c_str(),
                FormatError(msg->GetError()).c_str());
            ctx.Schedule(
                TDuration::Seconds(5),
                std::make_unique<IEventHandle>(
                    MakeStorageServiceId(),
                    SelfId(),
                    MakeAddOutdatedRequest(currenLeader->Link).release(),
                    TEventFlags{0},
                    currenLeader->Link.GetHash()));
        } else {
            LOG_WARN(
                ctx,
                TBlockStoreComponents::VOLUME,
                "%s %s Leader volume outdated tag set non-retriable error: %s",
                LogTitle.GetWithTime().c_str(),
                currenLeader->Link.Describe().c_str(),
                FormatError(msg->GetError()).c_str());
        }
        return;
    }

    UpdateLeaderLink(
        std::move(requestInfo),
        currenLeader->Link,
        TLeaderDiskInfo::EState::Leader,
        ctx);
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
        "%s Update link %s on follower %s",
        LogTitle.GetWithTime().c_str(),
        link.Describe().c_str(),
        NProto::ELinkAction_Name(msg->Record.GetAction()).c_str());

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

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
            AddOutdatedTagToLeader(
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
