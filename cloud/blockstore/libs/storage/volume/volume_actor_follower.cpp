#include "volume_actor.h"

#include <cloud/blockstore/libs/common/constants.h>
#include <cloud/blockstore/libs/storage/volume/model/helpers.h>

#include <cloud/storage/core/libs/common/format.h>

#include <util/generic/algorithm.h>

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
    Y_UNUSED(ctx);

    TVolumeDatabase db(tx.DB);
    db.WriteLeader(args.Leader);
}

void TVolumeActor::CompleteUpdateLeader(
    const TActorContext& ctx,
    TTxVolume::TUpdateLeader& args)
{
    const auto current = State->FindLeader(args.Leader.Link);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Persist leader %s %s -> %s",
        LogTitle.GetWithTime().c_str(),
        args.Leader.Link.Describe().c_str(),
        current ? current->Describe().c_str() : "{}",
        args.Leader.Describe().c_str());

    State->AddOrUpdateLeader(args.Leader);

    if (args.Leader.State == TLeaderDiskInfo::EState::Following) {
        State->FinishCreateLeaderRequest();
    }

    if (State->IsLeaderLinkBeingRemoved(args.Leader.Link)) {
        NCloud::Reply(
            ctx,
            *args.RequestInfo,
            std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerResponse>(
                MakeError(E_CANCELLED, "Leader link was removed")));
        return;
    }

    RemoveSourceDiskIdTagIfNeeded(ctx);
    if (args.Leader.State >= TLeaderDiskInfo::EState::Leader &&
        State->IsSourceDiskIdTagRemovalInProgress())
    {
        State->AddPendingLeadershipTransferRequest(
            args.RequestInfo,
            args.Leader.Link);
        return;
    }

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
    db.DeleteLeader(args.Link);
}

void TVolumeActor::CompleteRemoveLeader(
    const TActorContext& ctx,
    TTxVolume::TRemoveLeader& args)
{
    State->RemoveLeader(args.Link);
    State->FinishRemovingLeaderLink(args.Link);
    if (State->HasPendingLeadershipTransfer(args.Link)) {
        ReplyToPendingLeadershipTransferRequests(ctx);
    }

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

    if (State->IsVolumeOperationRestricted()) {
        // Link propagation retries E_REJECTED responses with backoff.
        auto response =
            std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerResponse>(
                MakeError(
                    E_REJECTED,
                    "CreateVolumeLink is not allowed while another exclusive "
                    "volume operation is in progress on the follower volume"));
        NCloud::Reply(ctx, *requestInfo, std::move(response));
        return;
    }

    State->StartCreateLeaderRequest();

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

    link = currentLeader->Link;
    if (currentLeader->State >= TLeaderDiskInfo::EState::Leader) {
        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerResponse>(
                MakeError(
                    E_INVALID_STATE,
                    "Cannot unlink after leadership has been transferred")));
        return;
    }

    State->StartRemovingLeaderLink(link);
    ExecuteTx<TRemoveLeader>(ctx, std::move(requestInfo), std::move(link));
}

void TVolumeActor::UpdateLeaderLink(
    TRequestInfoPtr requestInfo,
    TLeaderFollowerLink link,
    TLeaderDiskInfo::EState state,
    const NActors::TActorContext& ctx)
{
    auto currentLeader = State->FindLeader(link);

    if (!currentLeader || State->IsLeaderLinkBeingRemoved(currentLeader->Link))
    {
        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerResponse>(
                MakeError(E_CANCELLED, "Leader link was removed")));
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
        RemoveSourceDiskIdTagIfNeeded(ctx);
        if (State->IsSourceDiskIdTagRemovalInProgress()) {
            State->AddPendingLeadershipTransferRequest(
                std::move(requestInfo),
                currentLeader->Link);
            return;
        }

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
    RemoveSourceDiskIdTagIfNeeded(ctx);
    if (State->IsSourceDiskIdTagRemovalInProgress()) {
        return;
    }

    for (const auto& leader: State->GetAllLeaders()) {
        if (leader.State != TLeaderDiskInfo::EState::Leader ||
            State->IsLeaderLinkBeingRemoved(leader.Link))
        {
            continue;
        }

        const auto& destruction =
            State->StartOutdatedLeaderDestructionAttempt();

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
            destruction.TryCount,
            FormatDuration(destruction.DelayProvider.GetDelay()).c_str());

        ctx.Schedule(destruction.DelayProvider.GetDelay(), std::move(event));
    }
}

void TVolumeActor::ReplyToPendingLeadershipTransferRequests(
    const TActorContext& ctx)
{
    const auto pending = State->TakePendingLeadershipTransfer();
    if (!pending) {
        return;
    }

    const auto leader = State->FindLeader(pending->Link);
    const bool cancelled = !leader ||
                           leader->State < TLeaderDiskInfo::EState::Leader ||
                           State->IsLeaderLinkBeingRemoved(leader->Link);
    for (const auto& requestInfo: pending->Requests) {
        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerResponse>(
                cancelled ? MakeError(E_CANCELLED, "Leader link was removed")
                          : MakeError(S_OK)));
    }
}

void TVolumeActor::RemoveSourceDiskIdTagIfNeeded(const TActorContext& ctx)
{
    if (State->IsSourceDiskIdTagRemovalInProgress()) {
        return;
    }

    if (State->GetLeadershipStatus() != ELeadershipStatus::Principal) {
        return;
    }

    const bool hasPromotedLeader = AnyOf(
        State->GetAllLeaders(),
        [&](const TLeaderDiskInfo& leader)
        {
            return leader.State >= TLeaderDiskInfo::EState::Leader &&
                   !State->IsLeaderLinkBeingRemoved(leader.Link);
        });
    if (!hasPromotedLeader) {
        return;
    }

    const auto tags =
        ParseTags(State->GetMeta().GetVolumeConfig().GetTagsStr());
    const auto* sourceDiskId = tags.FindPtr(SourceDiskIdTagName);
    if (!sourceDiskId) {
        return;
    }

    State->StartSourceDiskIdTagRemoval();

    // Publish the committed principal before the source tells clients to
    // switch. Waiting until the source is deleted would leave discovery
    // rejecting the target while the source stays attached to those clients.
    auto request = std::make_unique<TEvService::TEvRemoveTagsRequest>(
        State->GetDiskId(),
        TVector<TString>{
            MakeTag(SourceDiskIdTagName, *sourceDiskId),
        });
    NCloud::Send(ctx, MakeStorageServiceId(), std::move(request));
    ctx.Schedule(
        TDuration::Seconds(1),
        new TEvVolumePrivate::TEvRetryRemoveSourceDiskIdTag());
}

void TVolumeActor::FinishSourceDiskIdTagRemoval(const TActorContext& ctx)
{
    State->FinishSourceDiskIdTagRemoval();
    ReplyToPendingLeadershipTransferRequests(ctx);
    DestroyOutdatedLeaderIfNeeded(ctx);
}

void TVolumeActor::HandleRemoveSourceDiskIdTagResponse(
    const TEvService::TEvRemoveTagsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    if (!State->IsSourceDiskIdTagRemovalInProgress()) {
        return;
    }

    const auto& error = ev->Get()->GetError();
    if (HasError(error)) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Failed to remove source-disk-id tag: %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(error).c_str());
        return;
    }

    FinishSourceDiskIdTagRemoval(ctx);
}

void TVolumeActor::HandleRetryRemoveSourceDiskIdTag(
    const TEvVolumePrivate::TEvRetryRemoveSourceDiskIdTag::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    if (!State->IsSourceDiskIdTagRemovalInProgress()) {
        return;
    }

    State->FinishSourceDiskIdTagRemoval();
    RemoveSourceDiskIdTagIfNeeded(ctx);
    if (!State->IsSourceDiskIdTagRemovalInProgress()) {
        FinishSourceDiskIdTagRemoval(ctx);
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
            State->IncreaseOutdatedLeaderDestructionDelay();
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
        .LeaderCellId = msg->Record.GetLeaderCellId(),
        .FollowerDiskId = msg->Record.GetDiskId(),
        .FollowerCellId = msg->Record.GetFollowerCellId()};

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
