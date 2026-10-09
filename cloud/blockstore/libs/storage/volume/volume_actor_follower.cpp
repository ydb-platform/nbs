#include "volume_actor.h"

#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

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
    if ((current && current->State == TLeaderDiskInfo::EState::Cancelled) ||
        (args.Leader.State != TLeaderDiskInfo::EState::Following &&
         (!current || current->State > args.Leader.State)))
    {
        args.Error = MakeError(E_INVALID_STATE,
                               "Leader link no longer exists or has advanced");
        return;
    }

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
    if (args.Leader.State == TLeaderDiskInfo::EState::Following) {
        State->FinishCreateLeaderRequest();
    }

    auto response =
        std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerResponse>(
            args.Error);
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
    if (!HasError(args.Error)) {
        if (args.Leader.State == TLeaderDiskInfo::EState::Principal &&
            OutdatedLeaderDestruction &&
            OutdatedLeaderDestruction->LinkUUID == args.Leader.Link.LinkUUID)
        {
            OutdatedLeaderDestruction.reset();
        }
    }
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
    Y_UNUSED(ctx);
    const auto leader = State->FindLeader(args.Link);
    if (leader && leader->State == TLeaderDiskInfo::EState::Cancelled) {
        args.Error = MakeError(S_ALREADY);
        return;
    }
    if (args.RequireCancellable && leader &&
        leader->State != TLeaderDiskInfo::EState::Following)
    {
        args.Error = MakeError(
            E_INVALID_STATE,
            "Cannot cancel a link after leadership transfer has started");
        return;
    }
    if (leader) {
        args.Link = leader->Link;
        args.Changed = true;
    }
    if (!args.Link.LinkUUID) {
        args.Error = MakeError(S_ALREADY);
        return;
    }

    // Retain a durable UUID fence even when CREATE has not arrived yet.
    TLeaderDiskInfo cancelled{.Link = args.Link, .CreatedAt = ctx.Now(),
                              .State = TLeaderDiskInfo::EState::Cancelled};
    TVolumeDatabase db(tx.DB);
    State->AddOrUpdateLeader(cancelled);
    db.WriteLeader(cancelled);
}

void TVolumeActor::CompleteRemoveLeader(
    const TActorContext& ctx,
    TTxVolume::TRemoveLeader& args)
{
    auto response =
        std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerResponse>(
            args.Error);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
    if (!HasError(args.Error) && args.Changed) {
        // Release the cancelled generation's slot before starting another.
        DestroyOutdatedLeaderIfNeeded(ctx);
        RestartPartition(ctx, {});
    }
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
                MakeError(
                    currentLeader->State == TLeaderDiskInfo::EState::Cancelled
                        ? E_INVALID_STATE
                        : S_ALREADY,
                    currentLeader->State == TLeaderDiskInfo::EState::Cancelled
                        ? "Link creation was cancelled"
                        : "")));
        return;
    }

    if (Config->GetSchemeShardDirForShard(link.LeaderShardId) !=
            Config->GetSchemeShardDirForShard(link.FollowerShardId) &&
        State->GetStorageMediaKind() != NProto::STORAGE_MEDIA_SSD &&
        State->GetStorageMediaKind() != NProto::STORAGE_MEDIA_HDD)
    {
        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<
                TEvVolume::TEvUpdateLinkOnFollowerResponse>(MakeError(
                E_NOT_IMPLEMENTED,
                "Cross-shard links support only replicated SSD/HDD volumes")));
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
    TRequestInfoPtr requestInfo, TLeaderFollowerLink link,
    bool requireCancellable, const NActors::TActorContext& ctx)
{
    ExecuteTx<TRemoveLeader>(ctx, std::move(requestInfo), std::move(link),
                             requireCancellable);
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

    link = currentLeader->Link;
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
    if (OutdatedLeaderDestruction) {
        const auto reserved = State->FindLeader(TLeaderFollowerLink{
            .LinkUUID = OutdatedLeaderDestruction->LinkUUID});
        if (reserved && reserved->State == TLeaderDiskInfo::EState::Leader) {
            return;
        }
        OutdatedLeaderDestruction.reset();
    }
    for (const auto& leader: State->GetAllLeaders()) {
        if (leader.State != TLeaderDiskInfo::EState::Leader) {
            continue;
        }
        OutdatedLeaderDestruction.emplace(TOutdatedLeaderDestruction{
            .TryCount = 0,
            .DelayProvider =
                TBackoffDelayProvider(OutdatedLeaderDestructionBackoffDelay,
                                      OutdatedLeaderDestructionMaxBackoffDelay),
            .LinkUUID = leader.Link.LinkUUID});
        ctx.Schedule(
            OutdatedLeaderDestruction->DelayProvider.GetDelay(),
            new TEvVolumePrivate::TEvDestroyOutdatedLeader(
                leader.Link.LinkUUID));
        return;
    }
}

void TVolumeActor::HandleDestroyOutdatedLeader(
    const TEvVolumePrivate::TEvDestroyOutdatedLeader::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    if (!OutdatedLeaderDestruction ||
        OutdatedLeaderDestruction->LinkUUID != ev->Get()->LinkUUID ||
        OutdatedLeaderDestruction->InFlight)
    {
        return;
    }
    const auto leader =
        State->FindLeader(TLeaderFollowerLink{.LinkUUID = ev->Get()->LinkUUID});
    if (!leader || leader->State != TLeaderDiskInfo::EState::Leader) {
        OutdatedLeaderDestruction.reset();
        DestroyOutdatedLeaderIfNeeded(ctx);
        return;
    }
    auto& cleanup = *OutdatedLeaderDestruction;
    cleanup.InFlight = true;
    cleanup.Cookie = ++OutdatedLeaderDestructionCookie;
    ++cleanup.TryCount;
    // Verify UUID and media kind on the current source before selecting the
    // replicated conditional-delete or existing local legacy workflow.
    cleanup.AwaitingSourceStatus = true;
    auto request = std::make_unique<TEvVolume::TEvGetLinkStatusRequest>();
    auto& record = request->Record;
    record.SetDiskId(leader->Link.LeaderDiskId);
    record.SetLeaderDiskId(leader->Link.LeaderDiskId);
    record.SetLeaderShardId(leader->Link.LeaderShardId);
    record.SetFollowerDiskId(leader->Link.FollowerDiskId);
    record.SetFollowerShardId(leader->Link.FollowerShardId);
    record.SetLinkUUID(leader->Link.LinkUUID);
    record.MutableHeaders()->SetShardId(leader->Link.LeaderShardId);
    record.MutableHeaders()->SetExactDiskIdMatch(true);
    NCloud::Send(ctx, MakeVolumeProxyServiceId(), std::move(request),
                 cleanup.Cookie);
}

void TVolumeActor::SendOutdatedLeaderDestroy(const NActors::TActorContext& ctx,
                                             const TLeaderFollowerLink& link,
                                             ui64 expectedTabletId)
{
    OutdatedLeaderDestruction->AwaitingSourceStatus = false;
    auto request = std::make_unique<TEvService::TEvDestroyVolumeRequest>();
    request->Record.SetDiskId(link.LeaderDiskId);
    request->Record.SetExpectedVolumeTabletId(expectedTabletId);
    request->Record.MutableHeaders()->SetShardId(link.LeaderShardId);
    request->Record.MutableHeaders()->SetExactDiskIdMatch(true);
    NCloud::Send(ctx, MakeStorageServiceId(), std::move(request),
                 OutdatedLeaderDestruction->Cookie);
}

void TVolumeActor::HandleOutdatedLeaderStatusResponse(
    const TEvVolume::TEvGetLinkStatusResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    if (!OutdatedLeaderDestruction || !OutdatedLeaderDestruction->InFlight ||
        !OutdatedLeaderDestruction->AwaitingSourceStatus ||
        OutdatedLeaderDestruction->Cookie != ev->Cookie)
    {
        return;
    }
    const auto leader = State->FindLeader(
        TLeaderFollowerLink{.LinkUUID = OutdatedLeaderDestruction->LinkUUID});
    if (!leader || leader->State != TLeaderDiskInfo::EState::Leader) {
        OutdatedLeaderDestruction.reset();
        DestroyOutdatedLeaderIfNeeded(ctx);
        return;
    }
    OutdatedLeaderDestruction->AwaitingSourceStatus = false;
    const auto& record = ev->Get()->Record;
    if (IsNotFoundSchemeShardError(ev->Get()->GetError())) {
        FinishOutdatedLeaderDestroy(ctx, {});
        return;
    }
    if (HasError(ev->Get()->GetError()) || !record.GetVolumeTabletId() ||
        (record.GetStatus() != NProto::LINK_STATUS_NOT_FOUND &&
         record.GetLinkUUID().empty()))
    {
        // An older or incomplete response is not proof of missing ownership.
        FinishOutdatedLeaderDestroy(
            ctx,
            MakeError(E_REJECTED, "Cannot verify the old source incarnation"));
        return;
    }
    if (record.GetStatus() == NProto::LINK_STATUS_NOT_FOUND ||
        record.GetLinkUUID() != leader->Link.LinkUUID)
    {
        FinishOutdatedLeaderDestroy(ctx, {});
        return;
    }
    if (record.GetStatus() != NProto::LINK_STATUS_LEADERSHIP_TRANSFERRED) {
        FinishOutdatedLeaderDestroy(
            ctx,
            MakeError(E_REJECTED, "The source has not transferred leadership"));
        return;
    }
    if (leader->Link.LeaderTabletId &&
        leader->Link.LeaderTabletId != record.GetVolumeTabletId())
    {
        FinishOutdatedLeaderDestroy(ctx, {});
        return;
    }
    const auto mediaKind = record.GetStorageMediaKind();
    const bool replicated = mediaKind == NProto::STORAGE_MEDIA_SSD ||
                            mediaKind == NProto::STORAGE_MEDIA_HDD;
    if (!replicated &&
        Config->GetSchemeShardDirForShard(leader->Link.LeaderShardId) !=
            Config->GetSchemeShardDirForShard(leader->Link.FollowerShardId))
    {
        FinishOutdatedLeaderDestroy(
            ctx, MakeError(E_REJECTED,
                           "Unsupported or unknown remote cleanup media kind"));
        return;
    }
    // Existing same-shard DR/default workflows do not opt into the new
    // conditional-delete contract. Explicit conditional DR requests reject.
    SendOutdatedLeaderDestroy(ctx, leader->Link,
                              replicated ? record.GetVolumeTabletId() : 0);
}

void TVolumeActor::FinishOutdatedLeaderDestroy(
    const NActors::TActorContext& ctx, const NProto::TError& error)
{
    if (!OutdatedLeaderDestruction) {
        return;
    }
    const auto leader = State->FindLeader(
        TLeaderFollowerLink{.LinkUUID = OutdatedLeaderDestruction->LinkUUID});
    if (!leader || leader->State != TLeaderDiskInfo::EState::Leader) {
        OutdatedLeaderDestruction.reset();
        DestroyOutdatedLeaderIfNeeded(ctx);
        return;
    }
    if (HasError(error)) {
        auto& cleanup = *OutdatedLeaderDestruction;
        cleanup.InFlight = false;
        cleanup.DelayProvider.IncreaseDelay();
        ctx.Schedule(
            cleanup.DelayProvider.GetDelay(),
            new TEvVolumePrivate::TEvDestroyOutdatedLeader(cleanup.LinkUUID));
        return;
    }
    // Keep the pending flag until the Principal transaction commits.
    UpdateLeaderLink(CreateRequestInfo({}, 0, MakeIntrusive<TCallContext>()),
                     leader->Link, TLeaderDiskInfo::EState::Principal, ctx);
}

void TVolumeActor::HandleDestroyOutdatedLeaderVolumeResponse(
    const TEvService::TEvDestroyVolumeResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    if (!OutdatedLeaderDestruction || !OutdatedLeaderDestruction->InFlight ||
        OutdatedLeaderDestruction->AwaitingSourceStatus ||
        OutdatedLeaderDestruction->Cookie != ev->Cookie)
    {
        return;
    }
    FinishOutdatedLeaderDestroy(ctx, ev->Get()->GetError());
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
        .FollowerShardId = msg->Record.GetFollowerShardId(),
        .LeaderTabletId = msg->Record.GetLeaderTabletId(),
        .FollowerTabletId = msg->Record.GetFollowerTabletId()};

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
            const bool crossShard =
                Config->GetSchemeShardDirForShard(link.LeaderShardId) !=
                Config->GetSchemeShardDirForShard(link.FollowerShardId);
            if ((link.FollowerTabletId &&
                 link.FollowerTabletId != TabletID()) ||
                (crossShard && !link.FollowerTabletId))
            {
                NCloud::Reply(
                    ctx,
                    *requestInfo,
                    std::make_unique<
                        TEvVolume::TEvUpdateLinkOnFollowerResponse>(MakeError(
                        E_INVALID_STATE,
                        "CREATE addressed a different destination "
                        "incarnation")));
                return;
            }
            CreateLeaderLink(std::move(requestInfo), std::move(link), ctx);
            break;
        }
        case NProto::LINK_ACTION_DESTROY: {
            DestroyLeaderLink(std::move(requestInfo), std::move(link),
                              msg->Record.GetRequireCancellable(), ctx);
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
