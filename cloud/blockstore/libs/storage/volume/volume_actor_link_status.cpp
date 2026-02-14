#include "volume_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::ELinkStatus TranslateState(TFollowerDiskInfo::EState state)
{
    switch (state) {
        case TFollowerDiskInfo::EState::None:
        case TFollowerDiskInfo::EState::Created:
        case TFollowerDiskInfo::EState::Preparing:
            return NProto::ELinkStatus::LINK_STATUS_PREPARING;

        case TFollowerDiskInfo::EState::DataReady:
            return NProto::ELinkStatus::LINK_STATUS_LEADERSHIP_TRANSFERRING;

        case TFollowerDiskInfo::EState::LeadershipTransferred:
            return NProto::ELinkStatus::LINK_STATUS_LEADERSHIP_TRANSFERRED;

        case TFollowerDiskInfo::EState::Error:
            return NProto::ELinkStatus::LINK_STATUS_ERROR;
    }
    return NProto::ELinkStatus::LINK_STATUS_NOT_FOUND;
}

NProto::ELinkStatus TranslateState(TLeaderDiskInfo::EState state)
{
    switch (state) {
        case TLeaderDiskInfo::EState::None:
        case TLeaderDiskInfo::EState::Following:
            return NProto::ELinkStatus::LINK_STATUS_PREPARING;

        case TLeaderDiskInfo::EState::Leader:
            return NProto::ELinkStatus::LINK_STATUS_LEADERSHIP_TRANSFERRED;

        case TLeaderDiskInfo::EState::Principal:
            return NProto::ELinkStatus::LINK_STATUS_COMPLETED;
    }
    return NProto::ELinkStatus::LINK_STATUS_NOT_FOUND;
}

}   // namespace

void TVolumeActor::HandleGetLinkStatus(
    const TEvVolume::TEvGetLinkStatusRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto link = TLeaderFollowerLink{
        .LinkUUID = "",
        .LeaderDiskId = msg->Record.GetLeaderDiskId(),
        .LeaderShardId = msg->Record.GetLeaderShardId(),
        .FollowerDiskId = msg->Record.GetFollowerDiskId(),
        .FollowerShardId = msg->Record.GetFollowerShardId()};

    auto follower = State->FindFollower(link);
    if (follower) {
        link = follower->Link;
    }
    auto leader = State->FindLeader(link);
    if (leader) {
        link = leader->Link;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Get link status %s Follower: %s, Leader: %s",
        LogTitle.GetWithTime().c_str(),
        link.Describe().c_str(),
        follower ? follower->Describe().c_str() : "<none>",
        leader ? leader->Describe().c_str() : "<none>");

    auto response =
        std::make_unique<TEvVolume::TEvGetLinkStatusResponse>(MakeError(S_OK));

    if (follower) {
        response->Record.SetStatus(TranslateState(follower->State));
        response->Record.SetMigratedBytes(follower->MigratedBytes.value_or(0));
        response->Record.SetErrorMessage(follower->ErrorMessage);
    } else if (leader) {
        response->Record.SetStatus(TranslateState(leader->State));
        response->Record.SetErrorMessage(leader->ErrorMessage);
    } else {
        response->Record.SetStatus(NProto::ELinkStatus::LINK_STATUS_NOT_FOUND);
    }

    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
