#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCreateVolumeLinkActor final
    : public TActorBootstrapped<TCreateVolumeLinkActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString LeaderDiskId;
    const TString LeaderCellId;
    const TString FollowerDiskId;
    const TString FollowerCellId;

public:
    TCreateVolumeLinkActor(
        TRequestInfoPtr requestInfo,
        TString leaderDiskId,
        TString leaderCellId,
        TString followerDiskId,
        TString followerCellId);

    void Bootstrap(const TActorContext& ctx);

private:
    void HandleLinkLeaderVolumeToFollowerResponse(
        const TEvVolume::TEvLinkLeaderVolumeToFollowerResponse::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvCreateVolumeLinkResponse> response);

private:
    STFUNC(StateWork);
};

////////////////////////////////////////////////////////////////////////////////

TCreateVolumeLinkActor::TCreateVolumeLinkActor(
        TRequestInfoPtr requestInfo,
        TString leaderDiskId,
        TString leaderCellId,
        TString followerDiskId,
        TString followerCellId)
    : RequestInfo(std::move(requestInfo))
    , LeaderDiskId(std::move(leaderDiskId))
    , LeaderCellId(std::move(leaderCellId))
    , FollowerDiskId(std::move(followerDiskId))
    , FollowerCellId(std::move(followerCellId))
{}

void TCreateVolumeLinkActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    auto request =
        std::make_unique<TEvVolume::TEvLinkLeaderVolumeToFollowerRequest>(
            RequestInfo->CallContext);
    request->Record.SetDiskId(LeaderDiskId);
    request->Record.SetLeaderCellId(LeaderCellId);
    request->Record.SetFollowerDiskId(FollowerDiskId);
    request->Record.SetFollowerCellId(FollowerCellId);

    NCloud::Send(
        ctx,
        MakeVolumeProxyServiceId(),
        std::move(request),
        RequestInfo->Cookie);
}

void TCreateVolumeLinkActor::HandleLinkLeaderVolumeToFollowerResponse(
    const TEvVolume::TEvLinkLeaderVolumeToFollowerResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* message = ev->Get();
    ReplyAndDie(
        ctx,
        std::make_unique<TEvService::TEvCreateVolumeLinkResponse>(
            message->GetError()));
}

void TCreateVolumeLinkActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvCreateVolumeLinkResponse> response)
{
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TCreateVolumeLinkActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvVolume::TEvLinkLeaderVolumeToFollowerResponse,
            HandleLinkLeaderVolumeToFollowerResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TServiceActor::HandleCreateVolumeLink(
    const TEvService::TEvCreateVolumeLinkRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    const auto& request = msg->Record;

    if (request.GetLeaderDiskId().empty()) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Empty Leader DiskId in CreateVolumeLink");

        auto response =
            std::make_unique<TEvService::TEvCreateVolumeLinkResponse>(
                MakeError(E_ARGUMENT, "Leader volume name cannot be empty"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }
    if (request.GetFollowerDiskId().empty()) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Empty Follower DiskId in CreateVolumeLink");

        auto response =
            std::make_unique<TEvService::TEvCreateVolumeLinkResponse>(
                MakeError(E_ARGUMENT, "Follower volume name cannot be empty"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }
    if (request.GetLeaderDiskId() == request.GetFollowerDiskId()) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Source and Target should be different in CreateVolumeLink");

        auto response =
            std::make_unique<TEvService::TEvCreateVolumeLinkResponse>(MakeError(
                E_ARGUMENT,
                "Leader and Follower should be different"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    const auto& followerCellId =
        request.GetFollowerCellId() ? request.GetFollowerCellId() : CellId;

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::SERVICE,
        "CreateVolumeLink leader: %s/%s, follower: %s/%s",
        CellId.Quote().data(),
        request.GetLeaderDiskId().Quote().data(),
        followerCellId.Quote().data(),
        request.GetFollowerDiskId().Quote().data());

    NCloud::Register<TCreateVolumeLinkActor>(
        ctx,
        std::move(requestInfo),
        request.GetLeaderDiskId(),
        CellId,
        request.GetFollowerDiskId(),
        followerCellId);
}

}   // namespace NCloud::NBlockStore::NStorage
