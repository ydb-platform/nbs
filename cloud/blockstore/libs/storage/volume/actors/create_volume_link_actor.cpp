#include "create_volume_link_actor.h"

#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/volume/actors/propagate_to_follower.h>

#include <utility>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

enum EDescribeKind : ui64
{
    DESCRIBE_KIND_LEADER = 0,
    DESCRIBE_KIND_FOLLOWER = 1
};

}   // namespace

TCreateVolumeLinkActor::TCreateVolumeLinkActor(
        TString logPrefix,
        NActors::TActorId volumeActorId,
        TLeaderFollowerLink link)
    : LogPrefix(std::move(logPrefix))
    , VolumeActorId(volumeActorId)
    , Follower{
          .Link = std::move(link),
          .CreatedAt = TInstant::Now(),
          .State = TFollowerDiskInfo::EState::None}
{}

void TCreateVolumeLinkActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(
            Follower.Link.LeaderDiskId),
        DESCRIBE_KIND_LEADER);
    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(
            Follower.Link.FollowerDiskId),
        DESCRIBE_KIND_FOLLOWER);
}

void TCreateVolumeLinkActor::LinkVolumes(const TActorContext& ctx)
{
    if (!LeaderVolume.GetDiskId() || !FollowerVolume.GetDiskId()) {
        return;
    }

    const auto sourceSize =
        LeaderVolume.GetBlocksCount() * LeaderVolume.GetBlockSize();
    const auto targetSize =
        FollowerVolume.GetBlocksCount() * FollowerVolume.GetBlockSize();

    if (sourceSize > targetSize) {
        auto errorMessage = TStringBuilder()
                            << "The size of the leader disk "
                            << Follower.Link.LeaderDiskIdForPrint().Quote()
                            << " is larger than the size of follower disk "
                            << Follower.Link.FollowerDiskIdForPrint().Quote() << " "
                            << sourceSize << " > " << targetSize;
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s %s",
            LogPrefix.c_str(),
            errorMessage.c_str());

        ReplyAndDie(ctx, MakeError(E_ARGUMENT, errorMessage));
        return;
    }

    Follower.MediaKind = FollowerVolume.GetStorageMediaKind();
    Follower.State = TFollowerDiskInfo::EState::Created;
    PersistOnLeader(ctx);
}

void TCreateVolumeLinkActor::PersistOnLeader(const NActors::TActorContext& ctx)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Persist %s on leader %s",
        LogPrefix.c_str(),
        Follower.Link.Describe().c_str(),
        Follower.Describe().c_str());

    auto request =
        std::make_unique<TEvVolumePrivate::TEvUpdateFollowerStateRequest>(
            Follower);

    NCloud::Send(ctx, VolumeActorId, std::move(request));
}

void TCreateVolumeLinkActor::PersistOnFollower(
    const NActors::TActorContext& ctx)
{
    NCloud::Register<TPropagateLinkToFollowerActor>(
        ctx,
        LogPrefix,
        CreateRequestInfo(SelfId(), 0, MakeIntrusive<TCallContext>()),
        Follower.Link,
        TPropagateLinkToFollowerActor::EReason::Creation);
}

void TCreateVolumeLinkActor::HandleDescribeVolumeResponse(
    const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& diskId = (ev->Cookie == DESCRIBE_KIND_LEADER)
                             ? Follower.Link.LeaderDiskIdForPrint()
                             : Follower.Link.FollowerDiskIdForPrint();
    auto& volume =
        (ev->Cookie == DESCRIBE_KIND_LEADER) ? LeaderVolume : FollowerVolume;

    const auto& error = msg->GetError();
    if (HasError(error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s %s Describe volume %s failed: %s",
            LogPrefix.c_str(),
            Follower.Link.Describe().c_str(),
            diskId.Quote().c_str(),
            FormatError(error).c_str());
        ReplyAndDie(ctx, error);
        return;
    }

    const auto& pathDescription = msg->PathDescription;
    const auto& volumeDescription =
        pathDescription.GetBlockStoreVolumeDescription();
    const auto& volumeConfig = volumeDescription.GetVolumeConfig();

    VolumeConfigToVolume(volumeConfig, "", volume);
    volume.SetTokenVersion(volumeDescription.GetTokenVersion());

    LinkVolumes(ctx);
}

void TCreateVolumeLinkActor::HandlePersistedOnLeader(
    const TEvVolumePrivate::TEvUpdateFollowerStateResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* message = ev->Get();
    auto error = message->GetError();

    if (HasError(error)) {
        ReplyAndDie(ctx, message->GetError());
        return;
    }

    switch (message->Follower.State) {
        case TFollowerDiskInfo::EState::DataReady:
        case TFollowerDiskInfo::EState::Error: {
            ReplyAndDie(
                ctx,
                MakeError(
                    E_INVALID_STATE,
                    TStringBuilder()
                        << "unexpected follower state during link creation: "
                        << ToString(message->Follower.State).Quote()
                        << " error: "
                        << message->Follower.ErrorMessage.Quote()));
            break;
        }
        case TFollowerDiskInfo::EState::None:
        case TFollowerDiskInfo::EState::Created: {
            PersistOnFollower(ctx);
            break;
        }
        case TFollowerDiskInfo::EState::Preparing: {
            ReplyAndDie(ctx, error);
            break;
        }
    }
}

void TCreateVolumeLinkActor::HandlePersistedOnFollower(
    const TEvVolumePrivate::TEvLinkOnFollowerCreated::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* message = ev->Get();
    auto error = message->GetError();

    if (HasError(error)) {
        Follower.State = TFollowerDiskInfo::EState::Error;
        Follower.ErrorMessage = FormatError(error);
    } else {
        Follower.State = TFollowerDiskInfo::EState::Preparing;
    }
    PersistOnLeader(ctx);
}

void TCreateVolumeLinkActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (HasError(error)) {
        Follower.State = TFollowerDiskInfo::EState::Error;
        Follower.ErrorMessage = FormatError(error);

        auto request =
            std::make_unique<TEvVolumePrivate::TEvUpdateFollowerStateRequest>(
                Follower);
        NCloud::Send(ctx, VolumeActorId, std::move(request));
    }

    auto response = std::make_unique<TEvVolumePrivate::TEvCreateLinkFinished>(
        error,
        Follower.Link);
    NCloud::Send(ctx, VolumeActorId, std::move(response));

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TCreateVolumeLinkActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvSSProxy::TEvDescribeVolumeResponse,
            HandleDescribeVolumeResponse);

        HFunc(
            TEvVolumePrivate::TEvUpdateFollowerStateResponse,
            HandlePersistedOnLeader);

        HFunc(
            TEvVolumePrivate::TEvLinkOnFollowerCreated,
            HandlePersistedOnFollower);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::VOLUME,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage

////////////////////////////////////////////////////////////////////////////////
