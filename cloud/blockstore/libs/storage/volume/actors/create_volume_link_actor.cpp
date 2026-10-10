#include "create_volume_link_actor.h"

#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/volume/actors/propagate_to_follower.h>

#include <cloud/storage/core/libs/common/media.h>

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
    TString logPrefix, NActors::TActorId volumeActorId,
    TLeaderFollowerLink link, bool allowDiskRegistryMedia,
    bool recoveringCreated)
    : LogPrefix(std::move(logPrefix))
    , VolumeActorId(volumeActorId)
    , AllowDiskRegistryMedia(allowDiskRegistryMedia)
    , Follower{
          .Link = std::move(link),
          .CreatedAt = TInstant::Now(),
          .State = recoveringCreated ? TFollowerDiskInfo::EState::Created
                                     : TFollowerDiskInfo::EState::None}
{}

void TCreateVolumeLinkActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(
            Follower.Link.LeaderDiskId, true, Follower.Link.LeaderShardId),
        DESCRIBE_KIND_LEADER);
    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(
            Follower.Link.FollowerDiskId, true, Follower.Link.FollowerShardId),
        DESCRIBE_KIND_FOLLOWER);
}

void TCreateVolumeLinkActor::LinkVolumes(const TActorContext& ctx)
{
    if (!LeaderVolume.GetDiskId() || !FollowerVolume.GetDiskId()) {
        return;
    }

    if (!AllowDiskRegistryMedia &&
        ((LeaderVolume.GetStorageMediaKind() != NProto::STORAGE_MEDIA_SSD &&
          LeaderVolume.GetStorageMediaKind() != NProto::STORAGE_MEDIA_HDD) ||
         (FollowerVolume.GetStorageMediaKind() != NProto::STORAGE_MEDIA_SSD &&
          FollowerVolume.GetStorageMediaKind() != NProto::STORAGE_MEDIA_HDD)))
    {
        ReplyAndDie(
            ctx,
            MakeError(
                E_NOT_IMPLEMENTED,
                "Cross-shard links support only replicated SSD/HDD volumes"));
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
                            << Follower.Link.FollowerDiskIdForPrint().Quote()
                            << " " << sourceSize << " > " << targetSize;
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
    if (Follower.Link.FollowerGeneration.value_or(0)) {
        // Recovery must not refresh a cancelled operation's generation.
        PersistOnLeader(ctx);
    } else if (!Follower.Link.FollowerTabletId && AllowDiskRegistryMedia) {
        // A description without a target identity cannot bind a generation.
        Follower.Link.FollowerGeneration.reset();
        PersistOnLeader(ctx);
    } else {
        auto request = std::make_unique<TEvVolume::TEvGetLinkStatusRequest>();
        auto& record = request->Record;
        record.SetDiskId(Follower.Link.FollowerDiskId);
        record.SetLinkUUID(Follower.Link.LinkUUID);
        record.MutableHeaders()->SetShardId(Follower.Link.FollowerShardId);
        record.MutableHeaders()->SetExactDiskIdMatch(true);
        NCloud::Send(ctx, MakeVolumeProxyServiceId(), std::move(request));
    }
}

void TCreateVolumeLinkActor::HandleGenerationResponse(
    const TEvVolume::TEvGetLinkStatusResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }
    const auto& record = msg->Record;
    if (record.GetNextLeaderLinkGeneration()) {
        if (record.GetVolumeTabletId() != Follower.Link.FollowerTabletId) {
            ReplyAndDie(ctx, MakeError(E_INVALID_STATE,
                                       "Destination incarnation changed"));
            return;
        }
        const bool knownUuid =
            record.GetLinkUUID() == Follower.Link.LinkUUID &&
            record.GetStatus() != NProto::LINK_STATUS_NOT_FOUND &&
            record.GetStatus() != NProto::LINK_STATUS_NONE &&
            record.GetStatus() != NProto::LINK_STATUS_ERROR;
        if (!Follower.Link.FollowerGeneration &&
            record.GetNextLeaderLinkGeneration() > 1 && !knownUuid)
        {
            // After the first generation is consumed, the legacy UUID
            // history may have been compacted. Do not assign a fresh token
            // to an unknown recovered UUID which could have been cancelled.
            ReplyAndDie(
                ctx,
                MakeError(
                    E_INVALID_STATE,
                    "Legacy link is no longer known on "
                    "the destination"));
            return;
        }
        Follower.Link.FollowerGeneration = record.GetNextLeaderLinkGeneration();
    } else {
        // An older owner has not enabled the generation protocol.
        Follower.Link.FollowerGeneration.reset();
    }
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
    CreationPropagator = NCloud::Register<TPropagateLinkToFollowerActor>(
        ctx, LogPrefix,
        CreateRequestInfo(SelfId(), 0, MakeIntrusive<TCallContext>()),
        Follower.Link, TPropagateLinkToFollowerActor::EReason::Creation);
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

    if (ev->Cookie == DESCRIBE_KIND_FOLLOWER) {
        const auto tabletId = volumeDescription.GetVolumeTabletId();
        if ((Follower.Link.FollowerTabletId &&
             Follower.Link.FollowerTabletId != tabletId) ||
            (!AllowDiskRegistryMedia && !tabletId))
        {
            ReplyAndDie(
                ctx,
                MakeError(
                    E_INVALID_STATE,
                    "Destination volume incarnation changed or is unknown"));
            return;
        }
        Follower.Link.FollowerTabletId = tabletId;
    }
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

    if (message->Follower.Link.LinkUUID != Follower.Link.LinkUUID) {
        ReplyAndDie(ctx, MakeError(E_INVALID_STATE,
                                   "Link creation was cancelled or replaced"));
        return;
    }
    Follower = message->Follower;
    switch (message->Follower.State) {
        case TFollowerDiskInfo::EState::DataReady:
        case TFollowerDiskInfo::EState::LeadershipTransferred:
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
    if (HasError(error) && Follower.State != TFollowerDiskInfo::EState::None) {
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

    if (CreationPropagator) {
        NCloud::Send(ctx, CreationPropagator,
                     std::make_unique<NActors::TEvents::TEvPoisonPill>());
    }
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TCreateVolumeLinkActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvSSProxy::TEvDescribeVolumeResponse,
            HandleDescribeVolumeResponse);

        HFunc(TEvVolume::TEvGetLinkStatusResponse, HandleGenerationResponse);

        HFunc(
            TEvVolumePrivate::TEvUpdateFollowerStateResponse,
            HandlePersistedOnLeader);

        HFunc(
            TEvVolumePrivate::TEvLinkOnFollowerCreated,
            HandlePersistedOnFollower);

        case NActors::TEvents::TEvPoisonPill::EventType:
            ReplyAndDie(
                ActorContext(),
                MakeError(E_INVALID_STATE, "Link creation was cancelled"));
            break;
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
