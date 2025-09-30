#include "follower_disk_actor.h"

#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/config.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl.h>
#include <cloud/blockstore/libs/storage/stats_service/stats_service_events_private.h>
#include <cloud/blockstore/libs/storage/volume/actors/propagate_to_follower.h>
#include <cloud/blockstore/libs/storage/volume/actors/volume_as_partition_actor.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>

#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/media.h>

using namespace NActors;

namespace NCloud::NBlockStore::NStorage {

namespace {

constexpr auto RebootVolumeTabletDelay = TDuration::Seconds(30);

////////////////////////////////////////////////////////////////////////////////

EDirectCopyPolicy GetDirectCopyUsage(const TFollowerDiskActorParams& params)
{
    const bool sameShard = params.FollowerDiskInfo.Link.LeaderShardId ==
                           params.FollowerDiskInfo.Link.FollowerShardId;
    const bool bothDiskRegistry =
        IsDiskRegistryMediaKind(params.LeaderMediaKind) &&
        IsDiskRegistryMediaKind(params.FollowerDiskInfo.MediaKind);
    return (sameShard && bothDiskRegistry) ? EDirectCopyPolicy::CanUse
                                           : EDirectCopyPolicy::DoNotUse;
}

}   // namespace

///////////////////////////////////////////////////////////////////////////////

TFollowerDiskActor::TFollowerDiskActor(
        const TLogTitle& parentLogTitle,
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticConfig,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr digestGenerator,
        TFollowerDiskActorParams params)
    : TNonreplicatedPartitionMigrationCommonActor(
          static_cast<IMigrationOwner*>(this),
          config,
          std::move(diagnosticConfig),
          params.LeaderDiskId,
          params.LeaderBlockCount,
          params.LeaderBlockSize,
          std::move(profileLog),
          std::move(digestGenerator),
          params.FollowerDiskInfo.MigratedBytes.value_or(0) /
              params.LeaderBlockSize,
          params.ClientId,
          params.LeaderVolumeActorId,
          GetLinkedDiskFillBandwidth(
              *config,
              params.LeaderMediaKind,
              params.FollowerDiskInfo.MediaKind)
              .IoDepth,
          params.LeaderVolumeActorId,
          GetDirectCopyUsage(params))
    , LogTitle(parentLogTitle.GetChildWithTags(
          GetCycleCount(),
          {{"l", params.FollowerDiskInfo.Link.Describe()}}))
    , LeaderMediaKind(params.LeaderMediaKind)
    , LeaderBlockCount(params.LeaderBlockCount)
    , LeaderBlockSize(params.LeaderBlockSize)
    , LeaderVolumeActorId(params.LeaderVolumeActorId)
    , LeaderPartitionActorId(params.LeaderPartitionActorId)
    , TakePartitionOwnership(params.TakePartitionOwnership)
    , ClientId(params.ClientId)
    , FollowerDiskInfo(params.FollowerDiskInfo)
{
    Y_DEBUG_ABORT_UNLESS(LeaderMediaKind != NProto::STORAGE_MEDIA_DEFAULT);
    Y_DEBUG_ABORT_UNLESS(
        FollowerDiskInfo.MediaKind != NProto::STORAGE_MEDIA_DEFAULT);
}

TFollowerDiskActor::~TFollowerDiskActor() = default;

void TFollowerDiskActor::OnBootstrap(const NActors::TActorContext& ctx)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Follower created",
        LogTitle.GetWithTime().c_str());

    FollowerPartitionActorId = NCloud::Register<TVolumeAsPartitionActor>(
        ctx,
        LogTitle,
        LeaderBlockSize,
        FollowerDiskInfo.Link.FollowerDiskId);

    InitWork(
        ctx,
        TInitParams{
            .MigrationSrcActorId = LeaderPartitionActorId,
            .SrcActorId = LeaderPartitionActorId,
            .DstActorId = FollowerPartitionActorId,
            .TakeOwnershipOverSrcActor = TakePartitionOwnership,
            .TakeOwnershipOverDstActor = true,
            .SendWritesToSrc = true,
            .TimeoutCalculator = std::make_unique<TMigrationTimeoutCalculator>(
                GetLinkedDiskFillBandwidth(
                    *GetConfig(),
                    LeaderMediaKind,
                    FollowerDiskInfo.MediaKind)
                    .Bandwidth,
                GetConfig()->GetExpectedDiskAgentSize(),
                nullptr)});

    StartWork(ctx);
}

bool TFollowerDiskActor::OnMessage(
    const TActorContext& ctx,
    TAutoPtr<NActors::IEventHandle>& ev)
{
    Y_UNUSED(ctx);
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvVolume::TEvRdmaUnavailable, HandleRdmaUnavailable);
        HFunc(
            TEvVolumePrivate::TEvUpdateFollowerStateResponse,
            HandleUpdateFollowerStateResponse);
        HFunc(
            TEvVolumePrivate::TEvLinkOnFollowerCompleted,
            HandlePropagateLeadershipToFollowerResponse);

        // Intercepting the message to block the sending of statistics by the
        // base class.
        IgnoreFunc(TEvNonreplPartitionPrivate::TEvUpdateCounters);

        // ClientId changed event.
        case TEvVolume::TEvRWClientIdChanged::EventType: {
            return HandleRWClientIdChanged(
                *reinterpret_cast<TEvVolume::TEvRWClientIdChanged::TPtr*>(&ev),
                ctx);
        }

        default:
            // Message processing by the base class is required.
            return false;
    }

    // We get here if we have processed an incoming message. And its processing
    // by the base class is not required.
    return true;
}

void TFollowerDiskActor::OnMigrationProgress(
    const NActors::TActorContext& ctx,
    ui64 migrationIndex)
{
    auto newFollowerInfo = FollowerDiskInfo;
    newFollowerInfo.State = TFollowerDiskInfo::EState::Preparing;
    newFollowerInfo.MigratedBytes = migrationIndex * LeaderBlockSize;

    PersistFollowerState(ctx, newFollowerInfo);
}

void TFollowerDiskActor::OnMigrationFinished(const NActors::TActorContext& ctx)
{
    auto newFollowerInfo = FollowerDiskInfo;
    newFollowerInfo.State = TFollowerDiskInfo::EState::DataReady;
    newFollowerInfo.MigratedBytes = LeaderBlockCount * LeaderBlockSize;

    PersistFollowerState(ctx, newFollowerInfo);
}

void TFollowerDiskActor::OnMigrationError(const NActors::TActorContext& ctx)
{
    auto newFollowerInfo = FollowerDiskInfo;
    newFollowerInfo.State = TFollowerDiskInfo::EState::Error;
    newFollowerInfo.ErrorMessage = "Migration error";

    PersistFollowerState(ctx, newFollowerInfo);
}

void TFollowerDiskActor::PersistFollowerState(
    const NActors::TActorContext& ctx,
    const TFollowerDiskInfo& newDiskInfo)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Persist follower state %s",
        LogTitle.GetWithTime().c_str(),
        newDiskInfo.Describe().c_str());

    auto request =
        std::make_unique<TEvVolumePrivate::TEvUpdateFollowerStateRequest>(
            newDiskInfo);

    NCloud::Send(ctx, LeaderVolumeActorId, std::move(request));
}

void TFollowerDiskActor::AdvanceState(
    const NActors::TActorContext& ctx,
    EState newState)
{
    Y_DEBUG_ABORT_UNLESS(newState >= State);

    if (State == newState) {
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s State changed %s -> %s",
        LogTitle.GetWithTime().c_str(),
        ToString(State).c_str(),
        ToString(newState).c_str());

    State = newState;

    switch (State) {
        case EState::DataTransfer: {
            break;
        }
        case EState::DataReady:{
            PropagateLeadershipToFollower(ctx);
            break;
        }
        case EState::LeadershipTransferred:{
            RebootLeaderVolume(ctx);
            break;
        }
    }
}

void TFollowerDiskActor::PropagateLeadershipToFollower(
    const NActors::TActorContext& ctx)
{
    NCloud::Register<TPropagateLinkToFollowerActor>(
        ctx,
        LogTitle.GetWithTime(),
        CreateRequestInfo(SelfId(), 0, MakeIntrusive<TCallContext>()),
        FollowerDiskInfo.Link,
        TPropagateLinkToFollowerActor::EReason::Completion);
}

void TFollowerDiskActor::RebootLeaderVolume(const NActors::TActorContext& ctx)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s  Scheduling reboot for leader volume after %s",
        LogTitle.GetWithTime().c_str(),
        FormatDuration(RebootVolumeTabletDelay).c_str());

    ctx.Schedule(
        RebootVolumeTabletDelay,
        std::make_unique<IEventHandle>(
            LeaderVolumeActorId,
            ctx.SelfID,
            new TEvents::TEvPoisonPill()));
}

template <typename TMethod>
void TFollowerDiskActor::ForwardRequestToLeaderPartition(
    const typename TMethod::TRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    ForwardMessageToActor(ev, ctx, LeaderPartitionActorId);
}

template <typename TMethod>
void TFollowerDiskActor::ForwardRequestToFollowerPartition(
    const typename TMethod::TRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    ForwardMessageToActor(ev, ctx, FollowerPartitionActorId);
}

void TFollowerDiskActor::HandleRdmaUnavailable(
    const TEvVolume::TEvRdmaUnavailable::TPtr& ev,
    const TActorContext& ctx)
{
    ForwardMessageToActor(ev, ctx, LeaderVolumeActorId);
}

bool TFollowerDiskActor::HandleRWClientIdChanged(
    const TEvVolume::TEvRWClientIdChanged::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Changed clientId %s -> %s ",
        LogTitle.GetWithTime().c_str(),
        ClientId.Quote().c_str(),
        ev->Get()->RWClientId.Quote().c_str());

    ClientId = ev->Get()->RWClientId;

    // Notify the source partition about the new clientId.
    NCloud::Send(
        ctx,
        LeaderPartitionActorId,
        std::make_unique<TEvVolume::TEvRWClientIdChanged>(ClientId));

    // It is necessary to handle the EvRWClientIdChanged message in the base
    // class TNonreplicatedPartitionMigrationCommonActor too.
    return false;
}

void TFollowerDiskActor::HandleUpdateFollowerStateResponse(
    const TEvVolumePrivate::TEvUpdateFollowerStateResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    FollowerDiskInfo = msg->Follower;

    if (FollowerDiskInfo.State == TFollowerDiskInfo::EState::DataReady) {
        AdvanceState(ctx, EState::DataReady);
    } else if (FollowerDiskInfo.State == TFollowerDiskInfo::EState::Error) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Link in error state. Restart volume tablet immediately",
            LogTitle.GetWithTime().c_str());
        NCloud::Send(
            ctx,
            LeaderVolumeActorId,
            std::make_unique<TEvents::TEvPoisonPill>());
    }
}

void TFollowerDiskActor::HandlePropagateLeadershipToFollowerResponse(
    const TEvVolumePrivate::TEvLinkOnFollowerCompleted::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const auto&  error = msg->GetError();
    if (HasError(error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Propagate ready state to follower error: %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(msg->GetError()).c_str());

        if (IsNotFoundSchemeShardError(error)) {
            // If it is not possible to transfer leadership because there is no
            // follower disk, then we should go into an error state.
            auto newFollowerInfo = FollowerDiskInfo;
            newFollowerInfo.State = TFollowerDiskInfo::EState::Error;
            newFollowerInfo.ErrorMessage = FormatError(msg->GetError());
            PersistFollowerState(ctx, newFollowerInfo);
        } else {
            // We need to repeat the leadership transfer, and not go into error,
            // since the client could have already switched to a new leader, and
            // passing into an error state can lead the client to switch to an
            // old leader who is already outdated.
            PropagateLeadershipToFollower(ctx);
        }
        return;
    }

    AdvanceState(ctx, EState::LeadershipTransferred);
}

}   // namespace NCloud::NBlockStore::NStorage
