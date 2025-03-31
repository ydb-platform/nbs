#include "follower_disk_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/config.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl.h>
#include <cloud/blockstore/libs/storage/stats_service/stats_service_events_private.h>
#include <cloud/blockstore/libs/storage/volume/actors/volume_as_partition_actor.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>

#include <cloud/storage/core/libs/common/media.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

using namespace NActors;

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

ui32 GetMaxIoDepth(
    const TStorageConfig& config,
    const TLeaderVolume& leaderVolume,
    const TFollowerVolume& followerVolume)
{
    auto followerMediaKind = followerVolume.DiskInfo.MediaKind
                                 ? *followerVolume.DiskInfo.MediaKind
                                 : NProto::STORAGE_MEDIA_DEFAULT;

    return GetLinkedDiskFillBandwidth(
               config,
               leaderVolume.MediaKind,
               followerMediaKind)
        .IoDepth;
}

ui32 GetFillBandwidth(
    const TStorageConfig& config,
    const TLeaderVolume& leaderVolume,
    const TFollowerVolume& followerVolume)
{
    auto followerMediaKind = followerVolume.DiskInfo.MediaKind
                                 ? *followerVolume.DiskInfo.MediaKind
                                 : NProto::STORAGE_MEDIA_DEFAULT;

    return GetLinkedDiskFillBandwidth(
               config,
               leaderVolume.MediaKind,
               followerMediaKind)
        .Bandwidth;
}

EDirectCopyPolicy GetDirectCopyUsage(
    const TLeaderVolume& leaderVolume,
    const TFollowerVolume& followerVolume)
{
    auto followerMediaKind = followerVolume.DiskInfo.MediaKind
                                 ? *followerVolume.DiskInfo.MediaKind
                                 : NProto::STORAGE_MEDIA_DEFAULT;

    return (IsDiskRegistryMediaKind(leaderVolume.MediaKind) &&
            IsDiskRegistryMediaKind(followerMediaKind))
               ? EDirectCopyPolicy::CanUse
               : EDirectCopyPolicy::DoNotUse;
}

}   // namespace

///////////////////////////////////////////////////////////////////////////////

TFollowerDiskActor::TFollowerDiskActor(
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticConfig,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr digestGenerator,
        TLeaderVolume leaderVolume,
        TFollowerVolume followerVolume)
    : TNonreplicatedPartitionMigrationCommonActor(
          static_cast<IMigrationOwner*>(this),
          config,
          std::move(diagnosticConfig),
          leaderVolume.DiskId,
          leaderVolume.BlockCount,
          leaderVolume.BlockSize,
          std::move(profileLog),
          std::move(digestGenerator),
          followerVolume.DiskInfo.MigratedBytes.value_or(0) /
              leaderVolume.BlockSize,
          leaderVolume.ClientId,
          leaderVolume.VolumeActorId,
          GetMaxIoDepth(*config, leaderVolume, followerVolume),
          leaderVolume.VolumeActorId,
          GetDirectCopyUsage(leaderVolume, followerVolume))
    , LeaderVolume(std::move(leaderVolume))
    , FollowerVolume(std::move(followerVolume))
{
    Y_DEBUG_ABORT_UNLESS(
        LeaderVolume.MediaKind != NProto::STORAGE_MEDIA_DEFAULT);
    Y_DEBUG_ABORT_UNLESS(FollowerVolume.DiskInfo.MediaKind.has_value());
}

TFollowerDiskActor::~TFollowerDiskActor() = default;

void TFollowerDiskActor::OnBootstrap(const NActors::TActorContext& ctx)
{
    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::VOLUME,
        "TFollowerDiskActor::TFollowerDiskActor "
            << LeaderVolume.DiskId.Quote() << " -> "
            << FollowerVolume.DiskInfo.Link.Describe());

    FollowerVolume.VolumeActorId = NCloud::Register<TVolumeAsPartitionActor>(
        ctx,
        LeaderVolume.DiskId,
        LeaderVolume.BlockSize,
        FollowerVolume.DiskInfo.Link.FollowerDiskId);

    InitWork(
        ctx,
        LeaderVolume.PartitionActorId,
        LeaderVolume.PartitionActorId,
        FollowerVolume.VolumeActorId,
        true,   // takeOwnershipOverActors
        std::make_unique<TMigrationTimeoutCalculator>(
            GetFillBandwidth(*GetConfig(), LeaderVolume, FollowerVolume),
            GetConfig()->GetExpectedDiskAgentSize(),
            nullptr));

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
    auto newFollowerInfo = FollowerVolume.DiskInfo;
    newFollowerInfo.State = TFollowerDiskInfo::EState::Preparing;
    newFollowerInfo.MigratedBytes = migrationIndex * LeaderVolume.BlockSize;

    PersistFollowerState(ctx, newFollowerInfo);
}

void TFollowerDiskActor::OnMigrationFinished(const NActors::TActorContext& ctx)
{
    auto newFollowerInfo = FollowerVolume.DiskInfo;
    newFollowerInfo.State = TFollowerDiskInfo::EState::DataReady;
    newFollowerInfo.MigratedBytes =
        LeaderVolume.BlockCount * LeaderVolume.BlockSize;

    PersistFollowerState(ctx, newFollowerInfo);
}

void TFollowerDiskActor::OnMigrationError(const NActors::TActorContext& ctx)
{
    auto newFollowerInfo = FollowerVolume.DiskInfo;
    newFollowerInfo.State = TFollowerDiskInfo::EState::Error;

    PersistFollowerState(ctx, newFollowerInfo);
}

void TFollowerDiskActor::PersistFollowerState(
    const NActors::TActorContext& ctx,
    const TFollowerDiskInfo& newDiskInfo)
{
    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::VOLUME,
        "TFollowerDiskActor::PersistFollowerState "
            << FollowerVolume.DiskInfo.Link.Describe() << " "
            << newDiskInfo.Describe());

    auto request =
        std::make_unique<TEvVolumePrivate::TEvUpdateFollowerStateRequest>(
            newDiskInfo);

    NCloud::Send(ctx, LeaderVolume.VolumeActorId, std::move(request));
}

template <typename TMethod>
void TFollowerDiskActor::ForwardRequestToLeaderPartition(
    const typename TMethod::TRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    ForwardMessageToActor(ev, ctx, LeaderVolume.PartitionActorId);
}

template <typename TMethod>
void TFollowerDiskActor::ForwardRequestToFollowerPartition(
    const typename TMethod::TRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    ForwardMessageToActor(ev, ctx, FollowerVolume.VolumeActorId);
}

void TFollowerDiskActor::HandleRdmaUnavailable(
    const TEvVolume::TEvRdmaUnavailable::TPtr& ev,
    const TActorContext& ctx)
{
    ForwardMessageToActor(ev, ctx, LeaderVolume.VolumeActorId);
    ForwardMessageToActor(ev, ctx, FollowerVolume.VolumeActorId);
}

bool TFollowerDiskActor::HandleRWClientIdChanged(
    const TEvVolume::TEvRWClientIdChanged::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::VOLUME,
        "Changed clientId for disk "
            << FollowerVolume.DiskInfo.Link.LeaderDiskIdForPrint() << " from "
            << LeaderVolume.ClientId.Quote() << " to "
            << ev->Get()->RWClientId.Quote());

    LeaderVolume.ClientId = ev->Get()->RWClientId;

    // Notify the source partition about the new clientId.
    NCloud::Send(
        ctx,
        LeaderVolume.PartitionActorId,
        std::make_unique<TEvVolume::TEvRWClientIdChanged>(
            LeaderVolume.ClientId));

    // It is necessary to handle the EvRWClientIdChanged message in the base
    // class TNonreplicatedPartitionMigrationCommonActor too.
    return false;
}

void TFollowerDiskActor::HandleUpdateFollowerStateResponse(
    const TEvVolumePrivate::TEvUpdateFollowerStateResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);

    const auto* msg = ev->Get();

    FollowerVolume.DiskInfo = msg->Follower;
}

}   // namespace NCloud::NBlockStore::NStorage
