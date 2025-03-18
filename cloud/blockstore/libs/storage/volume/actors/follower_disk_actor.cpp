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
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

using namespace NActors;

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

}   // namespace

///////////////////////////////////////////////////////////////////////////////

TFolowerDiskActor::TFolowerDiskActor(
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
          followerVolume.DiskInfo.MigrationBlockIndex.value_or(0),
          leaderVolume.ClientId,
          leaderVolume.VolumeActorId,
          config->GetMaxLinkedDiskFillIoDepth())
    , LeaderVolume(std::move(leaderVolume))
    , FollowerVolume(std::move(followerVolume))
{}

TFolowerDiskActor::~TFolowerDiskActor() = default;

void TFolowerDiskActor::OnBootstrap(const NActors::TActorContext& ctx)
{
    FollowerVolume.VolumeActorId = NCloud::Register<TVolumeAsPartitionActor>(
        ctx,
        LeaderVolume.DiskId,
        LeaderVolume.BlockSize,
        FollowerVolume.DiskInfo.FollowerDiskId);

    InitWork(
        ctx,
        LeaderVolume.PartitionActorId,
        LeaderVolume.PartitionActorId,
        FollowerVolume.VolumeActorId,
        true,   // takeOwnershipOverActors
        std::make_unique<TMigrationTimeoutCalculator>(
            GetConfig()->GetMaxLinkedDiskFillBandwidth(),
            GetConfig()->GetExpectedDiskAgentSize(),
            nullptr));

    StartWork(ctx);
}

bool TFolowerDiskActor::OnMessage(
    const TActorContext& ctx,
    TAutoPtr<NActors::IEventHandle>& ev)
{
    Y_UNUSED(ctx);
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvVolume::TEvRdmaUnavailable, HandleRdmaUnavailable);
        // HFunc(TEvService::TEvGetChangedBlocksRequest,
        // HandleGetChangedBlocks);
        IgnoreFunc(TEvVolumePrivate::TEvDeviceTimeoutedRequest);

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

void TFolowerDiskActor::OnMigrationProgress(
    const NActors::TActorContext& ctx,
    ui64 migrationIndex)
{
    Y_UNUSED(ctx);

    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::VOLUME,
        "TFolowerDiskActor::OnMigrationProgress "
            << LeaderVolume.DiskId.Quote() << " -> "
            << FollowerVolume.DiskInfo.GetDiskIdForPrint() << " "
            << migrationIndex);
}

void TFolowerDiskActor::OnMigrationFinished(const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);

    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::VOLUME,
        "TFolowerDiskActor::MigrationFinished "
            << LeaderVolume.DiskId.Quote() << " -> "
            << FollowerVolume.DiskInfo.GetDiskIdForPrint());
}

void TFolowerDiskActor::OnMigrationError(const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);

    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::VOLUME,
        "TFolowerDiskActor::MigrationError "
            << LeaderVolume.DiskId.Quote() << " -> "
            << FollowerVolume.DiskInfo.GetDiskIdForPrint());
}

template <typename TMethod>
void TFolowerDiskActor::ForwardRequestToLeaderPartition(
    const typename TMethod::TRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    ForwardMessageToActor(ev, ctx, LeaderVolume.PartitionActorId);
}

template <typename TMethod>
void TFolowerDiskActor::ForwardRequestToFollowerPartition(
    const typename TMethod::TRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    ForwardMessageToActor(ev, ctx, FollowerVolume.VolumeActorId);
}

void TFolowerDiskActor::HandleRdmaUnavailable(
    const TEvVolume::TEvRdmaUnavailable::TPtr& ev,
    const TActorContext& ctx)
{
    ForwardMessageToActor(ev, ctx, LeaderVolume.VolumeActorId);
    ForwardMessageToActor(ev, ctx, FollowerVolume.VolumeActorId);
}

bool TFolowerDiskActor::HandleRWClientIdChanged(
    const TEvVolume::TEvRWClientIdChanged::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::VOLUME,
        "Changed clientId for disk "
            << LeaderVolume.DiskId.Quote() << ", linked disk "
            << FollowerVolume.DiskInfo.GetDiskIdForPrint().Quote() << " from "
            << LeaderVolume.ClientId.Quote() << " to "
            << ev->Get()->RWClientId);

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

}   // namespace NCloud::NBlockStore::NStorage
