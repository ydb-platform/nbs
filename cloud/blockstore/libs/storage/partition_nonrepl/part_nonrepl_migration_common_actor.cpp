#include "part_nonrepl_migration_common_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/unimplemented.h>

#include <contrib/ydb/core/base/appdata.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TNonreplicatedPartitionMigrationCommonActor::
    TNonreplicatedPartitionMigrationCommonActor(
        IMigrationOwner* migrationOwner,
        TStorageConfigPtr config,
        TString diskId,
        ui64 blockCount,
        ui64 blockSize,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr digestGenerator,
        ui64 initialMigrationIndex,
        TString rwClientId,
        NActors::TActorId statActorId,
        ui32 maxIoDepth)
    : MigrationOwner(migrationOwner)
    , Config(std::move(config))
    , ProfileLog(std::move(profileLog))
    , DiskId(std::move(diskId))
    , BlockSize(blockSize)
    , BlockCount(blockCount)
    , BlockDigestGenerator(std::move(digestGenerator))
    , MaxIoDepth(maxIoDepth)
    , RWClientId(std::move(rwClientId))
    , ProcessingBlocks(blockCount, blockSize, initialMigrationIndex)
    , ChangedRangesMap(blockCount, blockSize, ProcessingRangeSize)
    , StatActorId(statActorId)
    , PoisonPillHelper(this)
{}

TNonreplicatedPartitionMigrationCommonActor::
    ~TNonreplicatedPartitionMigrationCommonActor() = default;

void TNonreplicatedPartitionMigrationCommonActor::Bootstrap(
    const TActorContext& ctx)
{
    ScheduleCountersUpdate(ctx);

    Become(&TThis::StateWork);

    MigrationOwner->OnBootstrap(ctx);
}

void TNonreplicatedPartitionMigrationCommonActor::MarkMigratedBlocks(
    TBlockRange64 range)
{
    ProcessingBlocks.MarkProcessed(range);
}

ui64 TNonreplicatedPartitionMigrationCommonActor::
    GetBlockCountNeedToBeProcessed() const
{
    return ProcessingBlocks.GetBlockCountNeedToBeProcessed();
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationCommonActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Become(&TThis::StateZombie);
    PoisonPillHelper.HandlePoisonPill(ev, ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationCommonActor::ScheduleCountersUpdate(
    const TActorContext& ctx)
{
    if (!UpdateCountersScheduled) {
        ctx.Schedule(
            UpdateCountersInterval,
            new TEvNonreplPartitionPrivate::TEvUpdateCounters());
        UpdateCountersScheduled = true;
    }
}

void TNonreplicatedPartitionMigrationCommonActor::HandleUpdateCounters(
    const TEvNonreplPartitionPrivate::TEvUpdateCounters::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    UpdateCountersScheduled = false;

    SendStats(ctx);
    ScheduleCountersUpdate(ctx);
}

void TNonreplicatedPartitionMigrationCommonActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    switch (msg->Tag) {
        case REGISTER_TRAFFIC_SOURCE:
            DoRegisterTrafficSource(ctx);
            break;
        default:
            // It should be unreachable.
            Y_DEBUG_ABORT_UNLESS(false);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST(name, ns)           \
    void TNonreplicatedPartitionMigrationCommonActor::Handle##name( \
        const ns::TEv##name##Request::TPtr& ev,                     \
        const TActorContext& ctx)                                   \
    {                                                               \
        RejectUnimplementedRequest<ns::T##name##Method>(ev, ctx);   \
    }                                                               \
                                                                    \
    // BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST

BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST(DescribeBlocks, TEvVolume);
BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST(CompactRange, TEvVolume);
BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST(GetCompactionStatus, TEvVolume);
BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST(RebuildMetadata, TEvVolume);
BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST(GetRebuildMetadataStatus, TEvVolume);
BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST(ScanDisk, TEvVolume);
BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST(GetScanDiskStatus, TEvVolume);

////////////////////////////////////////////////////////////////////////////////

STFUNC(TNonreplicatedPartitionMigrationCommonActor::StateWork)
{
    // Give the inheritor the opportunity to process the message first.
    if (MigrationOwner->OnMessage(this->ActorContext(), ev)) {
        return;
    }

    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvNonreplPartitionPrivate::TEvUpdateCounters,
            HandleUpdateCounters);

        HFunc(TEvService::TEvReadBlocksRequest, HandleReadBlocks);
        HFunc(TEvService::TEvWriteBlocksRequest, HandleWriteBlocks);
        HFunc(TEvService::TEvZeroBlocksRequest, HandleZeroBlocks);

        HFunc(TEvService::TEvReadBlocksLocalRequest, HandleReadBlocksLocal);
        HFunc(TEvService::TEvWriteBlocksLocalRequest, HandleWriteBlocksLocal);

        HFunc(TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest, HandleChecksumBlocks);

        HFunc(
            NPartition::TEvPartition::TEvDrainRequest,
            DrainActorCompanion.HandleDrain);
        HFunc(
            TEvService::TEvGetChangedBlocksRequest,
            GetChangedBlocksCompanion.HandleGetChangedBlocks);

        HFunc(TEvVolume::TEvDescribeBlocksRequest, HandleDescribeBlocks);
        HFunc(
            TEvVolume::TEvGetCompactionStatusRequest,
            HandleGetCompactionStatus);
        HFunc(TEvVolume::TEvCompactRangeRequest, HandleCompactRange);
        HFunc(TEvVolume::TEvRebuildMetadataRequest, HandleRebuildMetadata);
        HFunc(
            TEvVolume::TEvGetRebuildMetadataStatusRequest,
            HandleGetRebuildMetadataStatus);
        HFunc(TEvVolume::TEvScanDiskRequest, HandleScanDisk);
        HFunc(TEvVolume::TEvGetScanDiskStatusRequest, HandleGetScanDiskStatus);

        HFunc(
            TEvNonreplPartitionPrivate::TEvWriteOrZeroCompleted,
            HandleWriteOrZeroCompleted);
        HFunc(
            TEvNonreplPartitionPrivate::TEvRangeMigrated,
            HandleRangeMigrated);
        HFunc(
            TEvNonreplPartitionPrivate::TEvMigrateNextRange,
            HandleMigrateNextRange);
        HFunc(TEvVolume::TEvRWClientIdChanged, HandleRWClientIdChanged);
        HFunc(
            TEvVolume::TEvDiskRegistryBasedPartitionCounters,
            HandlePartCounters);

        HFunc(
            TEvStatsServicePrivate::TEvRegisterTrafficSourceResponse,
            TimeoutCalculator->HandleUpdateBandwidthLimit);

        HFunc(NActors::TEvents::TEvWakeup, HandleWakeup);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::VOLUME);
            break;
    }
}

STFUNC(TNonreplicatedPartitionMigrationCommonActor::StateZombie)
{
    // Give the inheritor the opportunity to process the message first.
    if (MigrationOwner->OnMessage(this->ActorContext(), ev)) {
        return;
    }

    switch (ev->GetTypeRewrite()) {
        IgnoreFunc(TEvNonreplPartitionPrivate::TEvUpdateCounters);

        HFunc(TEvService::TEvReadBlocksRequest, RejectReadBlocks);
        HFunc(TEvService::TEvWriteBlocksRequest, RejectWriteBlocks);
        HFunc(TEvService::TEvZeroBlocksRequest, RejectZeroBlocks);

        HFunc(TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest, RejectChecksumBlocks);

        HFunc(TEvService::TEvReadBlocksLocalRequest, RejectReadBlocksLocal);
        HFunc(TEvService::TEvWriteBlocksLocalRequest, RejectWriteBlocksLocal);

        HFunc(NPartition::TEvPartition::TEvDrainRequest, RejectDrain);

        HFunc(TEvVolume::TEvDescribeBlocksRequest, RejectDescribeBlocks);
        HFunc(
            TEvVolume::TEvGetCompactionStatusRequest,
            RejectGetCompactionStatus);
        HFunc(TEvVolume::TEvCompactRangeRequest, RejectCompactRange);
        HFunc(TEvVolume::TEvRebuildMetadataRequest, RejectRebuildMetadata);
        HFunc(
            TEvVolume::TEvGetRebuildMetadataStatusRequest,
            RejectGetRebuildMetadataStatus);
        HFunc(TEvVolume::TEvScanDiskRequest, RejectScanDisk);
        HFunc(TEvVolume::TEvGetScanDiskStatusRequest, RejectGetScanDiskStatus);

        IgnoreFunc(TEvNonreplPartitionPrivate::TEvWriteOrZeroCompleted);
        IgnoreFunc(TEvNonreplPartitionPrivate::TEvRangeMigrated);
        IgnoreFunc(TEvNonreplPartitionPrivate::TEvMigrateNextRange);
        IgnoreFunc(TEvDiskRegistry::TEvFinishMigrationResponse);
        IgnoreFunc(TEvVolume::TEvMigrationStateUpdated);
        IgnoreFunc(TEvVolume::TEvRWClientIdChanged);
        IgnoreFunc(TEvVolume::TEvDiskRegistryBasedPartitionCounters);

        IgnoreFunc(TEvents::TEvPoisonPill);
        IgnoreFunc(NActors::TEvents::TEvWakeup);
        HFunc(TEvents::TEvPoisonTaken, PoisonPillHelper.HandlePoisonTaken);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::VOLUME);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
