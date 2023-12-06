#include "part_nonrepl_migration_actor.h"
#include "part_nonrepl.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>

#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/unimplemented.h>

#include <ydb/core/base/appdata.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

TNonreplicatedPartitionMigrationActor::TNonreplicatedPartitionMigrationActor(
        TStorageConfigPtr config,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr digestGenerator,
        ui64 initialMigrationIndex,
        TString rwClientId,
        TNonreplicatedPartitionConfigPtr srcConfig,
        google::protobuf::RepeatedPtrField<NProto::TDeviceMigration> migrations,
        NRdma::IClientPtr rdmaClient,
        NActors::TActorId statActorId)
    : Config(std::move(config))
    , ProfileLog(std::move(profileLog))
    , BlockDigestGenerator(std::move(digestGenerator))
    , SrcConfig(std::move(srcConfig))
    , Migrations(std::move(migrations))
    , RdmaClient(std::move(rdmaClient))
    , StatActorId(statActorId)
    , State(Config, initialMigrationIndex, rwClientId, SrcConfig)
{
    ActivityType = TBlockStoreActivities::PARTITION;
}

TNonreplicatedPartitionMigrationActor::~TNonreplicatedPartitionMigrationActor()
{
}

void TNonreplicatedPartitionMigrationActor::Bootstrap(const TActorContext& ctx)
{
    SetupPartitions(ctx);
    ScheduleCountersUpdate(ctx);

    Become(&TThis::StateWork);
}

void TNonreplicatedPartitionMigrationActor::KillActors(const TActorContext& ctx)
{
    NCloud::Send<TEvents::TEvPoisonPill>(ctx, SrcActorId);

    if (DstActorId) {
        NCloud::Send<TEvents::TEvPoisonPill>(ctx, DstActorId);
    }
}

void TNonreplicatedPartitionMigrationActor::SetupPartitions(const TActorContext& ctx)
{
    Y_ABORT_UNLESS(Migrations.size());

    SrcActorId = NCloud::Register(
        ctx,
        CreateNonreplicatedPartition(
            Config,
            SrcConfig,
            SelfId(),
            RdmaClient));

    ui64 blockIndex = 0;
    bool failed = false;
    auto devices = SrcConfig->GetDevices();
    for (auto& device: devices) {
        auto* migration = FindIfPtr(
            Migrations,
            [&] (const NProto::TDeviceMigration& m) {
                return m.GetSourceDeviceId() == device.GetDeviceUUID();
            }
        );

        if (migration) {
            const auto& target = migration->GetTargetDevice();

            if (device.GetBlocksCount() != target.GetBlocksCount()) {
                LOG_ERROR(ctx, TBlockStoreComponents::PARTITION,
                    "[%s] source (%s) block count (%lu)"
                    " != target (%s) block count (%lu)",
                    SrcConfig->GetName().c_str(),
                    device.GetDeviceUUID().c_str(),
                    device.GetBlocksCount(),
                    target.GetDeviceUUID().c_str(),
                    target.GetBlocksCount());

                ReportBadMigrationConfig();

                failed = true;
                break;
            }

            device.CopyFrom(migration->GetTargetDevice());
        } else {
            State.MarkMigrated(TBlockRange64::WithLength(
                blockIndex,
                device.GetBlocksCount()
            ));

            device.ClearDeviceUUID();
        }

        blockIndex += device.GetBlocksCount();
    }

    if (failed) {
        State.AbortMigration();
    } else if (Config->GetNonReplicatedVolumeMigrationDisabled()) {
        State.AbortMigration();

        LOG_WARN(ctx, TBlockStoreComponents::PARTITION,
            "[%s] migration disabled => aborted",
            SrcConfig->GetName().c_str());
    } else {
        auto dstConfig = SrcConfig->Fork(std::move(devices));
        State.SetupDstPartition(dstConfig);

        DstActorId = NCloud::Register(
            ctx,
            CreateNonreplicatedPartition(
                Config,
                std::move(dstConfig),
                SelfId(),
                RdmaClient));

        State.SkipMigratedRanges();
        ContinueMigrationIfNeeded(ctx);
    }
}

void TNonreplicatedPartitionMigrationActor::ReplyAndDie(const TActorContext& ctx)
{
    NCloud::Reply(ctx, *Poisoner, std::make_unique<TEvents::TEvPoisonTaken>());
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Become(&TThis::StateZombie);

    KillActors(ctx);

    Poisoner = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        MakeIntrusive<TCallContext>());

    Y_DEBUG_ABORT_UNLESS(SrcActorId || DstActorId);

    if (SrcActorId || DstActorId) {
        return;
    }

    ReplyAndDie(ctx);
}

void TNonreplicatedPartitionMigrationActor::HandlePoisonTaken(
    const TEvents::TEvPoisonTaken::TPtr& ev,
    const TActorContext& ctx)
{
    if (SrcActorId == ev->Sender) {
        SrcActorId = {};
    }

    if (DstActorId == ev->Sender) {
        DstActorId = {};
    }

    if (SrcActorId || DstActorId) {
        return;
    }

    ReplyAndDie(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationActor::ScheduleCountersUpdate(
    const TActorContext& ctx)
{
    if (!UpdateCountersScheduled) {
        ctx.Schedule(UpdateCountersInterval,
            new TEvNonreplPartitionPrivate::TEvUpdateCounters());
        UpdateCountersScheduled = true;
    }
}

void TNonreplicatedPartitionMigrationActor::HandleUpdateCounters(
    const TEvNonreplPartitionPrivate::TEvUpdateCounters::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    UpdateCountersScheduled = false;

    SendStats(ctx);
    ScheduleCountersUpdate(ctx);
}

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST(name, ns)                      \
    void TNonreplicatedPartitionMigrationActor::Handle##name(                           \
        const ns::TEv##name##Request::TPtr& ev,                                \
        const TActorContext& ctx)                                              \
    {                                                                          \
        RejectUnimplementedRequest<ns::T##name##Method>(ev, ctx);              \
    }                                                                          \
                                                                               \
// BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST

BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST(DescribeBlocks,           TEvVolume);
BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST(CompactRange,             TEvVolume);
BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST(GetCompactionStatus,      TEvVolume);
BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST(RebuildMetadata,          TEvVolume);
BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST(GetRebuildMetadataStatus, TEvVolume);
BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST(ScanDisk,                 TEvVolume);
BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST(GetScanDiskStatus,        TEvVolume);


////////////////////////////////////////////////////////////////////////////////

STFUNC(TNonreplicatedPartitionMigrationActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvNonreplPartitionPrivate::TEvUpdateCounters,
            HandleUpdateCounters);

        HFunc(TEvService::TEvReadBlocksRequest, HandleReadBlocks);
        HFunc(TEvService::TEvWriteBlocksRequest, HandleWriteBlocks);
        HFunc(TEvService::TEvZeroBlocksRequest, HandleZeroBlocks);

        HFunc(TEvService::TEvReadBlocksLocalRequest, HandleReadBlocksLocal);
        HFunc(TEvService::TEvWriteBlocksLocalRequest, HandleWriteBlocksLocal);

        HFunc(NPartition::TEvPartition::TEvDrainRequest, DrainActorCompanion.HandleDrain);

        HFunc(TEvVolume::TEvDescribeBlocksRequest, HandleDescribeBlocks);
        HFunc(TEvVolume::TEvGetCompactionStatusRequest, HandleGetCompactionStatus);
        HFunc(TEvVolume::TEvCompactRangeRequest, HandleCompactRange);
        HFunc(TEvVolume::TEvRebuildMetadataRequest, HandleRebuildMetadata);
        HFunc(TEvVolume::TEvGetRebuildMetadataStatusRequest, HandleGetRebuildMetadataStatus);
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
        HFunc(
            TEvDiskRegistry::TEvFinishMigrationResponse,
            HandleFinishMigrationResponse);
        HFunc(
            TEvVolume::TEvMigrationStateUpdated,
            HandleMigrationStateUpdated);
        HFunc(
            TEvVolume::TEvRWClientIdChanged,
            HandleRWClientIdChanged);
        HFunc(
            TEvVolume::TEvDiskRegistryBasedPartitionCounters,
            HandlePartCounters);

        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION);
            break;
    }
}

STFUNC(TNonreplicatedPartitionMigrationActor::StateZombie)
{
    switch (ev->GetTypeRewrite()) {
        IgnoreFunc(TEvNonreplPartitionPrivate::TEvUpdateCounters);

        HFunc(TEvService::TEvReadBlocksRequest, RejectReadBlocks);
        HFunc(TEvService::TEvWriteBlocksRequest, RejectWriteBlocks);
        HFunc(TEvService::TEvZeroBlocksRequest, RejectZeroBlocks);

        HFunc(TEvService::TEvReadBlocksLocalRequest, RejectReadBlocksLocal);
        HFunc(TEvService::TEvWriteBlocksLocalRequest, RejectWriteBlocksLocal);

        HFunc(NPartition::TEvPartition::TEvDrainRequest, RejectDrain);

        HFunc(TEvVolume::TEvDescribeBlocksRequest, RejectDescribeBlocks);
        HFunc(TEvVolume::TEvGetCompactionStatusRequest, RejectGetCompactionStatus);
        HFunc(TEvVolume::TEvCompactRangeRequest, RejectCompactRange);
        HFunc(TEvVolume::TEvRebuildMetadataRequest, RejectRebuildMetadata);
        HFunc(TEvVolume::TEvGetRebuildMetadataStatusRequest, RejectGetRebuildMetadataStatus);
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
        HFunc(TEvents::TEvPoisonTaken, HandlePoisonTaken);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
