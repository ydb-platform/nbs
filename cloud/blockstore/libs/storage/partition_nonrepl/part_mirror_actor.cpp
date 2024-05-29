#include "part_mirror_actor.h"
#include "part_mirror_resync_util.h"
#include "part_nonrepl.h"
#include "part_nonrepl_migration.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>

#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/unimplemented.h>

#include <contrib/ydb/core/base/appdata.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

TMirrorPartitionActor::TMirrorPartitionActor(
        TStorageConfigPtr config,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr digestGenerator,
        TString rwClientId,
        TNonreplicatedPartitionConfigPtr partConfig,
        TMigrations migrations,
        TVector<TDevices> replicas,
        NRdma::IClientPtr rdmaClient,
        TActorId statActorId,
        TActorId resyncActorId)
    : Config(std::move(config))
    , ProfileLog(std::move(profileLog))
    , BlockDigestGenerator(std::move(digestGenerator))
    , RdmaClient(std::move(rdmaClient))
    , DiskId(partConfig->GetName())
    , StatActorId(statActorId)
    , ResyncActorId(resyncActorId)
    , State(
        Config,
        rwClientId,
        std::move(partConfig),
        std::move(migrations),
        std::move(replicas))
{}

TMirrorPartitionActor::~TMirrorPartitionActor()
{
}

void TMirrorPartitionActor::Bootstrap(const TActorContext& ctx)
{
    SetupPartitions(ctx);
    ScheduleCountersUpdate(ctx);

    if (Config->GetDataScrubbingEnabled() && !ResyncActorId) {
        ScheduleScrubbingNextRange(ctx);
    }

    Become(&TThis::StateWork);
}

void TMirrorPartitionActor::KillActors(const TActorContext& ctx)
{
    for (const auto& actorId: State.GetReplicaActors()) {
        NCloud::Send<TEvents::TEvPoisonPill>(ctx, actorId);
    }
}

void TMirrorPartitionActor::SetupPartitions(const TActorContext& ctx)
{
    Status = State.Validate();
    if (HasError(Status)) {
        return;
    }

    // if resync is in progress we should not run data migration among replicas
    // to avoid replication<->resync races
    if (!ResyncActorId) {
        State.PrepareMigrationConfig();
    }

    for (const auto& replicaInfo: State.GetReplicaInfos()) {
        IActorPtr actor;
        if (replicaInfo.Migrations.size()) {
            actor = CreateNonreplicatedPartitionMigration(
                Config,
                ProfileLog,
                BlockDigestGenerator,
                0,  // initialMigrationIndex
                State.GetRWClientId(),
                replicaInfo.Config,
                replicaInfo.Migrations,
                RdmaClient,
                SelfId());
        } else {
            actor = CreateNonreplicatedPartition(
                Config,
                replicaInfo.Config,
                SelfId(),
                RdmaClient);
        }

        State.AddReplicaActor(NCloud::Register(ctx, std::move(actor)));
    }

    ReplicaCounters.resize(State.GetReplicaInfos().size());
}

void TMirrorPartitionActor::CompareChecksums(const TActorContext& ctx)
{
    const auto& checksums = ChecksumRangeActorCompanion.GetChecksums();
    bool equal = true;
    for (size_t i = 1; i < checksums.size(); i++) {
        if (checksums[i] != checksums[0]) {
            equal = false;
            break;
        }
    }

    if (!equal && WriteIntersectsWithScrubbing) {
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::PARTITION,
            "[%s] Reschedule scrubbing for range %s due to inflight write",
            DiskId.c_str(),
            DescribeRange(
                RangeId2BlockRange(ScrubbingRangeId, State.GetBlockSize())).c_str());
        ScheduleScrubbingNextRange(ctx);
        return;
    }

    if (!equal) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::PARTITION,
            "[%s] Checksum mismatch for range %s",
            DiskId.c_str(),
            DescribeRange(
                RangeId2BlockRange(ScrubbingRangeId, State.GetBlockSize())).c_str());

        for (size_t i = 0; i < checksums.size(); i++) {
            LOG_ERROR(
                ctx,
                TBlockStoreComponents::PARTITION,
                "[%s] Replica %lu range %s checksum %lu",
                DiskId.c_str(),
                i,
                DescribeRange(
                    RangeId2BlockRange(ScrubbingRangeId, State.GetBlockSize())).c_str(),
                checksums[i]);
        }
        ReportMirroredDiskChecksumMismatch();
    }

    ++ScrubbingRangeId;
    ScheduleScrubbingNextRange(ctx);
}

void TMirrorPartitionActor::ReplyAndDie(const TActorContext& ctx)
{
    NCloud::Reply(ctx, *Poisoner, std::make_unique<TEvents::TEvPoisonTaken>());
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Become(&TThis::StateZombie);

    KillActors(ctx);

    AliveReplicas = State.GetReplicaActors().size();

    Poisoner = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        MakeIntrusive<TCallContext>());

    Y_DEBUG_ABORT_UNLESS(AliveReplicas != 0);

    if (AliveReplicas == 0) {
        ReplyAndDie(ctx);
    }
}

void TMirrorPartitionActor::HandlePoisonTaken(
    const TEvents::TEvPoisonTaken::TPtr& ev,
    const TActorContext& ctx)
{
    if (!FindPtr(State.GetReplicaActors(), ev->Sender)) {
        return;
    }

    Y_ABORT_UNLESS(AliveReplicas > 0);
    --AliveReplicas;

    if (AliveReplicas == 0) {
        ReplyAndDie(ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionActor::ScheduleCountersUpdate(
    const TActorContext& ctx)
{
    if (!UpdateCountersScheduled) {
        ctx.Schedule(UpdateCountersInterval,
            new TEvNonreplPartitionPrivate::TEvUpdateCounters());
        UpdateCountersScheduled = true;
    }
}

void TMirrorPartitionActor::HandleUpdateCounters(
    const TEvNonreplPartitionPrivate::TEvUpdateCounters::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    UpdateCountersScheduled = false;

    SendStats(ctx);
    ScheduleCountersUpdate(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionActor::ScheduleScrubbingNextRange(
    const TActorContext& ctx)
{
    if (!ScrubbingScheduled) {
        ctx.Schedule(
            Config->GetScrubbingInterval(),
            new TEvNonreplPartitionPrivate::TEvScrubbingNextRange());
        ScrubbingScheduled = true;
    }
}

void TMirrorPartitionActor::HandleScrubbingNextRange(
    const TEvNonreplPartitionPrivate::TEvScrubbingNextRange::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ScrubbingScheduled = false;
    WriteIntersectsWithScrubbing = false;
    auto scrubbingRange =
        RangeId2BlockRange(ScrubbingRangeId, State.GetBlockSize());
    if (scrubbingRange.Start >= State.GetBlockCount()) {
        ScrubbingRangeId = 0;
        scrubbingRange =
            RangeId2BlockRange(ScrubbingRangeId, State.GetBlockSize());
    }

    for (const auto& [key, requestInfo]: RequestsInProgress.AllRequests()) {
        if (!requestInfo.Write) {
            continue;
        }
        const auto& requestRange = requestInfo.Value;
        if (scrubbingRange.Overlaps(requestRange)) {
            LOG_DEBUG(
                ctx,
                TBlockStoreComponents::PARTITION,
                "[%s] Reschedule scrubbing for range %s due to inflight write to %s",
                DiskId.c_str(),
                DescribeRange(scrubbingRange).c_str(),
                DescribeRange(requestRange).c_str());

            ScheduleScrubbingNextRange(ctx);
            return;
        }
    }

    TVector<TReplicaDescriptor> replicas;
    const auto& replicaInfos = State.GetReplicaInfos();
    const auto& replicaActors = State.GetReplicaActors();
    for (ui32 i = 0; i < replicaInfos.size(); i++) {
        if (replicaInfos[i].Config->DevicesReadyForReading(scrubbingRange)) {
            replicas.emplace_back(
                replicaInfos[i].Config->GetName(),
                i,
                replicaActors[i]);
        }
    }

    if (replicas.size() < 2) {
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::PARTITION,
            "[%s] Skipping scrubbing for range %s, devices not ready for reading",
            DiskId.c_str(),
            DescribeRange(scrubbingRange).c_str());

        ++ScrubbingRangeId;
        ScheduleScrubbingNextRange(ctx);
        return;
    }

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Scrubbing range %s",
        DiskId.c_str(),
        DescribeRange(scrubbingRange).c_str());

    ScrubbingThroughput += scrubbingRange.Size() * State.GetBlockSize();
    ChecksumRangeActorCompanion = TChecksumRangeActorCompanion(replicas);
    ChecksumRangeActorCompanion.CalculateChecksums(ctx, scrubbingRange);
}

void TMirrorPartitionActor::HandleChecksumUndelivery(
    const TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ChecksumRangeActorCompanion.HandleChecksumUndelivery(ctx);
    if (ChecksumRangeActorCompanion.IsFinished()) {
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::PARTITION,
            "[%s] Reschedule scrubbing for range %s due to checksum error %s",
            DiskId.c_str(),
            DescribeRange(
                RangeId2BlockRange(ScrubbingRangeId, State.GetBlockSize())).c_str(),
            FormatError(ChecksumRangeActorCompanion.GetError()).c_str());
        ScheduleScrubbingNextRange(ctx);
    }
}

void TMirrorPartitionActor::HandleChecksumResponse(
    const TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ChecksumRangeActorCompanion.HandleChecksumResponse(ev, ctx);

    if (!ChecksumRangeActorCompanion.IsFinished()) {
        return;
    }

    if (HasError(ChecksumRangeActorCompanion.GetError())) {
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::PARTITION,
            "[%s] Reschedule scrubbing for range %s due to checksum error %s",
            DiskId.c_str(),
            DescribeRange(
                RangeId2BlockRange(ScrubbingRangeId, State.GetBlockSize())).c_str(),
            FormatError(ChecksumRangeActorCompanion.GetError()).c_str());
        ScheduleScrubbingNextRange(ctx);
        return;
    }

    CompareChecksums(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionActor::HandleRWClientIdChanged(
    const TEvVolume::TEvRWClientIdChanged::TPtr& ev,
    const TActorContext& ctx)
{
    auto rwClientId = ev->Get()->RWClientId;

    for (const auto& actorId: State.GetReplicaActors()) {
        NCloud::Send(
            ctx,
            actorId,
            std::make_unique<TEvVolume::TEvRWClientIdChanged>(rwClientId));
    }

    State.SetRWClientId(std::move(rwClientId));
}

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST(name, ns)                      \
    void TMirrorPartitionActor::Handle##name(                                  \
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

STFUNC(TMirrorPartitionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvNonreplPartitionPrivate::TEvUpdateCounters,
            HandleUpdateCounters);

        HFunc(
            TEvNonreplPartitionPrivate::TEvScrubbingNextRange,
            HandleScrubbingNextRange);

        HFunc(
            TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest,
            HandleChecksumUndelivery);
        HFunc(
            TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse,
            HandleChecksumResponse);

        HFunc(TEvService::TEvReadBlocksRequest, HandleReadBlocks);
        HFunc(TEvService::TEvWriteBlocksRequest, HandleWriteBlocks);
        HFunc(TEvService::TEvZeroBlocksRequest, HandleZeroBlocks);

        HFunc(TEvService::TEvReadBlocksLocalRequest, HandleReadBlocksLocal);
        HFunc(TEvService::TEvWriteBlocksLocalRequest, HandleWriteBlocksLocal);

        HFunc(NPartition::TEvPartition::TEvDrainRequest, DrainActorCompanion.HandleDrain);
        HFunc(
            TEvService::TEvGetChangedBlocksRequest,
            GetChangedBlocksCompanion.HandleGetChangedBlocks);

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

STFUNC(TMirrorPartitionActor::StateZombie)
{
    switch (ev->GetTypeRewrite()) {
        IgnoreFunc(TEvNonreplPartitionPrivate::TEvUpdateCounters);

        IgnoreFunc(TEvNonreplPartitionPrivate::TEvScrubbingNextRange);
        IgnoreFunc(TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest);
        IgnoreFunc(TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse);

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
