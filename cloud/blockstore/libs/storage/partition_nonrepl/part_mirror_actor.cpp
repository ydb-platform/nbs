#include "part_mirror_actor.h"

#include "part_mirror_resync_util.h"
#include "part_nonrepl.h"
#include "part_nonrepl_common.h"
#include "part_nonrepl_migration.h"
#include "resync_range.h"

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

TDuration CalculateScrubbingInterval(
    ui64 blockCount,
    ui32 blockSize,
    ui64 bandwidthPerTiB,
    ui64 maxBandwidth,
    ui64 minBandwidth)
{
    const auto bandwidth =
        Min(1.0 * maxBandwidth,
            Max(1.0 * minBandwidth,
                1.0 * blockCount * blockSize * bandwidthPerTiB / 1_TB));

    return TDuration::Seconds(ResyncRangeSize / (bandwidth * 1_MB));
}


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
        StartScrubbingRange(ctx, 0);
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

TBlockRange64 TMirrorPartitionActor::GetScrubbingRange() const
{
    return RangeId2BlockRange(ScrubbingRangeId, State.GetBlockSize());
}

void TMirrorPartitionActor::StartScrubbingRange(
    const NActors::TActorContext& ctx,
    ui64 scrubbingRangeId)
{
    if (ScrubbingRangeId != scrubbingRangeId) {
        ScrubbingRangeId = scrubbingRangeId;
        if (GetScrubbingRange().Start >= State.GetBlockCount()) {
            ScrubbingRangeId = 0;
        }
        ScrubbingRangeStarted = ctx.Now();
        ScrubbingRangeRescheduled = false;
    } else {
        ScrubbingRangeRescheduled = true;
    }
    ScheduleScrubbingNextRange(ctx);
}

void TMirrorPartitionActor::CompareChecksums(const TActorContext& ctx)
{
    const auto& checksums = ChecksumRangeActorCompanion.GetChecksums();
    THashMap<ui64, ui32> checksumCount;
    ui32 majorCount = 0;
    for (size_t i = 0; i < checksums.size(); i++) {
        ui64 checksum = checksums[i];
        if (++checksumCount[checksum] > majorCount) {
            majorCount = checksumCount[checksum];
        }
    }

    const bool equal = (majorCount == checksums.size());
    if (!equal && WriteIntersectsWithScrubbing) {
        if (!ScrubbingRangeRescheduled) {
            LOG_WARN(
                ctx,
                TBlockStoreComponents::PARTITION,
                "[%s] Reschedule scrubbing for range %s due to inflight write",
                DiskId.c_str(),
                DescribeRange(GetScrubbingRange()).c_str());
        }
        StartScrubbingRange(ctx, ScrubbingRangeId);
        return;
    }

    if (!equal) {
        if (ctx.Now() - ScrubbingRangeStarted <
            Config->GetScrubbingChecksumMismatchTimeout())
        {
            if (!ScrubbingRangeRescheduled) {
                LOG_WARN(
                    ctx,
                    TBlockStoreComponents::PARTITION,
                    "[%s] Checksum mismatch for range %s, reschedule scrubbing",
                    DiskId.c_str(),
                    DescribeRange(GetScrubbingRange()).c_str());
            }

            StartScrubbingRange(ctx, ScrubbingRangeId);
            return;
        }

        LOG_ERROR(
            ctx,
            TBlockStoreComponents::PARTITION,
            "[%s] Checksum mismatch for range %s",
            DiskId.c_str(),
            DescribeRange(GetScrubbingRange()).c_str());

        for (size_t i = 0; i < checksums.size(); i++) {
            LOG_ERROR(
                ctx,
                TBlockStoreComponents::PARTITION,
                "[%s] Replica %lu range %s checksum %lu",
                DiskId.c_str(),
                i,
                DescribeRange(GetScrubbingRange()).c_str(),
                checksums[i]);
        }
        ++ChecksumMismatches;

        const bool hasQuorum = majorCount > checksums.size() / 2;
        if (hasQuorum) {
            ReportMirroredDiskMinorityChecksumMismatch();
            if (Config->GetResyncRangeAfterScrubbing()) {
                StartResyncRange(ctx);
                return;
            }
        } else {
           ReportMirroredDiskMajorityChecksumMismatch();
        }
    }

    StartScrubbingRange(ctx, ScrubbingRangeId + 1);
}

void TMirrorPartitionActor::StartResyncRange(
    const NActors::TActorContext& ctx)
{
    LOG_WARN(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Resyncing range %s",
        DiskId.c_str(),
        DescribeRange(GetScrubbingRange()).c_str());
    ResyncRangeStarted = true;

    auto requestInfo = CreateRequestInfo(
        SelfId(),
        0,  // cookie
        MakeIntrusive<TCallContext>()
    );

    TVector<TReplicaDescriptor> replicas;
    const auto& replicaInfos = State.GetReplicaInfos();
    const auto& replicaActors = State.GetReplicaActors();
    for (ui32 i = 0; i < replicaInfos.size(); i++) {
        if (replicaInfos[i].Config->DevicesReadyForReading(GetScrubbingRange()))
        {
            replicas.emplace_back(
                replicaInfos[i].Config->GetName(),
                i,
                replicaActors[i]);
        }
    }

    NCloud::Register<TResyncRangeActor>(
        ctx,
        std::move(requestInfo),
        State.GetBlockSize(),
        GetScrubbingRange(),
        std::move(replicas),
        State.GetRWClientId(),
        BlockDigestGenerator);
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
            CalculateScrubbingInterval(
                State.GetBlockCount(),
                State.GetBlockSize(),
                Config->GetScrubbingBandwidth(),
                Config->GetMaxScrubbingBandwidth(),
                Config->GetMinScrubbingBandwidth()),
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
    auto scrubbingRange = GetScrubbingRange();

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

            StartScrubbingRange(ctx, ScrubbingRangeId);
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

        StartScrubbingRange(ctx, ScrubbingRangeId + 1);
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
            DescribeRange(GetScrubbingRange()).c_str(),
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
            DescribeRange(GetScrubbingRange()).c_str(),
            FormatError(ChecksumRangeActorCompanion.GetError()).c_str());
        StartScrubbingRange(ctx, ScrubbingRangeId);
        return;
    }

    CompareChecksums(ctx);
}

void TMirrorPartitionActor::HandleRangeResynced(
    const TEvNonreplPartitionPrivate::TEvRangeResynced::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto range = msg->Range;

    LOG_WARN(ctx, TBlockStoreComponents::PARTITION,
        "[%s] Range %s resync finished: %s",
        DiskId.c_str(),
        DescribeRange(range).c_str(),
        FormatError(msg->GetError()).c_str());

    ResyncRangeStarted = false;
    StartScrubbingRange(ctx, ScrubbingRangeId + 1);
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

        HFunc(
            TEvNonreplPartitionPrivate::TEvRangeResynced,
            HandleRangeResynced);

        HFunc(TEvService::TEvReadBlocksRequest, HandleReadBlocks);
        HFunc(TEvService::TEvWriteBlocksRequest, HandleWriteBlocks);
        HFunc(TEvService::TEvZeroBlocksRequest, HandleZeroBlocks);

        HFunc(TEvService::TEvReadBlocksLocalRequest, HandleReadBlocksLocal);
        HFunc(TEvService::TEvWriteBlocksLocalRequest, HandleWriteBlocksLocal);

        HFunc(NPartition::TEvPartition::TEvDrainRequest, DrainActorCompanion.HandleDrain);
        HFunc(TEvService::TEvGetChangedBlocksRequest, DeclineGetChangedBlocks);
        HFunc(
            TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest,
            HandleGetDeviceForRange);

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
            TEvNonreplPartitionPrivate::TEvMirroredReadCompleted,
            HandleMirroredReadCompleted);

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
        HFunc(TEvService::TEvGetChangedBlocksRequest, DeclineGetChangedBlocks);
        HFunc(
            TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest,
            GetDeviceForRangeCompanion.RejectGetDeviceForRange);

        HFunc(TEvVolume::TEvDescribeBlocksRequest, RejectDescribeBlocks);
        HFunc(TEvVolume::TEvGetCompactionStatusRequest, RejectGetCompactionStatus);
        HFunc(TEvVolume::TEvCompactRangeRequest, RejectCompactRange);
        HFunc(TEvVolume::TEvRebuildMetadataRequest, RejectRebuildMetadata);
        HFunc(TEvVolume::TEvGetRebuildMetadataStatusRequest, RejectGetRebuildMetadataStatus);
        HFunc(TEvVolume::TEvScanDiskRequest, RejectScanDisk);
        HFunc(TEvVolume::TEvGetScanDiskStatusRequest, RejectGetScanDiskStatus);

        IgnoreFunc(TEvNonreplPartitionPrivate::TEvWriteOrZeroCompleted);
        IgnoreFunc(TEvNonreplPartitionPrivate::TEvMirroredReadCompleted);

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
