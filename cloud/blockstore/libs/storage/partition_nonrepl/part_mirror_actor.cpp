#include "part_mirror_actor.h"

#include "lagging_agents_replica_proxy_actor.h"
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

using namespace NPartition;

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
        TDiagnosticsConfigPtr diagnosticsConfig,
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
    , DiagnosticsConfig(std::move(diagnosticsConfig))
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
    , MultiAgentWriteEnabled(Config->GetMultiAgentWriteEnabled())
    , MultiAgentWriteRequestSizeThreshold(
          Config->GetMultiAgentWriteRequestSizeThreshold())
{}

TMirrorPartitionActor::~TMirrorPartitionActor() = default;

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
    for (auto actorId : State.GetAllActors()) {
        NCloud::Send<TEvents::TEvPoisonPill>(
            ctx,
            actorId);
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
            auto migrationSrcActorId = State.IsMigrationConfigPreparedForFresh()
                                           ? SelfId()
                                           : TActorId();
            actor = CreateNonreplicatedPartitionMigration(
                Config,
                DiagnosticsConfig,
                ProfileLog,
                BlockDigestGenerator,
                0,   // initialMigrationIndex
                State.GetRWClientId(),
                replicaInfo.Config,
                replicaInfo.Migrations,
                RdmaClient,
                SelfId(),
                migrationSrcActorId);
        } else {
            actor = CreateNonreplicatedPartition(
                Config,
                DiagnosticsConfig,
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

        if (Config->GetAutomaticallyEnableBufferCopyingAfterChecksumMismatch())
        {
            AddTagForBufferCopying(ctx);
        }

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
            ReportMirroredDiskMinorityChecksumMismatch(
                TStringBuilder() << " disk: " << DiskId.Quote()
                                 << ", range: " << GetScrubbingRange());
        } else {
            ReportMirroredDiskMajorityChecksumMismatch(
                TStringBuilder() << " disk: " << DiskId.Quote()
                                 << ", range: " << GetScrubbingRange());
        }
        if (Config->GetResyncRangeAfterScrubbing() &&
            CanFixMismatch(hasQuorum, Config->GetScrubbingResyncPolicy()))
        {
            StartResyncRange(ctx, hasQuorum);
            return;
        }
    }

    StartScrubbingRange(ctx, ScrubbingRangeId + 1);
}

void TMirrorPartitionActor::StartResyncRange(
    const NActors::TActorContext& ctx,
    bool isMinor)
{
    LOG_WARN(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Resyncing range %s",
        DiskId.c_str(),
        DescribeRange(GetScrubbingRange()).c_str());
    ResyncRangeStarted = true;

    if (isMinor) {
        Minors.insert(GetScrubbingRange());
    } else {
        Majors.insert(GetScrubbingRange());
    }

    auto requestInfo = CreateRequestInfo(
        SelfId(),
        0,  // cookie
        MakeIntrusive<TCallContext>()
    );

    TVector<TReplicaDescriptor> replicas;
    const auto& replicaInfos = State.GetReplicaInfos();
    for (ui32 i = 0; i < replicaInfos.size(); i++) {
        if (State.DevicesReadyForReading(i, GetScrubbingRange())) {
            replicas.emplace_back(
                replicaInfos[i].Config->GetName(),
                i,
                State.GetReplicaActor(i));
        }
    }

    // Force usage TResyncRangeActor for minor errors.
    auto resyncPolicy = isMinor ? NProto::EResyncPolicy::RESYNC_POLICY_MINOR_4MB
                                : Config->GetScrubbingResyncPolicy();
    auto resyncActor = MakeResyncRangeActor(
        std::move(requestInfo),
        State.GetBlockSize(),
        GetScrubbingRange(),
        std::move(replicas),
        State.GetRWClientId(),
        BlockDigestGenerator,
        resyncPolicy,
        isMinor ? EBlockRangeChecksumStatus::MinorError
                : EBlockRangeChecksumStatus::MajorError,
        State.GetReplicaInfos()[0].Config->GetParentActorId(),
        Config->GetAssignIdToWriteAndZeroRequestsEnabled());
    ctx.Register(resyncActor.release());
}

void TMirrorPartitionActor::AddTagForBufferCopying(
    const NActors::TActorContext& ctx)
{
    auto requestInfo = CreateRequestInfo(
        SelfId(),
        0,  // cookie
        MakeIntrusive<TCallContext>());

    TVector<TString> tags({TString(IntermediateWriteBufferTagName)});
    auto request = std::make_unique<TEvService::TEvAddTagsRequest>(
        DiskId,
        std::move(tags));

    ctx.Send(MakeStorageServiceId(), std::move(request));
}

void TMirrorPartitionActor::ReplyAndDie(const TActorContext& ctx)
{
    NCloud::Reply(ctx, *Poisoner, std::make_unique<TEvents::TEvPoisonTaken>());
    Die(ctx);
}

auto TMirrorPartitionActor::TakeNextRequestIdentifier() -> ui64
{
    return RequestIdentifierCounter++;
}

bool TMirrorPartitionActor::CanMakeMultiAgentWrite(TBlockRange64 range) const
{
    return MultiAgentWriteEnabled && range.Size() * State.GetBlockSize() >=
                                         MultiAgentWriteRequestSizeThreshold;
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Become(&TThis::StateZombie);

    KillActors(ctx);

    AliveReplicas =
        State.GetReplicaActors().size() + State.LaggingReplicaCount();

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
    if (!State.IsReplicaActor(ev->Sender)) {
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
        if (!requestInfo.IsWrite) {
            continue;
        }
        const auto& requestRange = requestInfo.BlockRange;
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
    for (ui32 i = 0; i < replicaInfos.size(); i++) {
        if (State.DevicesReadyForReading(i, scrubbingRange)) {
            replicas.emplace_back(
                replicaInfos[i].Config->GetName(),
                i,
                State.GetReplicaActor(i));
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
    using EStatus = TEvNonreplPartitionPrivate::TEvRangeResynced::EStatus;

    const auto* msg = ev->Get();

    if (!HasError(msg->Error)) {
        if (msg->Status == EStatus::HealedAll) {
            Fixed.insert(msg->Range);
        } else if (msg->Status == EStatus::HealedPartial) {
            FixedPartial.insert(msg->Range);
        }
    }

    LOG_WARN(ctx, TBlockStoreComponents::PARTITION,
        "[%s] Range %s resync finished: %s %s",
        DiskId.c_str(),
        DescribeRange(msg->Range).c_str(),
        FormatError(msg->GetError()).c_str(),
        ToString(msg->Status).c_str());

    CpuUsage += CyclesToDurationSafe(msg->ExecCycles);

    ResyncRangeStarted = false;
    StartScrubbingRange(ctx, ScrubbingRangeId + 1);
}

void TMirrorPartitionActor::HandleAddLaggingAgent(
    const TEvNonreplPartitionPrivate::TEvAddLaggingAgentRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    const ui32 replicaIndex = msg->LaggingAgent.GetReplicaIndex();
    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Adding lagging agent: %s, replica index: %u",
        DiskId.c_str(),
        msg->LaggingAgent.GetAgentId().c_str(),
        replicaIndex);

    if (!State.IsLaggingProxySet(replicaIndex)) {
        Y_ABORT_UNLESS(replicaIndex < State.GetReplicaInfos().size());

        const auto& replicaInfo = State.GetReplicaInfos()[replicaIndex];
        auto proxyActorId = NCloud::Register(
            ctx,
            std::make_unique<TLaggingAgentsReplicaProxyActor>(
                Config,
                DiagnosticsConfig,
                replicaInfo.Config,
                replicaInfo.Migrations,
                ProfileLog,
                BlockDigestGenerator,
                State.GetRWClientId(),
                State.GetReplicaActorsBypassingProxies()[replicaIndex],
                SelfId()));
        State.SetLaggingReplicaProxy(replicaIndex, proxyActorId);
    }

    NCloud::Send<TEvNonreplPartitionPrivate::TEvAgentIsUnavailable>(
        ctx,
        State.GetReplicaActors()[replicaIndex],
        0,   // cookie
        msg->LaggingAgent);
    State.AddLaggingAgent(std::move(msg->LaggingAgent));
}

void TMirrorPartitionActor::HandleRemoveLaggingAgent(
    const TEvNonreplPartitionPrivate::TEvRemoveLaggingAgentRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Removing lagging agent: %s, replica index: %u",
        DiskId.c_str(),
        msg->LaggingAgent.GetAgentId().Quote().c_str(),
        msg->LaggingAgent.GetReplicaIndex());

    State.RemoveLaggingAgent(msg->LaggingAgent);

    const ui32 replicaIndex = msg->LaggingAgent.GetReplicaIndex();
    if (!State.HasLaggingAgents(replicaIndex) &&
        State.IsLaggingProxySet(replicaIndex))
    {
        auto proxy = State.GetReplicaActors()[replicaIndex];
        State.ResetLaggingReplicaProxy(replicaIndex);
        NCloud::Send<TEvents::TEvPoisonPill>(ctx, proxy);
    }
}

void TMirrorPartitionActor::HandleInconsistentDiskAgent(
    const TEvNonreplPartitionPrivate::TEvInconsistentDiskAgent::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_WARN(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Disable multi-agent writes due to bad response from %s.",
        DiskId.c_str(),
        msg->AgentId.Quote().c_str());

    ReportDiskAgentInconsistentMultiWriteResponse(
        TStringBuilder() << "DiskId: " << DiskId.Quote()
                         << ", DiskAgent: " << msg->AgentId.Quote());
    MultiAgentWriteEnabled = false;
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

void TMirrorPartitionActor::HandleAddTagsResponse(
    const TEvService::TEvAddTagsResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    auto error = msg->GetError();

    if (HasError(error)) {
        ReportMirroredDiskAddTagFailed(
            TStringBuilder()
            << "Failed to add " << IntermediateWriteBufferTagName
            << " tag for disk [" << DiskId << "] with "
            << FormatError(error));
        return;
    }
    LOG_WARN(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] %s tag added for disk",
        DiskId.c_str(),
        IntermediateWriteBufferTagName);
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionActor::HandleLockAndDrainRange(
    const TEvPartition::TEvLockAndDrainRangeRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (BlockRangeRequests.OverlapsWithRequest(msg->Range)) {
        auto response =
            std::make_unique<TEvPartition::TEvLockAndDrainRangeResponse>(
                MakeError(
                    E_REJECTED,
                    "request overlaps with other block range request"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }
    BlockRangeRequests.AddRequest(msg->Range);

    auto reqInfo = CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    DrainActorCompanion.AddDrainRangeRequest(
        ctx,
        std::move(reqInfo),
        msg->Range);

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Range %s is blocked for writing requests",
        DiskId.c_str(),
        DescribeRange(msg->Range).c_str());
}

void TMirrorPartitionActor::HandleReleaseRange(
    const TEvPartition::TEvReleaseRange::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);
    auto* msg = ev->Get();

    BlockRangeRequests.RemoveRequest(msg->Range);

    DrainActorCompanion.RemoveDrainRangeRequest(ctx, msg->Range);
    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Releasing range %s for writing requests",
        DiskId.c_str(),
        DescribeRange(msg->Range).c_str());
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

        HFunc(TEvNonreplPartitionPrivate::TEvAddLaggingAgentRequest, HandleAddLaggingAgent);
        HFunc(TEvNonreplPartitionPrivate::TEvRemoveLaggingAgentRequest, HandleRemoveLaggingAgent);
        HFunc(TEvPartition::TEvDrainRequest, DrainActorCompanion.HandleDrain);
        HFunc(
            TEvPartition::TEvWaitForInFlightWritesRequest,
            DrainActorCompanion.HandleWaitForInFlightWrites);
        HFunc(TEvService::TEvGetChangedBlocksRequest, DeclineGetChangedBlocks);
        HFunc(
            TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest,
            HandleGetDeviceForRange);
        HFunc(
            TEvNonreplPartitionPrivate::TEvInconsistentDiskAgent,
            HandleInconsistentDiskAgent);

        HFunc(TEvVolume::TEvDescribeBlocksRequest, HandleDescribeBlocks);
        HFunc(TEvVolume::TEvGetCompactionStatusRequest, HandleGetCompactionStatus);
        HFunc(TEvVolume::TEvCompactRangeRequest, HandleCompactRange);
        HFunc(TEvVolume::TEvRebuildMetadataRequest, HandleRebuildMetadata);
        HFunc(TEvVolume::TEvGetRebuildMetadataStatusRequest, HandleGetRebuildMetadataStatus);
        HFunc(TEvVolume::TEvScanDiskRequest, HandleScanDisk);
        HFunc(TEvVolume::TEvGetScanDiskStatusRequest, HandleGetScanDiskStatus);
        HFunc(TEvVolume::TEvCheckRangeRequest, HandleCheckRange);

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

        HFunc(
            TEvService::TEvAddTagsResponse,
            HandleAddTagsResponse);

        HFunc(
            TEvPartition::TEvLockAndDrainRangeRequest,
            HandleLockAndDrainRange);

        HFunc(TEvPartition::TEvReleaseRange, HandleReleaseRange);

        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        IgnoreFunc(TEvents::TEvPoisonTaken);

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

        HFunc(TEvPartition::TEvDrainRequest, RejectDrain);
        HFunc(
            TEvPartition::TEvWaitForInFlightWritesRequest,
            RejectWaitForInFlightWrites);
        HFunc(TEvService::TEvGetChangedBlocksRequest, DeclineGetChangedBlocks);
        HFunc(
            TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest,
            GetDeviceForRangeCompanion.RejectGetDeviceForRange);
        IgnoreFunc(TEvNonreplPartitionPrivate::TEvInconsistentDiskAgent)

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

        IgnoreFunc(TEvService::TEvAddTagsResponse);

        IgnoreFunc(TEvPartition::TEvLockAndDrainRangeRequest);

        IgnoreFunc(TEvPartition::TEvReleaseRange);

        IgnoreFunc(TEvents::TEvPoisonPill);
        HFunc(TEvents::TEvPoisonTaken, HandlePoisonTaken);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
