#include "part_mirror_resync_actor.h"

#include "part_mirror.h"
#include "part_nonrepl.h"
#include "part_nonrepl_common.h"

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

TMirrorPartitionResyncActor::TMirrorPartitionResyncActor(
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticsConfig,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr digestGenerator,
        TString rwClientId,
        TNonreplicatedPartitionConfigPtr partConfig,
        TMigrations migrations,
        TVector<TDevices> replicaDevices,
        NRdma::IClientPtr rdmaClient,
        NActors::TActorId volumeActorId,
        NActors::TActorId statActorId,
        ui64 initialResyncIndex,
        NProto::EResyncPolicy resyncPolicy,
        bool critOnChecksumMismatch)
    : Config(std::move(config))
    , DiagnosticsConfig(std::move(diagnosticsConfig))
    , ProfileLog(std::move(profileLog))
    , BlockDigestGenerator(std::move(digestGenerator))
    , ResyncPolicy(resyncPolicy)
    , CritOnChecksumMismatch(critOnChecksumMismatch)
    , VolumeActorId(volumeActorId)
    , RWClientId(std::move(rwClientId))
    , PartConfig(std::move(partConfig))
    , Migrations(std::move(migrations))
    , ReplicaDevices(std::move(replicaDevices))
    , RdmaClient(std::move(rdmaClient))
    , StatActorId(statActorId)
    , State(Config, RWClientId, PartConfig, ReplicaDevices, initialResyncIndex)
    , BackoffProvider(
          Config->GetInitialRetryDelayForServiceRequests(),
          Config->GetMaxRetryDelayForServiceRequests())
{}

TMirrorPartitionResyncActor::~TMirrorPartitionResyncActor() = default;

void TMirrorPartitionResyncActor::Bootstrap(const TActorContext& ctx)
{
    SetupPartitions(ctx);
    ScheduleCountersUpdate(ctx);
    ContinueResyncIfNeeded(ctx);

    Become(&TThis::StateWork);
}

void TMirrorPartitionResyncActor::RejectPostponedRead(TPostponedRead& pr)
{
    auto& ev = pr.Ev;
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvService::TEvReadBlocksRequest, RejectReadBlocks);
        HFunc(TEvService::TEvReadBlocksLocalRequest, RejectReadBlocksLocal);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION,
                __PRETTY_FUNCTION__);
            break;
    }
}

void TMirrorPartitionResyncActor::RejectFastPathRecord(TFastPathRecord& fpr)
{
    auto& ev = fpr.Ev;
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvService::TEvReadBlocksRequest, RejectReadBlocks);
        HFunc(TEvService::TEvReadBlocksLocalRequest, RejectReadBlocksLocal);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION,
                __PRETTY_FUNCTION__);
            break;
    }
}

void TMirrorPartitionResyncActor::KillActors(const TActorContext& ctx)
{
    NCloud::Send<TEvents::TEvPoisonPill>(ctx, MirrorActorId);

    for (const auto& replica: Replicas) {
        NCloud::Send<TEvents::TEvPoisonPill>(ctx, replica.ActorId);
    }
}

void TMirrorPartitionResyncActor::SetupPartitions(const TActorContext& ctx)
{
    MirrorActorId = NCloud::Register(
        ctx,
        CreateMirrorPartition(
            Config,
            DiagnosticsConfig,
            ProfileLog,
            BlockDigestGenerator,
            RWClientId,
            PartConfig,
            Migrations,
            ReplicaDevices,
            RdmaClient,
            VolumeActorId,
            SelfId(),
            SelfId()));

    GetDeviceForRangeCompanion.SetDelegate(MirrorActorId);

    const auto& replicaInfos = State.GetReplicaInfos();
    for (ui32 i = 0; i < replicaInfos.size(); i++) {
        IActorPtr actor = CreateNonreplicatedPartition(
            Config,
            DiagnosticsConfig,
            replicaInfos[i].Config,
            VolumeActorId,
            SelfId(),
            RdmaClient);

        TActorId actorId = NCloud::Register(ctx, std::move(actor));
        Replicas.push_back({replicaInfos[i].Config->GetName(), i, actorId});
    }
}

bool TMirrorPartitionResyncActor::IsAnybodyAlive() const {
    if (MirrorActorId) {
        return true;
    }
    for (const auto& replica: Replicas) {
        if (replica.ActorId) {
            return true;
        }
    }
    return false;
}

void TMirrorPartitionResyncActor::ReplyAndDie(const TActorContext& ctx)
{
    NCloud::Reply(ctx, *Poisoner, std::make_unique<TEvents::TEvPoisonTaken>());
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionResyncActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Become(&TThis::StateZombie);

    KillActors(ctx);

    for (auto& pr: PostponedReads) {
        RejectPostponedRead(pr);
    }
    PostponedReads.clear();

    for (auto& x: FastPathRecords) {
        RejectFastPathRecord(x.second);
    }
    FastPathRecords.clear();

    Poisoner = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        MakeIntrusive<TCallContext>());

    if (IsAnybodyAlive()) {
        return;
    }

    ReplyAndDie(ctx);
}

void TMirrorPartitionResyncActor::HandlePoisonTaken(
    const TEvents::TEvPoisonTaken::TPtr& ev,
    const TActorContext& ctx)
{
    if (MirrorActorId == ev->Sender) {
        MirrorActorId = {};
    }

    for (auto& replica: Replicas) {
        if (replica.ActorId == ev->Sender) {
            replica.ActorId = {};
        }
    }

    if (IsAnybodyAlive()) {
        return;
    }

    ReplyAndDie(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionResyncActor::ScheduleCountersUpdate(
    const TActorContext& ctx)
{
    if (!UpdateCountersScheduled) {
        ctx.Schedule(UpdateCountersInterval,
            new TEvNonreplPartitionPrivate::TEvUpdateCounters());
        UpdateCountersScheduled = true;
    }
}

void TMirrorPartitionResyncActor::HandleUpdateCounters(
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
    void TMirrorPartitionResyncActor::Handle##name(                           \
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

////////////////////////////////////////////////////////////////////////////////

STFUNC(TMirrorPartitionResyncActor::StateWork)
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
        HFunc(
            NPartition::TEvPartition::TEvWaitForInFlightWritesRequest,
            DrainActorCompanion.HandleWaitForInFlightWrites);
        HFunc(TEvService::TEvGetChangedBlocksRequest, DeclineGetChangedBlocks);
        HFunc(
            TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest,
            HandleGetDeviceForRange);

        HFunc(TEvVolume::TEvDescribeBlocksRequest, HandleDescribeBlocks);
        HFunc(TEvVolume::TEvGetCompactionStatusRequest, HandleGetCompactionStatus);
        HFunc(TEvVolume::TEvCompactRangeRequest, HandleCompactRange);
        HFunc(TEvVolume::TEvRebuildMetadataRequest, HandleRebuildMetadata);
        HFunc(TEvVolume::TEvGetRebuildMetadataStatusRequest, HandleGetRebuildMetadataStatus);

        HFunc(
            TEvNonreplPartitionPrivate::TEvWriteOrZeroCompleted,
            HandleWriteOrZeroCompleted);
        HFunc(
            TEvNonreplPartitionPrivate::TEvResyncNextRange,
            HandleResyncNextRange);
        HFunc(
            TEvNonreplPartitionPrivate::TEvRangeResynced,
            HandleRangeResynced);
        HFunc(
            TEvNonreplPartitionPrivate::TEvReadResyncFastPathResponse,
            HandleReadResyncFastPathResponse);
        HFunc(
            TEvVolume::TEvResyncStateUpdated,
            HandleResyncStateUpdated);
        HFunc(
            TEvVolume::TEvRWClientIdChanged,
            HandleRWClientIdChanged);
        HFunc(
            TEvVolume::TEvDiskRegistryBasedPartitionCounters,
            HandlePartCounters);
        HFunc(TEvVolume::TEvScrubberCounters, HandleScrubberCounters);

        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION,
                __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TMirrorPartitionResyncActor::StateZombie)
{
    switch (ev->GetTypeRewrite()) {
        IgnoreFunc(TEvNonreplPartitionPrivate::TEvUpdateCounters);

        HFunc(TEvService::TEvReadBlocksRequest, RejectReadBlocks);
        HFunc(TEvService::TEvWriteBlocksRequest, RejectWriteBlocks);
        HFunc(TEvService::TEvZeroBlocksRequest, RejectZeroBlocks);

        HFunc(TEvService::TEvReadBlocksLocalRequest, RejectReadBlocksLocal);
        HFunc(TEvService::TEvWriteBlocksLocalRequest, RejectWriteBlocksLocal);

        HFunc(NPartition::TEvPartition::TEvDrainRequest, RejectDrain);
        HFunc(
            NPartition::TEvPartition::TEvWaitForInFlightWritesRequest,
            RejectWaitForInFlightWrites);
        HFunc(TEvService::TEvGetChangedBlocksRequest, DeclineGetChangedBlocks);
        HFunc(
            TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest,
            GetDeviceForRangeCompanion.RejectGetDeviceForRange);

        HFunc(TEvVolume::TEvDescribeBlocksRequest, RejectDescribeBlocks);
        HFunc(TEvVolume::TEvGetCompactionStatusRequest, RejectGetCompactionStatus);
        HFunc(TEvVolume::TEvCompactRangeRequest, RejectCompactRange);
        HFunc(TEvVolume::TEvRebuildMetadataRequest, RejectRebuildMetadata);
        HFunc(TEvVolume::TEvGetRebuildMetadataStatusRequest, RejectGetRebuildMetadataStatus);

        IgnoreFunc(TEvNonreplPartitionPrivate::TEvWriteOrZeroCompleted);
        IgnoreFunc(TEvNonreplPartitionPrivate::TEvResyncNextRange);
        IgnoreFunc(TEvNonreplPartitionPrivate::TEvRangeResynced);
        IgnoreFunc(TEvNonreplPartitionPrivate::TEvReadResyncFastPathResponse);
        IgnoreFunc(TEvVolume::TEvRWClientIdChanged);
        IgnoreFunc(TEvVolume::TEvDiskRegistryBasedPartitionCounters);

        IgnoreFunc(TEvents::TEvPoisonPill);
        HFunc(TEvents::TEvPoisonTaken, HandlePoisonTaken);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
