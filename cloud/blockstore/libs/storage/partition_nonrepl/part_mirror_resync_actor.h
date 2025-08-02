#pragma once

#include "public.h"

#include "config.h"
#include "part_mirror_resync_state.h"
#include "part_nonrepl_events_private.h"
#include "resync_range.h"

#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/partition.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/disk_counters.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/model/requests_in_progress.h>
#include <cloud/blockstore/libs/storage/partition_common/drain_actor_companion.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/get_device_for_range_companion.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/mon.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration ResyncNextRangeInterval = TDuration::MilliSeconds(1);

////////////////////////////////////////////////////////////////////////////////

class TMirrorPartitionResyncActor final
    : public NActors::TActorBootstrapped<TMirrorPartitionResyncActor>
{
private:
    const TStorageConfigPtr Config;
    const TDiagnosticsConfigPtr DiagnosticsConfig;
    const IProfileLogPtr ProfileLog;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;
    const NProto::EResyncPolicy ResyncPolicy;
    const bool CritOnChecksumMismatch;
    TString RWClientId;
    TNonreplicatedPartitionConfigPtr PartConfig;
    TMigrations Migrations;
    TVector<TDevices> ReplicaDevices;
    NRdma::IClientPtr RdmaClient;
    NActors::TActorId StatActorId;

    TMirrorPartitionResyncState State;

    NActors::TActorId MirrorActorId;
    TVector<TReplicaDescriptor> Replicas;

    TPartitionDiskCountersPtr MirrorCounters;
    bool UpdateCountersScheduled = false;
    ui64 NetworkBytes = 0;
    TDuration CpuUsage;

    bool ResyncFinished = false;

    TRequestsInProgress<EAllowedRequests::WriteOnly, ui64, TBlockRange64>
        WriteAndZeroRequestsInProgress;
    TDrainActorCompanion DrainActorCompanion{
        WriteAndZeroRequestsInProgress,
        PartConfig->GetName()};
    TGetDeviceForRangeCompanion GetDeviceForRangeCompanion{
        TGetDeviceForRangeCompanion::EAllowedOperation::Read};

    TRequestInfoPtr Poisoner;

    struct TPostponedRead
    {
        NActors::IEventHandlePtr Ev;
        TBlockRange64 BlockRange;
    };
    TDeque<TPostponedRead> PostponedReads;
    struct TFastPathRecord
    {
        NActors::IEventHandlePtr Ev;
        TBlockRange64 BlockRange;
        TGuardedBuffer<TString> Buffer;
        TGuardedSgList SgList;
    };
    ui64 FastPathReadCount = 0;
    THashMap<ui64, TFastPathRecord> FastPathRecords;


public:
    TMirrorPartitionResyncActor(
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticsConfig,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr digestGenerator,
        TString rwClientId,
        TNonreplicatedPartitionConfigPtr partConfig,
        TMigrations migrations,
        TVector<TDevices> replicaDevices,
        NRdma::IClientPtr rdmaClient,
        NActors::TActorId statActorId,
        ui64 initialResyncIndex,
        NProto::EResyncPolicy resyncPolicy,
        bool critOnChecksumMismatch);

    ~TMirrorPartitionResyncActor();

    void Bootstrap(const NActors::TActorContext& ctx);

    // For unit tests
    void SetRequestIdentityKey(ui64 value) {
        WriteAndZeroRequestsInProgress.SetRequestIdentityKey(value);
    }

private:
    void RejectPostponedRead(TPostponedRead& pr);
    void RejectFastPathRecord(TFastPathRecord& fpr);
    void KillActors(const NActors::TActorContext& ctx);
    void SetupPartitions(const NActors::TActorContext& ctx);
    void ScheduleCountersUpdate(const NActors::TActorContext& ctx);
    void SendStats(const NActors::TActorContext& ctx);

    void ContinueResyncIfNeeded(const NActors::TActorContext& ctx);
    void ScheduleResyncNextRange(const NActors::TActorContext& ctx);
    void ResyncNextRange(const NActors::TActorContext& ctx);

    bool IsAnybodyAlive() const;
    void ReplyAndDie(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);
    STFUNC(StateZombie);

    void HandleResyncNextRange(
        const TEvNonreplPartitionPrivate::TEvResyncNextRange::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleRangeResynced(
        const TEvNonreplPartitionPrivate::TEvRangeResynced::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteOrZeroCompleted(
        const TEvNonreplPartitionPrivate::TEvWriteOrZeroCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleResyncStateUpdated(
        const TEvVolume::TEvResyncStateUpdated::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleRWClientIdChanged(
        const TEvVolume::TEvRWClientIdChanged::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePartCounters(
        const TEvVolume::TEvDiskRegistryBasedPartitionCounters::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleScrubberCounters(
        const TEvVolume::TEvScrubberCounters::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateCounters(
        const TEvNonreplPartitionPrivate::TEvUpdateCounters::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonTaken(
        const NActors::TEvents::TEvPoisonTaken::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReadResyncFastPathResponse(
        const TEvNonreplPartitionPrivate::TEvReadResyncFastPathResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    template <typename TMethod>
    void ForwardRequest(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    template <typename TMethod>
    void ProcessReadRequestSyncPath(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void ProcessReadRequestFastPath(
        const TEvService::TEvReadBlocksRequest::TPtr& ev,
        TVector<TReplicaDescriptor>&& replicas,
        TBlockRange64 range,
        const NActors::TActorContext& ctx);

    void ProcessReadRequestFastPath(
        const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
        TVector<TReplicaDescriptor>&& replicas,
        TBlockRange64 range,
        const NActors::TActorContext& ctx);

    void ProcessReadRequestSlowPath(
        NActors::IEventHandlePtr&& ev,
        TBlockRange64 range,
        const NActors::TActorContext& ctx);

    void ProcessReadResponseFastPath(
        const TFastPathRecord& record,
        const NActors::TActorContext& ctx);

    void ProcessReadResponseFastPathLocal(
        const TFastPathRecord& record,
        const NActors::TActorContext& ctx);

    void SendReadBlocksResponse(
        const NProto::TError& error,
        const TFastPathRecord& record,
        const NActors::TActorContext& ctx);

    void HandleGetDeviceForRange(
        const TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    BLOCKSTORE_IMPLEMENT_REQUEST(ReadBlocks, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(WriteBlocks, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(ReadBlocksLocal, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(WriteBlocksLocal, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(ZeroBlocks, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(Drain, NPartition::TEvPartition);
    BLOCKSTORE_IMPLEMENT_REQUEST(
        WaitForInFlightWrites,
        NPartition::TEvPartition);

    BLOCKSTORE_IMPLEMENT_REQUEST(DescribeBlocks, TEvVolume);
    BLOCKSTORE_IMPLEMENT_REQUEST(CompactRange, TEvVolume);
    BLOCKSTORE_IMPLEMENT_REQUEST(GetCompactionStatus, TEvVolume);
    BLOCKSTORE_IMPLEMENT_REQUEST(RebuildMetadata, TEvVolume);
    BLOCKSTORE_IMPLEMENT_REQUEST(GetRebuildMetadataStatus, TEvVolume);
};

}   // namespace NCloud::NBlockStore::NStorage
