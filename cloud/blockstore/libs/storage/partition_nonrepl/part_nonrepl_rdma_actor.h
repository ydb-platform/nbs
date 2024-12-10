#pragma once

#include "public.h"

#include "config.h"
#include "part_nonrepl_events_private.h"

#include <cloud/blockstore/libs/rdma/iface/client.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/disk_counters.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/model/requests_in_progress.h>
#include <cloud/blockstore/libs/storage/partition_common/drain_actor_companion.h>
#include <cloud/blockstore/libs/storage/partition_common/get_device_for_range_companion.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/mon.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TDeviceReadRequestContext: public NRdma::TNullContext
{
    ui64 StartIndexOffset = 0;
    ui64 BlockCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TNonreplicatedPartitionRdmaActor final
    : public NActors::TActorBootstrapped<TNonreplicatedPartitionRdmaActor>
{
private:
    const TStorageConfigPtr Config;
    const TNonreplicatedPartitionConfigPtr PartConfig;
    const NRdma::IClientPtr RdmaClient;
    const NActors::TActorId StatActorId;

    // TODO implement DeviceStats and similar stuff

    TRequestsInProgress<ui64> RequestsInProgress{EAllowedRequests::ReadWrite};
    TDrainActorCompanion DrainActorCompanion{
        RequestsInProgress,
        PartConfig->GetName()};
    TGetDeviceForRangeCompanion GetDeviceForRangeCompanion{
        TGetDeviceForRangeCompanion::EAllowedOperation::ReadWrite,
        Config,
        PartConfig};

    bool UpdateCountersScheduled = false;
    TPartitionDiskCountersPtr PartCounters;
    ui64 NetworkBytes = 0;
    TDuration CpuUsage;

    using TEndpointFuture = NThreading::TFuture<NRdma::IClientEndpointPtr>;
    THashMap<TString, TEndpointFuture> AgentId2EndpointFuture;
    THashMap<TString, NRdma::IClientEndpointPtr> AgentId2Endpoint;

    TRequestInfoPtr Poisoner;

    bool SentRdmaUnavailableNotification = false;

    const bool AssignIdToWriteAndZeroRequestsEnabled{
        Config->GetAssignIdToWriteAndZeroRequestsEnabled()};

public:
    TNonreplicatedPartitionRdmaActor(
        TStorageConfigPtr config,
        TNonreplicatedPartitionConfigPtr partConfig,
        NRdma::IClientPtr rdmaClient,
        NActors::TActorId statActorId);

    ~TNonreplicatedPartitionRdmaActor();

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void KillActors(const NActors::TActorContext& ctx);
    bool CheckReadWriteBlockRange(const TBlockRange64& range) const;
    void ScheduleCountersUpdate(const NActors::TActorContext& ctx);
    void SendStats(const NActors::TActorContext& ctx);
    void SendRdmaUnavailableIfNeeded(const NActors::TActorContext& ctx);
    void UpdateStats(const NProto::TPartitionStats& update);

    void ReplyAndDie(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);
    STFUNC(StateZombie);

    template <typename TMethod>
    bool InitRequests(
        const typename TMethod::TRequest& msg,
        const NActors::TActorContext& ctx,
        TRequestInfo& requestInfo,
        const TBlockRange64& blockRange,
        TVector<TDeviceRequest>* deviceRequests);

    NProto::TError SendReadRequests(
        const NActors::TActorContext& ctx,
        TCallContextPtr callContext,
        const NProto::THeaders& headers,
        NRdma::IClientHandlerPtr handler,
        const TVector<TDeviceRequest>& deviceRequests);

    void HandleUpdateCounters(
        const TEvNonreplPartitionPrivate::TEvUpdateCounters::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReadBlocksCompleted(
        const TEvNonreplPartitionPrivate::TEvReadBlocksCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteBlocksCompleted(
        const TEvNonreplPartitionPrivate::TEvWriteBlocksCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleZeroBlocksCompleted(
        const TEvNonreplPartitionPrivate::TEvZeroBlocksCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleChecksumBlocksCompleted(
        const TEvNonreplPartitionPrivate::TEvChecksumBlocksCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    bool HandleRequests(STFUNC_SIG);

    BLOCKSTORE_IMPLEMENT_REQUEST(ReadBlocks, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(WriteBlocks, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(ReadBlocksLocal, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(WriteBlocksLocal, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(ZeroBlocks, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(DescribeBlocks, TEvVolume);
    BLOCKSTORE_IMPLEMENT_REQUEST(ChecksumBlocks, TEvNonreplPartitionPrivate);
    BLOCKSTORE_IMPLEMENT_REQUEST(Drain, NPartition::TEvPartition);

    BLOCKSTORE_IMPLEMENT_REQUEST(CompactRange, TEvVolume);
    BLOCKSTORE_IMPLEMENT_REQUEST(GetCompactionStatus, TEvVolume);
    BLOCKSTORE_IMPLEMENT_REQUEST(RebuildMetadata, TEvVolume);
    BLOCKSTORE_IMPLEMENT_REQUEST(GetRebuildMetadataStatus, TEvVolume);
    BLOCKSTORE_IMPLEMENT_REQUEST(ScanDisk, TEvVolume);
    BLOCKSTORE_IMPLEMENT_REQUEST(GetScanDiskStatus, TEvVolume);
};

}   // namespace NCloud::NBlockStore::NStorage
