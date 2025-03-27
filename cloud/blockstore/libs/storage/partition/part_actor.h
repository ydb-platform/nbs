#pragma once

#include "public.h"

#include "part_counters.h"
#include "part_database.h"
#include "part_events_private.h"
#include "part_state.h"
#include "part_tx.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/api/partition.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/disk_counters.h>
#include <cloud/blockstore/libs/storage/core/metrics.h>
#include <cloud/blockstore/libs/storage/core/monitoring_utils.h>
#include <cloud/blockstore/libs/storage/core/pending_request.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/core/tablet.h>
#include <cloud/blockstore/libs/storage/partition_common/drain_actor_companion.h>
#include <cloud/blockstore/libs/storage/partition_common/events_private.h>
#include <cloud/blockstore/libs/storage/partition_common/long_running_operation_companion.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>
#include <cloud/storage/core/libs/tablet/blob_id.h>
#include <cloud/storage/core/libs/tablet/model/commit.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>
#include <contrib/ydb/library/actors/core/mon.h>

#include <util/generic/array_ref.h>
#include <util/generic/deque.h>
#include <util/generic/hash_set.h>
#include <util/generic/intrlist.h>

namespace NBlockCodecs {

////////////////////////////////////////////////////////////////////////////////

struct ICodec;

}   // namespace NBlockCodecs

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

class TPartitionActor final
    : public NActors::TActor<TPartitionActor>
    , public TTabletBase<TPartitionActor>
    , private IRequestsInProgress
{
    enum EState
    {
        STATE_BOOT,
        STATE_INIT,
        STATE_WORK,
        STATE_ZOMBIE,
        STATE_MAX,
    };

    struct TStateInfo
    {
        TString Name;
        NActors::IActor::TReceiveFunc Func;
    };

    struct TForcedCompactionInfo
    {
        TVector<ui32> RangesToCompact;
        TString OperationId;

        TForcedCompactionInfo(
                TVector<ui32> rangesToCompact,
                TString operationId)
            : RangesToCompact(std::move(rangesToCompact))
            , OperationId(std::move(operationId))
        {}
    };

    struct TForcedCompactionResult
    {
        ui32 NumRanges = 0;
        TInstant CompleteTs = {};

        TForcedCompactionResult(
                ui32 numRanges,
                TInstant completeTs)
            : NumRanges(numRanges)
            , CompleteTs(completeTs)
        {}
    };

private:
    const TStorageConfigPtr Config;
    const NProto::TPartitionConfig PartitionConfig;
    const TDiagnosticsConfigPtr DiagnosticsConfig;
    const IProfileLogPtr ProfileLog;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;
    const EStorageAccessMode StorageAccessMode;
    const ui32 SiblingCount;
    const NActors::TActorId VolumeActorId;
    const ui64 ChannelHistorySize;
    const NBlockCodecs::ICodec* BlobCodec;

    std::unique_ptr<TPartitionState> State;

    static const TStateInfo States[];
    EState CurrentState = STATE_BOOT;

    NKikimr::TTabletCountersWithTxTypes* Counters = nullptr;

    bool UpdateCountersScheduled = false;
    bool UpdateYellowStateScheduled = false;
    TInstant ReassignRequestSentTs;

    // Pending requests
    TDeque<TPendingRequest> PendingRequests;

    // Requests in-progress
    TRunningActors Actors;
    TIntrusiveList<TRequestInfo> ActiveTransactions;
    TDrainActorCompanion DrainActorCompanion{*this, TabletID()};
    ui32 WriteAndZeroRequestsInProgress = 0;

    TPartitionDiskCountersPtr PartCounters;

    ui64 SysCPUConsumption = 0;
    ui64 UserCPUConsumption = 0;

    // Pending forced compaction requests
    TDeque<TForcedCompactionInfo> PendingForcedCompactionRequests;
    THashMap<TString, TForcedCompactionResult> CompletedForcedCompactionRequests;

    NBlobMetrics::TBlobLoadMetrics PrevMetrics;
    NBlobMetrics::TBlobLoadMetrics OverlayMetrics;

    bool FirstGarbageCollectionCompleted = false;

public:
    TPartitionActor(
        const NActors::TActorId& owner,
        NKikimr::TTabletStorageInfoPtr storage,
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticsConfig,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        NProto::TPartitionConfig partitionConfig,
        EStorageAccessMode storageAccessMode,
        ui32 siblingCount,
        const NActors::TActorId& volumeActorId);
    ~TPartitionActor() override;

    static constexpr ui32 LogComponent = TBlockStoreComponents::PARTITION;
    using TCounters = TPartitionCounters;

    static TString GetStateName(ui32 state);

protected:
    void Enqueue(STFUNC_SIG) override;
    void DefaultSignalTabletActive(const NActors::TActorContext& ctx) override;

private:
    void Activate(const NActors::TActorContext& ctx);
    void Suicide(const NActors::TActorContext& ctx);
    void BecomeAux(const NActors::TActorContext& ctx, EState state);
    void ReportTabletState(const NActors::TActorContext& ctx);

    void RegisterCounters(const NActors::TActorContext& ctx);
    void ScheduleCountersUpdate(const NActors::TActorContext& ctx);
    void UpdateCounters(const NActors::TActorContext& ctx);

    void OnActivateExecutor(const NActors::TActorContext& ctx) override;

    bool OnRenderAppHtmlPage(
        NActors::NMon::TEvRemoteHttpInfo::TPtr ev,
        const NActors::TActorContext& ctx) override;

    void OnDetach(const NActors::TActorContext& ctx) override;

    void OnTabletDead(
        NKikimr::TEvTablet::TEvTabletDead::TPtr& ev,
        const NActors::TActorContext& ctx) override;

    void BeforeDie(const NActors::TActorContext& ctx);

    void SendGetUsedBlocksFromBaseDisk(const NActors::TActorContext& ctx);
    void FinalizeLoadState(const NActors::TActorContext& ctx);

    void LoadFreshBlobs(const NActors::TActorContext& ctx);
    void FreshBlobsLoaded(const NActors::TActorContext& ctx);

    void ConfirmBlobs(const NActors::TActorContext& ctx);
    void BlobsConfirmed(const NActors::TActorContext& ctx);

    void EnqueueFlushIfNeeded(const NActors::TActorContext& ctx);
    void EnqueueCompactionIfNeeded(const NActors::TActorContext& ctx);
    void EnqueueCleanupIfNeeded(const NActors::TActorContext& ctx);
    void EnqueueCollectGarbageIfNeeded(const NActors::TActorContext& ctx);
    void EnqueueProcessWriteQueueIfNeeded(const NActors::TActorContext& ctx);
    void EnqueueTrimFreshLogIfNeeded(const NActors::TActorContext& ctx);
    void EnqueueAddConfirmedBlobsIfNeeded(const NActors::TActorContext& ctx);

    void UpdateStats(const NProto::TPartitionStats& update);
    void UpdateActorStats(const NActors::TActorContext& ctx);

    void UpdateActorStatsSampled(const NActors::TActorContext& ctx)
    {
        static constexpr int SampleRate = 128;
        if (Y_UNLIKELY(GetHandledEvents() % SampleRate == 0)) {
            UpdateActorStats(ctx);
        }
    }

    void SendStatsToService(const NActors::TActorContext& ctx);

    // IRequestsInProgress implementation:
    bool WriteRequestInProgress() const override
    {
        return WriteAndZeroRequestsInProgress != 0;
    }

    void WaitForInFlightWrites() override {
        Y_ABORT("Unimplemented");
    }

    bool IsWaitingForInFlightWrites() const override {
        Y_ABORT("Unimplemented");
    }

    template <typename TMethod>
    void HandleWriteBlocksRequest(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx,
        bool replyLocal);

    template <typename TMethod>
    void HandleReadBlocksRequest(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx,
        bool replyLocal);

    TMaybe<ui64> VerifyReadBlocksCheckpoint(
        const NActors::TActorContext& ctx,
        const TString& checkpointId,
        TRequestInfo& requestInfo,
        bool replyLocal);

    TMaybe<ui64> VerifyDescribeBlocksCheckpoint(
        const NActors::TActorContext& ctx,
        const TString& checkpointId,
        TRequestInfo& requestInfo);

    void WriteBlocks(
        const NActors::TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        const TBlockRange32& writeRange,
        IWriteBlocksHandlerPtr writeHandler,
        bool replyLocal);

    void WriteFreshBlocks(
        const NActors::TActorContext& ctx,
        TRequestInBuffer<TWriteBufferRequestData> requestInBuffer);

    void WriteFreshBlocks(
        const NActors::TActorContext& ctx,
        TArrayRef<TRequestInBuffer<TWriteBufferRequestData>> requestsInBuffer);

    bool WriteMixedBlocks(
        const NActors::TActorContext& ctx,
        const TVector<TRequestGroup>& groups);

    void WriteMergedBlocks(
        const NActors::TActorContext& ctx,
        TRequestInBuffer<TWriteBufferRequestData> requestInBuffer);

    void ReadBlocks(
        const NActors::TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        ui64 commitId,
        const TBlockRange32& readRange,
        IReadBlocksHandlerPtr readHandler,
        bool replyLocal);

    void DescribeBlocks(
        const NActors::TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        ui64 commitId,
        const TBlockRange32& describeRange);

    void FillDescribeBlocksResponse(
        TTxPartition::TDescribeBlocks& args,
        TEvVolume::TEvDescribeBlocksResponse* response);

    void ProcessIOQueue(const NActors::TActorContext& ctx, ui32 channel);
    void ClearWriteQueue(const NActors::TActorContext& ctx);
    void ProcessCommitQueue(const NActors::TActorContext& ctx);
    void ProcessCheckpointQueue(const NActors::TActorContext& ctx);
    void ProcessNextCheckpointRequest(
        const NActors::TActorContext& ctx,
        const TString& checkpointId);

    template <typename TMethod>
    void DeleteCheckpoint(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx,
        bool deleteOnlyData);

    void KillActors(const NActors::TActorContext& ctx);
    void AddTransaction(
        TRequestInfo& transaction,
        TRequestInfo::TCancelRoutine cancelRoutine);

    template <typename TMethod>
    void AddTransaction(TRequestInfo& transaction)
    {
        auto cancelRoutine = [] (
            const NActors::TActorContext& ctx,
            TRequestInfo& requestInfo)
        {
            auto response = std::make_unique<typename TMethod::TResponse>(
                MakeError(E_REJECTED, "tablet is shutting down"));

            NCloud::Reply(ctx, requestInfo, std::move(response));
        };

        AddTransaction(transaction, cancelRoutine);
    }
    void RemoveTransaction(TRequestInfo& transaction);
    void TerminateTransactions(const NActors::TActorContext& ctx);
    void ReleaseTransactions();

    ui64 CalcChannelHistorySize() const;

    NKikimr::NMetrics::TResourceMetrics* GetResourceMetrics();

    void UpdateWriteThroughput(
        const TInstant& now,
        const NKikimr::NMetrics::TChannel& channel,
        const NKikimr::NMetrics::TGroupId& group,
        ui64 value);

    void UpdateReadThroughput(
        const TInstant& now,
        const NKikimr::NMetrics::TChannel& channel,
        const NKikimr::NMetrics::TGroupId& group,
        ui64 value,
        bool isOverlayDisk);

    void UpdateNetworkStat(
        const TInstant& now,
        ui64 value);

    void UpdateStorageStat(i64 value);
    void UpdateCPUUsageStat(ui64 value);
    void UpdateExecutorStats(const NActors::TActorContext& ctx);

    void ScheduleYellowStateUpdate(const NActors::TActorContext& ctx);
    void UpdateYellowState(const NActors::TActorContext& ctx);
    void ReassignChannelsIfNeeded(const NActors::TActorContext& ctx);

    bool InitReadWriteBlockRange(
        ui64 blockIndex,
        ui32 blockCount,
        TBlockRange64* range) const;

    bool InitChangedBlocksRange(
        ui64 blockIndex,
        ui32 blockCount,
        TBlockRange64* range) const;

    void UpdateChannelPermissions(
        const NActors::TActorContext& ctx,
        ui32 channel,
        EChannelPermissions permissions);

    void SendBackpressureReport(const NActors::TActorContext& ctx) const;

    void SendGarbageCollectorCompleted(const NActors::TActorContext& ctx) const;

    void RebootPartitionOnCommitIdOverflow(
        const NActors::TActorContext& ctx,
        const TStringBuf& requestName);

    void RebootPartitionOnCollectCounterOverflow(
        const NActors::TActorContext& ctx,
        const TStringBuf& requestName);

    void AddForcedCompaction(
        const NActors::TActorContext& ctx,
        TVector<ui32> rangesToCompact,
        TString operationId);

    void EnqueueForcedCompaction(const NActors::TActorContext& ctx);

    bool GetCompletedForcedCompactionRanges(
        const TString& operationId,
        TInstant now,
        ui32& ranges);

    void ChangeRangeCountPerRunIfNeeded(
        ui64 rangeRealCount,
        ui64 rangeThreshold,
        ui64 diskRealCount,
        ui64 diskThreshold,
        const NActors::TActorContext& ctx);

    bool IsCompactRangePending(
        const TString& operationId,
        ui32& ranges) const;

    NActors::IActorPtr CreateMetadataRebuildUsedBlocksActor(
        NActors::TActorId tablet,
        ui64 blocksPerBatch,
        ui64 blockCount,
        TDuration retryTimeout);

    NActors::IActorPtr CreateMetadataRebuildBlockCountActor(
        NActors::TActorId tablet,
        ui64 blobsPerBatch,
        ui64 finalCommitId,
        ui64 mixedBlocksCount,
        ui64 mergedBlocksCount,
        TDuration retryTimeout);

    TBlockBuffer CreateScanDiskBlockBuffer(ui32 blobsPerBatch);

    NActors::IActorPtr CreateScanDiskActor(
        NActors::TActorId tablet,
        ui64 blobsPerBatch,
        ui64 finalCommitId,
        TDuration retryTimeout,
        TBlockBuffer blockBuffer);

private:
    STFUNC(StateBoot);
    STFUNC(StateInit);
    STFUNC(StateWork);
    STFUNC(StateZombie);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleCheckBlobstorageStatusResult(
        const NKikimr::TEvTablet::TEvCheckBlobstorageStatusResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReassignTabletResponse(
        const NCloud::NStorage::TEvHiveProxy::TEvReassignTabletResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleHttpInfo(
        const NActors::NMon::TEvRemoteHttpInfo::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleHttpInfo_Default(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_Describe(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_Check(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_View(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_CreateCheckpoint(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_DeleteCheckpoint(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_ForceCompaction(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_AddGarbage(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_CollectGarbage(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_RebuildMetadata(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_ScanDisk(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void SendHttpResponse(
        const NActors::TActorContext& ctx,
        TRequestInfo& requestInfo,
        TString message);

    void SendHttpResponse(
        const NActors::TActorContext& ctx,
        TRequestInfo& requestInfo,
        TString message,
        NMonitoringUtils::EAlertLevel alertLevel);

    void RejectHttpRequest(
        const NActors::TActorContext& ctx,
        TRequestInfo& requestInfo,
        TString message);

    void HandleGetUsedBlocksResponse(
        const TEvVolume::TEvGetUsedBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateCounters(
        const TEvPartitionPrivate::TEvUpdateCounters::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateYellowState(
        const TEvPartitionPrivate::TEvUpdateYellowState::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleSendBackpressureReport(
        const TEvPartitionPrivate::TEvSendBackpressureReport::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleLoadFreshBlobsCompleted(
        const TEvPartitionCommonPrivate::TEvLoadFreshBlobsCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleConfirmBlobsCompleted(
        const TEvPartitionPrivate::TEvConfirmBlobsCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleProcessWriteQueue(
        const TEvPartitionPrivate::TEvProcessWriteQueue::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReadBlobCompleted(
        const TEvPartitionCommonPrivate::TEvReadBlobCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteBlobCompleted(
        const TEvPartitionPrivate::TEvWriteBlobCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePatchBlobCompleted(
        const TEvPartitionPrivate::TEvPatchBlobCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReadBlocksCompleted(
        const TEvPartitionPrivate::TEvReadBlocksCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void FinalizeReadBlocks(
        const NActors::TActorContext& ctx,
        TEvPartitionPrivate::TReadBlocksCompleted operation);

    void HandleWriteBlocksCompleted(
        const TEvPartitionPrivate::TEvWriteBlocksCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleZeroBlocksCompleted(
        const TEvPartitionPrivate::TEvZeroBlocksCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleFlushCompleted(
        const TEvPartitionPrivate::TEvFlushCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleCompactionCompleted(
        const TEvPartitionPrivate::TEvCompactionCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleMetadataRebuildCompleted(
        const TEvPartitionPrivate::TEvMetadataRebuildCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleScanDiskCompleted(
        const TEvPartitionPrivate::TEvScanDiskCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleCollectGarbageCompleted(
        const TEvPartitionPrivate::TEvCollectGarbageCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleForcedCompactionCompleted(
        const TEvPartitionPrivate::TEvForcedCompactionCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleTrimFreshLogCompleted(
        const TEvPartitionCommonPrivate::TEvTrimFreshLogCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleGetChangedBlocksCompleted(
        const TEvPartitionPrivate::TEvGetChangedBlocksCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void FinalizeGetChangedBlocks(
        const NActors::TActorContext& ctx,
        TEvPartitionPrivate::TOperationCompleted operation);

    void HandleAddConfirmedBlobsCompleted(
        const TEvPartitionPrivate::TEvAddConfirmedBlobsCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleLongRunningBlobOperation(
        const TEvPartitionCommonPrivate::TEvLongRunningOperation::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleDescribeBlocksCompleted(
        const TEvPartitionCommonPrivate::TEvDescribeBlocksCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    NProto::TError DoHandleMetadataRebuildBatch(
        const NActors::TActorContext& ctx,
        NProto::ERebuildMetadataType type,
        ui32 rangesPerBatch);

    NProto::TError DoHandleScanDisk(
        const NActors::TActorContext& ctx,
        ui32 blobsPerBatch);

    void MapBaseDiskIdToTabletId(const NActors::TActorContext& ctx);
    void ClearBaseDiskIdToTabletIdMapping(const NActors::TActorContext& ctx);

    bool HandleRequests(STFUNC_SIG);
    bool RejectRequests(STFUNC_SIG);

    void SetFirstGarbageCollectionCompleted();
    bool IsFirstGarbageCollectionCompleted() const;

    BLOCKSTORE_PARTITION_REQUESTS(BLOCKSTORE_IMPLEMENT_REQUEST, TEvPartition)
    BLOCKSTORE_PARTITION_REQUESTS_PRIVATE(BLOCKSTORE_IMPLEMENT_REQUEST, TEvPartitionPrivate)
    BLOCKSTORE_PARTITION_COMMON_REQUESTS_PRIVATE(BLOCKSTORE_IMPLEMENT_REQUEST, TEvPartitionCommonPrivate)
    BLOCKSTORE_PARTITION_REQUESTS_FWD_SERVICE(BLOCKSTORE_IMPLEMENT_REQUEST, TEvService)
    BLOCKSTORE_PARTITION_REQUESTS_FWD_VOLUME(BLOCKSTORE_IMPLEMENT_REQUEST, TEvVolume)

    BLOCKSTORE_PARTITION_TRANSACTIONS(BLOCKSTORE_IMPLEMENT_TRANSACTION, TTxPartition)
};

////////////////////////////////////////////////////////////////////////////////

void SetCounters(
    NProto::TIOCounters& counters,
    const TDuration execTime,
    const TDuration waitTime,
    ui64 blocksCount);

////////////////////////////////////////////////////////////////////////////////

NProto::TError VerifyBlockChecksum(
    const ui32 actualChecksum,
    const NKikimr::TLogoBlobID& blobID,
    const ui64 blockIndex,
    const ui16 blobOffset,
    const ui32 expectedChecksum);

}   // namespace NCloud::NBlockStore::NStorage::NPartition
