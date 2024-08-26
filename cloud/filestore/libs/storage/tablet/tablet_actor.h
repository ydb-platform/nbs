#pragma once

#include "public.h"

#include "tablet_counters.h"
#include "tablet_private.h"
#include "tablet_state.h"
#include "tablet_tx.h"

#include <cloud/filestore/libs/diagnostics/metrics/histogram.h>
#include <cloud/filestore/libs/diagnostics/metrics/window_calculator.h>
#include <cloud/filestore/libs/diagnostics/metrics/public.h>
#include <cloud/filestore/libs/diagnostics/public.h>
#include <cloud/filestore/libs/storage/api/service.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/core/config.h>
#include <cloud/filestore/libs/storage/core/tablet.h>
#include <cloud/filestore/libs/storage/model/public.h>
#include <cloud/filestore/libs/storage/model/range.h>
#include <cloud/filestore/libs/storage/model/utils.h>
#include <cloud/filestore/libs/storage/tablet/model/throttler_logger.h>
#include <cloud/filestore/libs/storage/tablet/model/verify.h>

#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/diagnostics/busy_idle_calculator.h>
#include <cloud/storage/core/libs/throttling/public.h>

#include <contrib/ydb/core/base/tablet_pipe.h>
#include <contrib/ydb/core/mind/local.h>
#include <contrib/ydb/core/filestore/core/filestore.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>
#include <contrib/ydb/library/actors/core/mon.h>

#include <util/generic/size_literals.h>
#include <util/generic/string.h>

#include <atomic>

namespace NBlockCodecs {

////////////////////////////////////////////////////////////////////////////////

struct ICodec;

}   // namespace NBlockCodecs

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TIndexTabletActor final
    : public NActors::TActor<TIndexTabletActor>
    , public TTabletBase<TIndexTabletActor>
    , public TIndexTabletState
{
    static constexpr size_t MaxBlobStorageBlobSize = 40_MB;

    enum EState
    {
        STATE_BOOT,
        STATE_INIT,
        STATE_WORK,
        STATE_ZOMBIE,
        STATE_BROKEN,
        STATE_MAX,
    };

    struct TStateInfo
    {
        TString Name;
        NActors::IActor::TReceiveFunc Func;
    };

private:
    struct TMetrics
    {
        bool Initialized{false};

        std::atomic<i64> TotalBytesCount{0};
        std::atomic<i64> UsedBytesCount{0};

        std::atomic<i64> TotalNodesCount{0};
        std::atomic<i64> UsedNodesCount{0};

        std::atomic<i64> UsedSessionsCount{0};
        std::atomic<i64> UsedHandlesCount{0};
        std::atomic<i64> UsedLocksCount{0};
        std::atomic<i64> StatefulSessionsCount{0};
        std::atomic<i64> StatelessSessionsCount{0};
        std::atomic<i64> SessionTimeouts{0};

        std::atomic<i64> AllocatedCompactionRangesCount{0};
        std::atomic<i64> UsedCompactionRangesCount{0};

        std::atomic<i64> ReassignCount{0};
        std::atomic<i64> WritableChannelCount{0};
        std::atomic<i64> UnwritableChannelCount{0};
        std::atomic<i64> ChannelsToMoveCount{0};

        std::atomic<i64> ReadAheadCacheHitCount{0};
        std::atomic<i64> ReadAheadCacheNodeCount{0};

        // Node index cache
        std::atomic<i64> NodeIndexCacheHitCount{0};
        std::atomic<i64> NodeIndexCacheNodeCount{0};

        // Data stats
        std::atomic<i64> FreshBytesCount{0};
        std::atomic<i64> DeletedFreshBytesCount{0};
        std::atomic<i64> MixedBytesCount{0};
        std::atomic<i64> MixedBlobsCount{0};
        std::atomic<i64> DeletionMarkersCount{0};
        std::atomic<i64> GarbageQueueSize{0};
        std::atomic<i64> GarbageBytesCount{0};
        std::atomic<i64> FreshBlocksCount{0};
        std::atomic<i64> CMMixedBlobsCount{0};
        std::atomic<i64> CMDeletionMarkersCount{0};

        // Throttling
        std::atomic<i64> MaxReadBandwidth{0};
        std::atomic<i64> MaxWriteBandwidth{0};
        std::atomic<i64> MaxReadIops{0};
        std::atomic<i64> MaxWriteIops{0};
        std::atomic<i64> RejectedRequests{0};
        std::atomic<i64> PostponedRequests{0};
        std::atomic<i64> UsedQuota{0};

        // Tablet busy/idle time
        std::atomic<i64> BusyTime{0};
        std::atomic<i64> IdleTime{0};
        TBusyIdleTimeCalculatorAtomics BusyIdleCalc;

        std::atomic<i64> UncompressedBytesWritten{0};
        std::atomic<i64> CompressedBytesWritten{0};

        std::atomic<i64> NodesOpenForWritingBySingleSession{0};
        std::atomic<i64> NodesOpenForWritingByMultipleSessions{0};
        std::atomic<i64> NodesOpenForReadingBySingleSession{0};
        std::atomic<i64> NodesOpenForReadingByMultipleSessions{0};

        NMetrics::TDefaultWindowCalculator MaxUsedQuota{0};
        using TLatHistogram =
            NMetrics::THistogram<NMetrics::EHistUnit::HU_TIME_MICROSECONDS>;
        TLatHistogram ReadDataPostponed;
        TLatHistogram WriteDataPostponed;

        struct TRequestMetrics
        {
            explicit TRequestMetrics(
                TVector<TRequestMetrics*>& allRequestMetrics)
            {
                allRequestMetrics.push_back(this);
            }

            std::atomic<i64> Count{0};
            std::atomic<i64> RequestBytes{0};
            TLatHistogram Time;

            void Update(ui64 requestCount, ui64 requestBytes, TDuration d)
            {
                Count.fetch_add(requestCount, std::memory_order_relaxed);
                RequestBytes.fetch_add(requestBytes, std::memory_order_relaxed);
                Time.Record(d);
            }
        };

        struct TCompactionMetrics: TRequestMetrics
        {
            explicit TCompactionMetrics(
                    TVector<TRequestMetrics*>& allRequestMetrics)
                : TRequestMetrics(allRequestMetrics)
            {}

            std::atomic<i64> DudCount{0};
        };
        TVector<TRequestMetrics*> AllRequestMetrics{Reserve(14)};

        TRequestMetrics ReadBlob{AllRequestMetrics};
        TRequestMetrics WriteBlob{AllRequestMetrics};
        TRequestMetrics PatchBlob{AllRequestMetrics};
        TRequestMetrics ReadData{AllRequestMetrics};
        TRequestMetrics DescribeData{AllRequestMetrics};
        TRequestMetrics WriteData{AllRequestMetrics};
        TRequestMetrics AddData{AllRequestMetrics};
        TRequestMetrics GenerateBlobIds{AllRequestMetrics};
        TCompactionMetrics Compaction{AllRequestMetrics};
        TRequestMetrics Cleanup{AllRequestMetrics};
        TRequestMetrics Flush{AllRequestMetrics};
        TRequestMetrics FlushBytes{AllRequestMetrics};
        TRequestMetrics TrimBytes{AllRequestMetrics};
        TRequestMetrics CollectGarbage{AllRequestMetrics};

        i64 LastNetworkMetric = 0;

        i64 TakeTotalRequestBytes();
        // Compaction/cleanup stats
        std::atomic<i64> MaxBlobsInRange{0};
        std::atomic<i64> MaxDeletionsInRange{0};

        const NMetrics::IMetricsRegistryPtr StorageRegistry;
        const NMetrics::IMetricsRegistryPtr StorageFsRegistry;

        NMetrics::IMetricsRegistryPtr FsRegistry;
        NMetrics::IMetricsRegistryPtr AggregatableFsRegistry;

        explicit TMetrics(NMetrics::IMetricsRegistryPtr metricsRegistry);

        void Register(const TString& fsId, const TString& mediaKind);
        void Update(
            const NProto::TFileSystem& fileSystem,
            const NProto::TFileSystemStats& stats,
            const NProto::TFileStorePerformanceProfile& performanceProfile,
            const TCompactionMapStats& compactionStats,
            const TSessionsStats& sessionsStats,
            const TChannelsStats& channelsStats,
            const TReadAheadCacheStats& readAheadStats,
            const TNodeIndexCacheStats& nodeIndexCacheStats);
    } Metrics;

    const IProfileLogPtr ProfileLog;
    const ITraceSerializerPtr TraceSerializer;

    static const TStateInfo States[];
    EState CurrentState = STATE_BOOT;

    NKikimr::TTabletCountersWithTxTypes* Counters = nullptr;
    bool UpdateCountersScheduled = false;
    bool UpdateLeakyBucketCountersScheduled = false;
    bool SyncSessionsScheduled = false;
    bool CleanupSessionsScheduled = false;

    TDeque<NActors::IEventHandlePtr> WaitReadyRequests;

    TSet<NActors::TActorId> WorkerActors;
    TIntrusiveList<TRequestInfo> ActiveTransactions;

    TInstant ReassignRequestSentTs;

    TThrottlerLogger ThrottlerLogger;
    ITabletThrottlerPtr Throttler;

    TStorageConfigPtr Config;

    const bool UseNoneCompactionPolicy;

    struct TCompactionStateLoadStatus
    {
        TDeque<TEvIndexTabletPrivate::TLoadCompactionMapChunkRequest> LoadQueue;
        ui32 MaxLoadedInOrderRangeId = 0;
        THashSet<ui32> LoadedOutOfOrderRangeIds;
        bool LoadChunkInProgress = false;
        bool Finished = false;
    } CompactionStateLoadStatus;

    // used on monpages
    NProto::TStorageConfig StorageConfigOverride;

    ui32 BackpressureErrorCount = 0;

    const NBlockCodecs::ICodec* BlobCodec;

    TVector<ui32> RangesWithEmptyCompactionScore;

public:
    TIndexTabletActor(
        const NActors::TActorId& owner,
        NKikimr::TTabletStorageInfoPtr storage,
        TStorageConfigPtr config,
        IProfileLogPtr profileLog,
        ITraceSerializerPtr traceSerializer,
        NMetrics::IMetricsRegistryPtr metricsRegistry,
        bool useNoneCompactionPolicy);
    ~TIndexTabletActor() override;

    static constexpr ui32 LogComponent = TFileStoreComponents::TABLET;
    using TCounters = TIndexTabletCounters;

    static TString GetStateName(ui32 state);

    void RebootTabletOnCommitOverflow(
        const NActors::TActorContext& ctx,
        const TString& request);

private:
    void Enqueue(STFUNC_SIG) override;
    void DefaultSignalTabletActive(const NActors::TActorContext& ctx) override;
    void OnActivateExecutor(const NActors::TActorContext& ctx) override;
    bool ReassignChannelsEnabled() const override;
    void RegisterEvPutResult(
        const NActors::TActorContext& ctx,
        ui32 generation,
        ui32 channel,
        const NKikimr::TStorageStatusFlags flags);
    void ReassignDataChannelsIfNeeded(const NActors::TActorContext& ctx);
    bool OnRenderAppHtmlPage(
        NActors::NMon::TEvRemoteHttpInfo::TPtr ev,
        const NActors::TActorContext& ctx) override;
    void OnDetach(const NActors::TActorContext& ctx) override;
    void OnTabletDead(
        NKikimr::TEvTablet::TEvTabletDead::TPtr& ev,
        const NActors::TActorContext& ctx) override;

    void Suicide(const NActors::TActorContext& ctx);
    void BecomeAux(const NActors::TActorContext& ctx, EState state);
    void ReportTabletState(const NActors::TActorContext& ctx);

    void RegisterStatCounters();
    void RegisterCounters(const NActors::TActorContext& ctx);
    void ScheduleUpdateCounters(const NActors::TActorContext& ctx);
    void UpdateCounters();
    void UpdateDelayCounter(
        TThrottlingPolicy::EOpType opType,
        TDuration time);

    void ScheduleSyncSessions(const NActors::TActorContext& ctx);
    void ScheduleCleanupSessions(const NActors::TActorContext& ctx);
    void CreateSessionsInFollowers(
        const NActors::TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        NProtoPrivate::TCreateSessionRequest request,
        std::unique_ptr<TEvIndexTablet::TEvCreateSessionResponse> response,
        TVector<TString> followerIds);
    void RestartCheckpointDestruction(const NActors::TActorContext& ctx);

    template <typename TMethod>
    void EnqueueWriteBatch(
        const NActors::TActorContext& ctx,
        std::unique_ptr<TWriteRequest> request)
    {
        request->RequestInfo->CancelRoutine = [] (
            const NActors::TActorContext& ctx,
            TRequestInfo& requestInfo)
        {
            auto response = std::make_unique<typename TMethod::TResponse>(
                MakeError(E_REJECTED, "tablet is shutting down"));

            NCloud::Reply(ctx, requestInfo, std::move(response));
        };

        if (TIndexTabletState::EnqueueWriteBatch(std::move(request))) {
            if (auto timeout = Config->GetWriteBatchTimeout()) {
                ctx.Schedule(timeout, new TEvIndexTabletPrivate::TEvWriteBatchRequest());
            } else {
                ctx.Send(SelfId(), new TEvIndexTabletPrivate::TEvWriteBatchRequest());
            }
        }
    }

    void EnqueueFlushIfNeeded(const NActors::TActorContext& ctx);
    void EnqueueBlobIndexOpIfNeeded(const NActors::TActorContext& ctx);
    void EnqueueCollectGarbageIfNeeded(const NActors::TActorContext& ctx);
    void EnqueueTruncateIfNeeded(const NActors::TActorContext& ctx);
    void EnqueueForcedRangeOperationIfNeeded(const NActors::TActorContext& ctx);
    void LoadNextCompactionMapChunkIfNeeded(const NActors::TActorContext& ctx);

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

    // Depending on whether the transaction is RO or RW, we will either attempt
    // to execute it using the in-memory index state, or it will be executed in
    // a regular way.

    template <typename TTx, typename... TArgs>
    std::enable_if_t<TTx::IsReadOnly, void> ExecuteTx(
        const NActors::TActorContext& ctx,
        TArgs&&... args)
    {
        typename TTx::TArgs tx(std::forward<TArgs>(args)...);

        // if we can execute the transaction using the in-memory index state,
        // we will do so and return immediately.
        if (TryExecuteTx(ctx, AccessInMemoryIndexState(), tx)) {
            return;
        }
        TTabletBase<TIndexTabletActor>::ExecuteTx<TTx>(ctx, tx);
    }

    template <typename TTx, typename... TArgs>
    std::enable_if_t<!TTx::IsReadOnly, void> ExecuteTx(
        const NActors::TActorContext& ctx,
        TArgs&&... args)
    {
        TTabletBase<TIndexTabletActor>::ExecuteTx<TTx>(
            ctx,
            std::forward<TArgs>(args)...);
    }

    void RemoveTransaction(TRequestInfo& transaction);
    void TerminateTransactions(const NActors::TActorContext& ctx);
    void ReleaseTransactions();

    void NotifySessionEvent(
        const NActors::TActorContext& ctx,
        const NProto::TSessionEvent& event);

    TBackpressureThresholds BuildBackpressureThresholds() const;

    void ResetThrottlingPolicy();

    void ExecuteTx_AddBlob_Write(
        const NActors::TActorContext& ctx,
        NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
        TTxIndexTablet::TAddBlob& args);

    void ExecuteTx_AddBlob_Flush(
        const NActors::TActorContext& ctx,
        NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
        TTxIndexTablet::TAddBlob& args);

    void ExecuteTx_AddBlob_FlushBytes(
        const NActors::TActorContext& ctx,
        NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
        TTxIndexTablet::TAddBlob& args);

    void ExecuteTx_AddBlob_Compaction(
        const NActors::TActorContext& ctx,
        NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
        TTxIndexTablet::TAddBlob& args);

    bool CheckSessionForDestroy(const TSession* session, ui64 seqNo);

    void RegisterCreateNodeInFollowerActor(
        const NActors::TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        NProto::TCreateNodeRequest request,
        ui64 requestId,
        ui64 opLogEntryId,
        TCreateNodeInFollowerResult result);

    void RegisterUnlinkNodeInFollowerActor(
        const NActors::TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        NProto::TUnlinkNodeRequest request,
        ui64 requestId,
        ui64 opLogEntryId,
        TUnlinkNodeInFollowerResult result);

    void ReplayOpLog(
        const NActors::TActorContext& ctx,
        const TVector<NProto::TOpLogEntry>& opLog);

private:
    template <typename TMethod>
    TSession* AcceptRequest(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx,
        const std::function<NProto::TError(
            const typename TMethod::TRequest::ProtoRecordType&)>& validator = {});

    template <typename TMethod>
    void CompleteResponse(
        typename TMethod::TResponse::ProtoRecordType& response,
        const TCallContextPtr& callContext,
        const NActors::TActorContext& ctx);

    template <typename TMethod>
    NProto::TError Throttle(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    template <typename TMethod>
    bool ThrottleIfNeeded(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    template <typename TRequest>
    NProto::TError ValidateWriteRequest(
        const NActors::TActorContext& ctx,
        const TRequest& request,
        const TByteRange& range);

    NProto::TError IsDataOperationAllowed() const;

    ui32 ScaleCompactionThreshold(ui32 t) const;
    TCompactionInfo GetCompactionInfo() const;
    TCleanupInfo GetCleanupInfo() const;

    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleHttpInfo(
        const NActors::NMon::TEvRemoteHttpInfo::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleHttpInfo_Default(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);
    void HandleHttpInfo_ForceOperation(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);
    void HandleHttpInfo_DumpCompactionRange(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleSessionDisconnected(
        const NKikimr::TEvTabletPipe::TEvServerDisconnected::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleTabletMetrics(
        const NKikimr::TEvLocal::TEvTabletMetrics::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateConfig(
        const NKikimr::TEvFileStore::TEvUpdateConfig::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateCounters(
        const TEvIndexTabletPrivate::TEvUpdateCounters::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateLeakyBucketCounters(
        const TEvIndexTabletPrivate::TEvUpdateLeakyBucketCounters::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReleaseCollectBarrier(
        const TEvIndexTabletPrivate::TEvReleaseCollectBarrier::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReadDataCompleted(
        const TEvIndexTabletPrivate::TEvReadDataCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteDataCompleted(
        const TEvIndexTabletPrivate::TEvWriteDataCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleAddDataCompleted(
        const TEvIndexTabletPrivate::TEvAddDataCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleForcedRangeOperationProgress(
        const TEvIndexTabletPrivate::TEvForcedRangeOperationProgress::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleNodeCreatedInFollower(
        const TEvIndexTabletPrivate::TEvNodeCreatedInFollower::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleNodeUnlinkedInFollower(
        const TEvIndexTabletPrivate::TEvNodeUnlinkedInFollower::TPtr& ev,
        const NActors::TActorContext& ctx);

    void SendMetricsToExecutor(const NActors::TActorContext& ctx);

    bool HandleRequests(STFUNC_SIG);
    bool RejectRequests(STFUNC_SIG);
    bool RejectRequestsByBrokenTablet(STFUNC_SIG);

    bool HandleCompletions(STFUNC_SIG);
    bool IgnoreCompletions(STFUNC_SIG);

    FILESTORE_TABLET_REQUESTS(FILESTORE_IMPLEMENT_REQUEST, TEvIndexTablet)
    FILESTORE_SERVICE_REQUESTS(FILESTORE_IMPLEMENT_REQUEST, TEvService)

    FILESTORE_TABLET_REQUESTS_PRIVATE_SYNC(FILESTORE_IMPLEMENT_REQUEST, TEvIndexTabletPrivate)
    FILESTORE_TABLET_REQUESTS_PRIVATE_ASYNC(FILESTORE_IMPLEMENT_ASYNC_REQUEST, TEvIndexTabletPrivate)

    FILESTORE_TABLET_RW_TRANSACTIONS(
        FILESTORE_IMPLEMENT_RW_TRANSACTION,
        TTxIndexTablet);
    FILESTORE_TABLET_INDEX_RO_TRANSACTIONS(
        FILESTORE_IMPLEMENT_RO_TRANSACTION,
        TTxIndexTablet,
        TIndexTabletDatabase,
        IIndexTabletDatabase);

    STFUNC(StateBoot);
    STFUNC(StateInit);
    STFUNC(StateWork);
    STFUNC(StateZombie);
    STFUNC(StateBroken);

    void RegisterFileStore(const NActors::TActorContext& ctx);
    void UnregisterFileStore(const NActors::TActorContext& ctx);

    void UpdateLogTag();
};

}   // namespace NCloud::NFileStore::NStorage
