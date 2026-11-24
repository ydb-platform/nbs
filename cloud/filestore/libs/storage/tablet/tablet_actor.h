#pragma once

#include "public.h"

#include "tablet_counters.h"
#include "tablet_state.h"
#include "tablet.h"
#include "tablet_tx.h"

#include <cloud/filestore/libs/diagnostics/metrics/histogram.h>
#include <cloud/filestore/libs/diagnostics/metrics/public.h>
#include <cloud/filestore/libs/diagnostics/metrics/window_calculator.h>
#include <cloud/filestore/libs/diagnostics/public.h>
#include <cloud/filestore/libs/storage/api/service.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/core/blob_id.h>
#include <cloud/filestore/libs/storage/core/config.h>
#include <cloud/filestore/libs/storage/core/system_counters.h>
#include <cloud/filestore/libs/storage/core/tablet.h>
#include <cloud/filestore/libs/storage/fastshard/iface/fs.h>
#include <cloud/filestore/libs/storage/fastshard/server/server.h>
#include <cloud/filestore/libs/storage/model/public.h>
#include <cloud/filestore/libs/storage/model/utils.h>
#include <cloud/filestore/libs/storage/tablet/events/tablet_private.h>
#include <cloud/filestore/libs/storage/tablet/model/filesystem_event_subscriptions.h>
#include <cloud/filestore/libs/storage/tablet/model/throttler_logger.h>
#include <cloud/filestore/libs/storage/tablet/model/verify.h>

#include <cloud/storage/core/libs/common/byte_range.h>
#include <cloud/storage/core/libs/diagnostics/busy_idle_calculator.h>
#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/throttling/public.h>

#include <contrib/ydb/core/base/tablet_pipe.h>
#include <contrib/ydb/core/filestore/core/filestore.h>
#include <contrib/ydb/core/mind/local.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/executor_thread.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>
#include <contrib/ydb/library/actors/core/mon.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/size_literals.h>
#include <util/generic/string.h>
#include <util/system/hostname.h>

#include <atomic>

namespace NBlockCodecs {

////////////////////////////////////////////////////////////////////////////////

struct ICodec;

}   // namespace NBlockCodecs

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

inline bool IsTabletOverloaded(
    const TStorageConfig& config,
    const TSystemCounters& systemCounters,
    ui32 tabletActorCpuUsageRate)
{
    const ui64 cl = systemCounters.CpuLack.load(std::memory_order_relaxed);
    return tabletActorCpuUsageRate >=
               config.GetTabletActorCpuUsageOverloadThreshold() ||
           cl >= config.GetCpuLackOverloadThreshold();
}

inline void BuildBackendInfo(
    const TStorageConfig& config,
    const TSystemCounters& systemCounters,
    TString fileSystemId,
    bool isFastShard,
    ui32 tabletActorCpuUsageRate,
    NProto::TBackendInfo* backendInfo)
{
    backendInfo->SetIsOverloaded(
        IsTabletOverloaded(config, systemCounters, tabletActorCpuUsageRate));

    const ui32 fastShardPort = config.GetFastShardServerPort();
    if (isFastShard && fastShardPort) {
        backendInfo->SetFastShardHost(FQDNHostName());
        backendInfo->SetFastShardPort(fastShardPort);
    }

    backendInfo->SetActualFileSystemId(std::move(fileSystemId));
}

template <typename T>
void BuildBackendInfo(
    const TStorageConfig& config,
    const TSystemCounters& systemCounters,
    TString fileSystemId,
    bool isFastShard,
    ui32 tabletActorCpuUsageRate,
    T& response)
{
    if constexpr (HasResponseHeaders<T>()) {
        auto* responseHeaders = response.MutableHeaders();
        auto* backendInfo = responseHeaders->MutableBackendInfo();
        BuildBackendInfo(
            config,
            systemCounters,
            std::move(fileSystemId),
            isFastShard,
            tabletActorCpuUsageRate,
            backendInfo);
    }
}

////////////////////////////////////////////////////////////////////////////////

NProtoPrivate::TRenameNodeInDestinationRequest
MakeRenameNodeInDestinationRequest(
    ui64 tabletId,
    ui64 commitId,
    NProto::TRenameNodeRequest originalRequest,
    TString sourceNodeShardId,
    TString sourceNodeShardNodeName,
    TString newParentShardId);

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void CompleteResponse(
    const TStorageConfig& config,
    const ITraceSerializerPtr& traceSerializer,
    TSystemCounters& systemCounters,
    const TString& fileSystemId,
    bool isFastShard,
    TTabletMetrics& metrics,
    typename TMethod::TResponse::ProtoRecordType& response,
    const TCallContextPtr& callContext,
    bool* builtTraceInfo);

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
        STATE_ADAPTER_INIT,
        STATE_ADAPTER,
        STATE_ZOMBIE,
        STATE_BROKEN,
        STATE_ADAPTER_BROKEN,
        STATE_MAX,
    };

    struct TStateInfo
    {
        TString Name;
        NActors::IActor::TReceiveFunc Func;
    };

private:
    TTabletMetricsPtr Metrics;
    TCPUUsageTimer CPUUsageTimer;

    NProtoPrivate::TStorageStats CachedAggregateStats;
    TInstant CachedStatsFetchingStartTs;
    TInstant CachedAggregateStatsTs;

    const IProfileLogPtr ProfileLog;
    const ITraceSerializerPtr TraceSerializer;
    const TSystemCountersPtr SystemCounters;

    static const TStateInfo States[];
    EState CurrentState = STATE_BOOT;

    NKikimr::TTabletCountersWithTxTypes* Counters = nullptr;
    bool UpdateCountersScheduled = false;
    bool UpdateLeakyBucketCountersScheduled = false;
    bool SyncSessionsScheduled = false;
    // An out-of-schedule sync (see ScheduleOutOfScheduleSyncSessions) is
    // scheduled.
    bool OutOfScheduleSyncSessionsScheduled = false;
    // The background shard sessions sync is running. Tracked only if
    // LazyShardSessionCreationEnabled is set.
    bool SyncSessionsInProgress = false;
    // One more sync should be run after the completion of the current one.
    bool SyncSessionsRequested = false;
    // An urgent sync (see ScheduleUrgentSyncSessions) is on its way.
    bool UrgentSyncSessionsScheduled = false;
    bool CleanupSessionsScheduled = false;
    bool EnqueueBlobIndexOpIfNeededScheduled = false;

    TDeque<NActors::IEventHandlePtr> WaitReadyRequests;

    //
    // Main tablet, LazyShardSessionCreationEnabled.
    //
    // This tablet owns the sessions, the shards follow. All the requests
    // which change the sessions of the shards are sent by the shard sessions
    // sync, which works in rounds. Everything that the shards should get has
    // a version (see ShardSessionsVersion), and this tablet remembers which
    // version each shard has been synced at:
    // * a regular round asks each shard for its sessions, compares them with
    //   the state of this tablet and makes the shard fix the difference;
    // * a round which is triggered by a change (an out-of-schedule one)
    //   doesn't ask the shards for anything: a shard gets only what has been
    //   changed since its last sync. The shards which this tablet knows
    //   nothing about and the shards which have been restarted are still
    //   compared in full.
    //
    // CreateSession, ResetSession and DestroySession are not acknowledged
    // until the sync delivers them to all the shards. Such a request is kept
    // here (see PendingShardSessionRequests) until then:
    // * CreateSession is processed by this tablet first - that's what tells
    //   which session the shards should get - and its response waits for the
    //   shards. So a client which has got the response is able to use the
    //   session in any shard, and no shard keeps anything of the previous
    //   incarnation of a session which has been recovered.
    // * ResetSession and DestroySession are processed by the shards first
    //   and by this tablet last, the session is not touched until then. So
    //   the shards make their decisions against the same subsessions as this
    //   tablet does.
    // There's no timeout for it: a request waits for as long as some of the
    // shards are unavailable. Each shard has to process a request only once
    // (see ShardSyncStates), so a request makes progress even if each round
    // fails for some of the shards. A deadline would throw this progress
    // away - the retry of the client would have to start over with all the
    // shards.
    //
    // The requests are idempotent, so the clients may retry them at any
    // moment. A retry is recognized by Headers.RequestId - the same thing
    // which the tablet relies upon in its DupCache:
    // * a retry of a request which is still waiting for the shards takes its
    //   place and keeps its version, so the shards which have processed the
    //   request are not waited for once again;
    // * a retry of a request which has been completed is acknowledged right
    //   away, see TSession::CompletedCreateRequestId and the like.
    //
    // The state below is not persistent: the clients retry the requests
    // which didn't manage to get through, and the first round after a
    // restart of this tablet compares all the shards in full.
    //

    // Grows upon each change which the shards should get: a change of a
    // session (see OnShardSessionChanged) and a request which gets to
    // PendingShardSessionRequests.
    ui64 ShardSessionsVersion = 0;
    // ShardSessionsVersion at the start of the current sync round.
    ui64 SyncSessionsStartVersion = 0;
    // The next round should compare all the shards in full even if it's an
    // out-of-schedule one.
    bool FullSyncSessionsRequested = false;

    struct TShardSyncState
    {
        // SyncSessionsStartVersion of the last round which managed to sync
        // the sessions of the shard: the shard has got all the changes which
        // are not newer than that.
        ui64 Version = 0;
        // The generation of the shard tablet which confirmed it.
        ui32 Generation = 0;
    };

    // ShardId -> what this tablet knows about the sessions of the shard.
    // There's nothing here about a shard whose sessions have to be compared
    // with the sessions of this tablet first.
    THashMap<TString, TShardSyncState> ShardSyncStates;

    // What the current round sends to the shards which are not asked for
    // their sessions, by the version which a shard has been synced at (see
    // GetShardSessionsDelta). Most of the shards have the same version, so
    // the changes are collected once per round, not once per shard.
    THashMap<ui64, TShardSessionsDiff> SyncSessionsDeltas;

    enum class EShardSessionRequestType
    {
        Create,
        Reset,
        Destroy,
    };

    struct TPendingShardSessionRequest
    {
        EShardSessionRequestType Type = EShardSessionRequestType::Destroy;
        // ShardSessionsVersion upon the arrival of the request. A retry
        // keeps the version of the request which it replaces.
        ui64 Version = 0;
        ui64 SessionSeqNo = 0;
        // Headers.RequestId of the request. The client sends the same value
        // with all the attempts of a request and different values with
        // different requests, so a request with the same value is a retry.
        // Zero means that the client hasn't provided it: such a request is
        // never treated as a retry.
        ui64 RequestId = 0;
        // CreateSession: the session existed and has been recovered by the
        // request, see TShardSessionInfo::Interrupted.
        bool Interrupted = false;
        // ResetSession and DestroySession: all the shards have processed
        // the request, the tx of this tablet is in progress.
        bool Applying = false;
        // Identifies the tx of the request as well.
        TRequestInfoPtr RequestInfo;
        NProtoPrivate::TCreateSessionRequest CreateRequest;
        // CreateSession: this tablet has already processed the request,
        // that's what the client gets when the shards are done.
        std::unique_ptr<TEvIndexTablet::TEvCreateSessionResponse>
            CreateResponse;
        NProto::TResetSessionRequest ResetRequest;
        NProtoPrivate::TDestroySessionRequest DestroyRequest;

        const char* GetName() const
        {
            switch (Type) {
                case EShardSessionRequestType::Create:
                    return "CreateSession";
                case EShardSessionRequestType::Reset:
                    return "ResetSession";
                case EShardSessionRequestType::Destroy:
                    return "DestroySession";
            }

            return "unknown request";
        }
    };

    // SessionId -> CreateSession, ResetSession or DestroySession which is
    // being delivered to the shards. There's at most one such request per
    // session. Nothing else is allowed to change the session until its
    // completion - except for one more CreateSession, which replaces a
    // CreateSession which is waiting here.
    THashMap<TString, TPendingShardSessionRequest> PendingShardSessionRequests;

    //
    // Main tablet. The shards which reported that they work in adapter mode
    // but aren't marked as fast shards in the shard topology.
    //

    THashSet<TString> AdapterModeShardIds;

    //
    // Shard, ShardSessionRecoveryOnDemandEnabled.
    //
    // A request which belongs to a session which is not active in this shard
    // is postponed until the session is requested from the main tablet. If
    // the main tablet confirms that the session is active, the session gets
    // recovered and the request is processed. Otherwise the request is
    // rejected.
    //

    struct TPostponedSessionRequest
    {
        using TRejectRoutine = void(
            const NActors::TActorContext& ctx,
            const NActors::IEventHandle& request,
            const NProto::TError& error);

        NActors::IEventHandlePtr Request;
        TRejectRoutine* Reject = nullptr;
        TString ClientId;
        ui64 SessionSeqNo = 0;
    };

    struct TSessionRecovery
    {
        ui64 Id = 0;
        TDeque<TPostponedSessionRequest> Requests;
    };

    ui64 LastSessionRecoveryId = 0;
    // SessionId -> the recovery which is in progress
    THashMap<TString, TSessionRecovery> SessionRecoveries;
    // SessionRecoveryId -> SessionId
    THashMap<ui64, TString> SessionRecoveryIds;
    // Set while the postponed requests are being processed: such requests
    // shouldn't trigger session recovery once again.
    bool ReplayingPostponedSessionRequests = false;

    TSet<NActors::TActorId> WorkerActors;
    TIntrusiveList<TRequestInfo> ActiveRequests;

    TInstant ReassignRequestSentTs;

    TInstant TabletStartTs;
    // The largest node id that existed when this tablet incarnation started.
    // A handle lost by the restart can only refer to this or an earlier node.
    ui64 MaxNodeIdAtTabletStart = 0;

    TThrottlerLogger ThrottlerLogger;
    ITabletThrottlerPtr Throttler;

    TStorageConfigPtr Config;
    TDiagnosticsConfigPtr DiagConfig;

    //
    // PipeServerId -> ActorId of the client on the other side of the pipe
    // (IndexTabletProxyActor). FileSystemEvents are sent to these clients.
    //

    THashMap<NActors::TActorId, NActors::TActorId> FileSystemEventClients;

    //
    // Which clients (identified by the pipe servers) have read which Nodes and
    // NodeRefs. FileSystemEvents are sent only to these clients. The clients
    // that have disconnected are filtered out via FileSystemEventClients upon
    // sending, their registrations go away with the generation rotation.
    //

    TFileSystemEventSubscriptions FileSystemEventSubscriptions;

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
    NProto::TStorageConfig BaseStorageConfig;

    ui32 BackpressureErrorCount = 0;
    TInstant BackpressurePeriodStart;

    const NBlockCodecs::ICodec* BlobCodec;

    TVector<ui32> RangesWithEmptyCompactionScore;

    TProtoMessagePrinter ProtoMessagePrinter;

    NFastShard::IFileSystemShardPtr FastShard;
    NFastShard::IServerPtr FastShardServer;
    NFastShard::IFileSystemShardFactoryPtr FastShardFactory;

    IRdmaTabletRegistryPtr RdmaTabletRegistry;

public:
    TIndexTabletActor(
        const NActors::TActorId& owner,
        NKikimr::TTabletStorageInfoPtr storage,
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagConfig,
        IProfileLogPtr profileLog,
        ITraceSerializerPtr traceSerializer,
        TSystemCountersPtr systemCounters,
        NMetrics::IMetricsRegistryPtr metricsRegistry,
        NFastShard::IServerPtr fastShardServer,
        NFastShard::IFileSystemShardFactoryPtr fastShardFactory,
        IRdmaTabletRegistryPtr rdmaTabletRegistry,
        ITxReschedulerPtr txRescheduler);
    ~TIndexTabletActor() override;

    static constexpr ui32 LogComponent = TFileStoreComponents::TABLET;
    using TCounters = TIndexTabletCounters;

    static TString GetStateName(ui32 state);

    void ScheduleRebootTabletOnCommitIdOverflow(
        const NActors::TActorContext& ctx,
        const TString& request);

    void SendPendingConfirmAddDataResponse(
        const NActors::TActorContext& ctx,
        ui64 commitId,
        const NProto::TError& error);

    void UnconfirmedAddBlobSafePointReached(
        const NActors::TActorContext& ctx,
        ui64 commitId,
        const NProto::TError& error);

private:
    void Enqueue(STFUNC_SIG) override;
    void DefaultSignalTabletActive(const NActors::TActorContext& ctx) override;
    void OnActivateExecutor(const NActors::TActorContext& ctx) override;
    bool ReassignChannelsEnabled() const override;
    void RegisterEvPutResult(
        const NActors::TActorContext& ctx,
        ui32 generation,
        ui32 channel,
        const NKikimr::TStorageStatusFlags flags,
        double freeSpaceShare);

    template <typename TBlobIds, typename TRecord>
    void ProcessStorageStatusFlags(
        const NActors::TActorContext& ctx,
        const TBlobIds& blobIds,
        const TRecord& record)
    {
        const auto& flags = record.GetStorageStatusFlags();
        const auto& shares = record.GetApproximateFreeSpaceShares();
        const auto count = Min<int>(blobIds.size(), flags.size());
        for (int i = 0; i < count; ++i) {
            const auto blobId =
                LogoBlobIDFromLogoBlobID(blobIds.Get(i));
            const double share = i < shares.size() ? shares.Get(i) : 0;
            RegisterEvPutResult(
                ctx,
                blobId.Generation(),
                blobId.Channel(),
                flags.Get(i),
                share);
        }
    }

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

    void RegisterStatCounters(TInstant now);
    void RegisterCounters(const NActors::TActorContext& ctx);
    void ScheduleUpdateCounters(const NActors::TActorContext& ctx);
    void UpdateMetrics(
        TInstant now,
        const TDiagnosticsConfig& diagConfig,
        const NProto::TFileSystem& fileSystem,
        const NProto::TFileSystemStats& stats,
        const NProto::TFileStorePerformanceProfile& performanceProfile,
        const TCompactionMapStats& compactionStats,
        const TSessionsStats& sessionsStats,
        const TChannelsStats& channelsStats,
        const TReadAheadCacheStats& readAheadStats,
        const TNodeToSessionCounters& nodeToSessionCounters,
        const TMiscNodeStats& miscNodeStats,
        const TInMemoryIndexStateStats& inMemoryIndexStateStats,
        const TBlobMetaMapStats& blobMetaMapStats,
        const TIndexTabletState::TBackpressureThresholds&
            backpressureThresholds,
        const TIndexTabletState::TBackpressureValues& backpressureValues,
        const THandlesStats& handlesStats,
        const TFileSystemEventSubscriptions::TStats& subscriptionsStats);
    void UpdateDelayCounter(
        TThrottlingPolicy::EOpType opType,
        TDuration time);
    void CalculateActorCPUUsage(const NActors::TActorContext& ctx);

    void DeleteOldResponseLogEntries(const NActors::TActorContext& ctx);
    void DestroyDeferredNodes(const NActors::TActorContext& ctx);
    void RunRegularTasks(const NActors::TActorContext& ctx);

    void ScheduleSyncSessions(const NActors::TActorContext& ctx);
    // Schedules a shard sessions sync which is not bound to the regular
    // schedule: the shards should get something sooner than that.
    void ScheduleOutOfScheduleSyncSessions(const NActors::TActorContext& ctx);
    // Starts a shard sessions sync as soon as possible: right now or right
    // after the completion of the current one.
    void ScheduleUrgentSyncSessions(const NActors::TActorContext& ctx);

    // True if CreateSession, ResetSession and DestroySession should be
    // delivered to the shards by the shard sessions sync: such a request is
    // not acknowledged until then.
    bool ShouldSyncShardSessionRequests() const;

    // Keeps a request until the shard sessions sync delivers it to all the
    // shards.
    void EnqueueShardSessionRequest(
        const NActors::TActorContext& ctx,
        TString sessionId,
        TPendingShardSessionRequest request);

    // Replies to the client whose request is not going to be completed.
    void ReplyToPendingShardSessionRequest(
        const NActors::TActorContext& ctx,
        const TPendingShardSessionRequest& request,
        const NProto::TError& error);

    // The same thing for a request whose place is being taken by a new one.
    // Does nothing if the response would get to the requester of the new
    // request as if it were the response to that new request.
    void ReplyToReplacedShardSessionRequest(
        const NActors::TActorContext& ctx,
        const TPendingShardSessionRequest& request,
        const TRequestInfo& newRequestInfo,
        const NProto::TError& error);

    // The newest version which all the shards whose sessions are managed by
    // the shard sessions sync have been synced at: these shards have
    // processed the requests which are not newer than that.
    ui64 GetShardSessionsSyncedVersion() const;

    // CreateSession which has been delivered to all the shards whose
    // sessions are managed by the shard sessions sync: the shards which
    // work in adapter mode are the only thing left.
    void CompletePendingCreateSession(
        const NActors::TActorContext& ctx,
        const TString& sessionId,
        TPendingShardSessionRequest request);

    // Should be called when a session of this tablet changes in a way which
    // the shards should know about: the session gets a new version, so the
    // shard sessions sync sends it to the shards.
    void OnShardSessionChanged(
        const NActors::TActorContext& ctx,
        const TString& sessionId);

    // Describes a pending ResetSession or DestroySession request for a
    // shard.
    NProtoPrivate::TShardSessionRequest BuildShardSessionRequest(
        const TString& sessionId,
        const TPendingShardSessionRequest& request) const;

    // Describes the session of a pending CreateSession request for a shard.
    TMaybe<NProtoPrivate::TShardSessionInfo> BuildCreatedShardSessionInfo(
        const TString& sessionId,
        const TPendingShardSessionRequest& request) const;

    // Tells the shard sessions sync which of these requests should be sent
    // to the shard which has described its sessions.
    void AddPendingShardSessionRequests(
        const TString& shardId,
        const NProtoPrivate::TDescribeShardSessionsResponse& shardSessions,
        TShardSessionsDiff& diff) const;

    // What the current sync round should send to a shard which has been
    // synced at fromVersion and is not asked for its sessions: the sessions
    // which have been changed since then and the requests which the shard
    // hasn't got yet.
    const TShardSessionsDiff& GetShardSessionsDelta(ui64 fromVersion);

    // Deals with the requests which have been delivered to all the shards:
    // CreateSession gets its response, ResetSession and DestroySession get
    // processed by this tablet. The rest of the requests keep waiting.
    void ProcessPendingShardSessionRequests(
        const NActors::TActorContext& ctx);

    // Should be called when this tablet completes ResetSession or
    // DestroySession: the request might be one of the pending ones.
    void OnShardSessionRequestCompleted(
        const NActors::TActorContext& ctx,
        const TString& sessionId,
        const TRequestInfoPtr& requestInfo,
        const NProto::TError& error);

    // Fails the requests which haven't been processed by all the shards yet.
    void RejectPendingShardSessionRequests(
        const NActors::TActorContext& ctx,
        const NProto::TError& error);

    void ScheduleCleanupSessions(const NActors::TActorContext& ctx);

    // The shards which work in adapter mode: the ones which are marked as
    // fast shards in the shard topology and the ones which reported it.
    TVector<TString> GetAdapterModeShardIds() const;

    //
    // On-demand session recovery (shards).
    //

    bool ShouldRecoverSessionsOnDemand() const;

    void PostponeRequestUntilSessionRecovered(
        const NActors::TActorContext& ctx,
        TString clientId,
        TString sessionId,
        ui64 sessionSeqNo,
        NActors::IEventHandlePtr request,
        TPostponedSessionRequest::TRejectRoutine* reject);

    void HandleRecoverShardSessionsResponse(
        const TEvIndexTablet::TEvRecoverShardSessionsResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    // Extracts the requests which wait for the session recovery.
    TDeque<TPostponedSessionRequest> FinishSessionRecovery(
        ui64 sessionRecoveryId,
        TString* sessionId);

    // Processes the requests which were waiting for the session recovery.
    void ReplayPostponedSessionRequests(
        const NActors::TActorContext& ctx,
        ui64 sessionRecoveryId);

    // Rejects the requests which were waiting for the session recovery. An
    // empty error means that the session is invalid.
    void RejectPostponedSessionRequests(
        const NActors::TActorContext& ctx,
        ui64 sessionRecoveryId,
        const NProto::TError& error);

    // Rejects all the requests which wait for the session recovery.
    void RejectAllPostponedSessionRequests(
        const NActors::TActorContext& ctx,
        const NProto::TError& error);

    void CreateSessionsInShards(
        const NActors::TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        NProtoPrivate::TCreateSessionRequest request,
        std::unique_ptr<TEvIndexTablet::TEvCreateSessionResponse> response,
        TVector<TString> shardIds);
    void RestartCheckpointDestruction(const NActors::TActorContext& ctx);

    void EnqueueFlushIfNeeded(const NActors::TActorContext& ctx);
    void EnqueueBlobIndexOpIfNeeded(const NActors::TActorContext& ctx);
    void AddBlobIndexOpIfNeeded(
        const NActors::TActorContext& ctx,
        const TCompactionInfo& compactionInfo,
        const TCleanupInfo& cleanupInfo);
    void EnqueueCollectGarbageIfNeeded(const NActors::TActorContext& ctx);
    void EnqueueTruncateIfNeeded(const NActors::TActorContext& ctx);
    void EnqueueForcedOperationIfNeeded(const NActors::TActorContext& ctx);
    void LoadNextCompactionMapChunkIfNeeded(const NActors::TActorContext& ctx);
    void ScheduleEnqueueBlobIndexOpIfNeeded(const NActors::TActorContext& ctx);

    TVector<ui32> GenerateForceDeleteZeroCompactionRanges() const;

    std::unique_ptr<TEvIndexTablet::TEvForcedOperationResponse>
    ProcessForcedRangeOperationRequest(
        const NProtoPrivate::TForcedOperationRequest& request,
        const NActors::TActorContext& ctx);

    std::unique_ptr<TEvIndexTablet::TEvForcedOperationResponse>
    ProcessForcedTabletOperationRequest(
        const NProtoPrivate::TForcedOperationRequest& request,
        const NActors::TActorContext& ctx);

    void HandleForcedOperationCompletedImpl(
        const NActors::TActorId& sender,
        const NProto::TError& error,
        const NActors::TActorContext& ctx);

    void AddInFlightRequest(
        TRequestInfo& requestInfo,
        TRequestInfo::TCancelRoutine cancelRoutine);

    template <typename TMethod>
    void AddInFlightRequest(TRequestInfo& requestInfo)
    {
        auto cancelRoutine = [] (
            const NActors::TActorContext& ctx,
            TRequestInfo& requestInfo)
        {
            auto response = std::make_unique<typename TMethod::TResponse>(
                MakeError(E_REJECTED, "tablet is shutting down"));

            NCloud::Reply(ctx, requestInfo, std::move(response));
        };

        AddInFlightRequest(requestInfo, cancelRoutine);
    }

    // Depending on whether the transaction is RO or RW, we will either attempt
    // to execute it using the in-memory index state, or it will be executed in
    // a regular way.

    template <typename TTx, typename... TArgs>
    std::enable_if_t<TTx::IsReadOnly, void> ExecuteTx(
        const NActors::TActorContext& ctx,
        TArgs&&... args)
    {
        TCPUUsageTimerGuard t(CPUUsageTimer);

        typename TTx::TArgs tx(std::forward<TArgs>(args)...);

        // if we can execute the transaction using the in-memory index state,
        // we will do so and return immediately.
        auto* indexState = AccessInMemoryIndexState();
        if (!indexState) {
            ReportInMemoryIndexStateNotInitialized();
        }

        if (indexState && TryExecuteTx(ctx, *indexState, tx)) {
            Metrics->InMemoryIndexStateROCacheHitCount.fetch_add(
                1,
                std::memory_order_relaxed);
            return;
        }
        Metrics->InMemoryIndexStateROCacheMissCount.fetch_add(
            1,
            std::memory_order_relaxed);
        TTabletBase<TIndexTabletActor>::ExecuteTx<TTx>(ctx, tx);
    }

    template <typename TTx, typename... TArgs>
    std::enable_if_t<!TTx::IsReadOnly, void> ExecuteTx(
        const NActors::TActorContext& ctx,
        TArgs&&... args)
    {
        Metrics->InMemoryIndexStateRWCount.fetch_add(
            1,
            std::memory_order_relaxed);
        TTabletBase<TIndexTabletActor>::ExecuteTx<TTx>(
            ctx,
            std::forward<TArgs>(args)...);
    }

    void RemoveInFlightRequest(TRequestInfo& requestInfo);
    void RejectInFlightRequests(const NActors::TActorContext& ctx);
    void ReleaseInFlightRequests();

    // Updates in-memory index state with the given node updates. Is to be
    // called upon every operation that changes node-related data. As of now, it
    // is called upon completion of every RW transaction that can change the
    // node-related data. Failure to perform this operation will lead to
    // inconsistent cache state between the localDB and the in-memory index
    // state
    template <typename T>
    void UpdateInMemoryIndexState(const T& args)
    {
        if constexpr (std::is_base_of_v<TIndexStateNodeUpdates, T>) {
            if (Config->GetInMemoryIndexCacheEnabled()) {
                TIndexTabletState::UpdateInMemoryIndexState(args.NodeUpdates);
            }
        }
    }

    NActors::TActorId RegisterFileSystemEventClient(
        const NActors::TActorId& recipient,
        const NActors::TActorId& clientId,
        const NProto::THeaders& headers);

    void AddFileSystemEventNodeSubscriber(
        const NActors::TActorContext& ctx,
        const NActors::TActorId& pipeServerId,
        ui64 nodeId);

    void AddFileSystemEventNodeRefSubscriber(
        const NActors::TActorContext& ctx,
        const NActors::TActorId& pipeServerId,
        ui64 parentNodeId,
        const TString& name);

    void AddFileSystemEventNodeXAttrSubscriber(
        const NActors::TActorContext& ctx,
        const NActors::TActorId& pipeServerId,
        ui64 nodeId,
        const TString& name);

    void RemoveFileSystemEventClient(const NActors::TActorId& pipeServerId);

    TDuration GetFileSystemEventsCacheTTL(
        const NActors::TActorId& pipeServerId) const;

    TDuration GetFileSystemListingEventsCacheTTL(
        const NActors::TActorId& pipeServerId) const;

    ui32 SendFileSystemEvents(
        const NActors::TActorContext& ctx,
        const TVector<NProto::TFileSystemEvent>& events);

    template <typename T>
    void SendTxFileSystemEvents(
        const NActors::TActorContext& ctx,
        const T& args)
    {
        //
        // Must be called after UpdateInMemoryIndexState(args): otherwise a
        // client, having received an event, may re-read the stale data from
        // the in-memory index cache.
        //

        if constexpr (std::is_base_of_v<TIndexStateNodeUpdates, T>) {
            if (!args.FileSystemEvents.empty()) {
                SendFileSystemEvents(ctx, args.FileSystemEvents);
            }
        }
    }

    TBackpressureThresholds BuildBackpressureThresholds() const;
    TBackpressureThresholds BuildBackpressureSoftThresholds() const;
    TBackpressureValues GetBackpressureValues() const;

    void ResetThrottlingPolicy();
    double CalculateWriteCostMultiplierBackpressure() const;
    void UpdateWriteCostMultiplierDueToBackpressure();

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

    bool ReadNodesToRemoveForSessionHandles(
        IIndexTabletDatabase& db,
        const TSession& session,
        ui32 maxHandlesPerTx,
        TNodeSet& nodesToRemove);

    void DestroySessionHandlesAndRemoveNodes(
        IIndexTabletDatabase& db,
        TVector<NProto::TFileSystemEvent>& fileSystemEvents,
        const NActors::TActorContext& ctx,
        TSession* session,
        ui64 commitId,
        ui32 maxHandlesPerTx,
        bool isContinuation,
        const TNodeSet& nodesToRemove,
        const char* operation);

    //
    // Helper actor factory funcs.
    //

    void RegisterCreateNodeInShardActor(
        const NActors::TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        NProto::TCreateNodeRequest request,
        NProto::TProfileLogRequestInfo profileLogRequest,
        ui64 requestId,
        ui64 opLogEntryId,
        TCreateNodeInShardResult result);

    void RegisterUnlinkNodeInShardActor(
        const NActors::TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        NProtoPrivate::TUnlinkNodeInShardRequest request,
        NProto::TProfileLogRequestInfo profileLogRequest,
        ui64 requestId,
        ui64 opLogEntryId,
        TUnlinkNodeInShardResult result);

    void RegisterRenameNodeInDestinationActor(
        const NActors::TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        NProtoPrivate::TRenameNodeInDestinationRequest request,
        NProto::TProfileLogRequestInfo profileLogRequest,
        ui64 requestId,
        ui64 opLogEntryId);

    void RegisterAbortUnlinkDirectoryInShardActor(
        const NActors::TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        NProtoPrivate::TRenameNodeInDestinationRequest request,
        NProto::TProfileLogRequestInfo profileLogRequest,
        NProto::TError originalError,
        TString shardId,
        ui64 nodeId,
        ui64 opLogEntryId,
        bool isLocalRename);

    void RegisterGetNodeInfoAndPrepareUnlinkActor(
        const NActors::TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        NProtoPrivate::TRenameNodeInDestinationRequest request,
        NProto::TProfileLogRequestInfo profileLogRequest,
        TString dstShardId,
        TString dstShardNodeName,
        bool isLocalRename);

    //
    // Misc.
    //

    void CreateFastShard(const NActors::TActorContext& ctx);

    void ReplayOpLog(
        const NActors::TActorContext& ctx,
        const TVector<NProto::TOpLogEntry>& opLog);

    void CompleteAdapterLoadState(
        const NActors::TActorContext& ctx,
        TTxIndexTablet::TLoadState& args);

    void ApplyStorageConfigOverrides(
        const NActors::TActorContext& ctx,
        const TString& cloudId,
        const TString& folderId,
        const TString& fileSystemId);

    bool BehaveAsShard(const NProto::THeaders& headers) const;

    void FillSelfStorageStats(NProtoPrivate::TStorageStats* stats);

private:
    template <typename TMethod>
    TSession* AcceptRequest(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx,
        const std::function<NProto::TError(
            const typename TMethod::TRequest::ProtoRecordType&)>& validator);

    template <typename TMethod>
    bool AcceptRequestNoSession(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx,
        const std::function<NProto::TError(
            const typename TMethod::TRequest::ProtoRecordType&)>& validator);

    template <typename TMethod>
    void CompleteResponse(
        typename TMethod::TResponse::ProtoRecordType& response,
        const TCallContextPtr& callContext,
        bool* builtTraceInfo);

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
        const TByteRange& range,
        bool validateHandle);

    NProto::TError IsDataOperationAllowed() const;
    NProto::TError CheckUnconfirmedDataOverlap(
        ui64 nodeId,
        const TByteRange& range) const;
    NProto::TError ForceLoadRangeIfNeeded(ui64 nodeId, const TByteRange& range);
    // Update shard creation state only when the requested version matches;
    // an unset or stale version leaves the stored state unchanged.
    NProto::TError TryUpdateShardCreationState(
        IIndexTabletDatabase& db,
        const NProtoPrivate::TFileSystemShardCreationState& requested);
    bool IsInUnconfirmedCreateHandleGracePeriod(
        const NActors::TActorContext& ctx) const;
    bool NeedsNodeDestructionDeferral(
        const NActors::TActorContext& ctx,
        const INodeIndexTabletDatabase::TNode& node) const;
    // True if unlinking this node should keep it in the index until the grace
    // period is over. Reports a critical event if the node needs the deferral
    // but the limit leaves no room for it.
    bool ShouldDeferNodeDestruction(
        const NActors::TActorContext& ctx,
        const INodeIndexTabletDatabase::TNode& node,
        TStringBuf operation);
    // Returns true if the node was actually removed, false if its destruction
    // was deferred or failed (in the latter case an orphan node is written).
    bool DeferNodeDestructionOrRemoveNode(
        IIndexTabletDatabase& db,
        TVector<NProto::TFileSystemEvent>& fileSystemEvents,
        const NActors::TActorContext& ctx,
        const INodeIndexTabletDatabase::TNode& node,
        ui64 commitId,
        const TString& operation);
    NProto::TError ErrorHandleNotFound(
        const NActors::TActorContext& ctx,
        ui64 handle) const;
    bool CanUseUnconfirmedData() const;
    bool IsTabletConsideredOverloaded() const;

    ui32 ScaleCompactionThreshold(ui32 t) const;
    TCompactionInfo GetCompactionInfo() const;
    TCleanupInfo GetCleanupInfo() const;
    bool ShouldThrottleCleanup(
        const NActors::TActorContext& ctx,
        const TCleanupInfo& cleanupInfo,
        const TBackgroundOpsBackpressureStatus& bpStatus) const;
    EBackgroundOpBackpressureStatus GetBackgroundOpBackpressureStatus(
        ui64 threshold,
        ui64 value) const;
    TBackgroundOpsBackpressureStatus GetBackgroundOpsBackpressureStatus() const;

    void ReplyListNodes(
        const NActors::TActorContext& ctx,
        TTxIndexTablet::TListNodes& args);

    void ReplyListNodesInternal(
        const NActors::TActorContext& ctx,
        TTxIndexTablet::TListNodes& args);

    void CompleteCreateHandle(
        const NActors::TActorContext& ctx,
        TTxIndexTablet::TCreateHandle& args);

    void CompleteDestroyHandle(
        const NActors::TActorContext& ctx,
        TTxIndexTablet::TDestroyHandle& args);

    //
    // Common event handlers.
    //

    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    //
    // Monpage handlers.
    //

    void HandleHttpInfo(
        const NActors::NMon::TEvRemoteHttpInfo::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleHttpInfo_Default(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);
    void RenderHttpInfo_OverviewTab(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        IOutputStream& out);
    void RenderHttpInfo_QuotasTab(IOutputStream& out);
    void HandleHttpInfo_ForceOperation(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);
    void HandleHttpInfo_DumpCompactionRange(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);
    void HandleHttpInfo_DirViewer(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);
    void HandleHttpInfo_Locks(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);
    void HandleHttpInfo_FastShardStatsJson(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_FastShardLayoutJson(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);
    void HandleHttpInfo_FastShardLayout(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);
    void HandleHttpInfo_Diagnostics(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    //
    // Custom event handlers.
    //

    void HandleServerConnected(
        const NKikimr::TEvTabletPipe::TEvServerConnected::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleSessionDisconnected(
        const NKikimr::TEvTabletPipe::TEvServerDisconnected::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleSessionDisconnectedInWork(
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

    void HandleRunRegularTasks(
        const TEvIndexTabletPrivate::TEvRunRegularTasks::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReleaseCollectBarrier(
        const TEvIndexTabletPrivate::TEvReleaseCollectBarrier::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleCancelUnconfirmedData(
        const TEvIndexTabletPrivate::TEvCancelUnconfirmedData::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReadDataCompleted(
        const TEvIndexTabletPrivate::TEvReadDataCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleFastShardInitCompleted(
        const TEvIndexTabletPrivate::TEvFastShardInitCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteDataCompleted(
        const TEvIndexTabletPrivate::TEvWriteDataCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleAddDataCompleted(
        const TEvIndexTabletPrivate::TEvAddDataCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    bool ValidateAddDataRequest(
        NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
        TTxIndexTablet::TAddDataBase& args);

    void HandleForcedRangeOperationProgress(
        const TEvIndexTabletPrivate::TEvForcedRangeOperationProgress::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleLoadNodeRefsRequest(
        const TEvIndexTabletPrivate::TEvLoadNodeRefsRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleLoadNodesRequest(
        const TEvIndexTabletPrivate::TEvLoadNodesRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleNodeCreatedInShard(
        const TEvIndexTabletPrivate::TEvNodeCreatedInShard::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleNodeUnlinkedInShard(
        const TEvIndexTabletPrivate::TEvNodeUnlinkedInShard::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUnlinkDirectoryNodeAbortedInShard(
        const TEvIndexTabletPrivate::TEvUnlinkDirectoryNodeAbortedInShard::TPtr& ev,
        const NActors::TActorContext& ctx);

    NProto::TError HandleCrossShardRenameNodeImpl(
        TRequestInfoPtr& requestInfo,
        NProto::TRenameNodeRequest& record,
        NProto::TProfileLogRequestInfo profileLogRequest,
        const NActors::TActorContext& ctx);

    void HandleDoRenameNodeInDestination(
        const TEvIndexTabletPrivate::TEvDoRenameNodeInDestination::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleDoRenameNode(
        const TEvIndexTabletPrivate::TEvDoRenameNode::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleNodeRenamedInDestination(
        const TEvIndexTabletPrivate::TEvNodeRenamedInDestination::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleResponseLogEntryDeleted(
        const TEvIndexTabletPrivate::TEvResponseLogEntryDeleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleAggregateStatsCompleted(
        const TEvIndexTabletPrivate::TEvAggregateStatsCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleShardRequestCompleted(
        const TEvIndexTabletPrivate::TEvShardRequestCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleLoadCompactionMapChunkResponse(
        const TEvIndexTabletPrivate::TEvLoadCompactionMapChunkResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleEnqueueBlobIndexOpIfNeeded(
        const TEvIndexTabletPrivate::TEvEnqueueBlobIndexOpIfNeeded::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleFakeDescribeData(
        const TEvIndexTablet::TEvDescribeDataRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    std::unique_ptr<TEvIndexTabletPrivate::TEvAddBlobRequest>
    BuildAddBlobRequest(ui64 commitId, const NProto::TUnconfirmedData& entry);

    //
    // Unconfirmed flow.
    //

    void AddBlobForUnconfirmedData(
        const NActors::TActorContext& ctx,
        ui64 commitId,
        const NProto::TUnconfirmedData& entry);
    void ConfirmData(ui64 commitId, const NActors::TActorContext& ctx);
    void ConfirmNextRecoveredData(const NActors::TActorContext& ctx);
    void SendDeferredConfirmAddDataResponse(
        const NActors::TActorContext& ctx,
        TPendingConfirmAddData pending,
        const NProto::TError& error);

    void ConfirmBlobs(const NActors::TActorContext& ctx);
    void HandleConfirmBlobsCompleted(
        const TEvIndexTabletPrivate::TEvConfirmBlobsCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);
    void BlobsConfirmed(const NActors::TActorContext& ctx);

    void DeleteUnconfirmedData(
        const NActors::TActorContext& ctx,
        const char* entityLogTag,
        const TString& entityLogValue,
        const std::function<bool(ui64, const TTrackedUnconfirmedData&)>&
            shouldDelete);

    void DeleteUnconfirmedDataForSession(
        const TString& sessionId,
        const NActors::TActorContext& ctx);

    void DeleteUnconfirmedDataForPipeServer(
        const NActors::TActorId& pipeServerId,
        const NActors::TActorContext& ctx);

    void SendMetricsToExecutor(const NActors::TActorContext& ctx);

    TCPUUsageTimer& AccessCPUUsageTimer();

    //
    // Request handlers.
    //

    bool HandleRequests(STFUNC_SIG);
    bool HandleRequestsByFrozenTablet(STFUNC_SIG);
    bool HandleRequestsByAdapter(STFUNC_SIG);
    bool RejectRequests(STFUNC_SIG);
    bool RejectRequestsByBrokenTablet(STFUNC_SIG);

    bool HandleCompletions(STFUNC_SIG);
    bool IgnoreCompletions(STFUNC_SIG);

    FILESTORE_TABLET_REQUESTS(FILESTORE_IMPLEMENT_REQUEST, TEvIndexTablet)
    FILESTORE_SERVICE_REQUESTS(FILESTORE_IMPLEMENT_REQUEST, TEvService)
    FILESTORE_SERVICE_ADAPTER_REQUESTS(
        FILESTORE_IMPLEMENT_ADAPTER_REQUEST,
        TEvService)
    FILESTORE_TABLET_ADAPTER_REQUESTS(
        FILESTORE_IMPLEMENT_ADAPTER_REQUEST,
        TEvIndexTablet)

    FILESTORE_TABLET_REQUESTS_PRIVATE_SYNC(
        FILESTORE_IMPLEMENT_REQUEST,
        TEvIndexTabletPrivate)
    FILESTORE_TABLET_REQUESTS_PRIVATE_ASYNC(
        FILESTORE_IMPLEMENT_ASYNC_REQUEST,
        TEvIndexTabletPrivate)

    FILESTORE_TABLET_RW_TRANSACTIONS(
        FILESTORE_IMPLEMENT_RW_TRANSACTION,
        TTxIndexTablet);
    FILESTORE_TABLET_INDEX_RO_TRANSACTIONS(
        FILESTORE_IMPLEMENT_RO_TRANSACTION,
        TTxIndexTablet,
        INodeIndexTabletDatabase);

    STFUNC(StateBoot);
    STFUNC(StateInit);
    STFUNC(StateWork);
    STFUNC(StateAdapter);
    STFUNC(StateAdapterInit);
    STFUNC(StateZombie);
    STFUNC(StateBroken);
    STFUNC(StateAdapterBroken);

    void RegisterFileStore(const NActors::TActorContext& ctx);
    void UnregisterFileStore(const NActors::TActorContext& ctx);

    void RegisterRdmaPipe(const NActors::TActorId& pipeServerId);
    void UnregisterRdmaPipe(const NActors::TActorId& pipeServerId);
    void UnregisterRdmaTablet();

    void UpdateLogTag();

    bool HasSpaceLeft(ui64 prevSize, ui64 newSize, ui32 quotaId) const;
    bool HasNodesLeft(ui32 quotaId) const;

    std::unique_ptr<IIndexTabletDatabase> CreateIndexTabletDatabase(
        NKikimr::NTable::TDatabase& database);

    std::unique_ptr<IIndexTabletDatabase> CreateIndexTabletDatabaseProxy(
        NKikimr::NTable::TDatabase& database,
        TVector<IInMemoryIndexState::TIndexStateRequest>& nodeUpdates);
};

}   // namespace NCloud::NFileStore::NStorage
