#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/config.h>
#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/diagnostics/metrics/operations.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>

#include <contrib/ydb/library/actors/core/actorsystem.h>
#include <contrib/ydb/library/actors/core/executor_thread.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NMetrics;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TAggregateStatsActor final
    : public TActorBootstrapped<TAggregateStatsActor>
{
private:
    const TString LogTag;
    const TActorId Tablet;
    const TRequestInfoPtr RequestInfo;
    const NProtoPrivate::TGetStorageStatsRequest Request;
    TString MainFileSystemId;
    const google::protobuf::RepeatedPtrField<TString> ShardIds;
    std::unique_ptr<TEvIndexTablet::TEvGetStorageStatsResponse> Response;
    TVector<TShardStats> ShardStats;
    int Responses = 0;

public:
    TAggregateStatsActor(
        TString logTag,
        TActorId tablet,
        TRequestInfoPtr requestInfo,
        NProtoPrivate::TGetStorageStatsRequest request,
        TString mainFileSystemId,
        google::protobuf::RepeatedPtrField<TString> shardIds,
        std::unique_ptr<TEvIndexTablet::TEvGetStorageStatsResponse> response);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void SendRequests(const TActorContext& ctx);
    void SendRequestToFileSystem(
        const TActorContext& ctx,
        const TString& fileSystemId,
        ui64 cookie);

    void HandleGetStorageStatsResponse(
        const TEvIndexTablet::TEvGetStorageStatsResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error);
};

////////////////////////////////////////////////////////////////////////////////

TAggregateStatsActor::TAggregateStatsActor(
        TString logTag,
        TActorId tablet,
        TRequestInfoPtr requestInfo,
        NProtoPrivate::TGetStorageStatsRequest request,
        TString mainFileSystemId,
        google::protobuf::RepeatedPtrField<TString> shardIds,
        std::unique_ptr<TEvIndexTablet::TEvGetStorageStatsResponse> response)
    : LogTag(std::move(logTag))
    , Tablet(tablet)
    , RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
    , MainFileSystemId(std::move(mainFileSystemId))
    , ShardIds(std::move(shardIds))
    , Response(std::move(response))
    , ShardStats(ShardIds.size())
{}

void TAggregateStatsActor::Bootstrap(const TActorContext& ctx)
{
    SendRequests(ctx);
    Become(&TThis::StateWork);
}

void TAggregateStatsActor::SendRequests(const TActorContext& ctx)
{
    ui64 cookie = 0;
    for (const auto& shardId: ShardIds) {
        SendRequestToFileSystem(ctx, shardId, cookie++);
    }

    if (!MainFileSystemId.empty()) {
        SendRequestToFileSystem(ctx, MainFileSystemId, cookie);
    }
}

void TAggregateStatsActor::SendRequestToFileSystem(
    const TActorContext& ctx,
    const TString& fileSystemId,
    ui64 cookie)
{
    auto request =
        std::make_unique<TEvIndexTablet::TEvGetStorageStatsRequest>();
    request->Record = Request;
    request->Record.SetFileSystemId(fileSystemId);
    request->Record.SetMode(NProtoPrivate::STATS_REQUEST_MODE_GET_ONLY_SELF);

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s Sending GetStorageStatsRequest to filesystem %s",
        LogTag.c_str(),
        fileSystemId.c_str());

    ctx.Send(
        MakeIndexTabletProxyServiceId(),
        request.release(),
        {}, // flags
        cookie);
}

void TAggregateStatsActor::HandleGetStorageStatsResponse(
    const TEvIndexTablet::TEvGetStorageStatsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const int requestsCount =
        ShardIds.size() + (MainFileSystemId.empty() ? 0 : 1);
    Y_ASSERT(requestsCount >= 0);
    TABLET_VERIFY_C(
        ev->Cookie < static_cast<ui64>(requestsCount),
        "ev->Cookie: "
            << ev->Cookie
            << " should be a request number and less than requestsCount: "
            << requestsCount);
    const int shardIndex = static_cast<int>(ev->Cookie);
    const TString& fileSystemId =
        shardIndex < ShardIds.size() ? ShardIds[shardIndex] : MainFileSystemId;

    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::TABLET_WORKER,
            "%s Shard storage stats retrieval failed for %s with error %s",
            LogTag.c_str(),
            fileSystemId.c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    const auto& src = msg->Record.GetStats();
    auto& dst = *Response->Record.MutableStats();

#define FILESTORE_TABLET_MERGE_COUNTER(name, ...)                              \
    dst.Set##name(dst.Get##name() + src.Get##name());                          \
// FILESTORE_TABLET_MERGE_COUNTER

    FILESTORE_TABLET_STATS(FILESTORE_TABLET_MERGE_COUNTER)

#undef FILESTORE_TABLET_MERGE_COUNTER

    dst.SetSevenBytesHandlesCount(
        dst.GetSevenBytesHandlesCount() + src.GetSevenBytesHandlesCount());

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s Got storage stats for filesystem %s, used: %lu, aggregate used: "
        "%lu",
        LogTag.c_str(),
        fileSystemId.c_str(),
        src.GetUsedBlocksCount(),
        dst.GetUsedBlocksCount());

    if (shardIndex < ShardIds.size()) {
        auto& ss = ShardStats[shardIndex];
        ss.CurrentLoad = src.GetCurrentLoad();
        ss.Suffer = src.GetSuffer();
        ss.TotalBlocksCount = src.GetTotalBlocksCount();
        ss.UsedBlocksCount = src.GetUsedBlocksCount();
    }

    if (++Responses == requestsCount) {
        ReplyAndDie(ctx, {});
    }
}

void TAggregateStatsActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

void TAggregateStatsActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (HasError(error)) {
        Response =
            std::make_unique<TEvIndexTablet::TEvGetStorageStatsResponse>(error);
    }
    using TCompletion = TEvIndexTabletPrivate::TEvAggregateStatsCompleted;
    TInstant startedTs;
    NProtoPrivate::TStorageStats statsForTablet = Response->Record.GetStats();
    if (RequestInfo) {
        startedTs = RequestInfo->StartedTs;
        auto* stats = Response->Record.MutableStats();
        for (size_t i = 0; i < ShardStats.size(); ++i) {
            auto* ss = stats->AddShardStats();
            ss->SetShardId(ShardIds[i]);
            ss->SetTotalBlocksCount(ShardStats[i].TotalBlocksCount);
            ss->SetUsedBlocksCount(ShardStats[i].UsedBlocksCount);
            ss->SetCurrentLoad(ShardStats[i].CurrentLoad);
            ss->SetSuffer(ShardStats[i].Suffer);
        }
        NCloud::Reply(ctx, *RequestInfo, std::move(Response));
    }
    auto response = std::make_unique<TCompletion>(
        error,
        std::move(statsForTablet),
        std::move(ShardStats),
        startedTs);
    NCloud::Send(ctx, Tablet, std::move(response));

    Die(ctx);
}

STFUNC(TAggregateStatsActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvIndexTablet::TEvGetStorageStatsResponse,
            HandleGetStorageStatsResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::TABLET_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::UpdateMetrics(
    TInstant now,
    const TDiagnosticsConfig& diagConfig,
    const NProto::TFileSystem& fileSystem,
    const NProto::TFileSystemStats& stats,
    const NProto::TFileStorePerformanceProfile& performanceProfile,
    const TCompactionMapStats& compactionStats,
    const TSessionsStats& sessionsStats,
    const TChannelsStats& channelsStats,
    const TReadAheadCacheStats& readAheadStats,
    const TNodeIndexCacheStats& nodeIndexCacheStats,
    const TNodeToSessionCounters& nodeToSessionCounters,
    const TMiscNodeStats& miscNodeStats,
    const TInMemoryIndexStateStats& inMemoryIndexStateStats,
    const TBlobMetaMapStats& blobMetaMapStats,
    const TIndexTabletState::TBackpressureThresholds& backpressureThresholds,
    const TIndexTabletState::TBackpressureValues& backpressureValues,
    const THandlesStats& handlesStats)
{
    const ui32 blockSize = fileSystem.GetBlockSize();

    Store(Metrics.TotalBytesCount, fileSystem.GetBlocksCount() * blockSize);
    Store(Metrics.UsedBytesCount, stats.GetUsedBlocksCount() * blockSize);

    Store(Metrics.TotalNodesCount, fileSystem.GetNodesCount());
    Store(Metrics.UsedNodesCount, stats.GetUsedNodesCount());

    Store(Metrics.UsedSessionsCount, stats.GetUsedSessionsCount());
    Store(Metrics.UsedHandlesCount, stats.GetUsedHandlesCount());
    Store(Metrics.UsedDirectHandlesCount, handlesStats.UsedDirectHandlesCount);
    Store(Metrics.SevenBytesHandlesCount, handlesStats.SevenBytesHandlesCount);
    Store(Metrics.UsedLocksCount, stats.GetUsedLocksCount());

    Store(
        Metrics.StrictFileSystemSizeEnforcementEnabled,
        fileSystem.GetStrictFileSystemSizeEnforcementEnabled());
    Store(
        Metrics.DirectoryCreationInShardsEnabled,
        fileSystem.GetDirectoryCreationInShardsEnabled());

    Store(Metrics.FreshBytesCount, stats.GetFreshBytesCount());
    Store(Metrics.FreshBytesItemCount, stats.GetFreshBytesItemCount());
    Store(Metrics.DeletedFreshBytesCount, stats.GetDeletedFreshBytesCount());
    Store(Metrics.MixedBytesCount, stats.GetMixedBlocksCount() * blockSize);
    Store(Metrics.MixedBlobsCount, stats.GetMixedBlobsCount());
    Store(Metrics.DeletionMarkersCount, stats.GetDeletionMarkersCount());
    Store(
        Metrics.LargeDeletionMarkersCount,
        stats.GetLargeDeletionMarkersCount());
    Store(Metrics.GarbageQueueSize, stats.GetGarbageQueueSize());
    Store(Metrics.GarbageBytesCount, stats.GetGarbageBlocksCount() * blockSize);
    Store(Metrics.FreshBlocksCount, stats.GetFreshBlocksCount());
    Store(Metrics.CMMixedBlobsCount, compactionStats.TotalBlobsCount);
    Store(Metrics.CMDeletionMarkersCount, compactionStats.TotalDeletionsCount);
    Store(
        Metrics.CMGarbageBlocksCount,
        compactionStats.TotalGarbageBlocksCount);

    TString backpressureReason;
    Store(
        Metrics.IsWriteAllowed,
        TIndexTabletActor::IsWriteAllowed(
            backpressureThresholds,
            backpressureValues,
            &backpressureReason));

    Store(Metrics.FlushBackpressureValue, backpressureValues.Flush);
    Store(Metrics.FlushBackpressureThreshold, backpressureThresholds.Flush);
    Store(Metrics.FlushBytesBackpressureValue, backpressureValues.FlushBytes);
    Store(
        Metrics.FlushBytesBackpressureThreshold,
        backpressureThresholds.FlushBytes);
    Store(
        Metrics.CompactionBackpressureValue,
        backpressureValues.CompactionScore);
    Store(
        Metrics.CompactionBackpressureThreshold,
        backpressureThresholds.CompactionScore);
    Store(Metrics.CleanupBackpressureValue, backpressureValues.CleanupScore);
    Store(
        Metrics.CleanupBackpressureThreshold,
        backpressureThresholds.CleanupScore);

    Store(Metrics.MaxReadIops, performanceProfile.GetMaxReadIops());
    Store(Metrics.MaxWriteIops, performanceProfile.GetMaxWriteIops());
    Store(Metrics.MaxReadBandwidth, performanceProfile.GetMaxReadBandwidth());
    Store(Metrics.MaxWriteBandwidth, performanceProfile.GetMaxWriteBandwidth());

    Store(
        Metrics.AllocatedCompactionRangesCount,
        compactionStats.AllocatedRangesCount);
    Store(Metrics.UsedCompactionRangesCount, compactionStats.UsedRangesCount);

    if (compactionStats.TopRangesByCompactionScore.empty()) {
        Store(Metrics.MaxBlobsInRange, 0);
    } else {
        Store(
            Metrics.MaxBlobsInRange,
            compactionStats.TopRangesByCompactionScore.front()
                .Stats.BlobsCount);
    }
    if (compactionStats.TopRangesByCleanupScore.empty()) {
        Store(Metrics.MaxDeletionsInRange, 0);
    } else {
        Store(
            Metrics.MaxDeletionsInRange,
            compactionStats.TopRangesByCleanupScore.front()
                .Stats.DeletionsCount);
    }
    if (compactionStats.TopRangesByGarbageScore.empty()) {
        Store(Metrics.MaxGarbageBlocksInRange, 0);
    } else {
        Store(
            Metrics.MaxGarbageBlocksInRange,
            compactionStats.TopRangesByGarbageScore.front()
                .Stats.GarbageBlocksCount);
    }

    Store(Metrics.StatefulSessionsCount, sessionsStats.StatefulSessionsCount);
    Store(Metrics.StatelessSessionsCount, sessionsStats.StatelessSessionsCount);
    Store(Metrics.ActiveSessionsCount, sessionsStats.ActiveSessionsCount);
    Store(Metrics.OrphanSessionsCount, sessionsStats.OrphanSessionsCount);
    Store(Metrics.WritableChannelCount, channelsStats.WritableChannelCount);
    Store(Metrics.UnwritableChannelCount, channelsStats.UnwritableChannelCount);
    Store(Metrics.ChannelsToMoveCount, channelsStats.ChannelsToMoveCount);
    Store(Metrics.ReadAheadCacheNodeCount, readAheadStats.NodeCount);
    Store(Metrics.NodeIndexCacheNodeCount, nodeIndexCacheStats.NodeCount);

    Store(
        Metrics.InMemoryIndexStateNodesCount,
        inMemoryIndexStateStats.NodesCount);
    Store(
        Metrics.InMemoryIndexStateNodesCapacity,
        inMemoryIndexStateStats.NodesCapacity);
    Store(
        Metrics.InMemoryIndexStateNodeRefsCount,
        inMemoryIndexStateStats.NodeRefsCount);
    Store(
        Metrics.InMemoryIndexStateNodeRefsCapacity,
        inMemoryIndexStateStats.NodeRefsCapacity);
    Store(
        Metrics.InMemoryIndexStateNodeAttrsCount,
        inMemoryIndexStateStats.NodeAttrsCount);
    Store(
        Metrics.InMemoryIndexStateNodeAttrsCapacity,
        inMemoryIndexStateStats.NodeAttrsCapacity);
    Store(
        Metrics.InMemoryIndexStateNodeRefsExhaustivenessCount,
        inMemoryIndexStateStats.NodeRefsExhaustivenessCount);
    Store(
        Metrics.InMemoryIndexStateNodeRefsExhaustivenessCapacity,
        inMemoryIndexStateStats.NodeRefsExhaustivenessCapacity);
    Store(
        Metrics.InMemoryIndexStateIsExhaustive,
        inMemoryIndexStateStats.IsNodeRefsExhaustive);

    Store(Metrics.MixedIndexLoadedRanges, blobMetaMapStats.LoadedRanges);
    Store(Metrics.MixedIndexOffloadedRanges, blobMetaMapStats.OffloadedRanges);

    Store(
        Metrics.NodesOpenForWritingBySingleSession,
        nodeToSessionCounters.NodesOpenForWritingBySingleSession);
    Store(
        Metrics.NodesOpenForWritingByMultipleSessions,
        nodeToSessionCounters.NodesOpenForWritingByMultipleSessions);
    Store(
        Metrics.NodesOpenForReadingBySingleSession,
        nodeToSessionCounters.NodesOpenForReadingBySingleSession);
    Store(
        Metrics.NodesOpenForReadingByMultipleSessions,
        nodeToSessionCounters.NodesOpenForReadingByMultipleSessions);

    Store(Metrics.OrphanNodesCount, miscNodeStats.OrphanNodesCount);

    Metrics.BusyIdleCalc.OnUpdateStats();
    Metrics.UpdatePerformanceMetrics(now, diagConfig, fileSystem);

    Metrics.ReadBlob.UpdatePrev(now);
    Metrics.WriteBlob.UpdatePrev(now);
    Metrics.PatchBlob.UpdatePrev(now);

    Metrics.DescribeData.UpdatePrev(now);
    Metrics.GenerateBlobIds.UpdatePrev(now);
    Metrics.AddData.UpdatePrev(now);
    Metrics.GetStorageStats.UpdatePrev(now);
    Metrics.GetNodeAttrBatch.UpdatePrev(now);
    Metrics.RenameNodeInDestination.UpdatePrev(now);
    Metrics.PrepareUnlinkDirectoryNodeInShard.UpdatePrev(now);
    Metrics.AbortUnlinkDirectoryNodeInShard.UpdatePrev(now);

    Metrics.ReadData.UpdatePrev(now);
    Metrics.WriteData.UpdatePrev(now);
    Metrics.ListNodes.UpdatePrev(now);
    Metrics.GetNodeAttr.UpdatePrev(now);
    Metrics.CreateHandle.UpdatePrev(now);
    Metrics.DestroyHandle.UpdatePrev(now);
    Metrics.CreateNode.UpdatePrev(now);
    Metrics.RenameNode.UpdatePrev(now);
    Metrics.UnlinkNode.UpdatePrev(now);
    Metrics.StatFileStore.UpdatePrev(now);
    Metrics.GetNodeXAttr.UpdatePrev(now);

    Metrics.Cleanup.UpdatePrev(now);
    Metrics.Flush.UpdatePrev(now);
    Metrics.FlushBytes.UpdatePrev(now);
    Metrics.TrimBytes.UpdatePrev(now);
    Metrics.CollectGarbage.UpdatePrev(now);
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::RegisterCounters(const TActorContext& ctx)
{
    if (!Counters) {
        auto counters = CreateIndexTabletCounters();

        // LAME: ownership transferred to executor
        Counters = counters.release();
        Executor()->RegisterExternalTabletCounters(Counters);

        // only aggregated statistics will be reported by default
        // (you can always turn on per-tablet statistics on monitoring page)
        // TabletCountersAddTablet(TabletID(), ctx);

        ScheduleUpdateCounters(ctx);
    }
}

void TIndexTabletActor::RegisterStatCounters(TInstant now)
{
    const auto& fsId = GetFileSystemId();
    if (!fsId) {
        // it's possible to have empty id for newly created volume
        // just wait for the config update
        return;
    }

    const auto& fs = GetFileSystem();
    const auto storageMediaKind = GetStorageMediaKind(fs);
    TABLET_VERIFY(!storageMediaKind.empty());

    // Update should be called before Register, because we want to write
    // correct values to solomon. If we reorder these two actions, we can
    // aggregate zero values, in the middle of the registration (or right after
    // registration, before update).
    UpdateMetrics(
        now,
        *DiagConfig,
        fs,
        GetFileSystemStats(),
        GetPerformanceProfile(),
        GetCompactionMapStats(1),
        CalculateSessionsStats(),
        CalculateChannelsStats(),
        CalculateReadAheadCacheStats(),
        CalculateNodeIndexCacheStats(),
        GetNodeToSessionCounters(),
        GetMiscNodeStats(),
        GetInMemoryIndexStateStats(),
        GetBlobMetaMapStats(),
        BuildBackpressureThresholds(),
        GetBackpressureValues(),
        GetHandlesStats());

    // TabletStartTimestamp is intialised once per tablet lifetime and thus it is
    // acceptable to set it in RegisterStatCounters if it is not set yet.
    i64 expected = 0;
    Metrics.TabletStartTimestamp.compare_exchange_strong(
        expected,
        now.MicroSeconds());

    Metrics.Register(fsId, fs.GetCloudId(), fs.GetFolderId(), storageMediaKind);
}

void TIndexTabletActor::ScheduleUpdateCounters(const TActorContext& ctx)
{
    if (!UpdateCountersScheduled) {
        ctx.Schedule(UpdateCountersInterval,
            new TEvIndexTabletPrivate::TEvUpdateCounters());
        UpdateCountersScheduled = true;
    }

    if (!UpdateLeakyBucketCountersScheduled) {
        ctx.Schedule(UpdateLeakyBucketCountersInterval,
            new TEvIndexTabletPrivate::TEvUpdateLeakyBucketCounters());
        UpdateLeakyBucketCountersScheduled = true;
    }
}

void TIndexTabletActor::SendMetricsToExecutor(const TActorContext& ctx)
{
    auto* resourceMetrics = Executor()->GetResourceMetrics();
    resourceMetrics->Network.Increment(
        Metrics.CalculateNetworkRequestBytes(
            Config->GetNonNetworkMetricsBalancingFactor()),
        ctx.Now());
    resourceMetrics->TryUpdate(ctx);
}

void TIndexTabletActor::CalculateActorCPUUsage(const TActorContext& ctx)
{
    TExecutorPoolStats poolStats;
    TVector<TExecutorThreadStats> threadStats;
    auto& actorSystem = *ctx.ExecutorThread.ActorSystem;
    const ui32 poolId = ctx.SelfID.PoolID();
    actorSystem.GetPoolStats(poolId, poolStats, threadStats);
    i64 ticks = 0;
    for (ui64 i = 0; i < threadStats.size(); ++i) {
        ticks += threadStats[i].ElapsedTicksByActivity[GetActivityType()];
    }

    const i64 prevUsageMicros =
        Metrics.CPUUsageMicros.load(std::memory_order_relaxed);
    const i64 curUsageMicros = ::NHPTimer::GetSeconds(ticks) * 1'000'000;
    const TInstant ts = ctx.Now();
    if (Metrics.PrevCPUUsageMicrosTs) {
        const auto timeDiff = ts - Metrics.PrevCPUUsageMicrosTs;
        if (timeDiff) {
            const double usageDiff = curUsageMicros - prevUsageMicros;
            Metrics.CPUUsageRate = 100 * (usageDiff / timeDiff.MicroSeconds());
        }
    }

    Metrics.PrevCPUUsageMicrosTs = ts;
    Store(Metrics.CPUUsageMicros, curUsageMicros);
}

void TIndexTabletActor::HandleUpdateCounters(
    const TEvIndexTabletPrivate::TEvUpdateCounters::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    UpdateMetrics(
        ctx.Now(),
        *DiagConfig,
        GetFileSystem(),
        GetFileSystemStats(),
        GetPerformanceProfile(),
        GetCompactionMapStats(1),
        CalculateSessionsStats(),
        CalculateChannelsStats(),
        CalculateReadAheadCacheStats(),
        CalculateNodeIndexCacheStats(),
        GetNodeToSessionCounters(),
        GetMiscNodeStats(),
        GetInMemoryIndexStateStats(),
        GetBlobMetaMapStats(),
        BuildBackpressureThresholds(),
        GetBackpressureValues(),
        GetHandlesStats());
    CalculateActorCPUUsage(ctx);
    SendMetricsToExecutor(ctx);

    UpdateCountersScheduled = false;
    ScheduleUpdateCounters(ctx);

    if (CachedStatsFetchingStartTs != TInstant::Zero()) {
        const auto delay = ctx.Now() - CachedStatsFetchingStartTs;
        const auto maxDelay = TDuration::Minutes(15);
        if (delay > maxDelay) {
            ReportShardStatsRetrievalTimeout();
            CachedStatsFetchingStartTs = TInstant::Zero();
        }
    }

    if (CachedStatsFetchingStartTs == TInstant::Zero()) {
        auto response =
            std::make_unique<TEvIndexTablet::TEvGetStorageStatsResponse>();
        auto* stats = response->Record.MutableStats();
        const auto& shardIds = GetFileSystem().GetShardFileSystemIds();
        // If shardIds isn't empty and the current tablet is a shard, it will
        // collect self stats via TAggregateStatsActor
        if (shardIds.empty() || IsMainTablet()) {
            FillSelfStorageStats(stats);
        }
        if (shardIds.empty()) {
            CachedAggregateStats = std::move(*stats);
            Store(
                Metrics.AggregateUsedBytesCount,
                CachedAggregateStats.GetUsedBlocksCount() * GetBlockSize());
            Store(
                Metrics.AggregateUsedNodesCount,
                CachedAggregateStats.GetUsedNodesCount());

            return;
        }

        auto actor = std::make_unique<TAggregateStatsActor>(
            LogTag,
            SelfId(),
            TRequestInfoPtr(),
            NProtoPrivate::TGetStorageStatsRequest(),
            !IsMainTablet() ? GetFileSystem().GetMainFileSystemId() : TString(),
            shardIds,
            std::move(response));

        auto actorId = NCloud::Register(ctx, std::move(actor));
        WorkerActors.insert(actorId);
        CachedStatsFetchingStartTs = ctx.Now();
    }
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::FillSelfStorageStats(
    NProtoPrivate::TStorageStats* stats)
{
#define FILESTORE_TABLET_UPDATE_COUNTER(name, ...)                             \
    stats->Set##name(Get##name());                                             \
// FILESTORE_TABLET_UPDATE_COUNTER

    FILESTORE_TABLET_STATS(FILESTORE_TABLET_UPDATE_COUNTER)

#undef FILESTORE_TABLET_UPDATE_COUNTER

    stats->SetTabletChannelCount(GetTabletChannelCount());
    stats->SetConfigChannelCount(GetConfigChannelCount());

    const auto txDeleteGarbageRwCompleted = Counters->TxCumulative(
        TIndexTabletCounters::ETransactionType::TX_DeleteGarbage,
        NKikimr::COUNTER_TT_RW_COMPLETED
    ).Get();
    stats->SetTxDeleteGarbageRwCompleted(txDeleteGarbageRwCompleted);

    auto cmStats = GetCompactionMapStats(0);
    stats->SetUsedCompactionRanges(cmStats.UsedRangesCount);
    stats->SetAllocatedCompactionRanges(cmStats.AllocatedRangesCount);

    stats->SetFlushState(static_cast<ui32>(FlushState.GetOperationState()));
    stats->SetBlobIndexOpState(static_cast<ui32>(
        BlobIndexOpState.GetOperationState()));
    stats->SetCollectGarbageState(static_cast<ui32>(
        CollectGarbageState.GetOperationState()));

    stats->SetCurrentLoad(Metrics.CurrentLoad.load(std::memory_order_relaxed));
    stats->SetSuffer(Metrics.Suffer.load(std::memory_order_relaxed));

    stats->SetTotalBlocksCount(GetFileSystem().GetBlocksCount());

    stats->SetFreshBytesItemCount(GetFreshBytesItemCount());

    stats->SetSevenBytesHandlesCount(Metrics.SevenBytesHandlesCount);
}

void TIndexTabletActor::HandleGetStorageStats(
    const TEvIndexTablet::TEvGetStorageStatsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto response =
        std::make_unique<TEvIndexTablet::TEvGetStorageStatsResponse>();
    response->Record.SetMediaKind(GetFileSystem().GetStorageMediaKind());
    auto& req = ev->Get()->Record;
    auto* stats = response->Record.MutableStats();

    // shards shouldn't collect other shards' stats (unless it's background
    // shard <-> shard stats exchange which is handled in HandleUpdateCounters
    // or it's required by STATS_REQUEST_MODE_FORCE_FETCH_SHARDS)
    const bool pollShards =
        req.GetMode() == NProtoPrivate::STATS_REQUEST_MODE_FORCE_FETCH_SHARDS ||
        (req.GetMode() != NProtoPrivate::STATS_REQUEST_MODE_GET_ONLY_SELF &&
         IsMainTablet());
    const auto& shardIds = pollShards
        ? GetFileSystem().GetShardFileSystemIds()
        : Default<google::protobuf::RepeatedPtrField<TString>>();

    if (req.GetAllowCache()) {
        *stats = CachedAggregateStats;
        const ui32 shardMetricsCount =
            Min<ui32>(shardIds.size(), CachedShardStats.size());
        for (ui32 i = 0; i < shardMetricsCount; ++i) {
            auto* ss = stats->AddShardStats();
            ss->SetShardId(shardIds[i]);
            ss->SetTotalBlocksCount(CachedShardStats[i].TotalBlocksCount);
            ss->SetUsedBlocksCount(CachedShardStats[i].UsedBlocksCount);
            ss->SetCurrentLoad(CachedShardStats[i].CurrentLoad);
            ss->SetSuffer(CachedShardStats[i].Suffer);
        }
    } else {
        FillSelfStorageStats(stats);
    }

    TVector<TCompactionRangeInfo> topRanges;

    if (req.GetCompactionRangeCountByCompactionScore()) {
        const auto r = GetTopRangesByCompactionScore(
            req.GetCompactionRangeCountByCompactionScore());
        topRanges.insert(topRanges.end(), r.begin(), r.end());
    }

    if (req.GetCompactionRangeCountByCleanupScore()) {
        const auto r = GetTopRangesByCleanupScore(
            req.GetCompactionRangeCountByCleanupScore());
        topRanges.insert(topRanges.end(), r.begin(), r.end());
    }

    if (req.GetCompactionRangeCountByGarbageScore()) {
        const auto r = GetTopRangesByGarbageScore(
            req.GetCompactionRangeCountByGarbageScore());
        topRanges.insert(topRanges.end(), r.begin(), r.end());
    }

    for (const auto& r: topRanges) {
        auto* out = stats->AddCompactionRangeStats();
        out->SetRangeId(r.RangeId);
        out->SetBlobCount(r.Stats.BlobsCount);
        out->SetDeletionCount(r.Stats.DeletionsCount);
        out->SetGarbageBlockCount(r.Stats.GarbageBlocksCount);
    }

    if (req.GetAllowCache() || shardIds.empty()) {
        Metrics.StatFileStore.Update(1, 0, TDuration::Zero());
        Metrics.GetStorageStats.Update(1, 0, TDuration::Zero());
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        ev->Get()->CallContext);
    requestInfo->StartedTs = ctx.Now();

    if (!IsMainTablet()) {
        // If the current tablet is a shard, it will collect self stats via
        // TAggregateStatsActor
        #define FILESTORE_TABLET_CLEAR_COUNTER(name, ...)                      \
            stats->Clear##name();                                              \
        // FILESTORE_TABLET_MERGE_COUNTER
        FILESTORE_TABLET_STATS(FILESTORE_TABLET_CLEAR_COUNTER)
        #undef FILESTORE_TABLET_CLEAR_COUNTER
    }

    auto actor = std::make_unique<TAggregateStatsActor>(
        LogTag,
        SelfId(),
        std::move(requestInfo),
        std::move(req),
        !IsMainTablet() ? GetFileSystem().GetMainFileSystemId() : TString(),
        shardIds,
        std::move(response));

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleAggregateStatsCompleted(
    const TEvIndexTabletPrivate::TEvAggregateStatsCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    const bool isBackgroundRequest = msg->StartedTs.GetValue() == 0;
    if (isBackgroundRequest) {
        LOG_DEBUG(
            ctx,
            TFileStoreComponents::TABLET_WORKER,
            "%s Background shard stats fetch completed in %s, ShardsCount: %lu",
            LogTag.c_str(),
            (ctx.Now() - CachedStatsFetchingStartTs).ToString().c_str(),
            msg->ShardStats.size());
        CachedStatsFetchingStartTs = TInstant::Zero();
    }
    if (!HasError(msg->Error)) {
        if (!isBackgroundRequest) {
            Metrics.StatFileStore.Update(1, 0, ctx.Now() - msg->StartedTs);
        }
        Metrics.GetStorageStats.Update(1, 0, ctx.Now() - msg->StartedTs);
        CachedAggregateStats = std::move(msg->AggregateStats);
        CachedShardStats = std::move(msg->ShardStats);
        UpdateShardBalancer(CachedShardStats);

        Store(
            Metrics.AggregateUsedBytesCount,
            CachedAggregateStats.GetUsedBlocksCount() * GetBlockSize());
        Store(
            Metrics.AggregateUsedNodesCount,
            CachedAggregateStats.GetUsedNodesCount());
    }
    WorkerActors.erase(ev->Sender);
}

}   // namespace NCloud::NFileStore::NStorage
