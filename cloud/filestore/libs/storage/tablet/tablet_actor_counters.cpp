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

void MergeQuotaUsages(
    NProtoPrivate::TStorageStats& dst,
    const NProtoPrivate::TStorageStats& src)
{
    for (const auto& [quotaId, srcUsage]: src.GetQuotaUsages()) {
        auto& dstUsage = (*dst.MutableQuotaUsages())[quotaId];
        dstUsage.SetUsedBytes(dstUsage.GetUsedBytes() + srcUsage.GetUsedBytes());
        dstUsage.SetUsedNodes(dstUsage.GetUsedNodes() + srcUsage.GetUsedNodes());
    }
}

////////////////////////////////////////////////////////////////////////////////
// TAggregateStatsActor always replies with
// TAggregateStatsCompleted to a TIndexTabletActor that created the actor.
// 1. IsBackgroundRequest. It means the actor was created in
//    TIndexTabletActor::HandleUpdateCounters.
//    If MainFileSystemId is not empty and
//    FanoutStatsCollectionInShardsDisabled, the actor requests cached
//    aggregated statistics from the main tablet. Otherwise it sends requests to
//    all shards.
// 2. !IsBackgroundRequest. It means that TAggregateStatsActor is created in
//    TIndexTabletActor::HandleGetStorageStats.
//    In this case the actor always sends requests to all shards and replies to
//    the original sender of the GetStorageStats request.

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
    ui64 RemainingResponses = 0;
    const bool IsBackgroundRequest;
    const bool FanoutStatsCollectionInShardsDisabled;

public:
    TAggregateStatsActor(
        TString logTag,
        TActorId tablet,
        TRequestInfoPtr requestInfo,
        NProtoPrivate::TGetStorageStatsRequest request,
        TString mainFileSystemId,
        google::protobuf::RepeatedPtrField<TString> shardIds,
        std::unique_ptr<TEvIndexTablet::TEvGetStorageStatsResponse> response,
        bool isBackgroundRequest,
        bool fanoutStatsCollectionInShardsDisabled);

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

    const TString& GetFileSystemId(ui64 cookie) const;

    bool ShouldOnlyGetStatsFromMainTablet() const
    {
        return IsBackgroundRequest && MainFileSystemId &&
               FanoutStatsCollectionInShardsDisabled;
    }
};

////////////////////////////////////////////////////////////////////////////////

TAggregateStatsActor::TAggregateStatsActor(
    TString logTag,
    TActorId tablet,
    TRequestInfoPtr requestInfo,
    NProtoPrivate::TGetStorageStatsRequest request,
    TString mainFileSystemId,
    google::protobuf::RepeatedPtrField<TString> shardIds,
    std::unique_ptr<TEvIndexTablet::TEvGetStorageStatsResponse> response,
    bool isBackgroundRequest,
    bool fanoutStatsCollectionInShardsDisabled)
    : LogTag(std::move(logTag))
    , Tablet(tablet)
    , RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
    , MainFileSystemId(std::move(mainFileSystemId))
    , ShardIds(std::move(shardIds))
    , Response(std::move(response))
    , IsBackgroundRequest(isBackgroundRequest)
    , FanoutStatsCollectionInShardsDisabled(
          fanoutStatsCollectionInShardsDisabled)
{
    auto& dst = *Response->Record.MutableStats();
    auto& shardStats = *dst.MutableShardStats();
    shardStats.Clear();
    shardStats.Reserve(ShardIds.size());
    for (const auto& shardId: ShardIds) {
        shardStats.Add()->SetShardId(shardId);
    }
}

void TAggregateStatsActor::Bootstrap(const TActorContext& ctx)
{
    SendRequests(ctx);
    Become(&TThis::StateWork);
}

void TAggregateStatsActor::SendRequests(const TActorContext& ctx)
{
    if (!ShouldOnlyGetStatsFromMainTablet()) {
        ui64 cookie = 0;
        for (const auto& shardId: ShardIds) {
            SendRequestToFileSystem(ctx, shardId, cookie++);
        }
    }

    if (MainFileSystemId) {
        SendRequestToFileSystem(ctx, MainFileSystemId, ShardIds.size());
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
    if (ShouldOnlyGetStatsFromMainTablet()) {
        // Get cached statistics even it's 'inifinetely' old.
        request->Record.SetCacheTTL(TDuration::Max().MilliSeconds());
    } else {
        request->Record.SetMode(
            NProtoPrivate::STATS_REQUEST_MODE_GET_ONLY_SELF);
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s Sending GetStorageStatsRequest to filesystem %s",
        LogTag.c_str(),
        fileSystemId.c_str());

    RemainingResponses++;
    ctx.Send(
        MakeIndexTabletProxyServiceId(),
        request.release(),
        {}, // flags
        cookie);
}

const TString& TAggregateStatsActor::GetFileSystemId(ui64 cookie) const
{
    TABLET_VERIFY_C(
        cookie <= static_cast<ui64>(ShardIds.size()),
        "ev->Cookie: " << cookie
                       << " should be a shard number or shard count for the "
                          "main tablet. Shard count: "
                       << ShardIds.size());

    const int shardIndex = static_cast<int>(cookie);
    return shardIndex < ShardIds.size() ? ShardIds[shardIndex]
                                        : MainFileSystemId;
}

void TAggregateStatsActor::HandleGetStorageStatsResponse(
    const TEvIndexTablet::TEvGetStorageStatsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const int shardIndex = static_cast<int>(ev->Cookie);
    const TString& fileSystemId = GetFileSystemId(ev->Cookie);

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

    if (ShouldOnlyGetStatsFromMainTablet()) {
        Response->Record = msg->Record;
        ReplyAndDie(ctx, {});
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

    MergeQuotaUsages(dst, src);

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s Got storage stats for filesystem %s, used blocks: %lu, nodes: %lu"
        ", aggregate used blocks: %lu, nodes: %lu",
        LogTag.c_str(),
        fileSystemId.c_str(),
        src.GetUsedBlocksCount(),
        src.GetUsedNodesCount(),
        dst.GetUsedBlocksCount(),
        dst.GetUsedNodesCount());

    if (shardIndex < ShardIds.size()) {
        auto& ss = *dst.MutableShardStats(shardIndex);
        ss.SetCurrentLoad(src.GetCurrentLoad());
        ss.SetSuffer(src.GetSuffer());
        ss.SetTotalBlocksCount(src.GetTotalBlocksCount());
        ss.SetUsedBlocksCount(src.GetUsedBlocksCount());
        ss.SetUsedNodesCount(src.GetUsedNodesCount());
    }

    if (--RemainingResponses == 0) {
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
        NCloud::Reply(ctx, *RequestInfo, std::move(Response));
    }

    auto response = std::make_unique<TCompletion>(
        error,
        std::move(statsForTablet),
        startedTs,
        IsBackgroundRequest);
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

////////////////////////////////////////////////////////////////////////////////

TString DescribeShardList(const TVector<TShardStats>& shards)
{
    TStringBuilder sb;
    for (ui32 i = 0; i < shards.size(); ++i) {
        if (i) {
            sb << ", ";
        }

        const auto& s = shards[i];
        sb << "id=" << s.ShardId
            << " blocks=" << s.UsedBlocksCount
            << "/" << s.TotalBlocksCount
            << " nodes=" << s.UsedNodesCount
            << " load=" << s.CurrentLoad
            << " suffer=" << s.Suffer;
    }
    return sb;
}

void FillShardStats(
    const NProtoPrivate::TStorageStats& storageStats,
    TVector<TShardStats>& shardStats)
{
    shardStats.reserve(storageStats.ShardStatsSize());
    for (const auto& srcShardStats: storageStats.GetShardStats()) {
        shardStats.emplace_back(
            srcShardStats.GetShardId(),
            srcShardStats.GetTotalBlocksCount(),
            srcShardStats.GetUsedBlocksCount(),
            srcShardStats.GetUsedNodesCount(),
            srcShardStats.GetCurrentLoad(),
            srcShardStats.GetSuffer());
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
    const TNodeToSessionCounters& nodeToSessionCounters,
    const TMiscNodeStats& miscNodeStats,
    const TInMemoryIndexStateStats& inMemoryIndexStateStats,
    const TBlobMetaMapStats& blobMetaMapStats,
    const TIndexTabletState::TBackpressureThresholds& backpressureThresholds,
    const TIndexTabletState::TBackpressureValues& backpressureValues,
    const THandlesStats& handlesStats)
{
    const ui32 blockSize = fileSystem.GetBlockSize();

    Store(Metrics->TotalBytesCount, fileSystem.GetBlocksCount() * blockSize);
    Store(Metrics->UsedBytesCount, stats.GetUsedBlocksCount() * blockSize);

    Store(Metrics->TotalNodesCount, fileSystem.GetNodesCount());
    Store(Metrics->UsedNodesCount, stats.GetUsedNodesCount());

    Store(Metrics->UsedSessionsCount, stats.GetUsedSessionsCount());
    Store(Metrics->UsedHandlesCount, stats.GetUsedHandlesCount());
    Store(Metrics->UsedDirectHandlesCount, handlesStats.UsedDirectHandlesCount);
    Store(Metrics->SevenBytesHandlesCount, handlesStats.SevenBytesHandlesCount);
    Store(Metrics->UsedLocksCount, stats.GetUsedLocksCount());

    Store(
        Metrics->StrictFileSystemSizeEnforcementEnabled,
        fileSystem.GetStrictFileSystemSizeEnforcementEnabled());
    Store(
        Metrics->DirectoryCreationInShardsEnabled,
        fileSystem.GetDirectoryCreationInShardsEnabled());

    Store(Metrics->FreshBytesCount, stats.GetFreshBytesCount());
    Store(Metrics->FreshBytesItemCount, stats.GetFreshBytesItemCount());
    Store(Metrics->DeletedFreshBytesCount, stats.GetDeletedFreshBytesCount());
    Store(Metrics->MixedBytesCount, stats.GetMixedBlocksCount() * blockSize);
    Store(Metrics->MixedBlobsCount, stats.GetMixedBlobsCount());
    Store(Metrics->DeletionMarkersCount, stats.GetDeletionMarkersCount());
    Store(
        Metrics->LargeDeletionMarkersCount,
        stats.GetLargeDeletionMarkersCount());
    Store(Metrics->GarbageQueueSize, stats.GetGarbageQueueSize());
    Store(
        Metrics->GarbageBytesCount,
        stats.GetGarbageBlocksCount() * blockSize);
    Store(Metrics->FreshBlocksCount, stats.GetFreshBlocksCount());
    Store(Metrics->CMMixedBlobsCount, compactionStats.TotalBlobsCount);
    Store(Metrics->CMDeletionMarkersCount, compactionStats.TotalDeletionsCount);
    Store(
        Metrics->CMGarbageBlocksCount,
        compactionStats.TotalGarbageBlocksCount);
    Store(Metrics->CollectCommitId, GetCollectCommitId());

    TString backpressureReason;
    Store(
        Metrics->IsWriteAllowed,
        TIndexTabletActor::IsWriteAllowed(
            backpressureThresholds,
            backpressureValues,
            &backpressureReason));

    Store(Metrics->FlushBackpressureValue, backpressureValues.Flush);
    Store(Metrics->FlushBackpressureThreshold, backpressureThresholds.Flush);
    Store(Metrics->FlushBytesBackpressureValue, backpressureValues.FlushBytes);
    Store(
        Metrics->FlushBytesBackpressureThreshold,
        backpressureThresholds.FlushBytes);
    Store(
        Metrics->FlushBytesItemCountBackpressureValue,
        backpressureValues.FlushBytesItemCount);
    Store(
        Metrics->FlushBytesItemCountBackpressureThreshold,
        backpressureThresholds.FlushBytesItemCount);
    Store(
        Metrics->CompactionBackpressureValue,
        backpressureValues.CompactionScore);
    Store(
        Metrics->CompactionBackpressureThreshold,
        backpressureThresholds.CompactionScore);
    Store(Metrics->CleanupBackpressureValue, backpressureValues.CleanupScore);
    Store(
        Metrics->CleanupBackpressureThreshold,
        backpressureThresholds.CleanupScore);
    Store(
        Metrics->CollectGarbageBackpressureValue,
        backpressureValues.CollectGarbage);
    Store(
        Metrics->CollectGarbageBackpressureThreshold,
        backpressureThresholds.CollectGarbage);

    Store(Metrics->MaxReadIops, performanceProfile.GetMaxReadIops());
    Store(Metrics->MaxWriteIops, performanceProfile.GetMaxWriteIops());
    Store(Metrics->MaxReadBandwidth, performanceProfile.GetMaxReadBandwidth());
    Store(
        Metrics->MaxWriteBandwidth,
        performanceProfile.GetMaxWriteBandwidth());

    Store(
        Metrics->AllocatedCompactionRangesCount,
        compactionStats.AllocatedRangesCount);
    Store(Metrics->UsedCompactionRangesCount, compactionStats.UsedRangesCount);

    if (compactionStats.TopRangesByCompactionScore.empty()) {
        Store(Metrics->MaxBlobsInRange, 0);
    } else {
        Store(
            Metrics->MaxBlobsInRange,
            compactionStats.TopRangesByCompactionScore.front()
                .Stats.BlobsCount);
    }
    if (compactionStats.TopRangesByCleanupScore.empty()) {
        Store(Metrics->MaxDeletionsInRange, 0);
    } else {
        Store(
            Metrics->MaxDeletionsInRange,
            compactionStats.TopRangesByCleanupScore.front()
                .Stats.DeletionsCount);
    }
    if (compactionStats.TopRangesByGarbageScore.empty()) {
        Store(Metrics->MaxGarbageBlocksInRange, 0);
    } else {
        Store(
            Metrics->MaxGarbageBlocksInRange,
            compactionStats.TopRangesByGarbageScore.front()
                .Stats.GarbageBlocksCount);
    }

    Store(Metrics->StatefulSessionsCount, sessionsStats.StatefulSessionsCount);
    Store(
        Metrics->StatelessSessionsCount,
        sessionsStats.StatelessSessionsCount);
    Store(Metrics->ActiveSessionsCount, sessionsStats.ActiveSessionsCount);
    Store(Metrics->OrphanSessionsCount, sessionsStats.OrphanSessionsCount);
    Store(
        Metrics->HandleStatsByNodeMaxSize,
        sessionsStats.HandleStatsByNodeMaxSize);
    Store(
        Metrics->HandleStatsByNodeSumSize,
        sessionsStats.HandleStatsByNodeSumSize);
    Store(
        Metrics->HandleStatsByNodeMaxTotalSize,
        sessionsStats.HandleStatsByNodeMaxTotalSize);
    Store(
        Metrics->HandleStatsByNodeSumTotalSize,
        sessionsStats.HandleStatsByNodeSumTotalSize);
    Store(Metrics->WritableChannelCount, channelsStats.WritableChannelCount);
    Store(
        Metrics->UnwritableChannelCount,
        channelsStats.UnwritableChannelCount);
    Store(Metrics->ChannelsToMoveCount, channelsStats.ChannelsToMoveCount);
    Store(Metrics->ReadAheadCacheNodeCount, readAheadStats.NodeCount);
    Store(Metrics->ReadNodeCacheBypassCount, GetReadNodeCacheBypassCount());
    Store(Metrics->ReadAheadCacheBypassCount, GetReadAheadCacheBypassCount());

    Store(
        Metrics->InMemoryIndexStateNodesCount,
        inMemoryIndexStateStats.NodesCount);
    Store(
        Metrics->InMemoryIndexStateNodesCapacity,
        inMemoryIndexStateStats.NodesCapacity);
    Store(
        Metrics->InMemoryIndexStateNodeRefsCount,
        inMemoryIndexStateStats.NodeRefsCount);
    Store(
        Metrics->InMemoryIndexStateNodeRefsCapacity,
        inMemoryIndexStateStats.NodeRefsCapacity);
    Store(
        Metrics->InMemoryIndexStateNodeAttrsCount,
        inMemoryIndexStateStats.NodeAttrsCount);
    Store(
        Metrics->InMemoryIndexStateNodeAttrsCapacity,
        inMemoryIndexStateStats.NodeAttrsCapacity);
    Store(
        Metrics->InMemoryIndexStateNodeRefsExhaustivenessCount,
        inMemoryIndexStateStats.NodeRefsExhaustivenessCount);
    Store(
        Metrics->InMemoryIndexStateNodeRefsExhaustivenessCapacity,
        inMemoryIndexStateStats.NodeRefsExhaustivenessCapacity);
    Store(
        Metrics->InMemoryIndexStateIsExhaustive,
        inMemoryIndexStateStats.IsNodeRefsExhaustive);

    Store(Metrics->MixedIndexLoadedRanges, blobMetaMapStats.LoadedRanges);
    Store(Metrics->MixedIndexOffloadedRanges, blobMetaMapStats.OffloadedRanges);

    Store(
        Metrics->NodesOpenForWritingBySingleSession,
        nodeToSessionCounters.NodesOpenForWritingBySingleSession);
    Store(
        Metrics->NodesOpenForWritingByMultipleSessions,
        nodeToSessionCounters.NodesOpenForWritingByMultipleSessions);
    Store(
        Metrics->NodesOpenForReadingBySingleSession,
        nodeToSessionCounters.NodesOpenForReadingBySingleSession);
    Store(
        Metrics->NodesOpenForReadingByMultipleSessions,
        nodeToSessionCounters.NodesOpenForReadingByMultipleSessions);

    Store(Metrics->OrphanNodesCount, miscNodeStats.OrphanNodesCount);
    Store(
        Metrics->DeferredNodeDestructionCount,
        miscNodeStats.DeferredNodeDestructionCount);

    Metrics->BusyIdleCalc.OnUpdateStats();
    Metrics->UpdatePerformanceMetrics(now, diagConfig, fileSystem);

#define FILESTORE_TABLET_UPDATE_REQUEST_METRICS(name, ...)                     \
    Metrics->name.UpdatePrev(now);                                             \
// FILESTORE_TABLET_METRICS_REQUEST

    FILESTORE_TABLET_METRICS_REQUESTS(FILESTORE_TABLET_UPDATE_REQUEST_METRICS)

#undef FILESTORE_TABLET_UPDATE_REQUEST_METRICS
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
        Y_UNUSED(ctx);
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
        GetNodeToSessionCounters(),
        GetMiscNodeStats(),
        GetInMemoryIndexStateStats(),
        GetBlobMetaMapStats(),
        BuildBackpressureThresholds(),
        GetBackpressureValues(),
        GetHandlesStats());

    // TabletStartTimestamp should be set only once
    i64 expected = 0;
    Metrics->TabletStartTimestamp.compare_exchange_strong(
        expected,
        now.MicroSeconds());
    Metrics->TabletId.store(TabletID());
    Metrics->TabletGeneration.store(GetGeneration());

    Metrics->Register(
        fsId,
        fs.GetCloudId(),
        fs.GetFolderId(),
        storageMediaKind);
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
        Metrics->CalculateNetworkRequestBytes(
            Config->GetNonNetworkMetricsBalancingFactor()),
        ctx.Now());
    resourceMetrics->TryUpdate(ctx);
}

void TIndexTabletActor::CalculateActorCPUUsage(const TActorContext& ctx)
{
    const i64 curUsageMicros =
        Metrics->CPUUsageMicros.load(std::memory_order_relaxed);
    const TInstant ts = ctx.Now();
    if (Metrics->PrevCPUUsageMicrosTs) {
        const auto timeDiff = ts - Metrics->PrevCPUUsageMicrosTs;
        if (timeDiff) {
            const double usageDiff =
                curUsageMicros - Metrics->PrevCPUUsageMicros;
            Metrics->CPUUsageRate.store(
                100 * (usageDiff / timeDiff.MicroSeconds()),
                std::memory_order_relaxed);
        }
    }

    Metrics->PrevCPUUsageMicrosTs = ts;
    Metrics->PrevCPUUsageMicros = curUsageMicros;
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
        GetNodeToSessionCounters(),
        GetMiscNodeStats(),
        GetInMemoryIndexStateStats(),
        GetBlobMetaMapStats(),
        BuildBackpressureThresholds(),
        GetBackpressureValues(),
        GetHandlesStats());
    CalculateActorCPUUsage(ctx);
    SendMetricsToExecutor(ctx);

    Store(Metrics->OpLogEntryCount, GetOpLogEntryCount());
    Store(Metrics->ResponseLogEntryCount, GetResponseLogEntryCount());

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
            // TIndexTabletActor can reply to a GetStorageStats request from
            // cache only if it's a main tablet.
            if (IsMainTablet()) {
                CachedAggregateStatsTs = ctx.Now();
            }
            Store(
                Metrics->AggregateUsedBytesCount,
                CachedAggregateStats.GetUsedBlocksCount() * GetBlockSize());
            Store(
                Metrics->AggregateUsedNodesCount,
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
            std::move(response),
            true, /* isBackgroundRequest */
            Config->GetFanoutStatsCollectionInShardsDisabled());

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

    stats->SetCurrentLoad(Metrics->CurrentLoad.load(std::memory_order_relaxed));
    stats->SetSuffer(Metrics->Suffer.load(std::memory_order_relaxed));

    stats->SetTotalBlocksCount(GetFileSystem().GetBlocksCount());

    stats->SetFreshBytesItemCount(GetFreshBytesItemCount());

    stats->SetSevenBytesHandlesCount(Metrics->SevenBytesHandlesCount);

    stats->SetUnconfirmedDataCount(
        UnconfirmedData.size() + UnconfirmedDataInProgress.size());
    stats->SetConfirmedDataCount(ConfirmedData.size());

    for (const auto& usage: GetQuotaUsages()) {
        auto& proto = (*stats->MutableQuotaUsages())[usage.QuotaId];
        proto.SetUsedBytes(usage.UsedBytes);
        proto.SetUsedNodes(usage.UsedNodes);
    }
}

void TIndexTabletActor::HandleGetStorageStats(
    const TEvIndexTablet::TEvGetStorageStatsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto response =
        std::make_unique<TEvIndexTablet::TEvGetStorageStatsResponse>();
    response->Record.SetMediaKind(GetFileSystem().GetStorageMediaKind());
    const i64 tabletStartTimestamp =
        Metrics->TabletStartTimestamp.load(std::memory_order_relaxed);
    if (tabletStartTimestamp) {
        const auto now = ctx.Now();
        const auto tabletStartTs = TInstant::MicroSeconds(tabletStartTimestamp);
        if (tabletStartTs < now) {
            response->Record.SetTabletUptimeMs(
                (now - tabletStartTs).MilliSeconds());
        }
    }
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

    const bool allowCache = req.GetCacheTTL() &&
                            ctx.Now() - CachedAggregateStatsTs <
                                TDuration::MilliSeconds(req.GetCacheTTL());

    if (allowCache && pollShards) {
        *stats = CachedAggregateStats;
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

    if (allowCache || shardIds.empty()) {
        Metrics->StatFileStore.Update(1, 0, TDuration::Zero());
        Metrics->GetStorageStats.Update(1, 0, TDuration::Zero());
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
        std::move(response),
        false, /* isBackgroundRequest */
        Config->GetFanoutStatsCollectionInShardsDisabled());

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);
}

void TIndexTabletActor::HandleGetDiagnosticStats(
    const TEvIndexTablet::TEvGetDiagnosticStatsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto response =
        std::make_unique<TEvIndexTablet::TEvGetDiagnosticStatsResponse>();

    for (const auto& accessStats:
         GetNodeAccessStats(ctx.Now(), ev->Get()->Record.GetLimit()))
    {
        auto* out = response->Record.AddNodeStats();
        out->SetShardId(GetFileSystemId());
        out->SetNodeId(accessStats.NodeId);
        out->SetRequestCount(accessStats.RequestCount);
        out->SetAccessScore(accessStats.AccessScore);
        out->SetLastAccessedTimestampUs(
            accessStats.LastAccessed.MicroSeconds());
    }

    for (const auto& latencyStats:
         GetLatencyStats(ctx.Now(), ev->Get()->Record.GetLimit()))
    {
        auto* out = response->Record.AddLatencyStats();
        out->SetShardId(GetFileSystemId());
        out->SetNodeId(latencyStats.NodeId);
        out->SetRequestType(GetFileStoreRequestName(latencyStats.RequestType));
        out->SetRequestCount(latencyStats.RequestCount);
        out->SetTotalLatencyUs(latencyStats.TotalLatencyUs);
        out->SetAverageLatencyDecayedUs(latencyStats.AverageLatencyDecayedUs);
        out->SetLastAccessedTimestampUs(
            latencyStats.LastAccessed.MicroSeconds());
    }

    NCloud::Reply(ctx, *ev, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleAggregateStatsCompleted(
    const TEvIndexTabletPrivate::TEvAggregateStatsCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    TDuration backgroundRequestDuration;

    if (msg->IsBackgroundRequest) {
        backgroundRequestDuration = ctx.Now() - CachedStatsFetchingStartTs;
        LOG_DEBUG(
            ctx,
            TFileStoreComponents::TABLET,
            "%s Background shard stats fetch completed in %s, ShardsCount: %lu",
            LogTag.c_str(),
            backgroundRequestDuration.ToString().c_str(),
            msg->AggregateStats.GetShardStats().size());
        CachedStatsFetchingStartTs = TInstant::Zero();
    }

    if (!HasError(msg->Error)) {
        if (msg->IsBackgroundRequest) {
            Metrics->GetStorageStats.Update(1, 0, backgroundRequestDuration);
        } else {
            Metrics->StatFileStore.Update(1, 0, ctx.Now() - msg->StartedTs);
            Metrics->GetStorageStats.Update(1, 0, ctx.Now() - msg->StartedTs);
        }
        CachedAggregateStats = std::move(msg->AggregateStats);
        // TIndexTabletActor can reply to a GetStorageStats request from cache
        // only if it's a main tablet.
        if (IsMainTablet()) {
            CachedAggregateStatsTs = ctx.Now();
        }

        TVector<TShardStats> shardStats;
        FillShardStats(CachedAggregateStats, shardStats);
        auto error = UpdateShardBalancer(shardStats);
        if (HasError(error)) {
            LOG_WARN(
                ctx,
                TFileStoreComponents::TABLET,
                "%s Failed to update shard balancer: %s",
                LogTag.c_str(),
                FormatError(error).Quote().c_str());

            Metrics->ShardBalancerUpdateErrorCount.fetch_add(
                1,
                std::memory_order_relaxed);
        } else {
            LOG_DEBUG(
                ctx,
                TFileStoreComponents::TABLET,
                "%s Updated shard balancer: %s",
                LogTag.c_str(),
                DescribeShardList(MakeOrderedShardList()).c_str());
        }

        Store(
            Metrics->AggregateUsedBytesCount,
            CachedAggregateStats.GetUsedBlocksCount() * GetBlockSize());
        Store(
            Metrics->AggregateUsedNodesCount,
            CachedAggregateStats.GetUsedNodesCount());
    }

    WorkerActors.erase(ev->Sender);
}

////////////////////////////////////////////////////////////////////////////////

TCPUUsageTimer& TIndexTabletActor::AccessCPUUsageTimer()
{
    return CPUUsageTimer;
}

}   // namespace NCloud::NFileStore::NStorage
