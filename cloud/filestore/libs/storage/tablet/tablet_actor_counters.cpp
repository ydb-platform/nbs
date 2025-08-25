#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/config.h>
#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/diagnostics/metrics/label.h>
#include <cloud/filestore/libs/diagnostics/metrics/operations.h>
#include <cloud/filestore/libs/diagnostics/metrics/registry.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NMetrics;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TGetShardStatsActor final
    : public TActorBootstrapped<TGetShardStatsActor>
{
private:
    const TString LogTag;
    const TActorId Tablet;
    const TRequestInfoPtr RequestInfo;
    const NProtoPrivate::TGetStorageStatsRequest Request;
    TString MainFileSystemId;
    bool PollMainFileSystem;
    const google::protobuf::RepeatedPtrField<TString> ShardIds;
    std::unique_ptr<TEvIndexTablet::TEvGetStorageStatsResponse> Response;
    TVector<TShardStats> ShardStats;
    int Responses = 0;

public:
    TGetShardStatsActor(
        TString logTag,
        TActorId tablet,
        TRequestInfoPtr requestInfo,
        NProtoPrivate::TGetStorageStatsRequest request,
        TString mainFileSystemId,
        bool pollMainFileSystem,
        google::protobuf::RepeatedPtrField<TString> shardIds,
        std::unique_ptr<TEvIndexTablet::TEvGetStorageStatsResponse> response);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void SendRequests(const TActorContext& ctx);
    void SendRequestToFileSystem(
        const TActorContext& ctx,
        const TString& fileSystemId,
        ui32 cookie);

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

TGetShardStatsActor::TGetShardStatsActor(
        TString logTag,
        TActorId tablet,
        TRequestInfoPtr requestInfo,
        NProtoPrivate::TGetStorageStatsRequest request,
        TString mainFileSystemId,
        bool pollMainFileSystem,
        google::protobuf::RepeatedPtrField<TString> shardIds,
        std::unique_ptr<TEvIndexTablet::TEvGetStorageStatsResponse> response)
    : LogTag(std::move(logTag))
    , Tablet(tablet)
    , RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
    , MainFileSystemId(std::move(mainFileSystemId))
    , PollMainFileSystem(pollMainFileSystem)
    , ShardIds(std::move(shardIds))
    , Response(std::move(response))
    , ShardStats(ShardIds.size())
{}

void TGetShardStatsActor::Bootstrap(const TActorContext& ctx)
{
    SendRequests(ctx);
    Become(&TThis::StateWork);
}

void TGetShardStatsActor::SendRequests(const TActorContext& ctx)
{
    ui32 cookie = 0;
    for (const auto& shardId: ShardIds) {
        SendRequestToFileSystem(ctx, shardId, cookie++);
    }

    if (PollMainFileSystem) {
        SendRequestToFileSystem(ctx, MainFileSystemId, cookie);
    }
}

void TGetShardStatsActor::SendRequestToFileSystem(
    const TActorContext& ctx,
    const TString& fileSystemId,
    ui32 cookie)
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

void TGetShardStatsActor::HandleGetStorageStatsResponse(
    const TEvIndexTablet::TEvGetStorageStatsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const int requestsCount =
        PollMainFileSystem ? ShardIds.size() + 1 : ShardIds.size();
    TABLET_VERIFY(ev->Cookie < static_cast<ui64>(requestsCount));
    const TString& fileSystemId =
        ev->Cookie < static_cast<ui64>(ShardIds.size()) ? ShardIds[ev->Cookie]
                                                        : MainFileSystemId;

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

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s Got storage stats for filesystem %s, used: %lu, aggregate used: "
        "%lu",
        LogTag.c_str(),
        fileSystemId.c_str(),
        src.GetUsedBlocksCount(),
        dst.GetUsedBlocksCount());

    if (ev->Cookie < static_cast<ui64>(ShardIds.size())) {
        auto& ss = ShardStats[ev->Cookie];
        ss.CurrentLoad = src.GetCurrentLoad();
        ss.Suffer = src.GetSuffer();
        ss.TotalBlocksCount = src.GetTotalBlocksCount();
        ss.UsedBlocksCount = src.GetUsedBlocksCount();
    }

    if (++Responses == requestsCount) {
        ReplyAndDie(ctx, {});
    }
}

void TGetShardStatsActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

void TGetShardStatsActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (HasError(error)) {
        Response =
            std::make_unique<TEvIndexTablet::TEvGetStorageStatsResponse>(error);
    }
    using TCompletion = TEvIndexTabletPrivate::TEvGetShardStatsCompleted;
    TInstant startedTs;
    NProtoPrivate::TStorageStats statsForTablet = Response->Record.GetStats();
    if (RequestInfo) {
        startedTs = RequestInfo->StartedTs;
        auto* stats = Response->Record.MutableStats();
        for (ui32 i = 0; i < ShardStats.size(); ++i) {
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

STFUNC(TGetShardStatsActor::StateWork)
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

void RegisterSensor(
    IMetricsRegistryPtr registry,
    TString name,
    const std::atomic<i64>& source,
    EAggregationType aggrType,
    EMetricType metrType)
{
    registry->Register(
        {CreateSensor(std::move(name))},
        source,
        aggrType,
        metrType);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TIndexTabletActor::TMetrics::TMetrics(IMetricsRegistryPtr metricsRegistry)
    : StorageRegistry(CreateScopedMetricsRegistry(
        {CreateLabel("component", "storage")},
        metricsRegistry))
    , StorageFsRegistry(CreateScopedMetricsRegistry(
        {CreateLabel("component", "storage_fs"), CreateLabel("host", "cluster")},
        metricsRegistry))
    , FsRegistry(CreateMetricsRegistryStub())
    , AggregatableFsRegistry(CreateMetricsRegistryStub())
{}

void TIndexTabletActor::TMetrics::Register(
    const TString& fsId,
    const TString& cloudId,
    const TString& folderId,
    const TString& mediaKind)
{
    if (Initialized) {
        return;
    }

    auto totalKindRegistry = CreateScopedMetricsRegistry(
        {CreateLabel("type", mediaKind)},
        StorageRegistry);

    FsRegistry = CreateScopedMetricsRegistry(
        {
            CreateLabel("filesystem", fsId),
            CreateLabel("cloud", cloudId),
            CreateLabel("folder", folderId),
        },
        StorageFsRegistry);
    AggregatableFsRegistry = CreateScopedMetricsRegistry(
        {},
        {std::move(totalKindRegistry), FsRegistry});

#define REGISTER(registry, name, aggrType, metrType)                           \
    RegisterSensor(registry, #name, name, aggrType, metrType)                  \
// REGISTER

#define REGISTER_AGGREGATABLE_SUM(name, metrType)                              \
    REGISTER(                                                                  \
        AggregatableFsRegistry,                                                \
        name,                                                                  \
        EAggregationType::AT_SUM,                                              \
        metrType)                                                              \
// REGISTER_AGGREGATABLE_SUM

#define REGISTER_LOCAL(name, metrType)                                         \
    REGISTER(                                                                  \
        FsRegistry,                                                            \
        name,                                                                  \
        EAggregationType::AT_SUM,                                              \
        metrType)                                                              \
// REGISTER_LOCAL

    REGISTER_AGGREGATABLE_SUM(TotalBytesCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(UsedBytesCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(
        AggregateUsedBytesCount,
        EMetricType::MT_ABSOLUTE);

    REGISTER_AGGREGATABLE_SUM(TotalNodesCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(UsedNodesCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(
        AggregateUsedNodesCount,
        EMetricType::MT_ABSOLUTE);

    REGISTER_AGGREGATABLE_SUM(UsedSessionsCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(UsedHandlesCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(UsedLocksCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(StatefulSessionsCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(StatelessSessionsCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(ActiveSessionsCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(OrphanSessionsCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(SessionTimeouts, EMetricType::MT_DERIVATIVE);
    REGISTER_AGGREGATABLE_SUM(SessionCleanupAttempts, EMetricType::MT_DERIVATIVE);

    REGISTER_AGGREGATABLE_SUM(ReassignCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(WritableChannelCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(UnwritableChannelCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(ChannelsToMoveCount, EMetricType::MT_ABSOLUTE);

    REGISTER_AGGREGATABLE_SUM(
        ReadAheadCacheHitCount,
        EMetricType::MT_DERIVATIVE);
    REGISTER_AGGREGATABLE_SUM(
        ReadAheadCacheNodeCount,
        EMetricType::MT_ABSOLUTE);

    REGISTER_AGGREGATABLE_SUM(
        NodeIndexCacheHitCount,
        EMetricType::MT_DERIVATIVE);
    REGISTER_AGGREGATABLE_SUM(
        NodeIndexCacheNodeCount,
        EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(
        InMemoryIndexStateROCacheHitCount,
        EMetricType::MT_DERIVATIVE);
    REGISTER_AGGREGATABLE_SUM(
        InMemoryIndexStateROCacheMissCount,
        EMetricType::MT_DERIVATIVE);
    REGISTER_AGGREGATABLE_SUM(
        InMemoryIndexStateRWCount,
        EMetricType::MT_DERIVATIVE);
    REGISTER_AGGREGATABLE_SUM(
        InMemoryIndexStateNodesCount,
        EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(
        InMemoryIndexStateNodesCapacity,
        EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(
        InMemoryIndexStateNodeRefsCount,
        EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(
        InMemoryIndexStateNodeRefsCapacity,
        EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(
        InMemoryIndexStateNodeAttrsCount,
        EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(
        InMemoryIndexStateNodeAttrsCapacity,
        EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(
        InMemoryIndexStateIsExhaustive,
        EMetricType::MT_ABSOLUTE);

    REGISTER_AGGREGATABLE_SUM(
        MixedIndexLoadedRanges,
        EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(
        MixedIndexOffloadedRanges,
        EMetricType::MT_ABSOLUTE);

    REGISTER_AGGREGATABLE_SUM(FreshBytesCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(FreshBytesItemCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(DeletedFreshBytesCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(MixedBytesCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(MixedBlobsCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(DeletionMarkersCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(
        LargeDeletionMarkersCount,
        EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(GarbageQueueSize, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(GarbageBytesCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(FreshBlocksCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(CMMixedBlobsCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(CMDeletionMarkersCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(CMGarbageBlocksCount, EMetricType::MT_ABSOLUTE);

    REGISTER_AGGREGATABLE_SUM(IsWriteAllowed, EMetricType::MT_ABSOLUTE);
    REGISTER_LOCAL(FlushBackpressureValue, EMetricType::MT_ABSOLUTE);
    REGISTER_LOCAL(FlushBackpressureThreshold, EMetricType::MT_ABSOLUTE);
    REGISTER_LOCAL(FlushBytesBackpressureValue, EMetricType::MT_ABSOLUTE);
    REGISTER_LOCAL(FlushBytesBackpressureThreshold, EMetricType::MT_ABSOLUTE);
    REGISTER_LOCAL(CompactionBackpressureValue, EMetricType::MT_ABSOLUTE);
    REGISTER_LOCAL(CompactionBackpressureThreshold, EMetricType::MT_ABSOLUTE);
    REGISTER_LOCAL(CleanupBackpressureValue, EMetricType::MT_ABSOLUTE);
    REGISTER_LOCAL(CleanupBackpressureThreshold, EMetricType::MT_ABSOLUTE);

    REGISTER_AGGREGATABLE_SUM(IdleTime, EMetricType::MT_DERIVATIVE);
    REGISTER_AGGREGATABLE_SUM(BusyTime, EMetricType::MT_DERIVATIVE);

    REGISTER_LOCAL(TabletStartTimestamp, EMetricType::MT_ABSOLUTE);

    REGISTER_AGGREGATABLE_SUM(AllocatedCompactionRangesCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(UsedCompactionRangesCount, EMetricType::MT_ABSOLUTE);

    REGISTER_AGGREGATABLE_SUM(
        NodesOpenForWritingBySingleSession,
        EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(
        NodesOpenForWritingByMultipleSessions,
        EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(
        NodesOpenForReadingBySingleSession,
        EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(
        NodesOpenForReadingByMultipleSessions,
        EMetricType::MT_ABSOLUTE);

    REGISTER_AGGREGATABLE_SUM(OrphanNodesCount, EMetricType::MT_ABSOLUTE);

    // Throttling
    REGISTER_LOCAL(MaxReadBandwidth, EMetricType::MT_ABSOLUTE);
    REGISTER_LOCAL(MaxWriteBandwidth, EMetricType::MT_ABSOLUTE);
    REGISTER_LOCAL(MaxReadIops, EMetricType::MT_ABSOLUTE);
    REGISTER_LOCAL(MaxWriteIops, EMetricType::MT_ABSOLUTE);
    REGISTER_LOCAL(RejectedRequests, EMetricType::MT_DERIVATIVE);
    REGISTER_LOCAL(PostponedRequests, EMetricType::MT_DERIVATIVE);
    REGISTER_LOCAL(UsedQuota, EMetricType::MT_DERIVATIVE);
    MaxUsedQuota.Register(
        FsRegistry,
        {CreateSensor("MaxUsedQuota")},
        EAggregationType::AT_MAX);
    ReadDataPostponed.Register(
        FsRegistry,
        {CreateLabel("request", "ReadData"), CreateLabel("histogram", "ThrottlerDelay")});
    WriteDataPostponed.Register(
        FsRegistry,
        {CreateLabel("request", "WriteData"), CreateLabel("histogram", "ThrottlerDelay")});

    REGISTER_AGGREGATABLE_SUM(
        UncompressedBytesWritten,
        EMetricType::MT_DERIVATIVE);
    REGISTER_AGGREGATABLE_SUM(
        CompressedBytesWritten,
        EMetricType::MT_DERIVATIVE);

#define REGISTER_REQUEST(name)                                                 \
    REGISTER_AGGREGATABLE_SUM(                                                 \
        name.Count,                                                            \
        EMetricType::MT_DERIVATIVE);                                           \
                                                                               \
    REGISTER_AGGREGATABLE_SUM(                                                 \
        name.RequestBytes,                                                     \
        EMetricType::MT_DERIVATIVE);                                           \
                                                                               \
    REGISTER_AGGREGATABLE_SUM(                                                 \
        name.TimeSumUs,                                                        \
        EMetricType::MT_DERIVATIVE);                                           \
                                                                               \
    name.Time.Register(                                                        \
        AggregatableFsRegistry,                                                \
        {CreateLabel("request", #name), CreateLabel("histogram", "Time")});    \
// REGISTER_REQUEST

    REGISTER_REQUEST(ReadBlob);
    REGISTER_REQUEST(WriteBlob);
    REGISTER_REQUEST(PatchBlob);

    REGISTER_REQUEST(ReadData);
    REGISTER_REQUEST(DescribeData);
    REGISTER_REQUEST(WriteData);
    REGISTER_REQUEST(AddData);
    REGISTER_REQUEST(GenerateBlobIds);
    REGISTER_REQUEST(ListNodes);
    REGISTER_REQUEST(GetNodeAttr);
    REGISTER_REQUEST(CreateHandle);
    REGISTER_REQUEST(DestroyHandle);
    REGISTER_REQUEST(CreateNode);
    REGISTER_REQUEST(RenameNode);
    REGISTER_REQUEST(UnlinkNode);
    REGISTER_REQUEST(StatFileStore);
    REGISTER_REQUEST(GetNodeXAttr);

    REGISTER_REQUEST(Compaction);
    REGISTER_AGGREGATABLE_SUM(Compaction.DudCount, EMetricType::MT_DERIVATIVE);
    REGISTER_REQUEST(Cleanup);
    REGISTER_REQUEST(Flush);
    REGISTER_REQUEST(FlushBytes);
    REGISTER_REQUEST(TrimBytes);
    REGISTER_REQUEST(CollectGarbage);

    REGISTER_LOCAL(MaxBlobsInRange, EMetricType::MT_ABSOLUTE);
    REGISTER_LOCAL(MaxDeletionsInRange, EMetricType::MT_ABSOLUTE);
    REGISTER_LOCAL(MaxGarbageBlocksInRange, EMetricType::MT_ABSOLUTE);

    REGISTER_LOCAL(CurrentLoad, EMetricType::MT_ABSOLUTE);
    REGISTER_LOCAL(Suffer, EMetricType::MT_ABSOLUTE);

#undef REGISTER_REQUEST
#undef REGISTER_LOCAL
#undef REGISTER_AGGREGATABLE_SUM
#undef REGISTER

    BusyIdleCalc.Register(&BusyTime, &IdleTime);

    Initialized = true;
}

void TIndexTabletActor::TMetrics::Update(
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
    const TIndexTabletState::TBackpressureValues& backpressureValues)
{
    const ui32 blockSize = fileSystem.GetBlockSize();

    Store(TotalBytesCount, fileSystem.GetBlocksCount() * blockSize);
    Store(UsedBytesCount, stats.GetUsedBlocksCount() * blockSize);

    Store(TotalNodesCount, fileSystem.GetNodesCount());
    Store(UsedNodesCount, stats.GetUsedNodesCount());

    Store(UsedSessionsCount, stats.GetUsedSessionsCount());
    Store(UsedHandlesCount, stats.GetUsedHandlesCount());
    Store(UsedLocksCount, stats.GetUsedLocksCount());

    Store(FreshBytesCount, stats.GetFreshBytesCount());
    Store(FreshBytesItemCount, stats.GetFreshBytesItemCount());
    Store(DeletedFreshBytesCount, stats.GetDeletedFreshBytesCount());
    Store(MixedBytesCount, stats.GetMixedBlocksCount() * blockSize);
    Store(MixedBlobsCount, stats.GetMixedBlobsCount());
    Store(DeletionMarkersCount, stats.GetDeletionMarkersCount());
    Store(LargeDeletionMarkersCount, stats.GetLargeDeletionMarkersCount());
    Store(GarbageQueueSize, stats.GetGarbageQueueSize());
    Store(GarbageBytesCount, stats.GetGarbageBlocksCount() * blockSize);
    Store(FreshBlocksCount, stats.GetFreshBlocksCount());
    Store(CMMixedBlobsCount, compactionStats.TotalBlobsCount);
    Store(CMDeletionMarkersCount, compactionStats.TotalDeletionsCount);
    Store(CMGarbageBlocksCount, compactionStats.TotalGarbageBlocksCount);

    TString backpressureReason;
    Store(
        IsWriteAllowed,
        TIndexTabletActor::IsWriteAllowed(
            backpressureThresholds,
            backpressureValues,
            &backpressureReason));

    Store(FlushBackpressureValue, backpressureValues.Flush);
    Store(FlushBackpressureThreshold, backpressureThresholds.Flush);
    Store(FlushBytesBackpressureValue, backpressureValues.FlushBytes);
    Store(FlushBytesBackpressureThreshold, backpressureThresholds.FlushBytes);
    Store(CompactionBackpressureValue, backpressureValues.CompactionScore);
    Store(CompactionBackpressureThreshold, backpressureThresholds.CompactionScore);
    Store(CleanupBackpressureValue, backpressureValues.CleanupScore);
    Store(CleanupBackpressureThreshold, backpressureThresholds.CleanupScore);

    Store(MaxReadIops, performanceProfile.GetMaxReadIops());
    Store(MaxWriteIops, performanceProfile.GetMaxWriteIops());
    Store(MaxReadBandwidth, performanceProfile.GetMaxReadBandwidth());
    Store(MaxWriteBandwidth, performanceProfile.GetMaxWriteBandwidth());

    Store(AllocatedCompactionRangesCount, compactionStats.AllocatedRangesCount);
    Store(UsedCompactionRangesCount, compactionStats.UsedRangesCount);

    if (compactionStats.TopRangesByCompactionScore.empty()) {
        Store(MaxBlobsInRange, 0);
    } else {
        Store(
            MaxBlobsInRange,
            compactionStats.TopRangesByCompactionScore.front()
                .Stats.BlobsCount);
    }
    if (compactionStats.TopRangesByCleanupScore.empty()) {
        Store(MaxDeletionsInRange, 0);
    } else {
        Store(
            MaxDeletionsInRange,
            compactionStats.TopRangesByCleanupScore.front()
                .Stats.DeletionsCount);
    }
    if (compactionStats.TopRangesByGarbageScore.empty()) {
        Store(MaxGarbageBlocksInRange, 0);
    } else {
        Store(
            MaxGarbageBlocksInRange,
            compactionStats.TopRangesByGarbageScore.front()
                .Stats.GarbageBlocksCount);
    }

    Store(StatefulSessionsCount, sessionsStats.StatefulSessionsCount);
    Store(StatelessSessionsCount, sessionsStats.StatelessSessionsCount);
    Store(ActiveSessionsCount, sessionsStats.ActiveSessionsCount);
    Store(OrphanSessionsCount, sessionsStats.OrphanSessionsCount);
    Store(WritableChannelCount, channelsStats.WritableChannelCount);
    Store(UnwritableChannelCount, channelsStats.UnwritableChannelCount);
    Store(ChannelsToMoveCount, channelsStats.ChannelsToMoveCount);
    Store(ReadAheadCacheNodeCount, readAheadStats.NodeCount);
    Store(NodeIndexCacheNodeCount, nodeIndexCacheStats.NodeCount);

    Store(InMemoryIndexStateNodesCount, inMemoryIndexStateStats.NodesCount);
    Store(InMemoryIndexStateNodesCapacity, inMemoryIndexStateStats.NodesCapacity);
    Store(InMemoryIndexStateNodeRefsCount, inMemoryIndexStateStats.NodeRefsCount);
    Store(InMemoryIndexStateNodeRefsCapacity, inMemoryIndexStateStats.NodeRefsCapacity);
    Store(InMemoryIndexStateNodeAttrsCount, inMemoryIndexStateStats.NodeAttrsCount);
    Store(InMemoryIndexStateNodeAttrsCapacity, inMemoryIndexStateStats.NodeAttrsCapacity);
    Store(InMemoryIndexStateIsExhaustive, inMemoryIndexStateStats.IsNodeRefsExhaustive);

    Store(MixedIndexLoadedRanges, blobMetaMapStats.LoadedRanges);
    Store(MixedIndexOffloadedRanges, blobMetaMapStats.OffloadedRanges);

    Store(
        NodesOpenForWritingBySingleSession,
        nodeToSessionCounters.NodesOpenForWritingBySingleSession);
    Store(
        NodesOpenForWritingByMultipleSessions,
        nodeToSessionCounters.NodesOpenForWritingByMultipleSessions);
    Store(
        NodesOpenForReadingBySingleSession,
        nodeToSessionCounters.NodesOpenForReadingBySingleSession);
    Store(
        NodesOpenForReadingByMultipleSessions,
        nodeToSessionCounters.NodesOpenForReadingByMultipleSessions);

    Store(OrphanNodesCount, miscNodeStats.OrphanNodesCount);

    BusyIdleCalc.OnUpdateStats();
    UpdatePerformanceMetrics(now, diagConfig, fileSystem);

    ReadBlob.UpdatePrev(now);
    WriteBlob.UpdatePrev(now);
    PatchBlob.UpdatePrev(now);

    ReadData.UpdatePrev(now);
    DescribeData.UpdatePrev(now);
    WriteData.UpdatePrev(now);
    AddData.UpdatePrev(now);
    GenerateBlobIds.UpdatePrev(now);
    ListNodes.UpdatePrev(now);
    GetNodeAttr.UpdatePrev(now);
    CreateHandle.UpdatePrev(now);
    DestroyHandle.UpdatePrev(now);
    CreateNode.UpdatePrev(now);
    RenameNode.UpdatePrev(now);
    UnlinkNode.UpdatePrev(now);
    StatFileStore.UpdatePrev(now);
    GetNodeXAttr.UpdatePrev(now);

    Cleanup.UpdatePrev(now);
    Flush.UpdatePrev(now);
    FlushBytes.UpdatePrev(now);
    TrimBytes.UpdatePrev(now);
    CollectGarbage.UpdatePrev(now);
}

void TIndexTabletActor::TMetrics::UpdatePerformanceMetrics(
    TInstant now,
    const TDiagnosticsConfig& diagConfig,
    const NProto::TFileSystem& fileSystem)
{
    const ui32 expectedParallelism = 32;
    double load = 0;
    bool suffer = false;
    auto calcSufferAndLoad = [&] (
        const TRequestPerformanceProfile& rpp,
        const TRequestMetrics& rm)
    {
        if (!rpp.RPS) {
            return;
        }

        load += rm.RPS(now) / rpp.RPS;
        ui64 expectedLatencyUs = 1'000'000 / rpp.RPS;
        if (rpp.Throughput) {
            expectedLatencyUs +=
                1'000'000 * rm.AverageRequestSize() / rpp.Throughput;
            load += rm.Throughput(now) / rpp.Throughput;
        }

        const auto averageLatency = rm.AverageLatency();
        suffer |= TDuration::MicroSeconds(expectedLatencyUs)
            < averageLatency / expectedParallelism;
    };

    const auto& pp =
        fileSystem.GetStorageMediaKind() == NProto::STORAGE_MEDIA_SSD
        ? diagConfig.GetSSDFileSystemPerformanceProfile()
        : diagConfig.GetHDDFileSystemPerformanceProfile();

    calcSufferAndLoad(pp.Read, ReadData);
    calcSufferAndLoad(pp.Read, DescribeData);
    calcSufferAndLoad(pp.Write, WriteData);
    calcSufferAndLoad(pp.Write, AddData);
    calcSufferAndLoad(pp.ListNodes, ListNodes);
    calcSufferAndLoad(pp.GetNodeAttr, GetNodeAttr);
    calcSufferAndLoad(pp.CreateHandle, CreateHandle);
    calcSufferAndLoad(pp.DestroyHandle, DestroyHandle);
    calcSufferAndLoad(pp.CreateNode, CreateNode);
    calcSufferAndLoad(pp.RenameNode, RenameNode);
    calcSufferAndLoad(pp.UnlinkNode, UnlinkNode);
    calcSufferAndLoad(pp.StatFileStore, StatFileStore);

    Store(CurrentLoad, load * 1000);
    Store(Suffer, load < 1 ? suffer : 0);
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
    Metrics.Update(
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
        GetBackpressureValues());

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

void TIndexTabletActor::HandleUpdateCounters(
    const TEvIndexTabletPrivate::TEvUpdateCounters::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    UpdateCounters();
    Metrics.Update(
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
        GetBackpressureValues());
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
        // if shardIds isn't empty and current tablet is a shard, it will
        // collect self stats via TGetShardStatsActor
        if (shardIds.empty() || IsMainTablet()) {
            FillSelfStorageStats(stats);
        }
        if (shardIds.empty()) {
            CachedAggregateStats = std::move(*stats);
            return;
        }

        auto actor = std::make_unique<TGetShardStatsActor>(
            LogTag,
            SelfId(),
            TRequestInfoPtr(),
            NProtoPrivate::TGetStorageStatsRequest(),
            GetFileSystem().GetMainFileSystemId(),
            !IsMainTablet(),
            shardIds,
            std::move(response));

        auto actorId = NCloud::Register(ctx, std::move(actor));
        WorkerActors.insert(actorId);
        CachedStatsFetchingStartTs = ctx.Now();
    }
}

void TIndexTabletActor::UpdateCounters()
{
#define FILESTORE_TABLET_UPDATE_COUNTER(name, ...)                             \
    {                                                                          \
        auto& counter = Counters->Simple()[                                    \
            TIndexTabletCounters::SIMPLE_COUNTER_Stats_##name];                \
        counter.Set(Get##name());                                              \
    }                                                                          \
// FILESTORE_TABLET_UPDATE_COUNTER

    FILESTORE_TABLET_STATS(FILESTORE_TABLET_UPDATE_COUNTER)

#undef FILESTORE_TABLET_UPDATE_COUNTER
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
        req.GetMode() != NProtoPrivate::STATS_REQUEST_MODE_GET_ONLY_SELF &&
        (req.GetMode() ==
             NProtoPrivate::STATS_REQUEST_MODE_FORCE_FETCH_SHARDS ||
         IsMainTablet());
    const auto& shardIds = pollShards
        ? GetFileSystem().GetShardFileSystemIds()
        : Default<google::protobuf::RepeatedPtrField<TString>>();

    const bool useCache =
        req.GetAllowCache() &&
        req.GetMode() != NProtoPrivate::STATS_REQUEST_MODE_GET_ONLY_SELF;
    if (useCache) {
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

    if (useCache || shardIds.empty()) {
        Metrics.StatFileStore.Update(1, 0, TDuration::Zero());
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        ev->Get()->CallContext);
    requestInfo->StartedTs = ctx.Now();

    if (!IsMainTablet()) {
        // if current tablet is a shard, it will collect self stats via
        // TGetShardStatsActor
        stats->Clear();
    }

    auto actor = std::make_unique<TGetShardStatsActor>(
        LogTag,
        SelfId(),
        std::move(requestInfo),
        std::move(req),
        GetFileSystem().GetMainFileSystemId(),
        !IsMainTablet(),
        shardIds,
        std::move(response));

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleGetShardStatsCompleted(
    const TEvIndexTabletPrivate::TEvGetShardStatsCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    const bool isBackgroundRequest = msg->StartedTs.GetValue() == 0;
    if (isBackgroundRequest) {
        LOG_DEBUG(
            ctx,
            TFileStoreComponents::TABLET_WORKER,
            "%s Background shard stats fetch completed in %s",
            LogTag.c_str(),
            (ctx.Now() - CachedStatsFetchingStartTs).ToString().c_str());
        CachedStatsFetchingStartTs = TInstant::Zero();
    }
    if (!HasError(msg->Error)) {
        if (!isBackgroundRequest) {
            Metrics.StatFileStore.Update(1, 0, ctx.Now() - msg->StartedTs);
        }
        CachedAggregateStats = std::move(msg->AggregateStats);
        CachedShardStats = std::move(msg->ShardStats);
        UpdateShardStats(CachedShardStats);

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
