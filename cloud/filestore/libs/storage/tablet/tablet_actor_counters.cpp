#include "tablet_actor.h"

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

class TGetFollowerStatsActor final
    : public TActorBootstrapped<TGetFollowerStatsActor>
{
private:
    const TString LogTag;
    const TRequestInfoPtr RequestInfo;
    const NProtoPrivate::TGetStorageStatsRequest Request;
    const google::protobuf::RepeatedPtrField<TString> FollowerIds;
    std::unique_ptr<TEvIndexTablet::TEvGetStorageStatsResponse> Response;
    int Responses = 0;

public:
    TGetFollowerStatsActor(
        TString logTag,
        TRequestInfoPtr requestInfo,
        NProtoPrivate::TGetStorageStatsRequest request,
        google::protobuf::RepeatedPtrField<TString> followerIds,
        std::unique_ptr<TEvIndexTablet::TEvGetStorageStatsResponse> response);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void SendRequests(const TActorContext& ctx);

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

TGetFollowerStatsActor::TGetFollowerStatsActor(
        TString logTag,
        TRequestInfoPtr requestInfo,
        NProtoPrivate::TGetStorageStatsRequest request,
        google::protobuf::RepeatedPtrField<TString> followerIds,
        std::unique_ptr<TEvIndexTablet::TEvGetStorageStatsResponse> response)
    : LogTag(std::move(logTag))
    , RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
    , FollowerIds(std::move(followerIds))
    , Response(std::move(response))
{}

void TGetFollowerStatsActor::Bootstrap(const TActorContext& ctx)
{
    SendRequests(ctx);
    Become(&TThis::StateWork);
}

void TGetFollowerStatsActor::SendRequests(const TActorContext& ctx)
{
    ui32 cookie = 0;
    for (const auto& followerId: FollowerIds) {
        auto request =
            std::make_unique<TEvIndexTablet::TEvGetStorageStatsRequest>();
        request->Record = Request;
        request->Record.SetFileSystemId(followerId);

        LOG_INFO(
            ctx,
            TFileStoreComponents::TABLET_WORKER,
            "%s Sending GetStorageStatsRequest to follower %s",
            LogTag.c_str(),
            followerId.c_str());

        ctx.Send(
            MakeIndexTabletProxyServiceId(),
            request.release(),
            {}, // flags
            cookie++);
    }
}

void TGetFollowerStatsActor::HandleGetStorageStatsResponse(
    const TEvIndexTablet::TEvGetStorageStatsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::TABLET_WORKER,
            "%s Follower storage stats retrieval failed for %s with error %s",
            LogTag.c_str(),
            FollowerIds[ev->Cookie].c_str(),
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
        "%s Got storage stats for follower %s",
        LogTag.c_str(),
        FollowerIds[ev->Cookie].c_str());

    if (++Responses == FollowerIds.size()) {
        ReplyAndDie(ctx, {});
    }
}

void TGetFollowerStatsActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

void TGetFollowerStatsActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (HasError(error)) {
        Response =
            std::make_unique<TEvIndexTablet::TEvGetStorageStatsResponse>(error);
    }
    NCloud::Reply(ctx, *RequestInfo, std::move(Response));

    Die(ctx);
}

STFUNC(TGetFollowerStatsActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvIndexTablet::TEvGetStorageStatsResponse,
            HandleGetStorageStatsResponse);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::TABLET_WORKER);
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
    const TString& mediaKind)
{
    if (Initialized) {
        return;
    }

    auto totalKindRegistry = CreateScopedMetricsRegistry(
        {CreateLabel("type", mediaKind)},
        StorageRegistry);

    FsRegistry = CreateScopedMetricsRegistry(
        {CreateLabel("filesystem", fsId)},
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

    REGISTER_AGGREGATABLE_SUM(TotalNodesCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(UsedNodesCount, EMetricType::MT_ABSOLUTE);

    REGISTER_AGGREGATABLE_SUM(UsedSessionsCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(UsedHandlesCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(UsedLocksCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(StatefulSessionsCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(StatelessSessionsCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(SessionTimeouts, EMetricType::MT_DERIVATIVE);

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

    REGISTER_AGGREGATABLE_SUM(FreshBytesCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(DeletedFreshBytesCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(MixedBytesCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(MixedBlobsCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(DeletionMarkersCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(GarbageQueueSize, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(GarbageBytesCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(FreshBlocksCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(CMMixedBlobsCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(CMDeletionMarkersCount, EMetricType::MT_ABSOLUTE);

    REGISTER_AGGREGATABLE_SUM(IdleTime, EMetricType::MT_DERIVATIVE);
    REGISTER_AGGREGATABLE_SUM(BusyTime, EMetricType::MT_DERIVATIVE);

    REGISTER_AGGREGATABLE_SUM(AllocatedCompactionRangesCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(UsedCompactionRangesCount, EMetricType::MT_ABSOLUTE);

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
    REGISTER_REQUEST(Compaction);
    REGISTER_AGGREGATABLE_SUM(Compaction.DudCount, EMetricType::MT_DERIVATIVE);
    REGISTER_REQUEST(Cleanup);
    REGISTER_REQUEST(Flush);
    REGISTER_REQUEST(FlushBytes);
    REGISTER_REQUEST(TrimBytes);
    REGISTER_REQUEST(CollectGarbage);

    REGISTER_LOCAL(MaxBlobsInRange, EMetricType::MT_ABSOLUTE);
    REGISTER_LOCAL(MaxDeletionsInRange, EMetricType::MT_ABSOLUTE);

#undef REGISTER_REQUEST
#undef REGISTER_LOCAL
#undef REGISTER_AGGREGATABLE_SUM
#undef REGISTER

    BusyIdleCalc.Register(&BusyTime, &IdleTime);

    Initialized = true;
}

void TIndexTabletActor::TMetrics::Update(
    const NProto::TFileSystem& fileSystem,
    const NProto::TFileSystemStats& stats,
    const NProto::TFileStorePerformanceProfile& performanceProfile,
    const TCompactionMapStats& compactionStats,
    const TSessionsStats& sessionsStats,
    const TChannelsStats& channelsStats,
    const TReadAheadCacheStats& readAheadStats,
    const TNodeIndexCacheStats& nodeIndexCacheStats)
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
    Store(DeletedFreshBytesCount, stats.GetDeletedFreshBytesCount());
    Store(MixedBytesCount, stats.GetMixedBlocksCount() * blockSize);
    Store(MixedBlobsCount, stats.GetMixedBlobsCount());
    Store(DeletionMarkersCount, stats.GetDeletionMarkersCount());
    Store(GarbageQueueSize, stats.GetGarbageQueueSize());
    Store(GarbageBytesCount, stats.GetGarbageBlocksCount() * blockSize);
    Store(FreshBlocksCount, stats.GetFreshBlocksCount());
    Store(CMMixedBlobsCount, compactionStats.TotalBlobsCount);
    Store(CMDeletionMarkersCount, compactionStats.TotalDeletionsCount);

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

    Store(StatefulSessionsCount, sessionsStats.StatefulSessionsCount);
    Store(StatelessSessionsCount, sessionsStats.StatelessSessionsCount);
    Store(WritableChannelCount, channelsStats.WritableChannelCount);
    Store(UnwritableChannelCount, channelsStats.UnwritableChannelCount);
    Store(ChannelsToMoveCount, channelsStats.ChannelsToMoveCount);
    Store(ReadAheadCacheNodeCount, readAheadStats.NodeCount);
    Store(NodeIndexCacheNodeCount, nodeIndexCacheStats.NodeCount);

    BusyIdleCalc.OnUpdateStats();
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

void TIndexTabletActor::RegisterStatCounters()
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
        fs,
        GetFileSystemStats(),
        GetPerformanceProfile(),
        GetCompactionMapStats(1),
        CalculateSessionsStats(),
        CalculateChannelsStats(),
        CalculateReadAheadCacheStats(),
        CalculateNodeIndexCacheStats());

    Metrics.Register(fsId, storageMediaKind);
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
        GetFileSystem(),
        GetFileSystemStats(),
        GetPerformanceProfile(),
        GetCompactionMapStats(1),
        CalculateSessionsStats(),
        CalculateChannelsStats(),
        CalculateReadAheadCacheStats(),
        CalculateNodeIndexCacheStats());
    SendMetricsToExecutor(ctx);

    UpdateCountersScheduled = false;
    ScheduleUpdateCounters(ctx);
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

void TIndexTabletActor::HandleGetStorageStats(
    const TEvIndexTablet::TEvGetStorageStatsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto response =
        std::make_unique<TEvIndexTablet::TEvGetStorageStatsResponse>();

    auto* stats = response->Record.MutableStats();

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

    response->Record.SetMediaKind(GetFileSystem().GetStorageMediaKind());

    auto cmStats = GetCompactionMapStats(0);
    stats->SetUsedCompactionRanges(cmStats.UsedRangesCount);
    stats->SetAllocatedCompactionRanges(cmStats.AllocatedRangesCount);

    auto& req = ev->Get()->Record;

    if (req.GetCompactionRangeCountByCompactionScore()) {
        const auto topRanges = GetTopRangesByCompactionScore(
            req.GetCompactionRangeCountByCompactionScore());
        for (const auto& r: topRanges) {
            auto* out = stats->AddCompactionRangeStats();
            out->SetRangeId(r.RangeId);
            out->SetBlobCount(r.Stats.BlobsCount);
            out->SetDeletionCount(r.Stats.DeletionsCount);
        }
    }

    if (req.GetCompactionRangeCountByCleanupScore()) {
        const auto topRanges = GetTopRangesByCleanupScore(
            req.GetCompactionRangeCountByCleanupScore());
        for (const auto& r: topRanges) {
            auto* out = stats->AddCompactionRangeStats();
            out->SetRangeId(r.RangeId);
            out->SetBlobCount(r.Stats.BlobsCount);
            out->SetDeletionCount(r.Stats.DeletionsCount);
        }
    }

    stats->SetFlushState(static_cast<ui32>(FlushState.GetOperationState()));
    stats->SetBlobIndexOpState(static_cast<ui32>(
        BlobIndexOpState.GetOperationState()));
    stats->SetCollectGarbageState(static_cast<ui32>(
        CollectGarbageState.GetOperationState()));

    const auto& followerIds = GetFileSystem().GetFollowerFileSystemIds();

    if (followerIds.empty()) {
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        ev->Get()->CallContext);

    auto actor = std::make_unique<TGetFollowerStatsActor>(
        LogTag,
        std::move(requestInfo),
        std::move(req),
        followerIds,
        std::move(response));

    auto actorId = NCloud::Register(ctx, std::move(actor));

    Y_UNUSED(actorId);
    // TODO(#1350): register actorId in WorkerActors, erase upon completion
}

}   // namespace NCloud::NFileStore::NStorage
