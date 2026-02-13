#include "tablet_counters.h"

#include <cloud/filestore/libs/diagnostics/config.h>
#include <cloud/filestore/libs/diagnostics/metrics/label.h>
#include <cloud/filestore/libs/diagnostics/metrics/operations.h>

#include <util/generic/singleton.h>

namespace NCloud::NFileStore::NStorage {

using namespace NMetrics;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

void RegisterSensor(
    IMetricsRegistry& registry,
    TString name,
    const std::atomic<i64>& source,
    EAggregationType aggrType,
    EMetricType metrType)
{
    registry.Register(
        {CreateSensor(std::move(name))},
        source,
        aggrType,
        metrType);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_COUNTER_NAME(name, category, ...)    #category "/" #name,

const char* const TIndexTabletCounters::TransactionTypeNames[] = {
    FILESTORE_TABLET_TRANSACTIONS(FILESTORE_COUNTER_NAME, Tx)
};

#undef FILESTORE_COUNTER_NAME

////////////////////////////////////////////////////////////////////////////////

TTabletCountersPtr CreateIndexTabletCounters()
{
    struct TInitializer
    {
        TTabletCountersDesc SimpleCounters = BuildTabletCounters(
            0 /* counter */,
            nullptr /* counterNames */,
            TIndexTabletCounters::TX_SIZE,
            TIndexTabletCounters::TransactionTypeNames,
            TxSimpleCounters());

        TTabletCountersDesc CumulativeCounters = BuildTabletCounters(
            0 /* counter */,
            nullptr /* counterNames */,
            TIndexTabletCounters::TX_SIZE,
            TIndexTabletCounters::TransactionTypeNames,
            TxCumulativeCounters());

        TTabletCountersDesc PercentileCounters = BuildTabletCounters(
            0 /* counter */,
            nullptr /* counterNames */,
            TIndexTabletCounters::TX_SIZE,
            TIndexTabletCounters::TransactionTypeNames,
            TxPercentileCounters());
    };

    const auto& initializer = Default<TInitializer>();

    return CreateTabletCountersWithTxTypes(
        initializer.SimpleCounters,
        initializer.CumulativeCounters,
        initializer.PercentileCounters);
}

////////////////////////////////////////////////////////////////////////////////

TTabletMetrics::TTabletMetrics(IMetricsRegistryPtr metricsRegistry)
    : StorageRegistry(CreateScopedMetricsRegistry(
        {CreateLabel("component", "storage")},
        metricsRegistry))
    , StorageFsRegistry(CreateScopedMetricsRegistry({
        CreateLabel("component", "storage_fs"),
        CreateLabel("host", "cluster")},
        metricsRegistry))
    , FsRegistry(CreateMetricsRegistryStub())
    , AggregatableFsRegistry(CreateMetricsRegistryStub())
{}

////////////////////////////////////////////////////////////////////////////////

void TTabletMetrics::Register(
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
    RegisterSensor(*registry, #name, name, aggrType, metrType)                 \
// REGISTER

#define REGISTER_AGGREGATABLE_SUM(name, metrType)                              \
    REGISTER(                                                                  \
        AggregatableFsRegistry,                                                \
        name,                                                                  \
        EAggregationType::AT_SUM,                                              \
        metrType)                                                              \
// REGISTER_AGGREGATABLE_SUM

#define REGISTER_AGGREGATABLE_SUM_EXT(name, metrName, metrType)                \
    RegisterSensor(                                                            \
        *AggregatableFsRegistry,                                               \
        metrName,                                                              \
        name,                                                                  \
        EAggregationType::AT_SUM,                                              \
        metrType)                                                              \
// REGISTER_AGGREGATABLE_SUM_EXT

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
    REGISTER_AGGREGATABLE_SUM(UsedDirectHandlesCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(SevenBytesHandlesCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(UsedLocksCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(StatefulSessionsCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(StatelessSessionsCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(ActiveSessionsCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(OrphanSessionsCount, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(SessionTimeouts, EMetricType::MT_DERIVATIVE);
    REGISTER_AGGREGATABLE_SUM(SessionCleanupAttempts, EMetricType::MT_DERIVATIVE);

    REGISTER_LOCAL(
        StrictFileSystemSizeEnforcementEnabled,
        EMetricType::MT_ABSOLUTE);
    REGISTER_LOCAL(DirectoryCreationInShardsEnabled, EMetricType::MT_ABSOLUTE);

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
        InMemoryIndexStateNodeRefsExhaustivenessCount,
        EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(
        InMemoryIndexStateNodeRefsExhaustivenessCapacity,
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

#define FILESTORE_TABLET_METRICS_REGISTER_REQUEST(name, ...)                   \
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
// FILESTORE_TABLET_METRICS_REGISTER_REQUEST

    FILESTORE_TABLET_METRICS_REQUESTS(FILESTORE_TABLET_METRICS_REGISTER_REQUEST)

#undef FILESTORE_TABLET_METRICS_REGISTER_REQUEST

    REGISTER_AGGREGATABLE_SUM_EXT(
        ListNodesExtra.RequestedBytesPrecharge,
        "ListNodes.RequestedBytesPrecharge",
        EMetricType::MT_DERIVATIVE);
    REGISTER_AGGREGATABLE_SUM_EXT(
        ListNodesExtra.PrepareAttempts,
        "ListNodes.PrepareAttempts",
        EMetricType::MT_DERIVATIVE);

    REGISTER_AGGREGATABLE_SUM_EXT(
        CompactionExtra.DudCount,
        "Compaction.DudCount",
        EMetricType::MT_DERIVATIVE);

    REGISTER_LOCAL(MaxBlobsInRange, EMetricType::MT_ABSOLUTE);
    REGISTER_LOCAL(MaxDeletionsInRange, EMetricType::MT_ABSOLUTE);
    REGISTER_LOCAL(MaxGarbageBlocksInRange, EMetricType::MT_ABSOLUTE);

    REGISTER_LOCAL(CurrentLoad, EMetricType::MT_ABSOLUTE);
    REGISTER_LOCAL(Suffer, EMetricType::MT_ABSOLUTE);
    REGISTER_AGGREGATABLE_SUM(OverloadedCount, EMetricType::MT_DERIVATIVE);

    //
    // Exporting this metric to be able to see what's our tablet actor's opinion
    // about its own CPU usage. ActorSystem's ElapsedMicrosecByActivity metric
    // should in theory give the same value but being able to verify it via
    // an explicit counter is useful.
    //

    REGISTER_AGGREGATABLE_SUM(CPUUsageMicros, EMetricType::MT_DERIVATIVE);

#undef REGISTER_LOCAL
#undef REGISTER_AGGREGATABLE_SUM
#undef REGISTER

    BusyIdleCalc.Register(&BusyTime, &IdleTime);

    Initialized = true;
}

////////////////////////////////////////////////////////////////////////////////

void TTabletMetrics::UpdatePerformanceMetrics(
    TInstant now,
    const TDiagnosticsConfig& diagConfig,
    const NProto::TFileSystem& fileSystem)
{
    const ui32 expectedParallelism = 32;
    double load = 0;
    bool suffer = false;
    auto calcSufferAndLoad = [&] (
        const TRequestPerformanceProfile& rpp,
        const TTabletRequestMetrics& rm)
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

i64 TTabletMetrics::CalculateNetworkRequestBytes(
    ui32 nonNetworkMetricsBalancingFactor)
{
    i64 sumRequestBytes =
        ReadBlob.RequestBytes + WriteBlob.RequestBytes +
        WriteData.RequestBytes +
        (DescribeData.Count + AddData.Count + ReadData.Count) *
            nonNetworkMetricsBalancingFactor;
    auto delta = sumRequestBytes - LastNetworkMetric;
    LastNetworkMetric = sumRequestBytes;
    return delta;
}

}   // namespace NCloud::NFileStore::NStorage
