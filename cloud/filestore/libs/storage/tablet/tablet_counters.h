#pragma once

#include "public.h"

#include "tablet_tx.h"

#include <cloud/filestore/libs/diagnostics/metrics/histogram.h>
#include <cloud/filestore/libs/diagnostics/metrics/public.h>
#include <cloud/filestore/libs/diagnostics/metrics/window_calculator.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/core/tablet_counters.h>
#include <cloud/filestore/libs/storage/model/block_buffer.h>

#include <cloud/storage/core/libs/diagnostics/busy_idle_calculator.h>

#include <contrib/ydb/core/tablet/tablet_counters_protobuf.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_TABLET_STATS(xxx, ...)                                       \
    xxx(UsedNodesCount,         __VA_ARGS__)                                   \
    xxx(UsedSessionsCount,      __VA_ARGS__)                                   \
    xxx(UsedHandlesCount,       __VA_ARGS__)                                   \
    xxx(UsedLocksCount,         __VA_ARGS__)                                   \
    xxx(UsedBlocksCount,        __VA_ARGS__)                                   \
                                                                               \
    xxx(FreshBlocksCount,           __VA_ARGS__)                               \
    xxx(MixedBlocksCount,           __VA_ARGS__)                               \
    xxx(MixedBlobsCount,            __VA_ARGS__)                               \
    xxx(DeletionMarkersCount,       __VA_ARGS__)                               \
    xxx(GarbageQueueSize,           __VA_ARGS__)                               \
    xxx(GarbageBlocksCount,         __VA_ARGS__)                               \
    xxx(CheckpointNodesCount,       __VA_ARGS__)                               \
    xxx(CheckpointBlocksCount,      __VA_ARGS__)                               \
    xxx(CheckpointBlobsCount,       __VA_ARGS__)                               \
    xxx(FreshBytesCount,            __VA_ARGS__)                               \
    xxx(DeletedFreshBytesCount,     __VA_ARGS__)                               \
    xxx(LastCollectCommitId,        __VA_ARGS__)                               \
    xxx(LargeDeletionMarkersCount,  __VA_ARGS__)                               \
// FILESTORE_TABLET_STATS

////////////////////////////////////////////////////////////////////////////////

struct TIndexTabletCounters
{
    enum ETransactionType
    {
#define FILESTORE_TRANSACTION_TYPE(name, ...)      TX_##name,

        FILESTORE_TABLET_TRANSACTIONS(FILESTORE_TRANSACTION_TYPE)
        TX_SIZE

#undef FILESTORE_TRANSACTION_TYPE
    };
    static const char* const TransactionTypeNames[TX_SIZE];
};

////////////////////////////////////////////////////////////////////////////////

using TTabletCountersPtr = std::unique_ptr<NKikimr::TTabletCountersWithTxTypes>;
TTabletCountersPtr CreateIndexTabletCounters();

////////////////////////////////////////////////////////////////////////////////

struct TTabletMetrics
{
    bool Initialized{false};

    std::atomic<i64> TotalBytesCount{0};
    std::atomic<i64> UsedBytesCount{0};
    std::atomic<i64> AggregateUsedBytesCount{0};

    std::atomic<i64> TotalNodesCount{0};
    std::atomic<i64> UsedNodesCount{0};
    std::atomic<i64> AggregateUsedNodesCount{0};

    std::atomic<i64> UsedSessionsCount{0};
    std::atomic<i64> UsedHandlesCount{0};
    std::atomic<i64> UsedDirectHandlesCount{0};
    std::atomic<i64> SevenBytesHandlesCount{0};
    std::atomic<i64> UsedLocksCount{0};

    std::atomic<i64> StrictFileSystemSizeEnforcementEnabled{0};
    std::atomic<i64> DirectoryCreationInShardsEnabled{0};

    // Session stats
    std::atomic<i64> StatefulSessionsCount{0};
    std::atomic<i64> StatelessSessionsCount{0};
    std::atomic<i64> ActiveSessionsCount{0};
    std::atomic<i64> OrphanSessionsCount{0};
    std::atomic<i64> SessionTimeouts{0};
    std::atomic<i64> SessionCleanupAttempts{0};

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
    // Read-only transactions that used fast path (in-memory index state)
    std::atomic<i64> InMemoryIndexStateROCacheHitCount{0};
    // Read-only transactions that used slow path
    std::atomic<i64> InMemoryIndexStateROCacheMissCount{0};
    // Read-write transactions
    std::atomic<i64> InMemoryIndexStateRWCount{0};

    std::atomic<i64> InMemoryIndexStateNodesCount;
    std::atomic<i64> InMemoryIndexStateNodesCapacity;
    std::atomic<i64> InMemoryIndexStateNodeRefsCount;
    std::atomic<i64> InMemoryIndexStateNodeRefsCapacity;
    std::atomic<i64> InMemoryIndexStateNodeAttrsCount;
    std::atomic<i64> InMemoryIndexStateNodeAttrsCapacity;
    std::atomic<i64> InMemoryIndexStateNodeRefsExhaustivenessCount;
    std::atomic<i64> InMemoryIndexStateNodeRefsExhaustivenessCapacity;
    std::atomic<i64> InMemoryIndexStateIsExhaustive;

    // Mixed index in-memory stats
    std::atomic<i64> MixedIndexLoadedRanges{0};
    std::atomic<i64> MixedIndexOffloadedRanges{0};

    // Data stats
    std::atomic<i64> FreshBytesCount{0};
    std::atomic<i64> FreshBytesItemCount{0};
    std::atomic<i64> DeletedFreshBytesCount{0};
    std::atomic<i64> MixedBytesCount{0};
    std::atomic<i64> MixedBlobsCount{0};
    std::atomic<i64> DeletionMarkersCount{0};
    std::atomic<i64> LargeDeletionMarkersCount{0};
    std::atomic<i64> GarbageQueueSize{0};
    std::atomic<i64> GarbageBytesCount{0};
    std::atomic<i64> FreshBlocksCount{0};
    std::atomic<i64> CMMixedBlobsCount{0};
    std::atomic<i64> CMDeletionMarkersCount{0};
    std::atomic<i64> CMGarbageBlocksCount{0};

    // Backpressure Write throttling
    std::atomic<i64> IsWriteAllowed{0};
    std::atomic<i64> FlushBackpressureValue{0};
    std::atomic<i64> FlushBackpressureThreshold{0};
    std::atomic<i64> FlushBytesBackpressureValue{0};
    std::atomic<i64> FlushBytesBackpressureThreshold{0};
    std::atomic<i64> CompactionBackpressureValue{0};
    std::atomic<i64> CompactionBackpressureThreshold{0};
    std::atomic<i64> CleanupBackpressureValue{0};
    std::atomic<i64> CleanupBackpressureThreshold{0};

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

    // Tablet-specific stats
    std::atomic<i64> TabletStartTimestamp{0};

    // Blob compression stats
    std::atomic<i64> UncompressedBytesWritten{0};
    std::atomic<i64> CompressedBytesWritten{0};

    // Opened nodes stats
    std::atomic<i64> NodesOpenForWritingBySingleSession{0};
    std::atomic<i64> NodesOpenForWritingByMultipleSessions{0};
    std::atomic<i64> NodesOpenForReadingBySingleSession{0};
    std::atomic<i64> NodesOpenForReadingByMultipleSessions{0};

    std::atomic<i64> OrphanNodesCount{0};

    NMetrics::TDefaultWindowCalculator MaxUsedQuota{0};
    using TLatHistogram =
        NMetrics::THistogram<NMetrics::EHistUnit::HU_TIME_MICROSECONDS>;
    TLatHistogram ReadDataPostponed;
    TLatHistogram WriteDataPostponed;

    struct TRequestMetrics
    {
        std::atomic<i64> Count = 0;
        std::atomic<i64> RequestBytes = 0;
        std::atomic<i64> TimeSumUs = 0;
        TLatHistogram Time;

        ui64 PrevCount = 0;
        ui64 PrevRequestBytes = 0;
        ui64 PrevTimeSumUs = 0;
        TInstant PrevTs;

        void Update(ui64 requestCount, ui64 requestBytes, TDuration d)
        {
            Count.fetch_add(requestCount, std::memory_order_relaxed);
            RequestBytes.fetch_add(requestBytes, std::memory_order_relaxed);
            TimeSumUs.fetch_add(
                d.MicroSeconds(),
                std::memory_order_relaxed);
            Time.Record(d);
        }

        void UpdatePrev(TInstant now)
        {
            PrevCount = Count.load(std::memory_order_relaxed);
            PrevRequestBytes = RequestBytes.load(std::memory_order_relaxed);
            PrevTimeSumUs = TimeSumUs.load(std::memory_order_relaxed);
            PrevTs = now;
        }

        double RPS(TInstant now) const
        {
            return Rate(now, Count, PrevCount);
        }

        double Throughput(TInstant now) const
        {
            return Rate(now, RequestBytes, PrevRequestBytes);
        }

        double AverageSecondsPerSecond(TInstant now) const
        {
            return Rate(now, TimeSumUs, PrevTimeSumUs) * 1e-6;
        }

        ui64 AverageRequestSize() const
        {
            const auto requestCount =
                Count.load(std::memory_order_relaxed) - PrevCount;
            if (!requestCount) {
                return 0;
            }

            const auto requestBytes =
                RequestBytes.load(std::memory_order_relaxed)
                - PrevRequestBytes;
            return requestBytes / requestCount;
        }

        TDuration AverageLatency() const
        {
            const auto requestCount =
                Count.load(std::memory_order_relaxed) - PrevCount;
            if (!requestCount) {
                return TDuration::Zero();
            }

            const auto timeSumUs =
                TimeSumUs.load(std::memory_order_relaxed) - PrevTimeSumUs;
            return TDuration::MicroSeconds(timeSumUs / requestCount);
        }

    private:
        double Rate(
            TInstant now,
            const std::atomic<i64>& counter,
            ui64 prevCounter) const
        {
            if (!PrevTs) {
                return 0;
            }

            auto micros = (now - PrevTs).MicroSeconds();
            if (!micros) {
                return 0;
            }

            auto cur = counter.load(std::memory_order_relaxed);
            return (cur - prevCounter) * 1'000'000. / micros;
        }
    };

    struct TCompactionMetrics: TRequestMetrics
    {
        std::atomic<i64> DudCount{0};
    };

    struct TListNodesMetrics: TRequestMetrics
    {
        std::atomic<i64> RequestedBytesPrecharge{0};
        std::atomic<i64> PrepareAttempts{0};
    };

    // internal requests
    TRequestMetrics ReadBlob;
    TRequestMetrics WriteBlob;
    TRequestMetrics PatchBlob;

    // private requests
    TRequestMetrics DescribeData;
    TRequestMetrics GenerateBlobIds;
    TRequestMetrics AddData;
    TRequestMetrics GetStorageStats;
    TRequestMetrics GetNodeAttrBatch;
    TRequestMetrics RenameNodeInDestination;
    TRequestMetrics PrepareUnlinkDirectoryNodeInShard;
    TRequestMetrics AbortUnlinkDirectoryNodeInShard;

    // public requests
    TRequestMetrics ReadData;
    TRequestMetrics WriteData;
    TListNodesMetrics ListNodes;
    TRequestMetrics GetNodeAttr;
    TRequestMetrics CreateHandle;
    TRequestMetrics DestroyHandle;
    TRequestMetrics CreateNode;
    TRequestMetrics RenameNode;
    TRequestMetrics UnlinkNode;
    TRequestMetrics StatFileStore;
    TRequestMetrics GetNodeXAttr;

    // background requests
    TCompactionMetrics Compaction;
    TRequestMetrics Cleanup;
    TRequestMetrics Flush;
    TRequestMetrics FlushBytes;
    TRequestMetrics TrimBytes;
    TRequestMetrics CollectGarbage;

    i64 LastNetworkMetric = 0;

    // Compaction/Cleanup stats
    std::atomic<i64> MaxBlobsInRange{0};
    std::atomic<i64> MaxDeletionsInRange{0};
    std::atomic<i64> MaxGarbageBlocksInRange{0};

    // performance evaluation
    std::atomic<i64> CurrentLoad{0};
    std::atomic<i64> Suffer{0};
    std::atomic<i64> OverloadedCount{0};

    TInstant PrevCPUUsageMicrosTs;
    std::atomic<i64> CPUUsageMicros{0};
    i64 CPUUsageRate = 0;

    const NMetrics::IMetricsRegistryPtr StorageRegistry;
    const NMetrics::IMetricsRegistryPtr StorageFsRegistry;

    NMetrics::IMetricsRegistryPtr FsRegistry;
    NMetrics::IMetricsRegistryPtr AggregatableFsRegistry;

    explicit TTabletMetrics(NMetrics::IMetricsRegistryPtr metricsRegistry);

    void Register(
        const TString& fsId,
        const TString& cloudId,
        const TString& folderId,
        const TString& mediaKind);

    void UpdatePerformanceMetrics(
        TInstant now,
        const TDiagnosticsConfig& diagConfig,
        const NProto::TFileSystem& fileSystem);

    i64 CalculateNetworkRequestBytes(ui32 nonNetworkMetricsBalancingFactor);
};

}   // namespace NCloud::NFileStore::NStorage
