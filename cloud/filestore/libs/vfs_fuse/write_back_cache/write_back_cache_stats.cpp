#include "write_back_cache_stats.h"

#include "relaxed_counters.h"

#include <cloud/filestore/libs/diagnostics/metrics/label.h>
#include <cloud/filestore/libs/diagnostics/metrics/metric.h>
#include <cloud/filestore/libs/diagnostics/metrics/registry.h>
#include <cloud/filestore/libs/diagnostics/module_stats.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

using namespace NMetrics;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCacheInternalStats
    : public std::enable_shared_from_this<TWriteBackCacheInternalStats>
    , public IWriteBackCacheInternalStats
{
private:
    std::atomic<i64> ReadDataCacheFullHit = 0;
    std::atomic<i64> ReadDataCachePartialHit = 0;
    std::atomic<i64> ReadDataCacheMiss = 0;

public:
    void IncrementReadDataCacheFullHit() override
    {
        ReadDataCacheFullHit.fetch_add(1, std::memory_order_release);
    }

    void IncrementReadDataCachePartialHit() override
    {
        ReadDataCachePartialHit.fetch_add(1, std::memory_order_release);
    }

    void IncrementReadDataCacheMiss() override
    {
        ReadDataCacheMiss.fetch_add(1, std::memory_order_release);
    }

    TWriteBackCacheInternalMetrics CreateMetrics()
    {
        auto self = shared_from_this();

        return {
            .ReadData = {
                .CacheFullHitCount = CreateMetric(
                    [self] { return self->ReadDataCacheFullHit.load(); }),
                .CachePartialHitCount = CreateMetric(
                    [self] { return self->ReadDataCachePartialHit.load(); }),
                .CacheMissCount = CreateMetric(
                    [self] { return self->ReadDataCacheMiss.load(); }),
            }};
    }
};

////////////////////////////////////////////////////////////////////////////////

class TStatsRegistry
{
private:
    IMetricsRegistry& LocalMetricsRegistry;
    IMetricsRegistry& AggregatableMetricsRegistry;

public:
    TStatsRegistry(
        IMetricsRegistry& localMetricsRegistry,
        IMetricsRegistry& aggregatableMetricsRegistry)
        : LocalMetricsRegistry(localMetricsRegistry)
        , AggregatableMetricsRegistry(aggregatableMetricsRegistry)
    {}

    void RegisterLocal(TStringBuf name, IMetricPtr metric)
    {
        LocalMetricsRegistry.Register(
            {CreateSensor(TString(name))},
            std::move(metric));
    }

    void RegisterAggregatableSum(TStringBuf name, IMetricPtr metric)
    {
        AggregatableMetricsRegistry.Register(
            {CreateSensor(TString(name))},
            std::move(metric),
            EAggregationType::AT_SUM,
            EMetricType::MT_DERIVATIVE);
    }

    void RegisterAggregatableMax(TStringBuf name, IMetricPtr metric)
    {
        AggregatableMetricsRegistry.Register(
            {CreateSensor(TString(name))},
            std::move(metric),
            EAggregationType::AT_MAX,
            EMetricType::MT_ABSOLUTE);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCacheStats: public IWriteBackCacheStats
{
private:
    const std::shared_ptr<TWriteBackCacheInternalStats>
        WriteBackCacheInternalStats =
            std::make_shared<TWriteBackCacheInternalStats>();

    const IWriteBackCacheStateStatsPtr WriteBackCacheStateStats =
        CreateWriteBackCacheStateStats();

    const INodeStateHolderStatsPtr NodeStateHolderStats =
        CreateNodeStateHolderStats();

    const IWriteDataRequestManagerStatsPtr WriteDataRequestManagerStats =
        CreateWriteDataRequestManagerStats();

    const IPersistentStorageStatsPtr PersistentStorageStats =
        CreatePersistentStorageStats();

public:
    IWriteBackCacheInternalStatsPtr
    GetWriteBackCacheInternalStats() const override
    {
        return WriteBackCacheInternalStats;
    }

    IWriteBackCacheStateStatsPtr GetWriteBackCacheStateStats() const override
    {
        return WriteBackCacheStateStats;
    }

    INodeStateHolderStatsPtr GetNodeStateHolderStats() const override
    {
        return NodeStateHolderStats;
    }

    IWriteDataRequestManagerStatsPtr
    GetWriteDataRequestManagerStats() const override
    {
        return WriteDataRequestManagerStats;
    }

    IPersistentStorageStatsPtr GetPersistentStorageStats() const override
    {
        return PersistentStorageStats;
    }

    TWriteBackCacheMetrics CreateMetrics() const override
    {
        return {
            WriteBackCacheInternalStats->CreateMetrics(),
            WriteBackCacheStateStats->CreateWriteBackCacheStateMetrics(),
            NodeStateHolderStats->CreateNodeStateHolderMetrics(),
            WriteDataRequestManagerStats
                ->CreateWriteDataRequestManagerMetrics(),
            PersistentStorageStats->CreatePersistentStorageMetrics()};
    }
};

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCacheModuleStats: public IModuleStats
{
private:
    const IWriteBackCacheStatsPtr Stats;
    const std::function<void(TInstant now)> UpdateStatsFunc;

public:
    TWriteBackCacheModuleStats(
        IWriteBackCacheStatsPtr stats,
        std::function<void(TInstant now)> updateStatsFunc)
        : Stats(std::move(stats))
        , UpdateStatsFunc(std::move(updateStatsFunc))
    {}

    TStringBuf GetName() const override
    {
        return "WriteBackCache";
    }

    void RegisterCounters(
        IMetricsRegistry& localMetricsRegistry,
        IMetricsRegistry& aggregatableMetricsRegistry) override
    {
        Stats->CreateMetrics().Register(
            localMetricsRegistry,
            aggregatableMetricsRegistry);
    }

    void UpdateStats(TInstant now) override
    {
        UpdateStatsFunc(now);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TWriteBackCacheMetrics::Register(
    NMetrics::IMetricsRegistry& localMetricsRegistry,
    NMetrics::IMetricsRegistry& aggregatableMetricsRegistry) const
{
    TStatsRegistry reg(localMetricsRegistry, aggregatableMetricsRegistry);

    // TWriteBackCacheInternalMetrics

    reg.RegisterAggregatableSum(
        "ReadData_CacheHit",
        ReadData.CacheFullHitCount);

    reg.RegisterAggregatableSum(
        "ReadData_CachePartialHit",
        ReadData.CachePartialHitCount);

    reg.RegisterAggregatableSum("ReadData_CacheMiss", ReadData.CacheMissCount);

    // TWriteBackCacheStateMetrics

    reg.RegisterLocal("Flush_InProgressCount", Flush.InProgressCount);
    reg.RegisterLocal("Flush_InProgressMaxCount", Flush.InProgressMaxCount);
    reg.RegisterLocal("Flush_CompletedCount", Flush.CompletedCount);
    reg.RegisterAggregatableSum("Flush_FailedCount", Flush.FailedCount);

    reg.RegisterAggregatableSum(
        "WriteDataRequest_DroppedCount",
        WriteDataRequestDroppedCount);

    // TNodeStateHolderMetrics

    reg.RegisterLocal("Nodes_Count", Nodes.Count);
    reg.RegisterLocal("Nodes_MaxCount", Nodes.MaxCount);

    // TWriteDataRequestManagerMetrics

    reg.RegisterLocal("PendingQueue_Count", PendingQueue.Count);

    reg.RegisterLocal("PendingQueue_MaxCount", PendingQueue.MaxCount);

    reg.RegisterLocal(
        "PendingQueue_ProcessedCount",
        PendingQueue.ProcessedCount);

    reg.RegisterLocal("PendingQueue_ProcessedTime", PendingQueue.ProcessedTime);

    reg.RegisterLocal("PendingQueue_MaxTime", PendingQueue.MaxTime);

    reg.RegisterLocal("UnflushedQueue_Count", UnflushedQueue.Count);

    reg.RegisterLocal("UnflushedQueue_MaxCount", UnflushedQueue.MaxCount);

    reg.RegisterLocal(
        "UnflushedQueue_ProcessedCount",
        UnflushedQueue.ProcessedCount);

    reg.RegisterLocal(
        "UnflushedQueue_ProcessedTime",
        UnflushedQueue.ProcessedTime);

    reg.RegisterLocal("UnflushedQueue_MaxTime", UnflushedQueue.MaxTime);

    reg.RegisterLocal("FlushedQueue_Count", FlushedQueue.Count);

    reg.RegisterLocal("FlushedQueue_MaxCount", FlushedQueue.MaxCount);

    reg.RegisterLocal(
        "FlushedQueue_ProcessedCount",
        FlushedQueue.ProcessedCount);

    // TPersistentStorageMetrics

    reg.RegisterLocal(
        "Storage_RawCapacityByteCount",
        Storage.RawCapacityByteCount);

    reg.RegisterLocal("Storage_RawUsedByteCount", Storage.RawUsedByteCount);

    reg.RegisterLocal(
        "Storage_RawUsedByteMaxCount",
        Storage.RawUsedByteMaxCount);

    reg.RegisterLocal("Storage_EntryCount", Storage.EntryCount);

    reg.RegisterLocal("Storage_EntryMaxCount", Storage.EntryMaxCount);

    reg.RegisterAggregatableMax("Storage_Corrupted", Storage.Corrupted);
}

////////////////////////////////////////////////////////////////////////////////

IWriteBackCacheStatsPtr CreateWriteBackCacheStats()
{
    return std::make_shared<TWriteBackCacheStats>();
}

IModuleStatsPtr CreateWriteBackCacheModuleStats(
    IWriteBackCacheStatsPtr stats,
    std::function<void(TInstant now)> updateStatsFunc)
{
    return std::make_shared<TWriteBackCacheModuleStats>(
        std::move(stats),
        std::move(updateStatsFunc));
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
