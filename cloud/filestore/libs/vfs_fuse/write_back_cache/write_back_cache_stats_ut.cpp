#include "write_back_cache_stats.h"

#include <cloud/filestore/libs/diagnostics/filesystem_counters.h>
#include <cloud/filestore/libs/diagnostics/metrics/label.h>
#include <cloud/filestore/libs/diagnostics/metrics/registry.h>
#include <cloud/filestore/libs/diagnostics/module_stats.h>

#include <cloud/storage/core/libs/common/timer_test.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

using namespace NMetrics;
using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TMetric: public IMetric
{
private:
    std::shared_ptr<i64> Multiplier;
    const i64 Value;

public:
    explicit TMetric(std::shared_ptr<i64> multiplicator, i64 value)
        : Multiplier(std::move(multiplicator))
        , Value(value)
    {}

    i64 Get() const override
    {
        return Value * (*Multiplier);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMetricFactory
{
private:
    std::shared_ptr<i64> Multiplier = std::make_shared<i64>(0);

public:
    void SetMultiplier(i64 value)
    {
        *Multiplier = value;
    }

    TWriteBackCacheMetrics Create()
    {
        return {
            TWriteBackCacheInternalMetrics{
                .ReadData =
                    {
                        .CacheFullHitCount = CreateMetric(111),
                        .CachePartialHitCount = CreateMetric(112),
                        .CacheMissCount = CreateMetric(113),
                    },
            },
            TWriteBackCacheStateMetrics{
                .Flush =
                    {
                        .InProgressCount = CreateMetric(211),
                        .InProgressMaxCount = CreateMetric(212),
                        .CompletedCount = CreateMetric(213),
                        .FailedCount = CreateMetric(214),
                    },
                .WriteDataRequestDroppedCount = CreateMetric(221),
            },
            TNodeStateHolderMetrics{
                .Nodes =
                    {
                        .Count = CreateMetric(311),
                        .MaxCount = CreateMetric(312),
                    },
            },
            TWriteDataRequestManagerMetrics{
                .PendingQueue =
                    {
                        .Count = CreateMetric(411),
                        .MaxCount = CreateMetric(412),
                        .ProcessedCount = CreateMetric(413),
                        .ProcessedTime = CreateMetric(414),
                        .MaxTime = CreateMetric(415),
                    },
                .UnflushedQueue =
                    {
                        .Count = CreateMetric(421),
                        .MaxCount = CreateMetric(422),
                        .ProcessedCount = CreateMetric(423),
                        .ProcessedTime = CreateMetric(424),
                        .MaxTime = CreateMetric(425),
                    },
                .FlushedQueue =
                    {
                        .Count = CreateMetric(431),
                        .MaxCount = CreateMetric(432),
                        .ProcessedCount = CreateMetric(433),
                    },
            },
            TPersistentStorageMetrics{
                .Storage =
                    {
                        .RawCapacityByteCount = CreateMetric(511),
                        .RawUsedByteCount = CreateMetric(512),
                        .RawUsedByteMaxCount = CreateMetric(513),
                        .EntryCount = CreateMetric(514),
                        .EntryMaxCount = CreateMetric(515),
                        .Corrupted = CreateMetric(516),
                    },
            },
        };
    }

private:
    IMetricPtr CreateMetric(i64 value) const
    {
        return std::make_shared<TMetric>(Multiplier, value);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TModuleStats: public IModuleStats
{
    TWriteBackCacheMetrics Metrics;

    explicit TModuleStats(TWriteBackCacheMetrics metrics)
        : Metrics(std::move(metrics))
    {}

    TStringBuf GetName() const override
    {
        return "WriteBackCache";
    }

    void RegisterCounters(
        NMetrics::IMetricsRegistry& localMetricsRegistry,
        NMetrics::IMetricsRegistry& aggregatableMetricsRegistry) override
    {
        Metrics.Register(localMetricsRegistry, aggregatableMetricsRegistry);
    }

    void UpdateStats(TInstant now) override
    {
        Y_UNUSED(now);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TBootstrap
{
    std::shared_ptr<TTestTimer> Timer;
    TDynamicCountersPtr RootCounters;
    TDynamicCountersPtr TotalCounters;
    IFsCountersProviderPtr FsCounters;
    IModuleStatsRegistryPtr ModuleStatsRegistry;

    TBootstrap()
        : Timer(std::make_shared<TTestTimer>())
        , RootCounters(MakeIntrusive<TDynamicCounters>())
        , TotalCounters(RootCounters->GetSubgroup("component", "client"))
        , FsCounters(CreateFsCountersProvider("client", RootCounters))
        , ModuleStatsRegistry(
              CreateModuleStatsRegistry(Timer, FsCounters, TotalCounters))
    {}

    void Register(
        const TString& fileSystemId,
        TWriteBackCacheMetrics metrics) const
    {
        auto moduleStats = std::make_shared<TModuleStats>(std::move(metrics));

        ModuleStatsRegistry->Register(
            {.FileSystemId = fileSystemId,
             .ClientId = fileSystemId + "_client",
             .CloudId = fileSystemId + "_cloud",
             .FolderId = fileSystemId + "_folder",
             .SessionId = fileSystemId + "_session",
             .ModuleStats = std::move(moduleStats)});
    }

    void Unregister(const TString& fileSystemId) const
    {
        ModuleStatsRegistry->Unregister(fileSystemId + "_session");
    }

    TString DumpCounters()
    {
        TStringStream ss;
        RootCounters->OutputPlainText(ss, "");
        return ss.Str();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

constexpr auto Expected1 = R"(
component=client:

    module=WriteBackCache:
        sensor=Flush_FailedCount: 2354
        sensor=ReadData_CacheHit: 1221
        sensor=ReadData_CacheMiss: 1243
        sensor=ReadData_CachePartialHit: 1232
        sensor=Storage_Corrupted: 5160
        sensor=WriteDataRequest_DroppedCount: 2431

component=client_fs:

    host=cluster:

        filesystem=test:

            client=test_client:

                cloud=test_cloud:

                    folder=test_folder:

                        module=WriteBackCache:
                            sensor=Flush_CompletedCount: 213
                            sensor=Flush_FailedCount: 214
                            sensor=Flush_InProgressCount: 211
                            sensor=Flush_InProgressMaxCount: 212
                            sensor=FlushedQueue_Count: 431
                            sensor=FlushedQueue_MaxCount: 432
                            sensor=FlushedQueue_ProcessedCount: 433
                            sensor=Nodes_Count: 311
                            sensor=Nodes_MaxCount: 312
                            sensor=PendingQueue_Count: 411
                            sensor=PendingQueue_MaxCount: 412
                            sensor=PendingQueue_MaxTime: 415
                            sensor=PendingQueue_ProcessedCount: 413
                            sensor=PendingQueue_ProcessedTime: 414
                            sensor=ReadData_CacheHit: 111
                            sensor=ReadData_CacheMiss: 113
                            sensor=ReadData_CachePartialHit: 112
                            sensor=Storage_Corrupted: 516
                            sensor=Storage_EntryCount: 514
                            sensor=Storage_EntryMaxCount: 515
                            sensor=Storage_RawCapacityByteCount: 511
                            sensor=Storage_RawUsedByteCount: 512
                            sensor=Storage_RawUsedByteMaxCount: 513
                            sensor=UnflushedQueue_Count: 421
                            sensor=UnflushedQueue_MaxCount: 422
                            sensor=UnflushedQueue_MaxTime: 425
                            sensor=UnflushedQueue_ProcessedCount: 423
                            sensor=UnflushedQueue_ProcessedTime: 424
                            sensor=WriteDataRequest_DroppedCount: 221

        filesystem=test2:

            client=test2_client:

                cloud=test2_cloud:

                    folder=test2_folder:

                        module=WriteBackCache:
                            sensor=Flush_CompletedCount: 2130
                            sensor=Flush_FailedCount: 2140
                            sensor=Flush_InProgressCount: 2110
                            sensor=Flush_InProgressMaxCount: 2120
                            sensor=FlushedQueue_Count: 4310
                            sensor=FlushedQueue_MaxCount: 4320
                            sensor=FlushedQueue_ProcessedCount: 4330
                            sensor=Nodes_Count: 3110
                            sensor=Nodes_MaxCount: 3120
                            sensor=PendingQueue_Count: 4110
                            sensor=PendingQueue_MaxCount: 4120
                            sensor=PendingQueue_MaxTime: 4150
                            sensor=PendingQueue_ProcessedCount: 4130
                            sensor=PendingQueue_ProcessedTime: 4140
                            sensor=ReadData_CacheHit: 1110
                            sensor=ReadData_CacheMiss: 1130
                            sensor=ReadData_CachePartialHit: 1120
                            sensor=Storage_Corrupted: 5160
                            sensor=Storage_EntryCount: 5140
                            sensor=Storage_EntryMaxCount: 5150
                            sensor=Storage_RawCapacityByteCount: 5110
                            sensor=Storage_RawUsedByteCount: 5120
                            sensor=Storage_RawUsedByteMaxCount: 5130
                            sensor=UnflushedQueue_Count: 4210
                            sensor=UnflushedQueue_MaxCount: 4220
                            sensor=UnflushedQueue_MaxTime: 4250
                            sensor=UnflushedQueue_ProcessedCount: 4230
                            sensor=UnflushedQueue_ProcessedTime: 4240
                            sensor=WriteDataRequest_DroppedCount: 2210
)";

constexpr auto Expected2 = R"(
component=client:

    module=WriteBackCache:
        sensor=Flush_FailedCount: 2354
        sensor=ReadData_CacheHit: 1221
        sensor=ReadData_CacheMiss: 1243
        sensor=ReadData_CachePartialHit: 1232
        sensor=Storage_Corrupted: 5160
        sensor=WriteDataRequest_DroppedCount: 2431

component=client_fs:

    host=cluster:

        filesystem=test:

        filesystem=test2:

            client=test2_client:

                cloud=test2_cloud:

                    folder=test2_folder:

                        module=WriteBackCache:
                            sensor=Flush_CompletedCount: 2130
                            sensor=Flush_FailedCount: 2140
                            sensor=Flush_InProgressCount: 2110
                            sensor=Flush_InProgressMaxCount: 2120
                            sensor=FlushedQueue_Count: 4310
                            sensor=FlushedQueue_MaxCount: 4320
                            sensor=FlushedQueue_ProcessedCount: 4330
                            sensor=Nodes_Count: 3110
                            sensor=Nodes_MaxCount: 3120
                            sensor=PendingQueue_Count: 4110
                            sensor=PendingQueue_MaxCount: 4120
                            sensor=PendingQueue_MaxTime: 4150
                            sensor=PendingQueue_ProcessedCount: 4130
                            sensor=PendingQueue_ProcessedTime: 4140
                            sensor=ReadData_CacheHit: 1110
                            sensor=ReadData_CacheMiss: 1130
                            sensor=ReadData_CachePartialHit: 1120
                            sensor=Storage_Corrupted: 5160
                            sensor=Storage_EntryCount: 5140
                            sensor=Storage_EntryMaxCount: 5150
                            sensor=Storage_RawCapacityByteCount: 5110
                            sensor=Storage_RawUsedByteCount: 5120
                            sensor=Storage_RawUsedByteMaxCount: 5130
                            sensor=UnflushedQueue_Count: 4210
                            sensor=UnflushedQueue_MaxCount: 4220
                            sensor=UnflushedQueue_MaxTime: 4250
                            sensor=UnflushedQueue_ProcessedCount: 4230
                            sensor=UnflushedQueue_ProcessedTime: 4240
                            sensor=WriteDataRequest_DroppedCount: 2210
)";

constexpr auto Expected3 = R"(
component=client:

component=client_fs:

    host=cluster:

        filesystem=test:

        filesystem=test2:

            client=test2_client:

                cloud=test2_cloud:

                    folder=test2_folder:
)";

constexpr auto Expected4 = R"(
component=client:

component=client_fs:

    host=cluster:

        filesystem=test:

        filesystem=test2:
)";

Y_UNIT_TEST_SUITE(TWriteBackCacheStatsTest)
{
    Y_UNIT_TEST(ShouldBindMetricsToDynamicCounters)
    {
        TBootstrap b;

        TMetricFactory factory;
        auto stats = factory.Create();
        b.Register("test", stats);

        TMetricFactory factory2;
        auto stats2 = factory2.Create();
        b.Register("test2", stats2);

        factory.SetMultiplier(1);
        factory2.SetMultiplier(10);

        b.ModuleStatsRegistry->UpdateStats(false);

        auto actual1 = b.DumpCounters();
        UNIT_ASSERT_VALUES_EQUAL_C(
            Expected1,
            actual1,
            "Expected:\n"
                << Expected1 << "\nActual:\n"
                << actual1);

        b.Unregister("test");

        // Ensure that:
        // - module counters are removed
        // - aggregate sum counters are not affected

        b.ModuleStatsRegistry->UpdateStats(false);

        auto actual2 = b.DumpCounters();
        UNIT_ASSERT_VALUES_EQUAL_C(
            Expected2,
            actual2,
            "Expected:\n"
                << Expected2 << "\nActual:\n"
                << actual2);

        b.FsCounters
            ->Register("test2", "test2_client", "test2_cloud", "test2_folder");

        b.Unregister("test2");

        // Ensure that:
        // - module counters are removed
        // - fs counters are not removed

        b.ModuleStatsRegistry->UpdateStats(false);

        auto actual3 = b.DumpCounters();
        UNIT_ASSERT_VALUES_EQUAL_C(
            Expected3,
            actual3,
            "Expected:\n"
                << Expected3 << "\nActual:\n"
                << actual3);

        b.FsCounters->Unregister("test2", "test2_client");

        b.ModuleStatsRegistry->UpdateStats(false);

        auto actual4 = b.DumpCounters();
        UNIT_ASSERT_VALUES_EQUAL_C(
            Expected4,
            actual4,
            "Expected:\n"
                << Expected4 << "\nActual:\n"
                << actual4);
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
