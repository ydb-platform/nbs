#include "write_back_cache_stats.h"

#include <cloud/storage/core/libs/user_stats/counter/user_counter.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TBootstrap
{
    TDynamicCountersPtr Counters;
    ITimerPtr Timer;
    IWriteBackCacheStatsPtr Stats;

    TBootstrap()
        : Counters{MakeIntrusive<TDynamicCounters>()}
        , Timer{CreateWallClockTimer()}
        , Stats(CreateWriteBackCacheStats(*Counters, Timer))
    {}

    void CheckValue(const TString& name, bool derivative) const
    {
        auto counter = Counters->FindCounter(name);
        UNIT_ASSERT_C(counter, "Counter " << name << " is not found");
        UNIT_ASSERT_EQUAL_C(derivative, counter->ForDerivative(),
            "Counter " << name << " was wrong ForDerivative value");
    }

    ui64 GetValue(const TString& name) const
    {
        auto counter = Counters->FindCounter(name);
        UNIT_ASSERT_C(counter, "Counter " << name << " is not found");
        return static_cast<ui64>(counter->GetAtomic());
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TWriteBackCacheStatsTest)
{
    Y_UNIT_TEST(ShouldCreateCounters)
    {
        TBootstrap bootstrap;

        bootstrap.CheckValue("CompletedFlush", true);
        bootstrap.CheckValue("FailedFlush", true);
        bootstrap.CheckValue("NodeCount", false);
        bootstrap.CheckValue("CachedWriteRequestCount", false);
        bootstrap.CheckValue("PendingWriteRequestCount", false);
        bootstrap.CheckValue("FlushedWriteRequestCount", true);
        bootstrap.CheckValue("PersistentQueueCapacity", false);
        bootstrap.CheckValue("PersistentQueueUsedBytesCount", false);
        bootstrap.CheckValue("PersistentQueueMaxAllocationSize", false);
        bootstrap.CheckValue("PersistentQueueCorrupted", false);
        bootstrap.CheckValue("PendingTime", true);
        bootstrap.CheckValue("WaitingTime", true);
        bootstrap.CheckValue("FlushTime", true);
        bootstrap.CheckValue("MaxPendingTime", false);
        bootstrap.CheckValue("MaxWaitingTime", false);
        bootstrap.CheckValue("MaxFlushTime", false);
    }

    Y_UNIT_TEST(ShouldUpdateCounters)
    {
        TBootstrap bootstrap;

        bootstrap.Stats->IncrementCompletedFlushCount();

        bootstrap.Stats->IncrementFailedFlushCount();
        bootstrap.Stats->IncrementFailedFlushCount();

        bootstrap.Stats->SetNodeCount(4);
        bootstrap.Stats->SetNodeCount(3);

        bootstrap.Stats->SetCachedWriteRequestCount(5);
        bootstrap.Stats->SetPendingWriteRequestCount(6);

        bootstrap.Stats->SetPersistentQueueStats({
            .Capacity = 9,
            .UsedBytesCount = 8,
            .MaxAllocationSize = 7,
            .IsCorrupted = true});

        // FlushedWriteRequestCount is incremented by PostWriteRequestStats
        bootstrap.Stats->PostWriteRequestStats({
            .PendingDuration = TDuration::MicroSeconds(10),
            .WaitingDuration = TDuration::MicroSeconds(50),
            .FlushDuration = TDuration::MicroSeconds(20)
        });

        bootstrap.Stats->PostWriteRequestStats({
            .PendingDuration = TDuration::MicroSeconds(15),
            .WaitingDuration = TDuration::MicroSeconds(30),
            .FlushDuration = TDuration::MicroSeconds(25)
        });

        UNIT_ASSERT_EQUAL(1, bootstrap.GetValue("CompletedFlush"));
        UNIT_ASSERT_EQUAL(2, bootstrap.GetValue("FailedFlush"));
        UNIT_ASSERT_EQUAL(3, bootstrap.GetValue("NodeCount"));
        UNIT_ASSERT_EQUAL(5, bootstrap.GetValue("CachedWriteRequestCount"));
        UNIT_ASSERT_EQUAL(6, bootstrap.GetValue("PendingWriteRequestCount"));
        UNIT_ASSERT_EQUAL(2, bootstrap.GetValue("FlushedWriteRequestCount"));

        UNIT_ASSERT_EQUAL(9, bootstrap.GetValue("PersistentQueueCapacity"));
        UNIT_ASSERT_EQUAL(8, bootstrap.GetValue("PersistentQueueUsedBytesCount"));
        UNIT_ASSERT_EQUAL(7, bootstrap.GetValue("PersistentQueueMaxAllocationSize"));
        UNIT_ASSERT_EQUAL(1, bootstrap.GetValue("PersistentQueueCorrupted"));

        UNIT_ASSERT_EQUAL(25, bootstrap.GetValue("PendingTime"));
        UNIT_ASSERT_EQUAL(80, bootstrap.GetValue("WaitingTime"));
        UNIT_ASSERT_EQUAL(45, bootstrap.GetValue("FlushTime"));
        UNIT_ASSERT_EQUAL(0, bootstrap.GetValue("MaxPendingTime"));
        UNIT_ASSERT_EQUAL(0, bootstrap.GetValue("MaxWaitingTime"));
        UNIT_ASSERT_EQUAL(0, bootstrap.GetValue("MaxFlushTime"));

        bootstrap.Stats->UpdateStats(true);

        bootstrap.Stats->PostWriteRequestStats({
            .PendingDuration = TDuration::MicroSeconds(20),
            .WaitingDuration = TDuration::MicroSeconds(40),
            .FlushDuration = TDuration::MicroSeconds(35)
        });

        UNIT_ASSERT_EQUAL(45, bootstrap.GetValue("PendingTime"));
        UNIT_ASSERT_EQUAL(120, bootstrap.GetValue("WaitingTime"));
        UNIT_ASSERT_EQUAL(80, bootstrap.GetValue("FlushTime"));
        UNIT_ASSERT_EQUAL(15, bootstrap.GetValue("MaxPendingTime"));
        UNIT_ASSERT_EQUAL(50, bootstrap.GetValue("MaxWaitingTime"));
        UNIT_ASSERT_EQUAL(25, bootstrap.GetValue("MaxFlushTime"));
    }
}

}   // namespace NCloud::NFileStore
