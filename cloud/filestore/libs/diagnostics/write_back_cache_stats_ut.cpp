#include "write_back_cache_stats.h"

#include <cloud/storage/core/libs/user_stats/counter/user_counter.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/map.h>

namespace NCloud::NFileStore {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTimer:
    public ITimer
{
    TInstant NowValue = TInstant::Now();

    TInstant Now() override
    {
        return NowValue;
    }

    void Sleep(TDuration duration) override
    {
        NowValue += duration;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TBootstrap
{
    TDynamicCountersPtr Counters;
    ITimerPtr Timer;
    IWriteBackCacheStatsPtr Stats;
    TMap<TString, ui64> Values;

    TBootstrap()
        : Counters{MakeIntrusive<TDynamicCounters>()}
        , Timer(std::make_unique<TTimer>())
        , Stats(CreateWriteBackCacheStats(*Counters, Timer))
    {
        // Initialize Values
        UpdateChanges();
    }

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

    TString UpdateChanges()
    {
        auto snapshot = Counters->ReadSnapshot();

        TStringBuilder sb;
        for (const auto& counter: snapshot) {
            UNIT_ASSERT_EQUAL("sensor", counter.first.LabelName);
            auto& value = Values[counter.first.LabelValue];
            auto newValue = GetValue(counter.first.LabelValue);

            if (value != newValue) {
                if (!sb.empty()) {
                    sb << ", ";
                }
                sb << counter.first.LabelValue << ": " << newValue;
            }
            value = newValue;
        }

        return sb;
    }

    void CheckChanges(const TString& expected)
    {
        auto actual = UpdateChanges();
        UNIT_ASSERT_VALUES_EQUAL_C(expected, actual,
            "Expected changes: " << expected << ", actual changes: " << actual);
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TWriteBackCacheStatsTest)
{
    Y_UNIT_TEST(ShouldCreateCounters)
    {
        TBootstrap bootstrap;
        bootstrap.Counters->ReadSnapshot();

        bootstrap.CheckValue("InProgressFlushCount", false);
        bootstrap.CheckValue("CompletedFlushCount", true);
        bootstrap.CheckValue("FailedFlushCount", true);

        bootstrap.CheckValue("NodeCount", false);

        bootstrap.CheckValue("WriteDataRequest_Pending_InProgressCount", false);
        bootstrap.CheckValue("WriteDataRequest_Pending_Count", true);
        bootstrap.CheckValue("WriteDataRequest_Pending_TimeSumSeconds", true);
        bootstrap.CheckValue("WriteDataRequest_Pending_TimeSumUs", true);
        bootstrap.CheckValue("WriteDataRequest_Pending_MaxTime", false);

        bootstrap.CheckValue("WriteDataRequest_Wait_InProgressCount", false);
        bootstrap.CheckValue("WriteDataRequest_Wait_Count", true);
        bootstrap.CheckValue("WriteDataRequest_Wait_TimeSumSeconds", true);
        bootstrap.CheckValue("WriteDataRequest_Wait_TimeSumUs", true);
        bootstrap.CheckValue("WriteDataRequest_Wait_MaxTime", false);

        bootstrap.CheckValue("WriteDataRequest_Flush_InProgressCount", false);
        bootstrap.CheckValue("WriteDataRequest_Flush_Count", true);
        bootstrap.CheckValue("WriteDataRequest_Flush_TimeSumSeconds", true);
        bootstrap.CheckValue("WriteDataRequest_Flush_TimeSumUs", true);
        bootstrap.CheckValue("WriteDataRequest_Flush_MaxTime", false);

        bootstrap.CheckValue("WriteDataRequest_Evict_InProgressCount", false);
        bootstrap.CheckValue("WriteDataRequest_Evict_Count", true);
        bootstrap.CheckValue("WriteDataRequest_Evict_TimeSumSeconds", true);
        bootstrap.CheckValue("WriteDataRequest_Evict_TimeSumUs", true);
        bootstrap.CheckValue("WriteDataRequest_Evict_MaxTime", false);

        bootstrap.CheckValue("WriteDataRequest_Cached_InProgressCount", false);
        bootstrap.CheckValue("WriteDataRequest_Cached_Count", true);
        bootstrap.CheckValue("WriteDataRequest_Cached_TimeSumSeconds", true);
        bootstrap.CheckValue("WriteDataRequest_Cached_TimeSumUs", true);
        bootstrap.CheckValue("WriteDataRequest_Cached_MaxTime", false);

        bootstrap.CheckValue("ReadDataRequest_CacheMissCount", true);
        bootstrap.CheckValue("ReadDataRequest_CachePartialHitCount", true);
        bootstrap.CheckValue("ReadDataRequest_CacheFullHitCount", true);

        // There is no ReadDataRequest_Wait_InProgressCount counter because
        // it is already reported by client request metrics
        bootstrap.CheckValue("ReadDataRequest_Wait_Count", true);
        bootstrap.CheckValue("ReadDataRequest_Wait_TimeSumSeconds", true);
        bootstrap.CheckValue("ReadDataRequest_Wait_TimeSumUs", true);
        bootstrap.CheckValue("ReadDataRequest_Wait_MaxTime", false);

        bootstrap.CheckValue("PersistentQueueRawCapacity", false);
        bootstrap.CheckValue("PersistentQueueRawUsedBytesCount", false);
        bootstrap.CheckValue("PersistentQueueMaxAllocationSize", false);
        bootstrap.CheckValue("PersistentQueueCorrupted", false);
    }

    Y_UNIT_TEST(ShouldUpdateCounters)
    {
        TBootstrap bootstrap;

        // Flush counters

        bootstrap.Stats->IncrementInProgressFlushCount();
        bootstrap.Stats->IncrementInProgressFlushCount();
        bootstrap.CheckChanges("InProgressFlushCount: 2");

        bootstrap.Stats->DecrementInProgressFlushCount();
        bootstrap.CheckChanges("InProgressFlushCount: 1");

        bootstrap.Stats->IncrementCompletedFlushCount();
        bootstrap.CheckChanges("CompletedFlushCount: 1");

        bootstrap.Stats->IncrementFailedFlushCount();
        bootstrap.CheckChanges("FailedFlushCount: 1");

        // Node counters

        bootstrap.Stats->IncrementNodeCount();
        bootstrap.Stats->IncrementNodeCount();
        bootstrap.CheckChanges("NodeCount: 2");

        bootstrap.Stats->DecrementNodeCount();
        bootstrap.CheckChanges("NodeCount: 1");

        // Write request stats

        struct TTestCase
        {
            TString Name;
            IWriteBackCacheStats::EWriteDataRequestState State;
        };

        const TTestCase testCases[] = {
            {"Pending", IWriteBackCacheStats::EWriteDataRequestState::Pending},
            {"Wait", IWriteBackCacheStats::EWriteDataRequestState::Wait},
            {"Flush", IWriteBackCacheStats::EWriteDataRequestState::Flush},
            {"Evict", IWriteBackCacheStats::EWriteDataRequestState::Evict},
            {"Cached", IWriteBackCacheStats::EWriteDataRequestState::Cached},
        };

        for (const auto& testCase: testCases) {
            bootstrap.Stats->IncrementInProgressWriteDataRequestCount(
                testCase.State);
            bootstrap.Stats->IncrementInProgressWriteDataRequestCount(
                testCase.State);
            bootstrap.CheckChanges(
                "WriteDataRequest_" + testCase.Name + "_InProgressCount: 2");

            bootstrap.Stats->DecrementInProgressWriteDataRequestCount(
                testCase.State);
            bootstrap.CheckChanges(
                "WriteDataRequest_" + testCase.Name + "_InProgressCount: 1");

            bootstrap.Stats->SetWriteDataRequestMinInstant(
                testCase.State,
                bootstrap.Timer->Now());
            bootstrap.CheckChanges("");

            bootstrap.Stats->AddWriteDataRequestStats(
                testCase.State,
                TDuration::MicroSeconds(25));
            bootstrap.Stats->AddWriteDataRequestStats(
                testCase.State,
                TDuration::MicroSeconds(3000025));
            bootstrap.CheckChanges(
                "WriteDataRequest_" + testCase.Name + "_Count: 2, " +
                "WriteDataRequest_" + testCase.Name + "_TimeSumSeconds: 3, " +
                "WriteDataRequest_" + testCase.Name + "_TimeSumUs: 50");

            bootstrap.Timer->Sleep(TDuration::Seconds(5));
            bootstrap.Stats->UpdateStats(false);
            bootstrap.CheckChanges(
                "WriteDataRequest_" + testCase.Name + "_MaxTime: 5000000");

            bootstrap.Stats->SetWriteDataRequestMinInstant(
                testCase.State,
                TInstant::Zero());
            bootstrap.Stats->UpdateStats(false);
            bootstrap.CheckChanges(
                "WriteDataRequest_" + testCase.Name + "_MaxTime: 3000025");
        }

        // MaxTimeCalc uses a sliding window of 15 buckets
        for (int i = 0; i < 15; i++) {
            bootstrap.Stats->UpdateStats(false);
        }

        bootstrap.CheckChanges(
            "WriteDataRequest_Cached_MaxTime: 0, "
            "WriteDataRequest_Evict_MaxTime: 0, "
            "WriteDataRequest_Flush_MaxTime: 0, "
            "WriteDataRequest_Pending_MaxTime: 0, "
            "WriteDataRequest_Wait_MaxTime: 0");

        for (const auto& testCase: testCases) {
            bootstrap.Stats->SetWriteDataRequestMinInstant(
                testCase.State,
                bootstrap.Timer->Now() - TDuration::Seconds(1));
        }
        bootstrap.Stats->UpdateStats(false);
        // MaxTime will have non-zero value - just update it without checking
        bootstrap.UpdateChanges();

        // Read request stats

        bootstrap.Stats->AddReadDataStats(
            IWriteBackCacheStats::EReadDataRequestCacheState::Miss,
            TDuration::Seconds(1));

        bootstrap.Stats->UpdateStats(false);

        bootstrap.CheckChanges(
            "ReadDataRequest_CacheMissCount: 1, "
            "ReadDataRequest_Wait_Count: 1, "
            "ReadDataRequest_Wait_MaxTime: 1000000, "
            "ReadDataRequest_Wait_TimeSumSeconds: 1");

        bootstrap.Stats->AddReadDataStats(
            IWriteBackCacheStats::EReadDataRequestCacheState::PartialHit,
            TDuration::MicroSeconds(50));

        bootstrap.Stats->UpdateStats(false);

        bootstrap.CheckChanges(
            "ReadDataRequest_CachePartialHitCount: 1, "
            "ReadDataRequest_Wait_Count: 2, "
            "ReadDataRequest_Wait_TimeSumUs: 50");

        bootstrap.Stats->AddReadDataStats(
            IWriteBackCacheStats::EReadDataRequestCacheState::FullHit,
            TDuration::Seconds(2));

        bootstrap.Stats->UpdateStats(false);

        bootstrap.CheckChanges(
            "ReadDataRequest_CacheFullHitCount: 1, "
            "ReadDataRequest_Wait_Count: 3, "
            "ReadDataRequest_Wait_MaxTime: 2000000, "
            "ReadDataRequest_Wait_TimeSumSeconds: 3");

        // Reset

        bootstrap.Stats->Reset();
        bootstrap.CheckChanges(
            "InProgressFlushCount: 0, NodeCount: 0, "
            "ReadDataRequest_Wait_MaxTime: 0, "
            "WriteDataRequest_Cached_InProgressCount: 0, "
            "WriteDataRequest_Cached_MaxTime: 0, "
            "WriteDataRequest_Evict_InProgressCount: 0, "
            "WriteDataRequest_Evict_MaxTime: 0, "
            "WriteDataRequest_Flush_InProgressCount: 0, "
            "WriteDataRequest_Flush_MaxTime: 0, "
            "WriteDataRequest_Pending_InProgressCount: 0, "
            "WriteDataRequest_Pending_MaxTime: 0, "
            "WriteDataRequest_Wait_InProgressCount: 0, "
            "WriteDataRequest_Wait_MaxTime: 0");

        // Check that MaxTime calculators are reset
        bootstrap.Stats->UpdateStats(false);
        bootstrap.CheckChanges("");
    }
}

}   // namespace NCloud::NFileStore
