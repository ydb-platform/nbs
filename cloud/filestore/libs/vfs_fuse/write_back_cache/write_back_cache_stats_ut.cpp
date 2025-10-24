#include "write_back_cache_stats.h"

#include <cloud/storage/core/libs/user_stats/counter/user_counter.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/map.h>

namespace NCloud::NFileStore::NFuse {

using namespace NMonitoring;

using EWriteDataRequestStatus = NFuse::TWriteBackCache::EWriteDataRequestStatus;
using TDynamicCountersPtr = TIntrusivePtr<TDynamicCounters>;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTimer
    : public ITimer
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
            "Counter " << name << " has wrong ForDerivative value");
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

        bootstrap.CheckValue("WriteDataRequest_Cached_InProgressCount", false);
        bootstrap.CheckValue("WriteDataRequest_Cached_Count", true);
        bootstrap.CheckValue("WriteDataRequest_Cached_TimeSumSeconds", true);
        bootstrap.CheckValue("WriteDataRequest_Cached_TimeSumUs", true);
        bootstrap.CheckValue("WriteDataRequest_Cached_MaxTime", false);

        bootstrap.CheckValue("WriteDataRequest_FlushRequested_InProgressCount", false);
        bootstrap.CheckValue("WriteDataRequest_FlushRequested_Count", true);
        bootstrap.CheckValue("WriteDataRequest_FlushRequested_TimeSumSeconds", true);
        bootstrap.CheckValue("WriteDataRequest_FlushRequested_TimeSumUs", true);
        bootstrap.CheckValue("WriteDataRequest_FlushRequested_MaxTime", false);

        bootstrap.CheckValue("WriteDataRequest_Flushing_InProgressCount", false);
        bootstrap.CheckValue("WriteDataRequest_Flushing_Count", true);
        bootstrap.CheckValue("WriteDataRequest_Flushing_TimeSumSeconds", true);
        bootstrap.CheckValue("WriteDataRequest_Flushing_TimeSumUs", true);
        bootstrap.CheckValue("WriteDataRequest_Flushing_MaxTime", false);

        bootstrap.CheckValue("WriteDataRequest_Flushed_InProgressCount", false);
        bootstrap.CheckValue("WriteDataRequest_Flushed_Count", true);
        bootstrap.CheckValue("WriteDataRequest_Flushed_TimeSumSeconds", true);
        bootstrap.CheckValue("WriteDataRequest_Flushed_TimeSumUs", true);
        bootstrap.CheckValue("WriteDataRequest_Flushed_MaxTime", false);

        bootstrap.CheckValue("ReadDataRequest_CacheMissCount", true);
        bootstrap.CheckValue("ReadDataRequest_CachePartialHitCount", true);
        bootstrap.CheckValue("ReadDataRequest_CacheFullHitCount", true);

        // There is no ReadDataRequest_Wait_InProgressCount counter because
        // it is already reported by client request metrics
        bootstrap.CheckValue("ReadDataRequest_Wait_Count", true);
        bootstrap.CheckValue("ReadDataRequest_Wait_TimeSumSeconds", true);
        bootstrap.CheckValue("ReadDataRequest_Wait_TimeSumUs", true);
        bootstrap.CheckValue("ReadDataRequest_Wait_MaxTime", false);

        bootstrap.CheckValue("PersistentQueue_RawCapacity", false);
        bootstrap.CheckValue("PersistentQueue_RawUsedBytesCount", false);
        bootstrap.CheckValue("PersistentQueue_MaxAllocationBytesCount", false);
        bootstrap.CheckValue("PersistentQueue_Corrupted", false);
    }

    Y_UNIT_TEST(ShouldUpdateCounters)
    {
        TBootstrap bootstrap;

        // Flush counters

        bootstrap.Stats->FlushStarted();
        bootstrap.Stats->FlushStarted();
        bootstrap.CheckChanges("InProgressFlushCount: 2");

        bootstrap.Stats->FlushCompleted();
        bootstrap.CheckChanges(
            "CompletedFlushCount: 1, InProgressFlushCount: 1");

        bootstrap.Stats->FlushFailed();
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
            NFuse::TWriteBackCache::EWriteDataRequestStatus Status;
        };

        const TTestCase testCases[] = {
            {"Pending", EWriteDataRequestStatus::Pending},
            {"Cached", EWriteDataRequestStatus::Cached},
            {"FlushRequested", EWriteDataRequestStatus::FlushRequested},
            {"Flushing", EWriteDataRequestStatus::Flushing},
            {"Flushed", EWriteDataRequestStatus::Flushed}};

        for (const auto& testCase: testCases) {
            bootstrap.Stats->WriteDataRequestEnteredStatus(testCase.Status);
            bootstrap.Stats->WriteDataRequestEnteredStatus(testCase.Status);
            bootstrap.Stats->WriteDataRequestEnteredStatus(testCase.Status);
            bootstrap.CheckChanges(
                "WriteDataRequest_" + testCase.Name + "_InProgressCount: 3");

            bootstrap.Stats->WriteDataRequestExitedStatus(
                testCase.Status,
                TDuration::MicroSeconds(25));
            bootstrap.CheckChanges(
                "WriteDataRequest_" + testCase.Name + "_Count: 1, " +
                "WriteDataRequest_" + testCase.Name + "_InProgressCount: 2, " +
                "WriteDataRequest_" + testCase.Name + "_TimeSumUs: 25");

            bootstrap.Stats->WriteDataRequestUpdateMinTime(
                testCase.Status,
                bootstrap.Timer->Now());
            bootstrap.CheckChanges("");

            bootstrap.Timer->Sleep(TDuration::Seconds(2));
            bootstrap.Stats->UpdateStats(false);
            bootstrap.CheckChanges(
                "WriteDataRequest_" + testCase.Name + "_MaxTime: 2000000");

            bootstrap.Stats->WriteDataRequestUpdateMinTime(
                testCase.Status,
                bootstrap.Timer->Now());

            bootstrap.Stats->UpdateStats(false);
            bootstrap.CheckChanges(
                "WriteDataRequest_" + testCase.Name + "_MaxTime: 25");

            bootstrap.Stats->WriteDataRequestExitedStatus(
                testCase.Status,
                TDuration::MicroSeconds(3000025));

            bootstrap.CheckChanges(
                "WriteDataRequest_" + testCase.Name + "_Count: 2, " +
                "WriteDataRequest_" + testCase.Name + "_InProgressCount: 1, " +
                "WriteDataRequest_" + testCase.Name + "_TimeSumSeconds: 3, " +
                "WriteDataRequest_" + testCase.Name + "_TimeSumUs: 50");

            bootstrap.Stats->WriteDataRequestUpdateMinTime(
                testCase.Status,
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
            "WriteDataRequest_FlushRequested_MaxTime: 0, "
            "WriteDataRequest_Flushed_MaxTime: 0, "
            "WriteDataRequest_Flushing_MaxTime: 0, "
            "WriteDataRequest_Pending_MaxTime: 0");

        for (const auto& testCase: testCases) {
            bootstrap.Stats->WriteDataRequestUpdateMinTime(
                testCase.Status,
                bootstrap.Timer->Now() - TDuration::Seconds(1));
        }
        bootstrap.Stats->UpdateStats(false);
        // MaxTime will have non-zero value - just update it without checking
        bootstrap.UpdateChanges();

        // Read request stats

        bootstrap.Stats->AddReadDataStats(
            IWriteBackCacheStats::EReadDataRequestCacheStatus::Miss,
            TDuration::Seconds(1));

        bootstrap.Stats->UpdateStats(false);

        bootstrap.CheckChanges(
            "ReadDataRequest_CacheMissCount: 1, "
            "ReadDataRequest_Wait_Count: 1, "
            "ReadDataRequest_Wait_MaxTime: 1000000, "
            "ReadDataRequest_Wait_TimeSumSeconds: 1");

        bootstrap.Stats->AddReadDataStats(
            IWriteBackCacheStats::EReadDataRequestCacheStatus::PartialHit,
            TDuration::MicroSeconds(50));

        bootstrap.Stats->UpdateStats(false);

        bootstrap.CheckChanges(
            "ReadDataRequest_CachePartialHitCount: 1, "
            "ReadDataRequest_Wait_Count: 2, "
            "ReadDataRequest_Wait_TimeSumUs: 50");

        bootstrap.Stats->AddReadDataStats(
            IWriteBackCacheStats::EReadDataRequestCacheStatus::FullHit,
            TDuration::Seconds(2));

        bootstrap.Stats->UpdateStats(false);

        bootstrap.CheckChanges(
            "ReadDataRequest_CacheFullHitCount: 1, "
            "ReadDataRequest_Wait_Count: 3, "
            "ReadDataRequest_Wait_MaxTime: 2000000, "
            "ReadDataRequest_Wait_TimeSumSeconds: 3");

        // Reset

        bootstrap.Stats->ResetNonDerivativeCounters();
        bootstrap.CheckChanges(
            "InProgressFlushCount: 0, NodeCount: 0, "
            "ReadDataRequest_Wait_MaxTime: 0, "
            "WriteDataRequest_Cached_InProgressCount: 0, "
            "WriteDataRequest_Cached_MaxTime: 0, "
            "WriteDataRequest_FlushRequested_InProgressCount: 0, "
            "WriteDataRequest_FlushRequested_MaxTime: 0, "
            "WriteDataRequest_Flushed_InProgressCount: 0, "
            "WriteDataRequest_Flushed_MaxTime: 0, "
            "WriteDataRequest_Flushing_InProgressCount: 0, "
            "WriteDataRequest_Flushing_MaxTime: 0, "
            "WriteDataRequest_Pending_InProgressCount: 0, "
            "WriteDataRequest_Pending_MaxTime: 0"
        );

        // Check that MaxTime calculators are reset
        bootstrap.Stats->UpdateStats(false);
        bootstrap.CheckChanges("");
    }
}

}   // namespace NCloud::NFileStore::NFuse
