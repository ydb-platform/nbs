#include "requests_time_tracker.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/cputimer.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

size_t DumpValues(const NJson::TJsonValue::TMapType& map)
{
    TMap<TString, TString> ordered;
    for (const auto& [key, val]: map) {
        ordered[key] = val.GetString();
    }

    size_t nonEmptyCount = 0;
    for (const auto& [key, val]: ordered) {
        if (val != "" && val != "0" && val != "0 B") {
            Cout << key << "=" << val << Endl;
            ++nonEmptyCount;
        }
    }
    return nonEmptyCount;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRequestsTimeTrackerTest)
{
    Y_UNIT_TEST(ShouldCountInflight)
    {
        TRequestsTimeTracker requestsTimeTracker(0);

        requestsTimeTracker.OnRequestStarted(
            TRequestsTimeTracker::ERequestType::Read,
            1,
            TBlockRange64::MakeOneBlock(0),
            0);

        requestsTimeTracker.OnRequestStarted(
            TRequestsTimeTracker::ERequestType::Read,
            2,
            TBlockRange64::MakeOneBlock(0),
            1000 * GetCyclesPerMillisecond());

        auto json = requestsTimeTracker.GetStatJson(
            2000 * GetCyclesPerMillisecond(),
            4096);
        NJson::TJsonValue value;
        NJson::ReadJsonTree(json, &value, true);
        const auto nonEmptyStatCount = DumpValues(value["stat"].GetMap());

        auto get = [&](const TString& key)
        {
            return value["stat"][key];
        };

        UNIT_ASSERT_VALUES_EQUAL("1", get("R_1_inflight_1000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("R_1_inflight_2000000"));
        UNIT_ASSERT_VALUES_EQUAL("2", get("R_1_inflight_Count"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("R_Total_inflight_1000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("R_Total_inflight_2000000"));
        UNIT_ASSERT_VALUES_EQUAL("2", get("R_Total_inflight_Count"));
        UNIT_ASSERT_VALUES_EQUAL("8.00 KiB", get("R_1_inflight_TotalSize"));
        UNIT_ASSERT_VALUES_EQUAL("8.00 KiB", get("R_Total_inflight_TotalSize"));
        UNIT_ASSERT_VALUES_EQUAL(8, nonEmptyStatCount);

        const auto nonEmptyPercentilesCount =
            DumpValues(value["percentiles"].GetMap());
        UNIT_ASSERT_VALUES_EQUAL(0, nonEmptyPercentilesCount);
    }

    Y_UNIT_TEST(ShouldCountFinishedSuccess)
    {
        TRequestsTimeTracker requestsTimeTracker(0);

        requestsTimeTracker.OnRequestStarted(
            TRequestsTimeTracker::ERequestType::Write,
            1,
            TBlockRange64::MakeOneBlock(0),
            1000 * GetCyclesPerMillisecond());

        requestsTimeTracker.OnRequestStarted(
            TRequestsTimeTracker::ERequestType::Write,
            2,
            TBlockRange64::MakeOneBlock(0),
            2000 * GetCyclesPerMillisecond());

        requestsTimeTracker.OnRequestStarted(
            TRequestsTimeTracker::ERequestType::Write,
            3,
            TBlockRange64::MakeOneBlock(0),
            3000 * GetCyclesPerMillisecond());

        auto stat = requestsTimeTracker.OnRequestFinished(
            2,
            true,
            4000 * GetCyclesPerMillisecond());
        UNIT_ASSERT_VALUES_EQUAL(
            TRequestsTimeTracker::ERequestType::Write,
            stat->RequestType);
        UNIT_ASSERT_DOUBLES_EQUAL(
            TDuration::MilliSeconds(2000).SecondsFloat(),
            stat->SuccessfulRequestStartTime.SecondsFloat(),
            1e-5);
        UNIT_ASSERT_DOUBLES_EQUAL(
            TDuration::MilliSeconds(4000).SecondsFloat(),
            stat->SuccessfulRequestFinishTime.SecondsFloat(),
            1e-5);
        UNIT_ASSERT_DOUBLES_EQUAL(
            TDuration::MilliSeconds(1000).SecondsFloat(),
            stat->FirstRequestStartTime.SecondsFloat(),
            1e-5);
        UNIT_ASSERT_VALUES_EQUAL(0, stat->FailCount);

        stat = requestsTimeTracker.OnRequestFinished(
            1,
            true,
            4000 * GetCyclesPerMillisecond());
        UNIT_ASSERT_EQUAL(std::nullopt, stat);

        stat = requestsTimeTracker.OnRequestFinished(
            3,
            true,
            4000 * GetCyclesPerMillisecond());
        UNIT_ASSERT_EQUAL(std::nullopt, stat);

        auto json = requestsTimeTracker.GetStatJson(
            6000 * GetCyclesPerMillisecond(),
            4096);
        NJson::TJsonValue value;
        NJson::ReadJsonTree(json, &value, true);

        const auto nonEmptyStatCount = DumpValues(value["stat"].GetMap());
        auto getStat = [&](const TString& key)
        {
            return value["stat"][key];
        };
        UNIT_ASSERT_VALUES_EQUAL("1", getStat("W_1_ok_1000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", getStat("W_1_ok_2000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", getStat("W_1_ok_5000000"));
        UNIT_ASSERT_VALUES_EQUAL("3", getStat("W_1_ok_Count"));
        UNIT_ASSERT_VALUES_EQUAL("1", getStat("W_Total_ok_1000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", getStat("W_Total_ok_2000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", getStat("W_Total_ok_5000000"));
        UNIT_ASSERT_VALUES_EQUAL("3", getStat("W_Total_ok_Count"));
        UNIT_ASSERT_VALUES_EQUAL("12.00 KiB", getStat("W_1_ok_TotalSize"));
        UNIT_ASSERT_VALUES_EQUAL("12.00 KiB", getStat("W_Total_ok_TotalSize"));
        UNIT_ASSERT_VALUES_EQUAL(10, nonEmptyStatCount);

        const auto nonEmptyPercentilesCount =
            DumpValues(value["percentiles"].GetMap());
        auto getPercentile = [&](const TString& key)
        {
            return value["percentiles"][key];
        };
        UNIT_ASSERT_VALUES_EQUAL("1.500s", getPercentile("W_1_ok_P50"));
        UNIT_ASSERT_VALUES_EQUAL("4.100s", getPercentile("W_1_ok_P90"));
        UNIT_ASSERT_VALUES_EQUAL("5.000s", getPercentile("W_1_ok_P100"));
        UNIT_ASSERT_VALUES_EQUAL("1.500s", getPercentile("W_Total_ok_P50"));
        UNIT_ASSERT_VALUES_EQUAL("4.100s", getPercentile("W_Total_ok_P90"));
        UNIT_ASSERT_VALUES_EQUAL("5.000s", getPercentile("W_Total_ok_P100"));
        UNIT_ASSERT_VALUES_EQUAL(24, nonEmptyPercentilesCount);
    }

    Y_UNIT_TEST(ShouldCountFinishedFail)
    {
        TRequestsTimeTracker requestsTimeTracker(0);

        requestsTimeTracker.OnRequestStarted(
            TRequestsTimeTracker::ERequestType::Zero,
            1,
            TBlockRange64::WithLength(0, 512),
            1000 * GetCyclesPerMillisecond());

        requestsTimeTracker.OnRequestStarted(
            TRequestsTimeTracker::ERequestType::Zero,
            2,
            TBlockRange64::WithLength(0, 600),
            2000 * GetCyclesPerMillisecond());

        requestsTimeTracker.OnRequestStarted(
            TRequestsTimeTracker::ERequestType::Zero,
            3,
            TBlockRange64::WithLength(0, 2000),
            3000 * GetCyclesPerMillisecond());

        auto stat = requestsTimeTracker.OnRequestFinished(
            1,
            false,
            4000 * GetCyclesPerMillisecond());
        UNIT_ASSERT_EQUAL(std::nullopt, stat);

        stat = requestsTimeTracker.OnRequestFinished(
            2,
            false,
            4000 * GetCyclesPerMillisecond());
        UNIT_ASSERT_EQUAL(std::nullopt, stat);

        stat = requestsTimeTracker.OnRequestFinished(
            3,
            false,
            4000 * GetCyclesPerMillisecond());
        UNIT_ASSERT_EQUAL(std::nullopt, stat);

        auto json = requestsTimeTracker.GetStatJson(
            6000 * GetCyclesPerMillisecond(),
            4096);
        NJson::TJsonValue value;
        NJson::ReadJsonTree(json, &value, true);

        const auto nonEmptyStatCount = DumpValues(value["stat"].GetMap());
        auto get = [&](const TString& key)
        {
            return value["stat"][key];
        };
        UNIT_ASSERT_VALUES_EQUAL("1", get("Z_512_fail_5000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("Z_512_fail_Count"));
        UNIT_ASSERT_VALUES_EQUAL("2.00 MiB", get("Z_512_fail_TotalSize"));

        UNIT_ASSERT_VALUES_EQUAL("1", get("Z_1024_fail_2000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("Z_1024_fail_Count"));
        UNIT_ASSERT_VALUES_EQUAL("2.34 MiB", get("Z_1024_fail_TotalSize"));

        UNIT_ASSERT_VALUES_EQUAL("1", get("Z_Inf_fail_1000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("Z_Inf_fail_Count"));
        UNIT_ASSERT_VALUES_EQUAL("7.81 MiB", get("Z_Inf_fail_TotalSize"));

        UNIT_ASSERT_VALUES_EQUAL("1", get("Z_Total_fail_1000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("Z_Total_fail_2000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("Z_Total_fail_5000000"));
        UNIT_ASSERT_VALUES_EQUAL("3", get("Z_Total_fail_Count"));
        UNIT_ASSERT_VALUES_EQUAL("12.16 MiB", get("Z_Total_fail_TotalSize"));

        UNIT_ASSERT_VALUES_EQUAL(14, nonEmptyStatCount);

        const auto nonEmptyPercentilesCount =
            DumpValues(value["percentiles"].GetMap());
        UNIT_ASSERT_VALUES_EQUAL(0, nonEmptyPercentilesCount);

        // Successful request
        requestsTimeTracker.OnRequestStarted(
            TRequestsTimeTracker::ERequestType::Zero,
            4,
            TBlockRange64::WithLength(0, 600),
            7000 * GetCyclesPerMillisecond());

        stat = requestsTimeTracker.OnRequestFinished(
            4,
            true,
            10000 * GetCyclesPerMillisecond());
        UNIT_ASSERT_VALUES_EQUAL(
            TRequestsTimeTracker::ERequestType::Zero,
            stat->RequestType);
        UNIT_ASSERT_DOUBLES_EQUAL(
            TDuration::MilliSeconds(7000).SecondsFloat(),
            stat->SuccessfulRequestStartTime.SecondsFloat(),
            1e-5);
        UNIT_ASSERT_DOUBLES_EQUAL(
            TDuration::MilliSeconds(10000).SecondsFloat(),
            stat->SuccessfulRequestFinishTime.SecondsFloat(),
            1e-5);
        UNIT_ASSERT_DOUBLES_EQUAL(
            TDuration::MilliSeconds(1000).SecondsFloat(),
            stat->FirstRequestStartTime.SecondsFloat(),
            1e-5);
        UNIT_ASSERT_VALUES_EQUAL(3, stat->FailCount);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
