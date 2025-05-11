#include "requests_time_tracker.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/cputimer.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

void DumpValues(const NJson::TJsonValue::TMapType& map)
{
    for (const auto& [key, val]: map) {
        if (val != "" && val != "0" && val != "0 B") {
            Cout << key << "=" << val << Endl;
        }
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////
Y_UNIT_TEST_SUITE(TRequestsTimeTrackerTest)
{
    Y_UNIT_TEST(ShouldCountInflight)
    {
        TRequestsTimeTracker requestsTimeTracker;

        requestsTimeTracker.OnRequestStart(
            TRequestsTimeTracker::ERequestType::Read,
            1,
            TBlockRange64::MakeOneBlock(0),
            0);

        requestsTimeTracker.OnRequestStart(
            TRequestsTimeTracker::ERequestType::Read,
            2,
            TBlockRange64::MakeOneBlock(0),
            1000 * GetCyclesPerMillisecond());

        auto json = requestsTimeTracker.GetStatJson(
            2000 * GetCyclesPerMillisecond(),
            4096);
        NJson::TJsonValue value;
        NJson::ReadJsonTree(json, &value, true);
        DumpValues(value["stat"].GetMap());

        auto get = [&](const TString& key)
        {
            return value["stat"][key];
        };

        UNIT_ASSERT_VALUES_EQUAL("1", get("R_1_inflight_1000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("R_1_inflight_2000000"));
        UNIT_ASSERT_VALUES_EQUAL("2", get("R_1_inflight_Total"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("R_Total_inflight_1000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("R_Total_inflight_2000000"));
        UNIT_ASSERT_VALUES_EQUAL("2", get("R_Total_inflight_Total"));
        UNIT_ASSERT_VALUES_EQUAL("8.00 KiB", get("R_1_inflight_TotalSize"));
        UNIT_ASSERT_VALUES_EQUAL("8.00 KiB", get("R_Total_inflight_TotalSize"));
    }

    Y_UNIT_TEST(ShouldCountFinishedSuccess)
    {
        TRequestsTimeTracker requestsTimeTracker;

        requestsTimeTracker.OnRequestStart(
            TRequestsTimeTracker::ERequestType::Write,
            1,
            TBlockRange64::MakeOneBlock(0),
            0);

        requestsTimeTracker.OnRequestStart(
            TRequestsTimeTracker::ERequestType::Write,
            2,
            TBlockRange64::MakeOneBlock(0),
            1000 * GetCyclesPerMillisecond());

        requestsTimeTracker.OnRequestStart(
            TRequestsTimeTracker::ERequestType::Write,
            3,
            TBlockRange64::MakeOneBlock(0),
            2000 * GetCyclesPerMillisecond());

        requestsTimeTracker.OnRequestFinished(
            1,
            true,
            3000 * GetCyclesPerMillisecond());
        requestsTimeTracker.OnRequestFinished(
            2,
            true,
            3000 * GetCyclesPerMillisecond());
        requestsTimeTracker.OnRequestFinished(
            3,
            true,
            3000 * GetCyclesPerMillisecond());

        auto json = requestsTimeTracker.GetStatJson(
            5000 * GetCyclesPerMillisecond(),
            4096);
        NJson::TJsonValue value;
        NJson::ReadJsonTree(json, &value, true);
        DumpValues(value["stat"].GetMap());

        auto get = [&](const TString& key)
        {
            return value["stat"][key];
        };

        UNIT_ASSERT_VALUES_EQUAL("1", get("W_1_ok_1000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("W_1_ok_2000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("W_1_ok_5000000"));
        UNIT_ASSERT_VALUES_EQUAL("3", get("W_1_ok_Total"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("W_Total_ok_1000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("W_Total_ok_2000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("W_Total_ok_5000000"));
        UNIT_ASSERT_VALUES_EQUAL("3", get("W_Total_ok_Total"));
        UNIT_ASSERT_VALUES_EQUAL("12.00 KiB", get("W_1_ok_TotalSize"));
        UNIT_ASSERT_VALUES_EQUAL("12.00 KiB", get("W_Total_ok_TotalSize"));
    }

    Y_UNIT_TEST(ShouldCountFinishedFail)
    {
        TRequestsTimeTracker requestsTimeTracker;

        requestsTimeTracker.OnRequestStart(
            TRequestsTimeTracker::ERequestType::Zero,
            1,
            TBlockRange64::WithLength(0, 512),
            0);

        requestsTimeTracker.OnRequestStart(
            TRequestsTimeTracker::ERequestType::Zero,
            2,
            TBlockRange64::WithLength(0, 600),
            1000 * GetCyclesPerMillisecond());

        requestsTimeTracker.OnRequestStart(
            TRequestsTimeTracker::ERequestType::Zero,
            3,
            TBlockRange64::WithLength(0, 2000),
            2000 * GetCyclesPerMillisecond());

        requestsTimeTracker.OnRequestFinished(
            1,
            false,
            3000 * GetCyclesPerMillisecond());
        requestsTimeTracker.OnRequestFinished(
            2,
            false,
            3000 * GetCyclesPerMillisecond());
        requestsTimeTracker.OnRequestFinished(
            3,
            false,
            3000 * GetCyclesPerMillisecond());

        auto json = requestsTimeTracker.GetStatJson(
            5000 * GetCyclesPerMillisecond(),
            4096);
        NJson::TJsonValue value;
        NJson::ReadJsonTree(json, &value, true);
        DumpValues(value["stat"].GetMap());

        auto get = [&](const TString& key)
        {
            return value["stat"][key];
        };

        UNIT_ASSERT_VALUES_EQUAL("1", get("Z_512_fail_5000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("Z_512_fail_Total"));
        UNIT_ASSERT_VALUES_EQUAL("2.00 MiB", get("Z_512_fail_TotalSize"));

        UNIT_ASSERT_VALUES_EQUAL("1", get("Z_1024_fail_2000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("Z_1024_fail_Total"));
        UNIT_ASSERT_VALUES_EQUAL("2.34 MiB", get("Z_1024_fail_TotalSize"));

        UNIT_ASSERT_VALUES_EQUAL("1", get("Z_Inf_fail_1000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("Z_Inf_fail_Total"));
        UNIT_ASSERT_VALUES_EQUAL("7.81 MiB", get("Z_Inf_fail_TotalSize"));

        UNIT_ASSERT_VALUES_EQUAL("1", get("Z_Total_fail_1000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("Z_Total_fail_2000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("Z_Total_fail_5000000"));
        UNIT_ASSERT_VALUES_EQUAL("3", get("Z_Total_fail_Total"));
        UNIT_ASSERT_VALUES_EQUAL("12.16 MiB", get("Z_Total_fail_TotalSize"));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
