#include "bs_group_operation_tracker.h"

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
        if (val != "" && val != "0") {
            Cout << key << "=" << val << Endl;
        }
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBSGroupOperationTimeTrackerTest)
{
    Y_UNIT_TEST(ShouldCountInflight)
    {
        TBSGroupOperationTimeTracker timeTracker;

        timeTracker.OnStarted(
            1,
            1,
            TBSGroupOperationTimeTracker::EOperationType::Write,
            0);

        timeTracker.OnStarted(
            2,
            2,
            TBSGroupOperationTimeTracker::EOperationType::Read,
            1000 * GetCyclesPerMillisecond());

        timeTracker.OnStarted(
            3,
            2,
            TBSGroupOperationTimeTracker::EOperationType::Read,
            2000 * GetCyclesPerMillisecond());

        timeTracker.OnStarted(
            4,
            4,
            TBSGroupOperationTimeTracker::EOperationType::Patch,
            1000 * GetCyclesPerMillisecond());

        auto json = timeTracker.GetStatJson(2000 * GetCyclesPerMillisecond());
        NJson::TJsonValue value;
        NJson::ReadJsonTree(json, &value, true);
        DumpValues(value["stat"].GetMap());

        auto get = [&](const TString& key)
        {
            return value["stat"][key];
        };
        UNIT_ASSERT_VALUES_EQUAL("+ 1", get("Write_1_inflight_2000000"));
        UNIT_ASSERT_VALUES_EQUAL("+ 1", get("Read_2_inflight_1000000"));
        UNIT_ASSERT_VALUES_EQUAL("+ 1", get("Patch_4_inflight_1000000"));
        UNIT_ASSERT_VALUES_EQUAL("+ 2", get("Read_2_inflight_Total"));
    }

    Y_UNIT_TEST(ShouldCountFinished)
    {
        TBSGroupOperationTimeTracker timeTracker;

        timeTracker.OnStarted(
            1,
            1,
            TBSGroupOperationTimeTracker::EOperationType::Read,
            0);
        timeTracker.OnStarted(
            2,
            2,
            TBSGroupOperationTimeTracker::EOperationType::Write,
            1000 * GetCyclesPerMillisecond());
        timeTracker.OnStarted(
            3,
            2,
            TBSGroupOperationTimeTracker::EOperationType::Patch,
            1000 * GetCyclesPerMillisecond());
        timeTracker.OnStarted(
            4,
            2,
            TBSGroupOperationTimeTracker::EOperationType::Write,
            2000 * GetCyclesPerMillisecond());

        timeTracker.OnFinished(1, 3000 * GetCyclesPerMillisecond());
        timeTracker.OnFinished(2, 3000 * GetCyclesPerMillisecond());
        timeTracker.OnFinished(3, 3000 * GetCyclesPerMillisecond());
        timeTracker.OnFinished(4, 3000 * GetCyclesPerMillisecond());

        auto json = timeTracker.GetStatJson(5000 * GetCyclesPerMillisecond());
        NJson::TJsonValue value;
        NJson::ReadJsonTree(json, &value, true);
        DumpValues(value["stat"].GetMap());

        auto get = [&](const TString& key)
        {
            return value["stat"][key];
        };

        UNIT_ASSERT_VALUES_EQUAL("1", get("Read_1_finished_5000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("Write_2_finished_2000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("Patch_2_finished_2000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("Write_2_finished_1000000"));
        UNIT_ASSERT_VALUES_EQUAL("2", get("Write_2_finished_Total"));
    }

    Y_UNIT_TEST(ShouldResetStatistics)
    {
        TBSGroupOperationTimeTracker timeTracker;

        timeTracker.OnStarted(
            1,
            1,
            TBSGroupOperationTimeTracker::EOperationType::Write,
            1000 * GetCyclesPerMillisecond());
        timeTracker.OnStarted(
            2,
            1,
            TBSGroupOperationTimeTracker::EOperationType::Write,
            1000 * GetCyclesPerMillisecond());
        timeTracker.OnStarted(
            3,
            2,
            TBSGroupOperationTimeTracker::EOperationType::Read,
            2000 * GetCyclesPerMillisecond());

        timeTracker.OnFinished(1, 3000 * GetCyclesPerMillisecond());
        timeTracker.OnFinished(2, 3000 * GetCyclesPerMillisecond());

        auto json = timeTracker.GetStatJson(5000 * GetCyclesPerMillisecond());
        NJson::TJsonValue value;
        NJson::ReadJsonTree(json, &value, true);
        DumpValues(value["stat"].GetMap());

        auto get = [&](const TString& key)
        {
            return value["stat"][key];
        };

        UNIT_ASSERT_VALUES_EQUAL("2", get("Write_1_finished_Total"));
        UNIT_ASSERT_VALUES_EQUAL("+ 1", get("Read_2_inflight_Total"));

        timeTracker.ResetStats();

        json = timeTracker.GetStatJson(5000 * GetCyclesPerMillisecond());
        NJson::ReadJsonTree(json, &value, true);
        DumpValues(value["stat"].GetMap());

        UNIT_ASSERT_VALUES_EQUAL("0", get("Write_1_finished_Total"));
        UNIT_ASSERT_VALUES_EQUAL("+ 1", get("Read_2_inflight_Total"));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
