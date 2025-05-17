#include "transaction_time_tracker.h"

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

Y_UNIT_TEST_SUITE(TTransactionTimeTrackerTest)
{
    Y_UNIT_TEST(ShouldCountInflight)
    {
        TTransactionTimeTracker timeTracker;

        timeTracker.OnStarted(ETransactionType::ReadBlocks, 1, 0);

        timeTracker.OnStarted(
            ETransactionType::ReadBlocks,
            2,
            1000 * GetCyclesPerMillisecond());

        auto json = timeTracker.GetStatJson(2000 * GetCyclesPerMillisecond());
        NJson::TJsonValue value;
        NJson::ReadJsonTree(json, &value, true);
        DumpValues(value["stat"].GetMap());

        auto get = [&](const TString& key)
        {
            return value["stat"][key];
        };

        UNIT_ASSERT_VALUES_EQUAL("+ 1", get("ReadBlocks_inflight_1000000"));
        UNIT_ASSERT_VALUES_EQUAL("+ 1", get("ReadBlocks_inflight_2000000"));
        UNIT_ASSERT_VALUES_EQUAL("+ 2", get("ReadBlocks_inflight_Total"));
    }

    Y_UNIT_TEST(ShouldCountFinished)
    {
        TTransactionTimeTracker timeTracker;

        timeTracker.OnStarted(ETransactionType::WriteFreshBlocks, 1, 0);

        timeTracker.OnStarted(
            ETransactionType::WriteFreshBlocks,
            2,
            1000 * GetCyclesPerMillisecond());

        timeTracker.OnStarted(
            ETransactionType::WriteFreshBlocks,
            3,
            2000 * GetCyclesPerMillisecond());

        timeTracker.OnFinished(1, 3000 * GetCyclesPerMillisecond());
        timeTracker.OnFinished(2, 3000 * GetCyclesPerMillisecond());
        timeTracker.OnFinished(3, 3000 * GetCyclesPerMillisecond());

        auto json = timeTracker.GetStatJson(5000 * GetCyclesPerMillisecond());
        NJson::TJsonValue value;
        NJson::ReadJsonTree(json, &value, true);
        DumpValues(value["stat"].GetMap());

        auto get = [&](const TString& key)
        {
            return value["stat"][key];
        };

        UNIT_ASSERT_VALUES_EQUAL("1", get("WriteFreshBlocks_finished_1000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("WriteFreshBlocks_finished_2000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("WriteFreshBlocks_finished_5000000"));
        UNIT_ASSERT_VALUES_EQUAL("3", get("WriteFreshBlocks_finished_Total"));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
