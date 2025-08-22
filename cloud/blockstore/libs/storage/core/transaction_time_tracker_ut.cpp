#include "transaction_time_tracker.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/cputimer.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString TransactionNames[] = {
    "ReadBlocks",
    "WriteBlocks",
    "Total",
};

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
        TTransactionTimeTracker timeTracker(TransactionNames);

        timeTracker.OnStarted(1, "ReadBlocks", 0);

        timeTracker.OnStarted(
            2,
            "ReadBlocks",
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
        TTransactionTimeTracker timeTracker(TransactionNames);

        timeTracker.OnStarted(1, "WriteBlocks", 0);

        timeTracker.OnStarted(
            2,
            "WriteBlocks",
            1000 * GetCyclesPerMillisecond());

        timeTracker.OnStarted(
            3,
            "WriteBlocks",
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

        UNIT_ASSERT_VALUES_EQUAL("1", get("WriteBlocks_finished_1000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("WriteBlocks_finished_2000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("WriteBlocks_finished_5000000"));
        UNIT_ASSERT_VALUES_EQUAL("3", get("WriteBlocks_finished_Total"));
    }

    Y_UNIT_TEST(ShouldResetStatistics)
    {
        TTransactionTimeTracker timeTracker(TransactionNames);

        timeTracker.OnStarted(1, "ReadBlocks", 0);

        timeTracker.OnStarted(
            2,
            "WriteBlocks",
            1000 * GetCyclesPerMillisecond());

        timeTracker.OnStarted(
            3,
            "WriteBlocks",
            2000 * GetCyclesPerMillisecond());

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
        UNIT_ASSERT_VALUES_EQUAL("+ 1", get("ReadBlocks_inflight_Total"));
        UNIT_ASSERT_VALUES_EQUAL("2", get("WriteBlocks_finished_Total"));

        timeTracker.ResetStats();

        json = timeTracker.GetStatJson(5000 * GetCyclesPerMillisecond());
        NJson::ReadJsonTree(json, &value, true);
        DumpValues(value["stat"].GetMap());

        UNIT_ASSERT_VALUES_EQUAL("+ 1", get("ReadBlocks_inflight_Total"));
        UNIT_ASSERT_VALUES_EQUAL("0", get("WriteBlocks_finished_Total"));
    }

    Y_UNIT_TEST(ShouldGetInflightTransactionsInOrder)
    {
        TTransactionTimeTracker timeTracker(TransactionNames);

        timeTracker.OnStarted(
            3,
            "ReadBlocks",
            3000 * GetCyclesPerMillisecond());

        timeTracker.OnStarted(
            1,
            "WriteBlocks",
            1000 * GetCyclesPerMillisecond());

        timeTracker.OnStarted(
            2,
            "WriteBlocks",
            2000 * GetCyclesPerMillisecond());

        auto inflight = timeTracker.GetInflightOperations();

        UNIT_ASSERT_VALUES_EQUAL(3, inflight.size());

        UNIT_ASSERT_VALUES_EQUAL(1, inflight[0].first);
        UNIT_ASSERT_VALUES_EQUAL(
            1000 * GetCyclesPerMillisecond(),
            inflight[0].second.StartTime);
        UNIT_ASSERT_VALUES_EQUAL(
            "WriteBlocks",
            inflight[0].second.TransactionName);

        UNIT_ASSERT_VALUES_EQUAL(2, inflight[1].first);
        UNIT_ASSERT_VALUES_EQUAL(
            2000 * GetCyclesPerMillisecond(),
            inflight[1].second.StartTime);
        UNIT_ASSERT_VALUES_EQUAL(
            "WriteBlocks",
            inflight[1].second.TransactionName);

        UNIT_ASSERT_VALUES_EQUAL(3, inflight[2].first);
        UNIT_ASSERT_VALUES_EQUAL(
            3000 * GetCyclesPerMillisecond(),
            inflight[2].second.StartTime);
        UNIT_ASSERT_VALUES_EQUAL(
            "ReadBlocks",
            inflight[2].second.TransactionName);

        timeTracker.OnFinished(2, 2500 * GetCyclesPerMillisecond());
        inflight = timeTracker.GetInflightOperations();

        UNIT_ASSERT_VALUES_EQUAL(2, inflight.size());
        UNIT_ASSERT_VALUES_EQUAL(1, inflight[0].first);
        UNIT_ASSERT_VALUES_EQUAL(3, inflight[1].first);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
