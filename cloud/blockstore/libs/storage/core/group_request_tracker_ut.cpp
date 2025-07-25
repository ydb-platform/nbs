#include "group_request_tracker.h"

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

Y_UNIT_TEST_SUITE(TGroupRequestTrackerTest)
{
    Y_UNIT_TEST(ShouldCountFinished)
    {
        TGroupOperationTimeTracker timeTracker;

        timeTracker.OnStarted(1, "Read_1", 0);

        timeTracker.OnStarted(2, "Write_2", 1000 * GetCyclesPerMillisecond());

        timeTracker.OnStarted(3, "Write_2", 2000 * GetCyclesPerMillisecond());

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

        UNIT_ASSERT_VALUES_EQUAL("1", get("Read_1_finished_5000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("Write_2_finished_2000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("Write_2_finished_1000000"));
        UNIT_ASSERT_VALUES_EQUAL("2", get("Write_2_finished_Total"));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
