#include "device_operation_tracker.h"

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

TSet<TString> MakeTestAgents()
{
    return {
        "agent-1",
        "agent-2",
    };
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDeviceOperationTrackerTest)
{
    Y_UNIT_TEST(ShouldCountInflightOperations)
    {
        TDeviceOperationTracker tracker;
        tracker.UpdateAgents(MakeTestAgents());

        tracker.OnStarted(
            1,
            "agent-1",
            TDeviceOperationTracker::ERequestType::Read,
            0);
        tracker.OnStarted(
            2,
            "agent-2",
            TDeviceOperationTracker::ERequestType::Write,
            500 * GetCyclesPerMillisecond());
        tracker.OnStarted(
            3,
            "agent-1",
            TDeviceOperationTracker::ERequestType::Read,
            1000 * GetCyclesPerMillisecond());

        auto json = tracker.GetStatJson(2000 * GetCyclesPerMillisecond());
        NJson::TJsonValue value;
        NJson::ReadJsonTree(json, &value, true);
        DumpValues(value["stat"].GetMap());

        auto get = [&](const TString& key)
        {
            return value["stat"][key];
        };

        UNIT_ASSERT_VALUES_EQUAL("+ 1", get("Read_agent-1_inflight_2000000"));
        UNIT_ASSERT_VALUES_EQUAL("+ 1", get("Write_agent-2_inflight_2000000"));
        UNIT_ASSERT_VALUES_EQUAL("+ 1", get("Read_agent-1_inflight_1000000"));

        UNIT_ASSERT_VALUES_EQUAL("+ 2", get("Read_agent-1_inflight_Total"));
        UNIT_ASSERT_VALUES_EQUAL("+ 1", get("Write_agent-2_inflight_Total"));
    }

    Y_UNIT_TEST(ShouldCountFinishedOperations)
    {
        TDeviceOperationTracker tracker;
        tracker.UpdateAgents(MakeTestAgents());

        tracker.OnStarted(
            1,
            "agent-1",
            TDeviceOperationTracker::ERequestType::Read,
            0);
        tracker.OnStarted(
            2,
            "agent-1",
            TDeviceOperationTracker::ERequestType::Write,
            1000 * GetCyclesPerMillisecond());
        tracker.OnStarted(
            3,
            "agent-2",
            TDeviceOperationTracker::ERequestType::Zero,
            2000 * GetCyclesPerMillisecond());

        tracker.OnFinished(1, 3000 * GetCyclesPerMillisecond());
        tracker.OnFinished(2, 3500 * GetCyclesPerMillisecond());
        tracker.OnFinished(3, 4000 * GetCyclesPerMillisecond());

        auto json = tracker.GetStatJson(5000 * GetCyclesPerMillisecond());
        NJson::TJsonValue value;
        NJson::ReadJsonTree(json, &value, true);
        DumpValues(value["stat"].GetMap());

        auto get = [&](const TString& key)
        {
            return value["stat"][key];
        };

        UNIT_ASSERT_VALUES_EQUAL("1", get("Read_agent-1_finished_5000000"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("Write_agent-1_finished_5000000"));

        UNIT_ASSERT_VALUES_EQUAL("1", get("Zero_agent-2_finished_2000000"));

        UNIT_ASSERT_VALUES_EQUAL("1", get("Read_agent-1_finished_Total"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("Write_agent-1_finished_Total"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("Zero_agent-2_finished_Total"));
    }

    Y_UNIT_TEST(ShouldUpdateDeviceList)
    {
        TDeviceOperationTracker tracker;
        tracker.UpdateAgents(MakeTestAgents());

        TSet<TString> newAgents = {
            "agent-3",
            "agent-4",
        };
        tracker.UpdateAgents(newAgents);

        auto agents = tracker.GetAgents();
        UNIT_ASSERT_VALUES_EQUAL(2, agents.size());
        UNIT_ASSERT_VALUES_EQUAL(true, agents.contains("agent-3"));
        UNIT_ASSERT_VALUES_EQUAL(true, agents.contains("agent-4"));
    }

    Y_UNIT_TEST(ShouldResetStatistics)
    {
        TDeviceOperationTracker tracker;
        tracker.UpdateAgents(MakeTestAgents());

        tracker.OnStarted(
            1,
            "agent-1",
            TDeviceOperationTracker::ERequestType::Read,
            0);
        tracker.OnStarted(
            2,
            "agent-2",
            TDeviceOperationTracker::ERequestType::Write,
            500 * GetCyclesPerMillisecond());

        tracker.OnFinished(1, 1000 * GetCyclesPerMillisecond());

        auto json = tracker.GetStatJson(2000 * GetCyclesPerMillisecond());
        NJson::TJsonValue value;
        NJson::ReadJsonTree(json, &value, true);

        auto get = [&](const TString& key)
        {
            return value["stat"][key];
        };

        UNIT_ASSERT_VALUES_EQUAL("1", get("Read_agent-1_finished_Total"));
        UNIT_ASSERT_VALUES_EQUAL("+ 1", get("Write_agent-2_inflight_Total"));

        tracker.ResetStats();

        json = tracker.GetStatJson(2000 * GetCyclesPerMillisecond());
        NJson::ReadJsonTree(json, &value, true);

        UNIT_ASSERT_VALUES_EQUAL("0", get("Read_agent-1_finished_Total"));
        UNIT_ASSERT_VALUES_EQUAL("+ 1", get("Write_agent-2_inflight_Total"));
    }

    Y_UNIT_TEST(ShouldGetInflightOperations)
    {
        TDeviceOperationTracker tracker;
        tracker.UpdateAgents(MakeTestAgents());

        tracker.OnStarted(
            1,
            "agent-1",
            TDeviceOperationTracker::ERequestType::Read,
            1000);
        tracker.OnStarted(
            2,
            "agent-2",
            TDeviceOperationTracker::ERequestType::Write,
            2000);
        tracker.OnStarted(
            3,
            "agent-1",
            TDeviceOperationTracker::ERequestType::Zero,
            3000);

        auto inflight = tracker.GetInflightOperations();

        UNIT_ASSERT_VALUES_EQUAL(3, inflight.size());

        UNIT_ASSERT(inflight.contains(1));
        UNIT_ASSERT_VALUES_EQUAL(
            TDeviceOperationTracker::ERequestType::Read,
            inflight.at(1).RequestType);
        UNIT_ASSERT_VALUES_EQUAL("agent-1", inflight.at(1).AgentId);
        UNIT_ASSERT_VALUES_EQUAL(1000, inflight.at(1).StartTime);

        UNIT_ASSERT(inflight.contains(2));
        UNIT_ASSERT_VALUES_EQUAL(
            TDeviceOperationTracker::ERequestType::Write,
            inflight.at(2).RequestType);
        UNIT_ASSERT_VALUES_EQUAL("agent-2", inflight.at(2).AgentId);

        UNIT_ASSERT(inflight.contains(3));
        UNIT_ASSERT_VALUES_EQUAL(
            TDeviceOperationTracker::ERequestType::Zero,
            inflight.at(3).RequestType);

        tracker.OnFinished(2, 5000);

        inflight = tracker.GetInflightOperations();
        UNIT_ASSERT_VALUES_EQUAL(2, inflight.size());
        UNIT_ASSERT(!inflight.contains(2));
    }

    Y_UNIT_TEST(ShouldHandleAllRequestTypes)
    {
        TDeviceOperationTracker tracker;
        tracker.UpdateAgents(MakeTestAgents());

        tracker.OnStarted(
            1,
            "agent-1",
            TDeviceOperationTracker::ERequestType::Read,
            0);
        tracker.OnStarted(
            2,
            "agent-1",
            TDeviceOperationTracker::ERequestType::Write,
            0);
        tracker.OnStarted(
            3,
            "agent-1",
            TDeviceOperationTracker::ERequestType::Zero,
            0);
        tracker.OnStarted(
            4,
            "agent-1",
            TDeviceOperationTracker::ERequestType::Checksum,
            0);

        tracker.OnFinished(1, 1000 * GetCyclesPerMillisecond());
        tracker.OnFinished(2, 1000 * GetCyclesPerMillisecond());
        tracker.OnFinished(3, 1000 * GetCyclesPerMillisecond());
        tracker.OnFinished(4, 1000 * GetCyclesPerMillisecond());

        auto json = tracker.GetStatJson(2000 * GetCyclesPerMillisecond());
        NJson::TJsonValue value;
        NJson::ReadJsonTree(json, &value, true);

        auto get = [&](const TString& key)
        {
            return value["stat"][key];
        };

        UNIT_ASSERT_VALUES_EQUAL("1", get("Read_agent-1_finished_Total"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("Write_agent-1_finished_Total"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("Zero_agent-1_finished_Total"));
        UNIT_ASSERT_VALUES_EQUAL("1", get("Checksum_agent-1_finished_Total"));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
