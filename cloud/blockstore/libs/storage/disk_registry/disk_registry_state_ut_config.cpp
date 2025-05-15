#include "disk_registry_state.h"

#include "disk_registry_database.h"

#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_state.h>
#include <cloud/blockstore/libs/storage/testlib/test_executor.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/guid.h>
#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NDiskRegistryStateTest;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryStateConfigTest)
{
    Y_UNIT_TEST(ShouldUpdateKnownDevicesAfterConfig)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "disk_registry");

        auto statePtr = TDiskRegistryStateBuilder().With(counters).Build();
        TDiskRegistryState& state = *statePtr;

        const TString uuid = "uuid-1.1";
        const auto agentConfig = AgentConfig(1, { Device("dev-1", uuid) });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(
                state.RegisterAgent(db, agentConfig, Now()).GetError());
        });

        auto agentCounters = counters->FindSubgroup(
            "agent", agentConfig.GetAgentId());

        UNIT_ASSERT(agentCounters);
        UNIT_ASSERT(!agentCounters->FindSubgroup("device", uuid));

        UNIT_ASSERT_VALUES_EQUAL(0, state.GetDirtyDevices().size());

        {
            auto* agent = state.FindAgent(agentConfig.GetAgentId());
            UNIT_ASSERT(agent);

            UNIT_ASSERT_VALUES_EQUAL(0, agent->DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(1, agent->UnknownDevicesSize());
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> affectedDisks;
            NProto::TDiskRegistryConfig config;

            auto& agent = *config.AddKnownAgents();
            agent.SetAgentId(agentConfig.GetAgentId());
            agent.AddDevices()->SetDeviceUUID(uuid);

            UNIT_ASSERT_SUCCESS(state.UpdateConfig(
                db,
                config,
                true,   // ignore version
                affectedDisks));
        });

        {
            auto* agent = state.FindAgent(agentConfig.GetAgentId());
            UNIT_ASSERT(agent);

            UNIT_ASSERT_VALUES_EQUAL(1, agent->DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(0, agent->UnknownDevicesSize());
        }

        UNIT_ASSERT(agentCounters->FindSubgroup("device", "agent-1:dev-1"));
    }

    Y_UNIT_TEST(ShouldRemoveDeviceFromConfig)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const TVector agents {
            AgentConfig(1, {
                Device("dev-1", "uuid-1.1"),
                Device("dev-2", "uuid-1.2"),
            }),
        };

        auto statePtr = TDiskRegistryStateBuilder().WithConfig(agents).Build();
        TDiskRegistryState& state = *statePtr;

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(
                state.RegisterAgent(db, agents[0], Now()).GetError());

            UNIT_ASSERT_VALUES_EQUAL(2, state.GetDirtyDevices().size());

            UNIT_ASSERT_SUCCESS(state.SuspendDevice(db, "uuid-1.2"));
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetSuspendedDevices().size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto config = state.GetConfig();

            auto& devices = *config.MutableKnownAgents(0)->MutableDevices();

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-1.2",
                devices[devices.size() - 1].GetDeviceUUID());

            // remove uuid-1.2
            devices.RemoveLast();

            TVector<TString> affectedDisks;
            UNIT_ASSERT_SUCCESS(state.UpdateConfig(
                db,
                std::move(config),
                false, // ignoreVersion,
                affectedDisks));

            UNIT_ASSERT_VALUES_EQUAL(1, state.GetDirtyDevices().size());
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetSuspendedDevices().size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto config = state.GetConfig();

            // return uuid-1.2
            auto* device = config.MutableKnownAgents(0)->MutableDevices()->Add();
            device->SetDeviceUUID("uuid-1.2");

            TVector<TString> affectedDisks;
            UNIT_ASSERT_SUCCESS(state.UpdateConfig(
                db,
                std::move(config),
                false, // ignoreVersion,
                affectedDisks));

            UNIT_ASSERT_VALUES_EQUAL(2, state.GetDirtyDevices().size());
        });
    }
}

}   // namespace NCloud::NBlockStore::NStorage
