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

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .With(counters)
            .Build();

        const TString uuid = "uuid-1.1";
        const auto agentConfig = AgentConfig(1, { Device("dev-1", uuid) });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> affectedDisks;
            TVector<TString> disksToReallocate;

            UNIT_ASSERT_SUCCESS(state.RegisterAgent(
                db,
                agentConfig,
                Now(),
                &affectedDisks,
                &disksToReallocate
            ));
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
}

}   // namespace NCloud::NBlockStore::NStorage
