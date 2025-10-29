#include "disk_registry.h"
#include "disk_registry_actor.h"

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/notify/iface/notify.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_env.h>
#include <cloud/blockstore/libs/storage/testlib/ss_proxy_client.h>

#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NDiskRegistryTest;

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

auto GetBackup(TDiskRegistryClient& dr)
    -> NProto::TDiskRegistryStateBackup
{
    auto response = dr.BackupDiskRegistryState(
        true // localDB
    );

    return response->Record.GetBackup();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryTest)
{
    Y_UNIT_TEST(ShouldRestoreState)
    {
        auto runtime = TTestRuntimeBuilder()
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.SetWritableState(true);
        diskRegistry.WaitReady();

        TDiskRegistryStateSnapshot snapshot;

        NProto::TDiskConfig diskConfig;
        diskConfig.SetDiskId("Disk-1");
        snapshot.Disks.push_back(diskConfig);
        diskConfig.SetDiskId("Disk-2");
        snapshot.Disks.push_back(diskConfig);

        NProto::TAgentConfig agentConfig;
        agentConfig.SetAgentId("Agent-1");
        snapshot.Agents.push_back(agentConfig);
        agentConfig.SetAgentId("Agent-2");
        snapshot.Agents.push_back(agentConfig);

        TActorId edgeActor = runtime->AllocateEdgeActor();
        auto validatorResponse = std::make_unique<
            TEvDiskRegistryPrivate::TEvRestoreDiskRegistryValidationResponse>(
                MakeError(S_OK),
                CreateRequestInfo(
                    edgeActor,
                    0,
                    MakeIntrusive<TCallContext>(),
                    {}),
                snapshot);
        diskRegistry.SendRequest(std::move(validatorResponse));

        runtime->GrabEdgeEvent<TEvDiskRegistry::TEvRestoreDiskRegistryStateResponse>();
        {
            auto backup = GetBackup(diskRegistry);

            UNIT_ASSERT_VALUES_EQUAL(2, backup.GetDisks().size());
            UNIT_ASSERT_VALUES_EQUAL("Disk-1", backup.GetDisks()[0].GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL("Disk-2", backup.GetDisks()[1].GetDiskId());

            UNIT_ASSERT_VALUES_EQUAL(2, backup.GetAgents().size());
            UNIT_ASSERT_VALUES_EQUAL("Agent-1", backup.GetAgents()[0].GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL("Agent-2", backup.GetAgents()[1].GetAgentId());
        }

        // TODO: test other fields - BrokenDisks, SuspendedDevices, etc.
    }
}

}   // namespace NCloud::NBlockStore::NStorage
