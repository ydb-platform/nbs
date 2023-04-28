#include "disk_registry.h"
#include "disk_registry_actor.h"

#include <cloud/blockstore/config/disk.pb.h>

#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_env.h>
#include <cloud/blockstore/libs/storage/testlib/ss_proxy_client.h>

#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/size_literals.h>

#include <atomic>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NDiskRegistryTest;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryTest)
{
    Y_UNIT_TEST(ShouldSuspendDevices)
    {
        TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1"),
                Device("dev-2", "uuid-2"),
                Device("dev-3", "uuid-3"),
                Device("dev-4", "uuid-4")
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-1", "uuid-5"),
                Device("dev-2", "uuid-6"),
                Device("dev-3", "uuid-7"),
                Device("dev-4", "uuid-8")
            })
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .With([] {
                auto config = CreateDefaultStorageConfig();
                config.SetDiskRegistryCountersHost("test");

                return config;
            }())
            .Build();

        auto waitForCounters = [&] {
            runtime->AdvanceCurrentTime(TDuration::Seconds(20));
            runtime->DispatchEvents({}, TDuration::MilliSeconds(10));
        };

        auto counters = runtime->GetAppData(0).Counters
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "disk_registry")
            ->GetSubgroup("host", "test");

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agents[0]}));

        size_t cleanDevices = 0;

        runtime->SetObserverFunc(
            [&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvDiskRegistryPrivate::EvSecureEraseResponse) {
                    auto* msg = event->Get<TEvDiskRegistryPrivate::TEvSecureEraseResponse>();

                    cleanDevices += msg->CleanDevices;
                }

                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
            });

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            counters->GetCounter("AgentsInOnlineState")->Val());

        RegisterAgent(*runtime, 0);
        WaitForAgent(*runtime, 0);
        waitForCounters();

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            counters->GetCounter("AgentsInOnlineState")->Val());

        UNIT_ASSERT_VALUES_EQUAL(agents[0].DevicesSize(), cleanDevices);

        {
            diskRegistry.SendAllocateDiskRequest("vol0", 80_GB);
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                E_BS_DISK_ALLOCATION_FAILED,
                response->GetStatus());
        }

        for (auto& d: agents[1].GetDevices()) {
            diskRegistry.SuspendDevice(d.GetDeviceUUID());
        }

        diskRegistry.UpdateConfig(CreateRegistryConfig(1, agents));

        cleanDevices = 0;

        RegisterAgent(*runtime, 1);
        WaitForAgent(*runtime, 1);
        waitForCounters();

        UNIT_ASSERT_VALUES_EQUAL(
            2,
            counters->GetCounter("AgentsInOnlineState")->Val());

        UNIT_ASSERT_VALUES_EQUAL(0, cleanDevices);

        {
            diskRegistry.SendAllocateDiskRequest("vol0", 80_GB);
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                E_BS_DISK_ALLOCATION_FAILED,
                response->GetStatus());
        }

        diskRegistry.ResumeDevice(
            agents[1].GetAgentId(),
            agents[1].GetDevices(0).GetDeviceName());
        diskRegistry.ResumeDevice(
            agents[1].GetAgentId(),
            agents[1].GetDevices(1).GetDeviceName());

        waitForCounters();
        UNIT_ASSERT_VALUES_EQUAL(2, cleanDevices);

        for (auto& d: agents[1].GetDevices()) {
            diskRegistry.ResumeDevice(agents[1].GetAgentId(), d.GetDeviceName());
        }

        while (agents[1].DevicesSize() != cleanDevices) {
            waitForCounters();
        }

        diskRegistry.AllocateDisk("vol0", 80_GB);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
