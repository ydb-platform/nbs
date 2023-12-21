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

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NDiskRegistryTest;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TFixture
    : public NUnitTest::TBaseFixture
{
    const TVector<NProto::TAgentConfig> Agents {
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
            Device("dev-4", "uuid-8"),
            Device("dev-5", "uuid-9") | WithPool("local", NProto::DEVICE_POOL_KIND_LOCAL),
            Device("dev-6", "uuid-10") | WithPool("local", NProto::DEVICE_POOL_KIND_LOCAL)
        }),
        CreateAgentConfig("agent-3", {
            Device("dev-1", "uuid-11") | WithPool("local", NProto::DEVICE_POOL_KIND_LOCAL),
            Device("dev-2", "uuid-12") | WithPool("local", NProto::DEVICE_POOL_KIND_LOCAL),
            Device("dev-3", "uuid-13") | WithPool("local", NProto::DEVICE_POOL_KIND_LOCAL),
            Device("dev-4", "uuid-14") | WithPool("local", NProto::DEVICE_POOL_KIND_LOCAL)
        })
    };

    std::unique_ptr<NActors::TTestActorRuntime> Runtime;
    NMonitoring::TDynamicCounterPtr Counters;

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        Runtime = TTestRuntimeBuilder()
            .WithAgents(Agents)
            .With([] {
                auto config = CreateDefaultStorageConfig();
                config.SetDiskRegistryCountersHost("test");
                config.SetNonReplicatedDontSuspendDevices(true);

                return config;
            }())
            .Build();

        Counters = Runtime->GetAppData(0).Counters
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "disk_registry")
            ->GetSubgroup("host", "test");
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryTest)
{
    Y_UNIT_TEST_F(ShouldSuspendDevices, TFixture)
    {
        auto waitForCounters = [&] {
            Runtime->AdvanceCurrentTime(TDuration::Seconds(20));
            Runtime->DispatchEvents({}, TDuration::MilliSeconds(10));
        };

        TDiskRegistryClient diskRegistry(*Runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {Agents[0]})
            | WithPoolConfig("local", NProto::DEVICE_POOL_KIND_LOCAL, 10_GB));

        size_t cleanDevices = 0;

        Runtime->SetObserverFunc([&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvDiskRegistryPrivate::EvSecureEraseResponse) {
                    auto* msg = event->Get<TEvDiskRegistryPrivate::TEvSecureEraseResponse>();

                    cleanDevices += msg->CleanDevices;
                }

                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
            });

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            Counters->GetCounter("AgentsInOnlineState")->Val());

        RegisterAgent(*Runtime, 0);
        WaitForAgent(*Runtime, 0);
        waitForCounters();

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            Counters->GetCounter("AgentsInOnlineState")->Val());

        UNIT_ASSERT_VALUES_EQUAL(Agents[0].DevicesSize(), cleanDevices);

        {
            diskRegistry.SendAllocateDiskRequest("vol0", 80_GB);
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                E_BS_DISK_ALLOCATION_FAILED,
                response->GetStatus());
        }

        for (auto& d: Agents[1].GetDevices()) {
            diskRegistry.SuspendDevice(d.GetDeviceUUID());
        }

        diskRegistry.UpdateConfig(CreateRegistryConfig(1, Agents)
            | WithPoolConfig("local", NProto::DEVICE_POOL_KIND_LOCAL, 10_GB));

        cleanDevices = 0;

        RegisterAgent(*Runtime, 1);
        WaitForAgent(*Runtime, 1);
        waitForCounters();

        UNIT_ASSERT_VALUES_EQUAL(
            2,
            Counters->GetCounter("AgentsInOnlineState")->Val());

        UNIT_ASSERT_VALUES_EQUAL(0, cleanDevices);

        {
            diskRegistry.SendAllocateDiskRequest("vol0", 80_GB);
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                E_BS_DISK_ALLOCATION_FAILED,
                response->GetStatus());
        }

        diskRegistry.ResumeDevice(
            Agents[1].GetAgentId(),
            Agents[1].GetDevices(0).GetDeviceName());
        diskRegistry.ResumeDevice(
            Agents[1].GetAgentId(),
            Agents[1].GetDevices(1).GetDeviceName());

        waitForCounters();
        UNIT_ASSERT_VALUES_EQUAL(2, cleanDevices);

        for (auto& d: Agents[1].GetDevices()) {
            diskRegistry.ResumeDevice(Agents[1].GetAgentId(), d.GetDeviceName());
        }

        while (Agents[1].DevicesSize() != cleanDevices) {
            waitForCounters();
        }

        diskRegistry.AllocateDisk("vol0", 80_GB);
    }

    Y_UNIT_TEST_F(ShouldSuspendDevicesOnCMSRequest, TFixture)
    {
        TDiskRegistryClient diskRegistry(*Runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);
        diskRegistry.UpdateConfig(CreateRegistryConfig(0, Agents)
            | WithPoolConfig("local", NProto::DEVICE_POOL_KIND_LOCAL, 10_GB));

        RegisterAndWaitForAgents(*Runtime, Agents);

        auto query = [&] {
            auto response = diskRegistry.QueryAvailableStorage(
                TVector<TString> {"agent-1", "agent-2", "agent-3"},
                "",
                NProto::STORAGE_POOL_KIND_LOCAL);

            auto& msg = response->Record;
            SortBy(*msg.MutableAvailableStorage(), [] (auto& info) {
                return info.GetAgentId();
            });

            UNIT_ASSERT_VALUES_EQUAL(3, msg.AvailableStorageSize());

            return std::make_tuple(
                msg.GetAvailableStorage(0).GetChunkCount(),
                msg.GetAvailableStorage(1).GetChunkCount(),
                msg.GetAvailableStorage(2).GetChunkCount()
            );
        };

        {
            auto [n1, n2, n3] = query();

            UNIT_ASSERT_VALUES_EQUAL(0, n1);
            UNIT_ASSERT_VALUES_EQUAL(2, n2);
            UNIT_ASSERT_VALUES_EQUAL(4, n3);
        }

        {
            TVector<NProto::TAction> actions(1);
            actions[0].SetHost("agent-2");
            actions[0].SetDevice("dev-5");
            actions[0].SetType(NProto::TAction::REMOVE_DEVICE);

            auto response = diskRegistry.CmsAction(actions);
            const auto& result = response->Record.GetActionResults(0);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, result.GetResult().GetCode());
        }

        {
            auto [n1, n2, n3] = query();

            UNIT_ASSERT_VALUES_EQUAL(0, n1);
            UNIT_ASSERT_VALUES_EQUAL(1, n2);
            UNIT_ASSERT_VALUES_EQUAL(4, n3);
        }

        {
            TVector<NProto::TAction> actions(1);
            actions[0].SetHost("agent-3");
            actions[0].SetType(NProto::TAction::REMOVE_HOST);

            auto response = diskRegistry.CmsAction(actions);
            const auto& result = response->Record.GetActionResults(0);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, result.GetResult().GetCode());
        }

        {
            auto [n1, n2, n3] = query();

            UNIT_ASSERT_VALUES_EQUAL(0, n1);
            UNIT_ASSERT_VALUES_EQUAL(1, n2);
            UNIT_ASSERT_VALUES_EQUAL(0, n3);
        }

        {
            TVector<NProto::TAction> actions(2);
            actions[0].SetHost("agent-2");
            actions[0].SetDevice("dev-5");
            actions[0].SetType(NProto::TAction::ADD_DEVICE);

            actions[1].SetHost("agent-3");
            actions[1].SetType(NProto::TAction::ADD_HOST);

            auto response = diskRegistry.CmsAction(actions);
            for (const auto& result: response->Record.GetActionResults()) {
                UNIT_ASSERT_VALUES_EQUAL(S_OK, result.GetResult().GetCode());
            }
        }

        {
            auto [n1, n2, n3] = query();

            UNIT_ASSERT_VALUES_EQUAL(0, n1);
            UNIT_ASSERT_VALUES_EQUAL(2, n2);
            UNIT_ASSERT_VALUES_EQUAL(0, n3);
        }

        for (const auto& d: Agents[2].GetDevices()) {
            diskRegistry.ResumeDevice(Agents[2].GetAgentId(), d.GetDeviceName());
        }

        {
            auto [n1, n2, n3] = query();

            UNIT_ASSERT_VALUES_EQUAL(0, n1);
            UNIT_ASSERT_VALUES_EQUAL(2, n2);
            UNIT_ASSERT_VALUES_EQUAL(4, n3);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
