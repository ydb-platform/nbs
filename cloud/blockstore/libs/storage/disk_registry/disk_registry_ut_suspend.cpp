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

#include <chrono>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NDiskRegistryTest;

using namespace std::chrono_literals;

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
    NMonitoring::TDynamicCounterPtr CriticalEvents;
    std::unique_ptr<TDiskRegistryClient> DiskRegistry;

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {}

    void SetUpRuntime(bool nonReplicatedDontSuspendDevices = true)
    {
        Runtime = TTestRuntimeBuilder()
            .WithAgents(Agents)
            .With([&] {
                auto config = CreateDefaultStorageConfig();
                config.SetDiskRegistryCountersHost("test");
                config.SetNonReplicatedDontSuspendDevices(
                    nonReplicatedDontSuspendDevices);

                return config;
            }())
            .Build();
        DiskRegistry = std::make_unique<TDiskRegistryClient>(*Runtime);

        Counters = Runtime->GetAppData(0).Counters
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "disk_registry")
            ->GetSubgroup("host", "test");

        CriticalEvents = MakeIntrusive<NMonitoring::TDynamicCounters>();
        InitCriticalEventsCounter(CriticalEvents);
    }

    auto QueryAvailableStorage() const
    {
        auto response = DiskRegistry->QueryAvailableStorage(
            TVector<TString>{"agent-1", "agent-2", "agent-3"},
            "",
            NProto::STORAGE_POOL_KIND_LOCAL);

        auto& msg = response->Record;
        SortBy(
            *msg.MutableAvailableStorage(),
            [](auto& info) { return info.GetAgentId(); });

        UNIT_ASSERT_VALUES_EQUAL(3, msg.AvailableStorageSize());

        return std::make_tuple(
            msg.GetAvailableStorage(0).GetChunkCount(),
            msg.GetAvailableStorage(1).GetChunkCount(),
            msg.GetAvailableStorage(2).GetChunkCount());
    }

    void WaitForCounters() const
    {
        Runtime->AdvanceCurrentTime(20s);
        Runtime->DispatchEvents({}, 10ms);
    }

    auto CmsAction(const NProto::TAction& action) const
    {
        auto response = DiskRegistry->CmsAction(TVector{action});
        UNIT_ASSERT_VALUES_EQUAL(1, response->Record.ActionResultsSize());
        return response->Record.GetActionResults(0);
    }

    auto RemoveHost(TString host) const
    {
        NProto::TAction action;
        action.SetHost(std::move(host));
        action.SetType(NProto::TAction::REMOVE_HOST);

        return CmsAction(action);
    }

    auto PurgeHost(const TString& agentId)
    {
        NProto::TAction action;
        action.SetHost(agentId);
        action.SetType(NProto::TAction::PURGE_HOST);

        return CmsAction(std::move(action));
    }

    auto RemoveDevice(TString host, TString path) const
    {
        NProto::TAction action;
        action.SetHost(std::move(host));
        action.SetDevice(std::move(path));
        action.SetType(NProto::TAction::REMOVE_DEVICE);

        return CmsAction(action);
    }

    auto AddHost(TString host) const
    {
        NProto::TAction action;
        action.SetHost(std::move(host));
        action.SetType(NProto::TAction::ADD_HOST);

        return CmsAction(action);
    }

    auto AddDevice(TString host, TString path) const
    {
        NProto::TAction action;
        action.SetHost(std::move(host));
        action.SetDevice(std::move(path));
        action.SetType(NProto::TAction::ADD_DEVICE);

        return CmsAction(action);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryTest)
{
    Y_UNIT_TEST_F(ShouldSuspendDevices, TFixture)
    {
        SetUpRuntime();

        DiskRegistry->WaitReady();
        DiskRegistry->SetWritableState(true);

        DiskRegistry->UpdateConfig(CreateRegistryConfig(0, {Agents[0]})
            | WithPoolConfig("local", NProto::DEVICE_POOL_KIND_LOCAL, 10_GB));

        size_t cleanDevices = 0;

        Runtime->SetEventFilter(
            [&](auto&, TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvDiskRegistryPrivate::EvSecureEraseResponse)
                {
                    auto* msg = event->Get<
                        TEvDiskRegistryPrivate::TEvSecureEraseResponse>();

                    cleanDevices += msg->CleanDevices;
                }
                return false;
            });

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            Counters->GetCounter("AgentsInOnlineState")->Val());

        RegisterAgent(*Runtime, 0);
        WaitForAgent(*Runtime, 0);
        WaitForCounters();

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            Counters->GetCounter("AgentsInOnlineState")->Val());

        UNIT_ASSERT_VALUES_EQUAL(Agents[0].DevicesSize(), cleanDevices);

        {
            DiskRegistry->SendAllocateDiskRequest("vol0", 80_GB);
            auto response = DiskRegistry->RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                E_BS_DISK_ALLOCATION_FAILED,
                response->GetStatus());
        }

        for (const auto& d: Agents[1].GetDevices()) {
            DiskRegistry->SuspendDevice(d.GetDeviceUUID());
        }

        DiskRegistry->UpdateConfig(CreateRegistryConfig(1, Agents)
            | WithPoolConfig("local", NProto::DEVICE_POOL_KIND_LOCAL, 10_GB));

        cleanDevices = 0;

        RegisterAgent(*Runtime, 1);
        WaitForAgent(*Runtime, 1);
        WaitForCounters();

        UNIT_ASSERT_VALUES_EQUAL(
            2,
            Counters->GetCounter("AgentsInOnlineState")->Val());

        UNIT_ASSERT_VALUES_EQUAL(0, cleanDevices);

        {
            DiskRegistry->SendAllocateDiskRequest("vol0", 80_GB);
            auto response = DiskRegistry->RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                E_BS_DISK_ALLOCATION_FAILED,
                response->GetStatus());
        }

        DiskRegistry->ResumeDevice(
            Agents[1].GetAgentId(),
            Agents[1].GetDevices(0).GetDeviceName());
        DiskRegistry->ResumeDevice(
            Agents[1].GetAgentId(),
            Agents[1].GetDevices(1).GetDeviceName());

        WaitForCounters();
        UNIT_ASSERT_VALUES_EQUAL(2, cleanDevices);

        for (const auto& d: Agents[1].GetDevices()) {
            DiskRegistry->ResumeDevice(
                Agents[1].GetAgentId(),
                d.GetDeviceName());
        }

        while (Agents[1].DevicesSize() != cleanDevices) {
            WaitForCounters();
        }

        DiskRegistry->AllocateDisk("vol0", 80_GB);
    }

    Y_UNIT_TEST_F(ShouldSuspendDevicesOnCMSRequest, TFixture)
    {
        SetUpRuntime();

        DiskRegistry->WaitReady();
        DiskRegistry->SetWritableState(true);
        DiskRegistry->UpdateConfig(CreateRegistryConfig(0, Agents)
            | WithPoolConfig("local", NProto::DEVICE_POOL_KIND_LOCAL, 10_GB));

        RegisterAndWaitForAgents(*Runtime, Agents);

        {
            auto [n1, n2, n3] = QueryAvailableStorage();

            UNIT_ASSERT_VALUES_EQUAL(0, n1);
            UNIT_ASSERT_VALUES_EQUAL(2, n2);
            UNIT_ASSERT_VALUES_EQUAL(4, n3);
        }

        {
            const auto result = RemoveDevice("agent-2", "dev-5");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, result.GetResult().GetCode());
        }

        {
            auto [n1, n2, n3] = QueryAvailableStorage();

            UNIT_ASSERT_VALUES_EQUAL(0, n1);
            UNIT_ASSERT_VALUES_EQUAL(1, n2);
            UNIT_ASSERT_VALUES_EQUAL(4, n3);
        }

        {
            auto request = std::make_unique<TEvDiskRegistry::TEvAllocateDiskRequest>();

            request->Record.SetDiskId("vol0");
            request->Record.SetBlockSize(DefaultBlockSize);
            request->Record.SetBlocksCount(30_GB / DefaultBlockSize);
            request->Record.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD_LOCAL);
            request->Record.AddAgentIds("agent-3");

            DiskRegistry->SendRequest(std::move(request));

            auto response = DiskRegistry->RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        {
            auto [n1, n2, n3] = QueryAvailableStorage();

            UNIT_ASSERT_VALUES_EQUAL(0, n1);
            UNIT_ASSERT_VALUES_EQUAL(1, n2);
            UNIT_ASSERT_VALUES_EQUAL(4, n3);
        }

        {
            const auto result = RemoveHost("agent-3");
            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, result.GetResult().GetCode());
        }

        {
            auto [n1, n2, n3] = QueryAvailableStorage();

            UNIT_ASSERT_VALUES_EQUAL(0, n1);
            UNIT_ASSERT_VALUES_EQUAL(1, n2);
            UNIT_ASSERT_VALUES_EQUAL(4, n3);
        }

        size_t cleanDevices = 0;

        Runtime->SetEventFilter([&] (auto&, TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvDiskRegistryPrivate::EvSecureEraseResponse) {
                auto* msg = event->Get<TEvDiskRegistryPrivate::TEvSecureEraseResponse>();

                cleanDevices += msg->CleanDevices;
            }

            return false;
        });

        DiskRegistry->MarkDiskForCleanup("vol0");
        DiskRegistry->SendDeallocateDiskRequest(
            "vol0",
            true // sync
        );

        Runtime->DispatchEvents({
            .CustomFinalCondition = [&] {
                return cleanDevices == 3;
            }
        }, 15s);

        {
            const auto result = RemoveHost("agent-3");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, result.GetResult().GetCode());
        }

        {
            auto [n1, n2, n3] = QueryAvailableStorage();

            UNIT_ASSERT_VALUES_EQUAL(0, n1);
            UNIT_ASSERT_VALUES_EQUAL(1, n2);
            UNIT_ASSERT_VALUES_EQUAL(4, n3);
        }

        {
            const auto result = PurgeHost("agent-3");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, result.GetResult().GetCode());
        }

        {
            auto [n1, n2, n3] = QueryAvailableStorage();

            UNIT_ASSERT_VALUES_EQUAL(0, n1);
            UNIT_ASSERT_VALUES_EQUAL(1, n2);
            UNIT_ASSERT_VALUES_EQUAL(0, n3);
        }

        {
            const auto result = AddHost("agent-3");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, result.GetResult().GetCode());
        }

        {
            auto [n1, n2, n3] = QueryAvailableStorage();

            UNIT_ASSERT_VALUES_EQUAL(0, n1);
            UNIT_ASSERT_VALUES_EQUAL(1, n2);
            UNIT_ASSERT_VALUES_EQUAL(4, n3);
        }

        {
            const auto result = AddDevice("agent-2", "dev-5");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, result.GetResult().GetCode());
        }

        {
            auto [n1, n2, n3] = QueryAvailableStorage();

            UNIT_ASSERT_VALUES_EQUAL(0, n1);
            UNIT_ASSERT_VALUES_EQUAL(2, n2);
            UNIT_ASSERT_VALUES_EQUAL(4, n3);
        }

        for (const auto& d: Agents[2].GetDevices()) {
            DiskRegistry->ResumeDevice(
                Agents[2].GetAgentId(),
                d.GetDeviceName());
        }

        {
            auto [n1, n2, n3] = QueryAvailableStorage();

            UNIT_ASSERT_VALUES_EQUAL(0, n1);
            UNIT_ASSERT_VALUES_EQUAL(2, n2);
            UNIT_ASSERT_VALUES_EQUAL(4, n3);
        }
    }

    Y_UNIT_TEST_F(ShouldRejectResumeWhenAgentIsNotOnline, TFixture)
    {
        SetUpRuntime();

        DiskRegistry->WaitReady();
        DiskRegistry->SetWritableState(true);
        DiskRegistry->UpdateConfig(CreateRegistryConfig(0, Agents)
            | WithPoolConfig("local", NProto::DEVICE_POOL_KIND_LOCAL, 10_GB));

        RegisterAndWaitForAgents(*Runtime, {Agents[0], Agents[1]});

        auto resumeFailed = CriticalEvents->FindCounter(
            "AppCriticalEvents/DiskRegistryResumeDeviceFailed");
        UNIT_ASSERT(resumeFailed);
        UNIT_ASSERT_VALUES_EQUAL(0, resumeFailed->Val());

        {
            DiskRegistry->SendResumeDeviceRequest(
                Agents[2].GetAgentId(),
                Agents[2].GetDevices()[0].GetDeviceUUID());
            auto response = DiskRegistry->RecvResumeDeviceResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(1, resumeFailed->Val());
        }
    }

    Y_UNIT_TEST_F(ResumeDeviceShouldAddDevice, TFixture)
    {
        SetUpRuntime(
            false   // nonReplicatedDontSuspendDevices
        );

        DiskRegistry->WaitReady();
        DiskRegistry->SetWritableState(true);

        DiskRegistry->UpdateConfig(
            CreateRegistryConfig(0, Agents) |
            WithPoolConfig("local", NProto::DEVICE_POOL_KIND_LOCAL, 10_GB));
        RegisterAgents(*Runtime, Agents.size());

        int nrdDevices = 0;
        for (const auto& agent: Agents) {
            for (const auto& device: agent.GetDevices()) {
                if (device.GetPoolKind() == NProto::DEVICE_POOL_KIND_DEFAULT) {
                    nrdDevices++;
                }
            }
        }
        WaitForSecureErase(*Runtime, nrdDevices);

        {
            auto [n1, n2, n3] = QueryAvailableStorage();

            UNIT_ASSERT_VALUES_EQUAL(0, n1);
            UNIT_ASSERT_VALUES_EQUAL(0, n2);
            UNIT_ASSERT_VALUES_EQUAL(0, n3);
        }

        int localDevices = 0;
        for (const auto& d: Agents[1].GetDevices()) {
            if (d.GetPoolKind() == NProto::DEVICE_POOL_KIND_LOCAL) {
                localDevices++;
                DiskRegistry->ResumeDevice(
                    Agents[1].GetAgentId(),
                    d.GetDeviceName());
            }
        }
        WaitForSecureErase(*Runtime, localDevices);

        {
            auto [n1, n2, n3] = QueryAvailableStorage();

            UNIT_ASSERT_VALUES_EQUAL(0, n1);
            UNIT_ASSERT_VALUES_EQUAL(2, n2);
            UNIT_ASSERT_VALUES_EQUAL(0, n3);
        }

        auto resumeFailed = CriticalEvents->FindCounter(
            "AppCriticalEvents/DiskRegistryResumeDeviceFailed");
        UNIT_ASSERT(resumeFailed);
        UNIT_ASSERT_VALUES_EQUAL(0, resumeFailed->Val());

        {
            DiskRegistry->SendResumeDeviceRequest(
                Agents[1].GetAgentId(),
                "wrong_device_name",
                true);  // dryRun
            auto response = DiskRegistry->RecvResumeDeviceResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0, resumeFailed->Val());
        }

        {
            DiskRegistry->SendResumeDeviceRequest(
                Agents[1].GetAgentId(),
                "wrong_device_name");
            auto response = DiskRegistry->RecvResumeDeviceResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(1, resumeFailed->Val());
        }
    }

    Y_UNIT_TEST_F(ShouldSuspendLocalDevices, TFixture)
    {
        SetUpRuntime(
            false   // nonReplicatedDontSuspendDevices
        );

        DiskRegistry->WaitReady();
        DiskRegistry->SetWritableState(true);
        DiskRegistry->UpdateConfig(CreateRegistryConfig(0, Agents)
            | WithPoolConfig("local", NProto::DEVICE_POOL_KIND_LOCAL, 10_GB));

        RegisterAndWaitForAgent(*Runtime, 0, 4);
        RegisterAndWaitForAgent(*Runtime, 1, 4);
        RegisterAndWaitForAgent(*Runtime, 2, 0);

        size_t cleanDevices = 0;

        Runtime->SetEventFilter(
            [&](auto&, TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvDiskRegistryPrivate::EvSecureEraseResponse)
                {
                    auto* msg = event->Get<
                        TEvDiskRegistryPrivate::TEvSecureEraseResponse>();

                    cleanDevices += msg->CleanDevices;
                }

                return false;
            });

        auto waitDevices = [&] (size_t count) {
            return Runtime->DispatchEvents({
                .CustomFinalCondition = [&] {
                    return cleanDevices == count;
                }
            }, 15s);
        };

        {
            auto [n1, n2, n3] = QueryAvailableStorage();

            UNIT_ASSERT_VALUES_EQUAL(0, n1);
            UNIT_ASSERT_VALUES_EQUAL(0, n2);
            UNIT_ASSERT_VALUES_EQUAL(0, n3);
        }

        DiskRegistry->ResumeDevice("agent-2", "dev-5");
        DiskRegistry->ResumeDevice("agent-2", "dev-6");

        UNIT_ASSERT(waitDevices(2));
        cleanDevices = 0;

        {
            auto [n1, n2, n3] = QueryAvailableStorage();

            UNIT_ASSERT_VALUES_EQUAL(0, n1);
            UNIT_ASSERT_VALUES_EQUAL(2, n2);
            UNIT_ASSERT_VALUES_EQUAL(0, n3);
        }

        DiskRegistry->ResumeDevice("agent-3", "dev-1");
        DiskRegistry->ResumeDevice("agent-3", "dev-2");
        DiskRegistry->ResumeDevice("agent-3", "dev-3");
        DiskRegistry->ResumeDevice("agent-3", "dev-4");

        UNIT_ASSERT(waitDevices(4));
        cleanDevices = 0;

        {
            auto [n1, n2, n3] = QueryAvailableStorage();

            UNIT_ASSERT_VALUES_EQUAL(0, n1);
            UNIT_ASSERT_VALUES_EQUAL(2, n2);
            UNIT_ASSERT_VALUES_EQUAL(4, n3);
        }

        {
            const auto result = RemoveHost("agent-3");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, result.GetResult().GetCode());
        }

        {
            auto [n1, n2, n3] = QueryAvailableStorage();

            UNIT_ASSERT_VALUES_EQUAL(0, n1);
            UNIT_ASSERT_VALUES_EQUAL(2, n2);
            UNIT_ASSERT_VALUES_EQUAL(4, n3);
        }

        {
            const auto result = PurgeHost("agent-3");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, result.GetResult().GetCode());
        }

        {
            auto [n1, n2, n3] = QueryAvailableStorage();

            UNIT_ASSERT_VALUES_EQUAL(0, n1);
            UNIT_ASSERT_VALUES_EQUAL(2, n2);
            UNIT_ASSERT_VALUES_EQUAL(0, n3); // devices from agent-3 were removed
        }

        {
            const auto result = AddHost("agent-3");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, result.GetResult().GetCode());
        }

        {
            auto [n1, n2, n3] = QueryAvailableStorage();

            UNIT_ASSERT_VALUES_EQUAL(0, n1);
            UNIT_ASSERT_VALUES_EQUAL(2, n2);
            UNIT_ASSERT_VALUES_EQUAL(0, n3); // devices from agent-3 are suspended
        }

        DiskRegistry->ResumeDevice("agent-3", "dev-1");
        WaitForSecureErase(*Runtime, 1);

        {
            auto [n1, n2, n3] = QueryAvailableStorage();

            UNIT_ASSERT_VALUES_EQUAL(0, n1);
            UNIT_ASSERT_VALUES_EQUAL(2, n2);
            UNIT_ASSERT_VALUES_EQUAL(1, n3);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
