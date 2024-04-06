#include "disk_registry.h"
#include "disk_registry_actor.h"

#include <cloud/blockstore/config/disk.pb.h>

#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_env.h>
#include <cloud/blockstore/libs/storage/testlib/ss_proxy_client.h>

#include <contrib/ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NDiskRegistryTest;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TDiskRegistryTestFixture
    : public NUnitTest::TBaseFixture
{
    TDiskRegistryTestFixture() = default;

    explicit TDiskRegistryTestFixture(bool nonReplicatedDontSuspendDevices)
        : NonReplicatedDontSuspendDevices(nonReplicatedDontSuspendDevices)
    {}

    ~TDiskRegistryTestFixture() override = default;

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
    bool NonReplicatedDontSuspendDevices = true;

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        Runtime = TTestRuntimeBuilder()
            .WithAgents(Agents)
            .With([&] {
                auto config = CreateDefaultStorageConfig();
                config.SetDiskRegistryCountersHost("test");
                config.SetNonReplicatedDontSuspendDevices(
                    NonReplicatedDontSuspendDevices);

                return config;
            }())
            .Build();
        DiskRegistry = std::make_unique<TDiskRegistryClient>(*Runtime);

        Counters = Runtime->GetAppData(0).Counters
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "disk_registry")
            ->GetSubgroup("host", "test");

        CriticalEvents = new NMonitoring::TDynamicCounters();
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

    auto GetResumeDeviceResponse() const
    {
        TAutoPtr<NActors::IEventHandle> handle;
        Runtime->GrabEdgeEventRethrow<TEvService::TEvResumeDeviceResponse>(
            handle,
            WaitTimeout);
        UNIT_ASSERT(handle);
        return std::unique_ptr<TEvService::TEvResumeDeviceResponse>(
            handle->Release<TEvService::TEvResumeDeviceResponse>().Release());
    }

    void WaitForCounters() const
    {
        Runtime->AdvanceCurrentTime(TDuration::Seconds(20));
        Runtime->DispatchEvents({}, TDuration::MilliSeconds(10));
    }
};

struct TDiskRegistryWithAutoSuspendTestFixture: public TDiskRegistryTestFixture
{
    TDiskRegistryWithAutoSuspendTestFixture()
        : TDiskRegistryTestFixture(false)
    {}
    ~TDiskRegistryWithAutoSuspendTestFixture() override = default;
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryTest)
{
    Y_UNIT_TEST_F(ShouldSuspendDevices, TDiskRegistryTestFixture)
    {
        DiskRegistry->WaitReady();
        DiskRegistry->SetWritableState(true);

        DiskRegistry->UpdateConfig(CreateRegistryConfig(0, {Agents[0]})
            | WithPoolConfig("local", NProto::DEVICE_POOL_KIND_LOCAL, 10_GB));

        size_t cleanDevices = 0;

        Runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvDiskRegistryPrivate::EvSecureEraseResponse) {
                    auto* msg = event->Get<TEvDiskRegistryPrivate::TEvSecureEraseResponse>();

                    cleanDevices += msg->CleanDevices;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
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

        for (auto& d: Agents[1].GetDevices()) {
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
            Agents[1].GetDevices(0).GetDeviceName(),
            /*dryRun=*/false);
        DiskRegistry->ResumeDevice(
            Agents[1].GetAgentId(),
            Agents[1].GetDevices(1).GetDeviceName(),
            /*dryRun=*/false);

        WaitForCounters();
        UNIT_ASSERT_VALUES_EQUAL(2, cleanDevices);

        for (auto& d: Agents[1].GetDevices()) {
            DiskRegistry->ResumeDevice(
                Agents[1].GetAgentId(),
                d.GetDeviceName(),
                /*dryRun=*/false);
        }

        while (Agents[1].DevicesSize() != cleanDevices) {
            WaitForCounters();
        }

        DiskRegistry->AllocateDisk("vol0", 80_GB);
    }

    Y_UNIT_TEST_F(ShouldSuspendDevicesOnCMSRequest, TDiskRegistryTestFixture)
    {
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
            TVector<NProto::TAction> actions(1);
            actions[0].SetHost("agent-2");
            actions[0].SetDevice("dev-5");
            actions[0].SetType(NProto::TAction::REMOVE_DEVICE);

            auto response = DiskRegistry->CmsAction(actions);
            const auto& result = response->Record.GetActionResults(0);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, result.GetResult().GetCode());
        }

        {
            auto [n1, n2, n3] = QueryAvailableStorage();

            UNIT_ASSERT_VALUES_EQUAL(0, n1);
            UNIT_ASSERT_VALUES_EQUAL(1, n2);
            UNIT_ASSERT_VALUES_EQUAL(4, n3);
        }

        {
            TVector<NProto::TAction> actions(1);
            actions[0].SetHost("agent-3");
            actions[0].SetType(NProto::TAction::REMOVE_HOST);

            auto response = DiskRegistry->CmsAction(actions);
            const auto& result = response->Record.GetActionResults(0);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, result.GetResult().GetCode());
        }

        {
            auto [n1, n2, n3] = QueryAvailableStorage();

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

            auto response = DiskRegistry->CmsAction(actions);
            for (const auto& result: response->Record.GetActionResults()) {
                UNIT_ASSERT_VALUES_EQUAL(S_OK, result.GetResult().GetCode());
            }
        }

        {
            auto [n1, n2, n3] = QueryAvailableStorage();

            UNIT_ASSERT_VALUES_EQUAL(0, n1);
            UNIT_ASSERT_VALUES_EQUAL(2, n2);
            UNIT_ASSERT_VALUES_EQUAL(0, n3);
        }

        for (const auto& d: Agents[2].GetDevices()) {
            DiskRegistry->ResumeDevice(
                Agents[2].GetAgentId(),
                d.GetDeviceName(),
                /*dryRun=*/false);
        }

        {
            auto [n1, n2, n3] = QueryAvailableStorage();

            UNIT_ASSERT_VALUES_EQUAL(0, n1);
            UNIT_ASSERT_VALUES_EQUAL(2, n2);
            UNIT_ASSERT_VALUES_EQUAL(4, n3);
        }
    }

    Y_UNIT_TEST_F(ShouldRejectResumeWhenAgentIsNotOnline, TDiskRegistryTestFixture)
    {
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
                Agents[2].GetDevices()[0].GetDeviceUUID(),
                /*dryRun=*/false);
            auto response = GetResumeDeviceResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(1, resumeFailed->Val());
        }
    }
}

Y_UNIT_TEST_SUITE(TDiskRegistryWithAutoSuspendTest) {

    Y_UNIT_TEST_F(ResumeDeviceShouldAddDevice, TDiskRegistryWithAutoSuspendTestFixture)
    {
        DiskRegistry->WaitReady();
        DiskRegistry->SetWritableState(true);

        DiskRegistry->UpdateConfig(CreateRegistryConfig(0, Agents)
            | WithPoolConfig("local", NProto::DEVICE_POOL_KIND_LOCAL, 10_GB));
        RegisterAgents(*Runtime, Agents.size());

        int nrdDevices = 0;
        for (const auto& agent : Agents) {
            for (auto& device : agent.GetDevices()) {
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
                    d.GetDeviceName(),
                    /*dryRun=*/false);
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
                /*dryRun=*/true);
            auto response = GetResumeDeviceResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0, resumeFailed->Val());
        }

        {
            DiskRegistry->SendResumeDeviceRequest(
                Agents[1].GetAgentId(),
                "wrong_device_name",
                /*dryRun=*/false);
            auto response = GetResumeDeviceResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(1, resumeFailed->Val());
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
