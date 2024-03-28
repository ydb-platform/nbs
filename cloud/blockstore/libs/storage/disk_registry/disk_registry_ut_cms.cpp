#include "disk_registry.h"
#include "disk_registry_actor.h"

#include <cloud/blockstore/config/disk.pb.h>

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_env.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_logbroker.h>
#include <cloud/blockstore/libs/storage/testlib/ss_proxy_client.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <contrib/ydb/core/testlib/basics/runtime.h>

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

auto CmsAction(TDiskRegistryClient& dr, NProto::TAction action)
{
    TVector<NProto::TAction> actions {action};

    auto response = dr.CmsAction(actions);

    UNIT_ASSERT_VALUES_EQUAL(1, response->Record.ActionResultsSize());
    const auto& r = response->Record.GetActionResults(0);

    return std::make_pair(r.GetResult(), r.GetTimeout());
}

auto RemoveHost(TDiskRegistryClient& dr, const TString& agentId)
{
    NProto::TAction action;
    action.SetHost(agentId);
    action.SetType(NProto::TAction::REMOVE_HOST);

    return CmsAction(dr, std::move(action));
}

auto AddHost(TDiskRegistryClient& dr, const TString& agentId)
{
    NProto::TAction action;
    action.SetHost(agentId);
    action.SetType(NProto::TAction::ADD_HOST);

    return CmsAction(dr, std::move(action));
}

auto AddDevice(
    TDiskRegistryClient& dr,
    const TString& agentId,
    const TString& path)
{
    NProto::TAction action;
    action.SetHost(agentId);
    action.SetType(NProto::TAction::ADD_DEVICE);
    action.SetDevice(path);

    return CmsAction(dr, std::move(action));
}

auto RemoveDevice(
    TDiskRegistryClient& dr,
    const TString& agentId,
    const TString& path)
{
    NProto::TAction action;
    action.SetHost(agentId);
    action.SetType(NProto::TAction::REMOVE_DEVICE);
    action.SetDevice(path);

    return CmsAction(dr, std::move(action));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryTest)
{
    Y_UNIT_TEST(ShouldRemoveAgentsUponCmsRequest)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent}));

        RegisterAndWaitForAgents(*runtime, {agent});

        TString freeDevice;
        TString diskDevice;
        {
            auto response = diskRegistry.AllocateDisk("vol1", 10_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, msg.DevicesSize());
            diskDevice = msg.GetDevices(0).GetDeviceUUID();

            freeDevice = diskDevice == "uuid-1"
                ? "uuid-2"
                : "uuid-1";
        }

        ui32 cmsTimeout = 0;
        {
            auto [error, timeout] = RemoveHost(diskRegistry, "agent-1");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);

            cmsTimeout = timeout;
        }

        // request will be ignored
        diskRegistry.ChangeAgentState("agent-1", NProto::EAgentState::AGENT_STATE_ONLINE);

        {
            diskRegistry.SendAllocateDiskRequest("vol2", 10_GB);
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, response->GetStatus());
        }

        runtime->AdvanceCurrentTime(TDuration::Days(1) / 2);

        {
            auto [error, timeout] = RemoveHost(diskRegistry, "agent-1");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);
            UNIT_ASSERT_LT(timeout, cmsTimeout);
        }

        runtime->AdvanceCurrentTime(TDuration::Days(1) / 2);

        {
            auto [error, timeout] = RemoveHost(diskRegistry, "agent-1");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);
            UNIT_ASSERT_VALUES_EQUAL(timeout, cmsTimeout);
        }

        diskRegistry.ChangeDeviceState(freeDevice, NProto::EDeviceState::DEVICE_STATE_ERROR);

        {
            auto [error, timeout] = RemoveHost(diskRegistry, "agent-1");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);
        }

        diskRegistry.ChangeDeviceState(diskDevice, NProto::EDeviceState::DEVICE_STATE_ERROR);

        {
            auto [error, timeout] = RemoveHost(diskRegistry, "agent-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        diskRegistry.ChangeAgentState("agent-1", NProto::EAgentState::AGENT_STATE_UNAVAILABLE);
        diskRegistry.ChangeAgentState("agent-1", NProto::EAgentState::AGENT_STATE_ONLINE);

        diskRegistry.ChangeDeviceState("uuid-1", NProto::EDeviceState::DEVICE_STATE_ONLINE);
        diskRegistry.ChangeDeviceState("uuid-2", NProto::EDeviceState::DEVICE_STATE_ONLINE);

        diskRegistry.AllocateDisk("vol2", 10_GB);
    }

    Y_UNIT_TEST(ShouldFailCmsRequestIfActionIsUnknown)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent}));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agent});

        NProto::TAction action;
        action.SetHost("agent-1");
        action.SetType(NProto::TAction::UNKNOWN);
        TVector<NProto::TAction> actions { action };

        auto response = diskRegistry.CmsAction(std::move(actions));
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            response->Record.GetActionResults(0).GetResult().GetCode());
    }

    Y_UNIT_TEST(ShouldFailCmsRequestIfAgentIsNotFound)
    {
        auto runtime = TTestRuntimeBuilder()
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        auto [error, timeout] = RemoveHost(diskRegistry, "agent-1");

        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(0, timeout);
    }

    Y_UNIT_TEST(ShouldFailCmsRequestIfDeviceNotFound)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent}));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agent});

        {
            auto [error, timeout] = RemoveDevice(diskRegistry, "agent-2", "dev-1");

            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        {
            auto [error, timeout] = RemoveDevice(diskRegistry, "agent-1", "dev-10");

            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }
    }

    Y_UNIT_TEST(ShouldRemoveDeviceUponCmsRequest)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent}));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agent});

        diskRegistry.AllocateDisk("vol1", 10_GB);

        ui32 cmsTimeout = 0;
        {
            auto [error, timeout] = RemoveDevice(diskRegistry, "agent-1", "dev-1");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);

            cmsTimeout = timeout;
        }

        // request will be ignored
        diskRegistry.ChangeDeviceState("uuid-1", NProto::EDeviceState::DEVICE_STATE_ONLINE);

        {
            auto response = diskRegistry.AllocateDisk("vol1", 10_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, msg.DevicesSize());
            const auto& device = msg.GetDevices(0);

            UNIT_ASSERT_VALUES_EQUAL("uuid-1", device.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(NProto::DEVICE_STATE_WARNING),
                static_cast<ui32>(device.GetState()));
        }

        runtime->AdvanceCurrentTime(TDuration::Days(1) / 2);

        {
            auto [error, timeout] = RemoveDevice(diskRegistry, "agent-1", "dev-1");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);
            UNIT_ASSERT_LT(timeout, cmsTimeout);
        }

        runtime->AdvanceCurrentTime(TDuration::Days(1) / 2);

        {
            auto [error, timeout] = RemoveDevice(diskRegistry, "agent-1", "dev-1");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);

            cmsTimeout = timeout;
        }

        diskRegistry.ChangeDeviceState("uuid-1", NProto::EDeviceState::DEVICE_STATE_ERROR);

        {
            auto [error, timeout] = RemoveDevice(diskRegistry, "agent-1", "dev-1");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        diskRegistry.ChangeDeviceState("uuid-1", NProto::EDeviceState::DEVICE_STATE_ONLINE);

        {
            auto response = diskRegistry.AllocateDisk("vol1", 10_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, msg.DevicesSize());
            const auto& device = msg.GetDevices(0);

            UNIT_ASSERT_VALUES_EQUAL("uuid-1", device.GetDeviceUUID());
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ONLINE, device.GetState());
        }
    }

    Y_UNIT_TEST(ShouldRemoveMultipleDevicesUponCmsRequest)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("some/path", "uuid-1", "rack-1", 10_GB),
            Device("some/path", "uuid-2", "rack-1", 10_GB),
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent}));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agent});

        diskRegistry.AllocateDisk("vol1", 10_GB);

        ui32 cmsTimeout = 0;
        {
            auto [error, timeout] =
                RemoveDevice(diskRegistry, "agent-1", "some/path");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);

            cmsTimeout = timeout;
        }

        runtime->AdvanceCurrentTime(TDuration::Seconds(cmsTimeout));

        diskRegistry.MarkDiskForCleanup("vol1");
        diskRegistry.DeallocateDisk("vol1");

        {
            auto [error, timeout] =
                RemoveDevice(diskRegistry, "agent-1", "some/path");

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                error.GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        {
            diskRegistry.SendAllocateDiskRequest("vol1", 10_GB);
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                E_BS_DISK_ALLOCATION_FAILED,
                response->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldReturnOkIfRemovedDeviceHasNoDisks)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent}));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agent});

        {
            auto [error, timeout] = RemoveDevice(diskRegistry, "agent-1", "dev-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        {
            diskRegistry.SendAllocateDiskRequest("vol1", 10_GB);
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, response->GetStatus());
        }

        runtime->AdvanceCurrentTime(TDuration::Days(1) / 2);
        {
            auto [error, timeout] = RemoveDevice(diskRegistry, "agent-1", "dev-1");

            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        diskRegistry.ChangeDeviceState("uuid-1", NProto::EDeviceState::DEVICE_STATE_ERROR);
        diskRegistry.ChangeDeviceState("uuid-1", NProto::EDeviceState::DEVICE_STATE_ONLINE);

        diskRegistry.AllocateDisk("vol1", 10_GB);
    }

    Y_UNIT_TEST(ShouldFailCmsRequestIfDiskRegistryRestarts)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent}));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agent});

        NProto::TAction action;
        action.SetHost("agent-2");
        action.SetType(NProto::TAction::REMOVE_DEVICE);
        action.SetDevice("dev-1");
        TVector<NProto::TAction> actions;
        actions.push_back(action);

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
            switch (event->GetTypeRewrite()) {
                case TEvDiskRegistryPrivate::EvUpdateCmsHostDeviceStateRequest: {
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        diskRegistry.SendCmsActionRequest(std::move(actions));

        runtime->AdvanceCurrentTime(TDuration::Seconds(1));
        runtime->DispatchEvents({}, TDuration::MicroSeconds(40));
        diskRegistry.RebootTablet();
        diskRegistry.WaitReady();

        auto response = diskRegistry.RecvCmsActionResponse();

        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            response->Record.GetError().GetCode());
    }

    void ShouldRestoreCMSTimeoutAfterReboot(auto removeAction)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB)
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, { agent }));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agent});

        diskRegistry.AllocateDisk("vol1", 10_GB);

        ui32 cmsTimeout = 0;
        {
            auto [error, timeout] = removeAction(diskRegistry);

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);

            cmsTimeout = timeout;
        }

        runtime->AdvanceCurrentTime(TDuration::Days(1) / 2);

        {
            auto [error, timeout] = removeAction(diskRegistry);

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);
            UNIT_ASSERT_GT(cmsTimeout, timeout);
        }

        diskRegistry.RebootTablet();
        diskRegistry.WaitReady();

        {
            auto [error, timeout] = removeAction(diskRegistry);

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);
            UNIT_ASSERT_GT(cmsTimeout, timeout);
        }

        runtime->AdvanceCurrentTime(TDuration::Days(1) / 2);

        {
            auto [error, timeout] = removeAction(diskRegistry);

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);
            UNIT_ASSERT_VALUES_EQUAL(cmsTimeout, timeout);
        }

        diskRegistry.MarkDiskForCleanup("vol1");
        diskRegistry.DeallocateDisk("vol1");

        {
            auto [error, timeout] = removeAction(diskRegistry);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }
    }

    Y_UNIT_TEST(ShouldRestoreAgentCMSTimeoutAfterReboot)
    {
        ShouldRestoreCMSTimeoutAfterReboot([] (auto& diskRegistry) {
            return RemoveHost(diskRegistry, "agent-1");
        });
    }

    Y_UNIT_TEST(ShouldRestoreDeviceCMSTimeoutAfterReboot)
    {
        ShouldRestoreCMSTimeoutAfterReboot([] (auto& diskRegistry) {
            return RemoveDevice(diskRegistry, "agent-1", "dev-1");
        });
    }

    Y_UNIT_TEST(ShouldReturnOkForCmsRequestIfAgentDoesnotHaveUserDisks)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent}));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agent});

        {
            auto [error, timeout] = RemoveHost(diskRegistry, "agent-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        {
            diskRegistry.SendAllocateDiskRequest("vol1", 10_GB);
            auto response = diskRegistry.RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, response->GetStatus());
        }

        runtime->AdvanceCurrentTime(TDuration::Days(1) / 2);
        {
            auto [error, timeout] = RemoveHost(diskRegistry, "agent-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        diskRegistry.ChangeAgentState("agent-1", NProto::EAgentState::AGENT_STATE_UNAVAILABLE);
        diskRegistry.ChangeAgentState("agent-1", NProto::EAgentState::AGENT_STATE_ONLINE);

        diskRegistry.AllocateDisk("vol1", 10_GB);
    }

    Y_UNIT_TEST(ShouldReturnOkForCmsRequestIfDisksWereDeallocated)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent}));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agent});

        TSSProxyClient ss(*runtime);

        diskRegistry.AllocateDisk("vol1", 10_GB);

        {
            auto [error, timeout] = RemoveHost(diskRegistry, "agent-1");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);
        }

        diskRegistry.MarkDiskForCleanup("vol1");
        diskRegistry.DeallocateDisk("vol1");

        runtime->AdvanceCurrentTime(TDuration::Days(1) / 2);
        {
            auto [error, timeout] = RemoveHost(diskRegistry, "agent-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        diskRegistry.ChangeAgentState("agent-1", NProto::EAgentState::AGENT_STATE_UNAVAILABLE);
        diskRegistry.ChangeAgentState("agent-1", NProto::EAgentState::AGENT_STATE_ONLINE);
    }

    Y_UNIT_TEST(ShouldAllowToTakeAwayAlreadyUnavailableAgents)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1", "rack-1", 10_GB)
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-1", "uuid-2", "rack-2", 10_GB)
            })
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, agents));

        RegisterAndWaitForAgents(*runtime, agents);

        diskRegistry.AllocateDisk("vol1", 20_GB);
        diskRegistry.ChangeAgentState("agent-1", NProto::EAgentState::AGENT_STATE_UNAVAILABLE);

        ui32 cmsTimeout = 0;
        {
            auto [error, timeout] = RemoveHost(diskRegistry, "agent-1");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);
            cmsTimeout = timeout;
        }

        runtime->AdvanceCurrentTime(TDuration::Seconds(cmsTimeout / 2));

        {
            auto [error, timeout] = RemoveHost(diskRegistry, "agent-1");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);
            cmsTimeout = timeout;
        }

        runtime->AdvanceCurrentTime(TDuration::Seconds(cmsTimeout + 1));

        {
            auto [error, timeout] = RemoveHost(diskRegistry, "agent-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        // Take away empty agent without timeout

        diskRegistry.ChangeAgentState("agent-2", NProto::EAgentState::AGENT_STATE_UNAVAILABLE);

        {
            auto [error, timeout] = RemoveHost(diskRegistry, "agent-2");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);
        }

        diskRegistry.MarkDiskForCleanup("vol1");
        diskRegistry.DeallocateDisk("vol1");

        {
            auto [error, timeout] = RemoveHost(diskRegistry, "agent-2");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }
    }

    Y_UNIT_TEST(ShouldAllowToTakeAwayAlreadyUnavailableDevice)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent}));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agent});

        diskRegistry.AllocateDisk("vol1", 20_GB);
        diskRegistry.ChangeDeviceState("uuid-1", NProto::EDeviceState::DEVICE_STATE_ERROR);

        auto [error, timeout] = RemoveDevice(diskRegistry, "agent-1", "dev-1");

        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(0, timeout);

        diskRegistry.ChangeDeviceState("uuid-1", NProto::EDeviceState::DEVICE_STATE_ONLINE);
    }

    Y_UNIT_TEST(ShouldGetDependentDisks)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
                Device("dev-2", "uuid-2", "rack-1", 10_GB),
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-1", "uuid-3", "rack-2", 10_GB),
                Device("dev-2", "uuid-4", "rack-2", 10_GB),
            })
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, agents));

        RegisterAndWaitForAgents(*runtime, agents);

        TString vol1Device;
        TString vol2Device;
        TString vol3Device;
        TString vol4Device;
        TVector<TString> agent1Disks;
        TVector<TString> agent2Disks;

        auto allocateDisk = [&] (const TString& diskId, TString& devName) {
            auto response = diskRegistry.AllocateDisk(diskId, 10_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, msg.DevicesSize());
            devName = msg.GetDevices(0).GetDeviceName();
            if (msg.GetDevices(0).GetAgentId() == "agent-1") {
                agent1Disks.push_back(diskId);
            } else {
                agent2Disks.push_back(diskId);
            }
        };

        allocateDisk("vol1", vol1Device);
        allocateDisk("vol2", vol2Device);
        allocateDisk("vol3", vol3Device);
        allocateDisk("vol4", vol4Device);

        TVector<NProto::TAction> actions;
        {
            NProto::TAction action;
            action.SetHost("agent-1");
            action.SetType(NProto::TAction::GET_DEPENDENT_DISKS);
            actions.push_back(action);
        }

        {
            auto response = diskRegistry.CmsAction(actions);
            const auto& result = response->Record.GetActionResults(0);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, result.GetResult().GetCode());
            const auto& diskIds = result.GetDependentDisks();
            ASSERT_VECTOR_CONTENTS_EQUAL(diskIds, agent1Disks);
        }

        actions.back().SetHost("agent-2");

        {
            auto response = diskRegistry.CmsAction(actions);
            const auto& result = response->Record.GetActionResults(0);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, result.GetResult().GetCode());
            const auto& diskIds = result.GetDependentDisks();
            ASSERT_VECTOR_CONTENTS_EQUAL(diskIds, agent2Disks);
        }

        actions.back().SetHost("agent-3");

        {
            auto response = diskRegistry.CmsAction(actions);
            const auto& result = response->Record.GetActionResults(0);
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, result.GetResult().GetCode());
        }

        actions.back().SetHost("agent-1");
        actions.back().SetDevice(vol1Device);

        {
            auto response = diskRegistry.CmsAction(actions);
            const auto& result = response->Record.GetActionResults(0);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, result.GetResult().GetCode());
            const auto& diskIds = result.GetDependentDisks();
            ASSERT_VECTOR_CONTENTS_EQUAL(diskIds, TVector<TString>{"vol1"});
        }
    }

    Y_UNIT_TEST(ShouldGetDependentDisksAndIgnoreReplicated)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
                Device("dev-2", "uuid-2", "rack-1", 10_GB),
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-1", "uuid-3", "rack-2", 10_GB),
                Device("dev-2", "uuid-4", "rack-2", 10_GB),
            })
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, agents));

        RegisterAndWaitForAgents(*runtime, agents);

        {
            const uint32_t replicaCount = 1;
            auto response = diskRegistry.AllocateDisk(
                "replicated-vol", 10_GB, 4_KB, "", 0, "", "",
                replicaCount, NProto::STORAGE_MEDIA_SSD_MIRROR2);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(1, response->Record.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(1, response->Record.ReplicasSize());
        }

        {
            auto response = diskRegistry.AllocateDisk("nonrepl-vol", 20_GB);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(2, response->Record.DevicesSize());
        }

        for (const auto& agentId: {"agent-1", "agent-2"}) {
            NProto::TAction action;
            action.SetHost(agentId);
            action.SetType(NProto::TAction::GET_DEPENDENT_DISKS);
            TVector<NProto::TAction> actions = {action};

            auto response = diskRegistry.CmsAction(actions);
            const auto& result = response->Record.GetActionResults(0);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, result.GetResult().GetCode());

            ASSERT_VECTOR_CONTENTS_EQUAL(
                result.GetDependentDisks(),
                TVector<TString>({"nonrepl-vol"}));
        }
    }

    Y_UNIT_TEST(ShouldAddHostAndDevicesAfterRemoval)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
                Device("dev-2", "uuid-2", "rack-1", 10_GB)
            })};

        auto runtime = TTestRuntimeBuilder().WithAgents(agents).Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.SetWritableState(true);
        diskRegistry.WaitReady();
        diskRegistry.UpdateConfig(CreateRegistryConfig(0, agents));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, agents);

        {
            auto [error, timeout] = RemoveHost(diskRegistry, "agent-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        {
            auto [error, timeout] = AddHost(diskRegistry, "agent-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        {
            auto [error, timeout] = RemoveHost(diskRegistry, "agent-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        diskRegistry.ChangeAgentState(
            "agent-1",
            NProto::EAgentState::AGENT_STATE_UNAVAILABLE);

        {
            auto [error, timeout] = AddHost(diskRegistry, "agent-1");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);
        }

        diskRegistry.ChangeAgentState(
            "agent-1",
            NProto::EAgentState::AGENT_STATE_WARNING);

        {
            auto [error, timeout] = AddHost(diskRegistry, "agent-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        // Check idempotency.
        {
            auto [error, timeout] = AddHost(diskRegistry, "agent-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        diskRegistry.AllocateDisk("vol1", 10_GB);

        {
            auto [error, timeout] = RemoveHost(diskRegistry, "agent-1");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);
        }

        {
            auto [error, timeout] = AddHost(diskRegistry, "agent-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        diskRegistry.MarkDiskForCleanup("vol1");
        diskRegistry.DeallocateDisk("vol1");

        diskRegistry.ChangeDeviceState(
            "uuid-1",
            NProto::EDeviceState::DEVICE_STATE_ERROR);

        {
            auto [error, timeout] = AddDevice(diskRegistry, "agent-1", "dev-1");

            UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        diskRegistry.ChangeDeviceState(
            "uuid-1",
            NProto::EDeviceState::DEVICE_STATE_WARNING);

        {
            auto [error, timeout] = AddDevice(diskRegistry, "agent-1", "dev-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        {
            auto [error, timeout] = RemoveDevice(diskRegistry, "agent-1", "dev-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        {
            auto [error, timeout] = AddDevice(diskRegistry, "agent-1", "dev-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        // Check idempotency.
        {
            auto [error, timeout] = AddDevice(diskRegistry, "agent-1", "dev-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        diskRegistry.AllocateDisk("vol1", 10_GB);

        {
            auto [error, timeout] = RemoveDevice(diskRegistry, "agent-1", "dev-2");

            UNIT_ASSERT_VALUES_EQUAL_C(E_TRY_AGAIN, error.GetCode(), error.GetMessage());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);
        }

        {
            auto [error, timeout] = AddDevice(diskRegistry, "agent-1", "dev-2");

            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }
    }

    Y_UNIT_TEST(ShouldRejectAddHostForUnavailableAgent)
    {
        const TVector agents{CreateAgentConfig(
            "agent-1",
            {Device("dev-1", "uuid-1", "rack-1", 10_GB),
             Device("dev-2", "uuid-2", "rack-1", 10_GB)})};

        auto runtime = TTestRuntimeBuilder().WithAgents(agents).Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.SetWritableState(true);
        diskRegistry.WaitReady();
        diskRegistry.UpdateConfig(CreateRegistryConfig(0, agents));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, agents);

        {
            auto [error, timeout] = RemoveHost(diskRegistry, "agent-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        runtime->AdvanceCurrentTime(15min);

        diskRegistry.ChangeAgentState(
            "agent-1",
            NProto::EAgentState::AGENT_STATE_UNAVAILABLE);

        runtime->AdvanceCurrentTime(15min);

        {
            auto [error, timeout] = AddHost(diskRegistry, "agent-1");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                TDuration{5min},
                TDuration::Seconds(timeout));
        }

        runtime->AdvanceCurrentTime(2min);

        {
            auto [error, timeout] = AddHost(diskRegistry, "agent-1");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_LE(TDuration::Seconds(timeout), TDuration{3min});
            UNIT_ASSERT_GT(TDuration::Seconds(timeout), TDuration{2min});
        }

        runtime->AdvanceCurrentTime(10min);

        {
            auto [error, timeout] = AddHost(diskRegistry, "agent-1");

            UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        runtime->AdvanceCurrentTime(10min);

        {
            auto [error, timeout] = AddHost(diskRegistry, "agent-1");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                TDuration{5min},
                TDuration::Seconds(timeout));
        }

        runtime->AdvanceCurrentTime(1min);

        diskRegistry.ChangeAgentState(
            "agent-1",
            NProto::EAgentState::AGENT_STATE_WARNING);

        runtime->AdvanceCurrentTime(1min);

        {
            auto [error, timeout] = AddHost(diskRegistry, "agent-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
