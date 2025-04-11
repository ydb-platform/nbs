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

struct TFixture
    : public NUnitTest::TBaseFixture
{
    std::unique_ptr<NActors::TTestActorRuntime> Runtime;
    std::optional<TDiskRegistryClient> DiskRegistry;

    void SetUpRuntime(std::unique_ptr<NActors::TTestActorRuntime> runtime)
    {
        Runtime = std::move(runtime);

        DiskRegistry.emplace(*Runtime);
        DiskRegistry->WaitReady();
    }

    auto CmsAction(NProto::TAction action)
    {
        auto response = DiskRegistry->CmsAction(TVector {action});

        UNIT_ASSERT_VALUES_EQUAL(1, response->Record.ActionResultsSize());
        const auto& r = response->Record.GetActionResults(0);

        return std::make_pair(r.GetResult(), r.GetTimeout());
    }

    auto RemoveHost(const TString& agentId)
    {
        NProto::TAction action;
        action.SetHost(agentId);
        action.SetType(NProto::TAction::REMOVE_HOST);

        return CmsAction(std::move(action));
    }

    auto PurgeHost(const TString& agentId)
    {
        NProto::TAction action;
        action.SetHost(agentId);
        action.SetType(NProto::TAction::PURGE_HOST);

        return CmsAction(std::move(action));
    }

    auto AddHost(const TString& agentId)
    {
        NProto::TAction action;
        action.SetHost(agentId);
        action.SetType(NProto::TAction::ADD_HOST);

        return CmsAction(std::move(action));
    }

    auto AddDevice(const TString& agentId, const TString& path)
    {
        NProto::TAction action;
        action.SetHost(agentId);
        action.SetType(NProto::TAction::ADD_DEVICE);
        action.SetDevice(path);

        return CmsAction(std::move(action));
    }

    auto RemoveDevice(const TString& agentId, const TString& path)
    {
        NProto::TAction action;
        action.SetHost(agentId);
        action.SetType(NProto::TAction::REMOVE_DEVICE);
        action.SetDevice(path);

        return CmsAction(std::move(action));
    }

    void ShouldRestoreCMSTimeoutAfterReboot(auto removeAction)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB)
        });

        SetUpRuntime(TTestRuntimeBuilder()
            .WithAgents({agent})
            .Build());

        DiskRegistry->SetWritableState(true);

        DiskRegistry->UpdateConfig(CreateRegistryConfig(0, { agent }));

        RegisterAgents(*Runtime, 1);
        WaitForAgents(*Runtime, 1);
        WaitForSecureErase(*Runtime, {agent});

        DiskRegistry->AllocateDisk("vol1", 10_GB);

        ui32 cmsTimeout = 0;
        {
            auto [error, timeout] = removeAction();

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);

            cmsTimeout = timeout;
        }

        Runtime->AdvanceCurrentTime(TDuration::Days(1) / 2);

        {
            auto [error, timeout] = removeAction();

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);
            UNIT_ASSERT_GT(cmsTimeout, timeout);
        }

        DiskRegistry->RebootTablet();
        DiskRegistry->WaitReady();

        {
            auto [error, timeout] = removeAction();

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);
            UNIT_ASSERT_GT(cmsTimeout, timeout);
        }

        Runtime->AdvanceCurrentTime(TDuration::Days(1) / 2);

        {
            auto [error, timeout] = removeAction();

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);
            UNIT_ASSERT_VALUES_EQUAL(cmsTimeout, timeout);
        }

        DiskRegistry->MarkDiskForCleanup("vol1");
        DiskRegistry->DeallocateDisk("vol1");

        {
            auto [error, timeout] = removeAction();

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryTest)
{
    Y_UNIT_TEST_F(ShouldRemoveAgentsUponCmsRequest, TFixture)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        SetUpRuntime(TTestRuntimeBuilder()
            .WithAgents({agent})
            .Build());

        DiskRegistry->SetWritableState(true);

        DiskRegistry->UpdateConfig(CreateRegistryConfig(0, {agent}));

        RegisterAndWaitForAgents(*Runtime, {agent});

        TString freeDevice;
        TString diskDevice;
        {
            auto response = DiskRegistry->AllocateDisk("vol1", 10_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, msg.DevicesSize());
            diskDevice = msg.GetDevices(0).GetDeviceUUID();

            freeDevice = diskDevice == "uuid-1"
                ? "uuid-2"
                : "uuid-1";
        }

        ui32 cmsTimeout = 0;
        {
            auto [error, timeout] = RemoveHost("agent-1");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);

            cmsTimeout = timeout;
        }

        // request will be ignored
        DiskRegistry->ChangeAgentState("agent-1", NProto::EAgentState::AGENT_STATE_ONLINE);

        {
            DiskRegistry->SendAllocateDiskRequest("vol2", 10_GB);
            auto response = DiskRegistry->RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, response->GetStatus());
        }

        Runtime->AdvanceCurrentTime(TDuration::Days(1) / 2);

        {
            auto [error, timeout] = RemoveHost("agent-1");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);
            UNIT_ASSERT_LT(timeout, cmsTimeout);
        }

        Runtime->AdvanceCurrentTime(TDuration::Days(1) / 2);

        {
            auto [error, timeout] = RemoveHost("agent-1");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);
            UNIT_ASSERT_VALUES_EQUAL(timeout, cmsTimeout);
        }

        DiskRegistry->ChangeDeviceState(freeDevice, NProto::EDeviceState::DEVICE_STATE_ERROR);

        {
            auto [error, timeout] = RemoveHost("agent-1");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);
        }

        DiskRegistry->ChangeDeviceState(diskDevice, NProto::EDeviceState::DEVICE_STATE_ERROR);

        {
            auto [error, timeout] = RemoveHost("agent-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        DiskRegistry->ChangeAgentState("agent-1", NProto::EAgentState::AGENT_STATE_UNAVAILABLE);
        DiskRegistry->ChangeAgentState("agent-1", NProto::EAgentState::AGENT_STATE_ONLINE);

        DiskRegistry->ChangeDeviceState("uuid-1", NProto::EDeviceState::DEVICE_STATE_ONLINE);
        DiskRegistry->ChangeDeviceState("uuid-2", NProto::EDeviceState::DEVICE_STATE_ONLINE);

        DiskRegistry->AllocateDisk("vol2", 10_GB);
    }

    Y_UNIT_TEST_F(ShouldFailCmsRequestIfActionIsUnknown, TFixture)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        SetUpRuntime(TTestRuntimeBuilder()
            .WithAgents({agent})
            .Build());

        DiskRegistry->SetWritableState(true);

        DiskRegistry->UpdateConfig(CreateRegistryConfig(0, {agent}));

        RegisterAgents(*Runtime, 1);
        WaitForAgents(*Runtime, 1);
        WaitForSecureErase(*Runtime, {agent});

        NProto::TAction action;
        action.SetHost("agent-1");
        action.SetType(NProto::TAction::UNKNOWN);
        TVector<NProto::TAction> actions { action };

        auto response = DiskRegistry->CmsAction(std::move(actions));
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            response->Record.GetActionResults(0).GetResult().GetCode());
    }

    Y_UNIT_TEST_F(ShouldFailCmsRequestIfAgentIsNotFound, TFixture)
    {
        SetUpRuntime(TTestRuntimeBuilder()
            .Build());

        DiskRegistry->SetWritableState(true);

        auto [error, timeout] = RemoveHost("agent-1");

        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(0, timeout);
    }

    Y_UNIT_TEST_F(ShouldFailCmsRequestIfDeviceNotFound, TFixture)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        SetUpRuntime(TTestRuntimeBuilder()
            .WithAgents({agent})
            .Build());

        DiskRegistry->SetWritableState(true);

        DiskRegistry->UpdateConfig(CreateRegistryConfig(0, {agent}));

        RegisterAgents(*Runtime, 1);
        WaitForAgents(*Runtime, 1);
        WaitForSecureErase(*Runtime, {agent});

        {
            auto [error, timeout] = RemoveDevice("agent-2", "dev-1");

            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        {
            auto [error, timeout] = RemoveDevice("agent-1", "dev-10");

            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }
    }

    Y_UNIT_TEST_F(ShouldRemoveDeviceUponCmsRequest, TFixture)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
        });

        SetUpRuntime(TTestRuntimeBuilder()
            .WithAgents({agent})
            .Build());

        DiskRegistry->SetWritableState(true);

        DiskRegistry->UpdateConfig(CreateRegistryConfig(0, {agent}));

        RegisterAgents(*Runtime, 1);
        WaitForAgents(*Runtime, 1);
        WaitForSecureErase(*Runtime, {agent});

        DiskRegistry->AllocateDisk("vol1", 10_GB);

        ui32 cmsTimeout = 0;
        {
            auto [error, timeout] = RemoveDevice("agent-1", "dev-1");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);

            cmsTimeout = timeout;
        }

        // request will be ignored
        DiskRegistry->ChangeDeviceState("uuid-1", NProto::EDeviceState::DEVICE_STATE_ONLINE);

        {
            auto response = DiskRegistry->AllocateDisk("vol1", 10_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, msg.DevicesSize());
            const auto& device = msg.GetDevices(0);

            UNIT_ASSERT_VALUES_EQUAL("uuid-1", device.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(NProto::DEVICE_STATE_WARNING),
                static_cast<ui32>(device.GetState()));
        }

        Runtime->AdvanceCurrentTime(TDuration::Days(1) / 2);

        {
            auto [error, timeout] = RemoveDevice("agent-1", "dev-1");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);
            UNIT_ASSERT_LT(timeout, cmsTimeout);
        }

        Runtime->AdvanceCurrentTime(TDuration::Days(1) / 2);

        {
            auto [error, timeout] = RemoveDevice("agent-1", "dev-1");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);

            cmsTimeout = timeout;
        }

        DiskRegistry->ChangeDeviceState("uuid-1", NProto::EDeviceState::DEVICE_STATE_ERROR);

        {
            auto [error, timeout] = RemoveDevice("agent-1", "dev-1");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        DiskRegistry->ChangeDeviceState("uuid-1", NProto::EDeviceState::DEVICE_STATE_ONLINE);

        {
            auto response = DiskRegistry->AllocateDisk("vol1", 10_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, msg.DevicesSize());
            const auto& device = msg.GetDevices(0);

            UNIT_ASSERT_VALUES_EQUAL("uuid-1", device.GetDeviceUUID());
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ONLINE, device.GetState());
        }
    }

    Y_UNIT_TEST_F(ShouldRemoveMultipleDevicesUponCmsRequest, TFixture)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("some/path", "uuid-1", "rack-1", 10_GB),
            Device("some/path", "uuid-2", "rack-1", 10_GB),
        });

        SetUpRuntime(TTestRuntimeBuilder()
            .WithAgents({agent})
            .Build());

        DiskRegistry->SetWritableState(true);

        DiskRegistry->UpdateConfig(CreateRegistryConfig(0, {agent}));

        RegisterAgents(*Runtime, 1);
        WaitForAgents(*Runtime, 1);
        WaitForSecureErase(*Runtime, {agent});

        DiskRegistry->AllocateDisk("vol1", 10_GB);

        ui32 cmsTimeout = 0;
        {
            auto [error, timeout] =
                RemoveDevice("agent-1", "some/path");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);

            cmsTimeout = timeout;
        }

        Runtime->AdvanceCurrentTime(TDuration::Seconds(cmsTimeout));

        DiskRegistry->MarkDiskForCleanup("vol1");
        DiskRegistry->DeallocateDisk("vol1");

        {
            auto [error, timeout] =
                RemoveDevice("agent-1", "some/path");

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                error.GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        {
            DiskRegistry->SendAllocateDiskRequest("vol1", 10_GB);
            auto response = DiskRegistry->RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                E_BS_DISK_ALLOCATION_FAILED,
                response->GetStatus());
        }
    }

    Y_UNIT_TEST_F(ShouldReturnOkIfRemovedDeviceHasNoDisks, TFixture)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
        });

        SetUpRuntime(TTestRuntimeBuilder()
            .WithAgents({agent})
            .Build());

        DiskRegistry->SetWritableState(true);

        DiskRegistry->UpdateConfig(CreateRegistryConfig(0, {agent}));

        RegisterAgents(*Runtime, 1);
        WaitForAgents(*Runtime, 1);
        WaitForSecureErase(*Runtime, {agent});

        {
            auto [error, timeout] = RemoveDevice("agent-1", "dev-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        {
            DiskRegistry->SendAllocateDiskRequest("vol1", 10_GB);
            auto response = DiskRegistry->RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                E_BS_DISK_ALLOCATION_FAILED,
                response->GetStatus());
        }

        Runtime->AdvanceCurrentTime(TDuration::Days(1) / 2);
        {
            auto [error, timeout] = RemoveDevice("agent-1", "dev-1");

            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        DiskRegistry->ChangeDeviceState(
            "uuid-1",
            NProto::EDeviceState::DEVICE_STATE_ERROR);
        DiskRegistry->ChangeDeviceState(
            "uuid-1",
            NProto::EDeviceState::DEVICE_STATE_ONLINE);

        DiskRegistry->AllocateDisk("vol1", 10_GB);
    }

    Y_UNIT_TEST_F(ShouldFailCmsRequestIfDiskRegistryRestarts, TFixture)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        SetUpRuntime(TTestRuntimeBuilder()
            .WithAgents({agent})
            .Build());

        DiskRegistry->SetWritableState(true);

        DiskRegistry->UpdateConfig(CreateRegistryConfig(0, {agent}));

        RegisterAgents(*Runtime, 1);
        WaitForAgents(*Runtime, 1);
        WaitForSecureErase(*Runtime, {agent});

        NProto::TAction action;
        action.SetHost("agent-2");
        action.SetType(NProto::TAction::REMOVE_DEVICE);
        action.SetDevice("dev-1");
        TVector<NProto::TAction> actions;
        actions.push_back(action);

        Runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvDiskRegistryPrivate::
                        EvUpdateCmsHostDeviceStateRequest: {
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        DiskRegistry->SendCmsActionRequest(std::move(actions));

        Runtime->AdvanceCurrentTime(1s);
        Runtime->DispatchEvents({}, 40us);
        DiskRegistry->RebootTablet();
        DiskRegistry->WaitReady();

        auto response = DiskRegistry->RecvCmsActionResponse();

        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            response->Record.GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldRestoreAgentCMSTimeoutAfterReboot, TFixture)
    {
        ShouldRestoreCMSTimeoutAfterReboot([this] {
            return RemoveHost("agent-1");
        });
    }

    Y_UNIT_TEST_F(ShouldRestoreDeviceCMSTimeoutAfterReboot, TFixture)
    {
        ShouldRestoreCMSTimeoutAfterReboot([this] {
            return RemoveDevice("agent-1", "dev-1");
        });
    }

    Y_UNIT_TEST_F(ShouldReturnOkForCmsRequestIfAgentDoesnotHaveUserDisks, TFixture)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        SetUpRuntime(TTestRuntimeBuilder()
            .WithAgents({agent})
            .Build());

        DiskRegistry->SetWritableState(true);

        DiskRegistry->UpdateConfig(CreateRegistryConfig(0, {agent}));

        RegisterAgents(*Runtime, 1);
        WaitForAgents(*Runtime, 1);
        WaitForSecureErase(*Runtime, {agent});

        {
            auto [error, timeout] = RemoveHost("agent-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        {
            DiskRegistry->SendAllocateDiskRequest("vol1", 10_GB);
            auto response = DiskRegistry->RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, response->GetStatus());
        }

        Runtime->AdvanceCurrentTime(TDuration::Days(1) / 2);
        {
            auto [error, timeout] = RemoveHost("agent-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        DiskRegistry->ChangeAgentState("agent-1", NProto::EAgentState::AGENT_STATE_UNAVAILABLE);
        DiskRegistry->ChangeAgentState("agent-1", NProto::EAgentState::AGENT_STATE_ONLINE);

        DiskRegistry->AllocateDisk("vol1", 10_GB);
    }

    Y_UNIT_TEST_F(ShouldReturnOkForCmsRequestIfDisksWereDeallocated, TFixture)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        SetUpRuntime(TTestRuntimeBuilder()
            .WithAgents({agent})
            .Build());

        DiskRegistry->SetWritableState(true);

        DiskRegistry->UpdateConfig(CreateRegistryConfig(0, {agent}));

        RegisterAgents(*Runtime, 1);
        WaitForAgents(*Runtime, 1);
        WaitForSecureErase(*Runtime, {agent});

        TSSProxyClient ss(*Runtime);

        DiskRegistry->AllocateDisk("vol1", 10_GB);

        {
            auto [error, timeout] = RemoveHost("agent-1");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);
        }

        DiskRegistry->MarkDiskForCleanup("vol1");
        DiskRegistry->DeallocateDisk("vol1");

        Runtime->AdvanceCurrentTime(TDuration::Days(1) / 2);
        {
            auto [error, timeout] = RemoveHost("agent-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        DiskRegistry->ChangeAgentState("agent-1", NProto::EAgentState::AGENT_STATE_UNAVAILABLE);
        DiskRegistry->ChangeAgentState("agent-1", NProto::EAgentState::AGENT_STATE_ONLINE);
    }

    Y_UNIT_TEST_F(ShouldAllowToTakeAwayAlreadyUnavailableAgents, TFixture)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1", "rack-1", 10_GB)
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-1", "uuid-2", "rack-2", 10_GB)
            })
        };

        SetUpRuntime(TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build());

        DiskRegistry->SetWritableState(true);

        DiskRegistry->UpdateConfig(CreateRegistryConfig(0, agents));

        RegisterAndWaitForAgents(*Runtime, agents);

        DiskRegistry->AllocateDisk("vol1", 20_GB);
        DiskRegistry->ChangeAgentState("agent-1", NProto::EAgentState::AGENT_STATE_UNAVAILABLE);

        ui32 cmsTimeout = 0;
        {
            auto [error, timeout] = RemoveHost("agent-1");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);
            cmsTimeout = timeout;
        }

        Runtime->AdvanceCurrentTime(TDuration::Seconds(cmsTimeout / 2));

        {
            auto [error, timeout] = RemoveHost("agent-1");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);
            cmsTimeout = timeout;
        }

        Runtime->AdvanceCurrentTime(TDuration::Seconds(cmsTimeout + 1));

        {
            auto [error, timeout] = RemoveHost("agent-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        // Take away empty agent without timeout

        DiskRegistry->ChangeAgentState("agent-2", NProto::EAgentState::AGENT_STATE_UNAVAILABLE);

        {
            auto [error, timeout] = RemoveHost("agent-2");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);
        }

        DiskRegistry->MarkDiskForCleanup("vol1");
        DiskRegistry->DeallocateDisk("vol1");

        {
            auto [error, timeout] = RemoveHost("agent-2");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }
    }

    Y_UNIT_TEST_F(ShouldAllowToTakeAwayAlreadyUnavailableDevice, TFixture)
    {
        const auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        SetUpRuntime(TTestRuntimeBuilder()
            .WithAgents({agent})
            .Build());

        DiskRegistry->SetWritableState(true);

        DiskRegistry->UpdateConfig(CreateRegistryConfig(0, {agent}));

        RegisterAgents(*Runtime, 1);
        WaitForAgents(*Runtime, 1);
        WaitForSecureErase(*Runtime, {agent});

        DiskRegistry->AllocateDisk("vol1", 20_GB);
        DiskRegistry->ChangeDeviceState("uuid-1", NProto::EDeviceState::DEVICE_STATE_ERROR);

        auto [error, timeout] = RemoveDevice("agent-1", "dev-1");

        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(0, timeout);

        DiskRegistry->ChangeDeviceState("uuid-1", NProto::EDeviceState::DEVICE_STATE_ONLINE);
    }

    Y_UNIT_TEST_F(ShouldGetDependentDisks, TFixture)
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

        SetUpRuntime(TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build());

        DiskRegistry->SetWritableState(true);

        DiskRegistry->UpdateConfig(CreateRegistryConfig(0, agents));

        RegisterAndWaitForAgents(*Runtime, agents);

        TString vol1Device;
        TString vol2Device;
        TString vol3Device;
        TString vol4Device;
        TVector<TString> agent1Disks;
        TVector<TString> agent2Disks;

        auto allocateDisk = [&] (const TString& diskId, TString& devName) {
            auto response = DiskRegistry->AllocateDisk(diskId, 10_GB);
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
            auto response = DiskRegistry->CmsAction(actions);
            const auto& result = response->Record.GetActionResults(0);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, result.GetResult().GetCode());
            const auto& diskIds = result.GetDependentDisks();
            ASSERT_VECTOR_CONTENTS_EQUAL(diskIds, agent1Disks);
        }

        actions.back().SetHost("agent-2");

        {
            auto response = DiskRegistry->CmsAction(actions);
            const auto& result = response->Record.GetActionResults(0);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, result.GetResult().GetCode());
            const auto& diskIds = result.GetDependentDisks();
            ASSERT_VECTOR_CONTENTS_EQUAL(diskIds, agent2Disks);
        }

        actions.back().SetHost("agent-3");

        {
            auto response = DiskRegistry->CmsAction(actions);
            const auto& result = response->Record.GetActionResults(0);
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, result.GetResult().GetCode());
        }

        actions.back().SetHost("agent-1");
        actions.back().SetDevice(vol1Device);

        {
            auto response = DiskRegistry->CmsAction(actions);
            const auto& result = response->Record.GetActionResults(0);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, result.GetResult().GetCode());
            const auto& diskIds = result.GetDependentDisks();
            ASSERT_VECTOR_CONTENTS_EQUAL(diskIds, TVector<TString>{"vol1"});
        }
    }

    Y_UNIT_TEST_F(ShouldGetDependentDisksAndIgnoreReplicated, TFixture)
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

        SetUpRuntime(TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build());

        DiskRegistry->SetWritableState(true);

        DiskRegistry->UpdateConfig(CreateRegistryConfig(0, agents));

        RegisterAndWaitForAgents(*Runtime, agents);

        {
            const uint32_t replicaCount = 1;
            auto response = DiskRegistry->AllocateDisk(
                "replicated-vol", 10_GB, 4_KB, "", 0, "", "",
                replicaCount, NProto::STORAGE_MEDIA_SSD_MIRROR2);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(1, response->Record.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(1, response->Record.ReplicasSize());
        }

        {
            auto response = DiskRegistry->AllocateDisk("nonrepl-vol", 20_GB);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(2, response->Record.DevicesSize());
        }

        for (const auto& agentId: {"agent-1", "agent-2"}) {
            NProto::TAction action;
            action.SetHost(agentId);
            action.SetType(NProto::TAction::GET_DEPENDENT_DISKS);
            TVector<NProto::TAction> actions = {action};

            auto response = DiskRegistry->CmsAction(actions);
            const auto& result = response->Record.GetActionResults(0);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, result.GetResult().GetCode());

            ASSERT_VECTOR_CONTENTS_EQUAL(
                result.GetDependentDisks(),
                TVector<TString>({"nonrepl-vol"}));
        }
    }

    Y_UNIT_TEST_F(ShouldAddHostAndDevicesAfterRemoval, TFixture)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
                Device("dev-2", "uuid-2", "rack-1", 10_GB)
            })};

        SetUpRuntime(TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build());

        DiskRegistry->SetWritableState(true);
        DiskRegistry->UpdateConfig(CreateRegistryConfig(0, agents));

        RegisterAgents(*Runtime, 1);
        WaitForAgents(*Runtime, 1);
        WaitForSecureErase(*Runtime, agents);

        {
            auto [error, timeout] = RemoveHost("agent-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        {
            auto [error, timeout] = AddHost("agent-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        {
            auto [error, timeout] = RemoveHost("agent-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        {
            auto [error, timeout] = PurgeHost("agent-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        DiskRegistry->ChangeAgentState(
            "agent-1",
            NProto::EAgentState::AGENT_STATE_UNAVAILABLE);

        {
            auto [error, timeout] = AddHost("agent-1");

            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        RegisterAgents(*Runtime, 1);
        WaitForAgents(*Runtime, 1);

        DiskRegistry->ChangeAgentState(
            "agent-1",
            NProto::EAgentState::AGENT_STATE_WARNING);

        {
            auto [error, timeout] = AddHost("agent-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        WaitForSecureErase(*Runtime, agents);

        // Check idempotency.
        {
            auto [error, timeout] = AddHost("agent-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        DiskRegistry->AllocateDisk("vol1", 10_GB);

        {
            auto [error, timeout] = RemoveHost("agent-1");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);
        }

        {
            auto [error, timeout] = AddHost("agent-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        DiskRegistry->MarkDiskForCleanup("vol1");
        DiskRegistry->DeallocateDisk("vol1");

        DiskRegistry->ChangeDeviceState(
            "uuid-1",
            NProto::EDeviceState::DEVICE_STATE_ERROR);

        {
            auto [error, timeout] = AddDevice("agent-1", "dev-1");

            UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        DiskRegistry->ChangeDeviceState(
            "uuid-1",
            NProto::EDeviceState::DEVICE_STATE_WARNING);

        {
            auto [error, timeout] = AddDevice("agent-1", "dev-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        {
            auto [error, timeout] = RemoveDevice("agent-1", "dev-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        {
            auto [error, timeout] = AddDevice("agent-1", "dev-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        // Check idempotency.
        {
            auto [error, timeout] = AddDevice("agent-1", "dev-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        DiskRegistry->AllocateDisk("vol1", 10_GB);

        {
            auto [error, timeout] = RemoveDevice("agent-1", "dev-2");

            UNIT_ASSERT_VALUES_EQUAL_C(E_TRY_AGAIN, error.GetCode(), error.GetMessage());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);
        }

        {
            auto [error, timeout] = AddDevice("agent-1", "dev-2");

            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }
    }

    Y_UNIT_TEST_F(ShouldRejectAddHostForUnavailableAgent, TFixture)
    {
        const TVector agents{CreateAgentConfig(
            "agent-1",
            {Device("dev-1", "uuid-1", "rack-1", 10_GB),
             Device("dev-2", "uuid-2", "rack-1", 10_GB)})};

        auto config = CreateDefaultStorageConfig();
        config.SetIdleAgentDeployByCmsDelay(
            TDuration::Minutes(10).MilliSeconds());
        SetUpRuntime(
            TTestRuntimeBuilder().WithAgents(agents).With(config).Build());

        DiskRegistry->SetWritableState(true);
        DiskRegistry->UpdateConfig(CreateRegistryConfig(0, agents));

        RegisterAgents(*Runtime, 1);
        WaitForAgents(*Runtime, 1);
        WaitForSecureErase(*Runtime, agents);

        DiskRegistry->ChangeAgentState(
            "agent-1",
            NProto::EAgentState::AGENT_STATE_WARNING);

        Runtime->AdvanceCurrentTime(15min);

        DiskRegistry->ChangeAgentState(
            "agent-1",
            NProto::EAgentState::AGENT_STATE_UNAVAILABLE);

        Runtime->AdvanceCurrentTime(15min);

        {
            auto [error, timeout] = AddHost("agent-1");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                TDuration::MilliSeconds(
                    config.GetIdleAgentDeployByCmsDelay()),
                TDuration::Seconds(timeout));
        }

        Runtime->AdvanceCurrentTime(2min);

        {
            auto [error, timeout] = AddHost("agent-1");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_LE(
                TDuration::Seconds(timeout),
                TDuration::MilliSeconds(
                    config.GetIdleAgentDeployByCmsDelay()) -
                    2min);
            UNIT_ASSERT_GT(
                TDuration::Seconds(timeout),
                TDuration::MilliSeconds(
                    config.GetIdleAgentDeployByCmsDelay()) -
                    3min);
        }

        Runtime->AdvanceCurrentTime(
            TDuration::MilliSeconds(config.GetIdleAgentDeployByCmsDelay()));

        {
            auto [error, timeout] = AddHost("agent-1");

            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }

        Runtime->AdvanceCurrentTime(10min);

        {
            auto [error, timeout] = AddHost("agent-1");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                TDuration::MilliSeconds(
                    config.GetIdleAgentDeployByCmsDelay()),
                TDuration::Seconds(timeout));
        }

        Runtime->AdvanceCurrentTime(1min);

        DiskRegistry->ChangeAgentState(
            "agent-1",
            NProto::EAgentState::AGENT_STATE_WARNING);

        Runtime->AdvanceCurrentTime(1min);

        {
            auto [error, timeout] = AddHost("agent-1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }
    }

    Y_UNIT_TEST_F(ShouldCleanupLocalDevicesBeforeRemoveHost, TFixture)
    {
        const TVector agents {CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 93_GB),
            Device("dev-2", "uuid-2", "rack-1", 93_GB)
                | WithPool("local", NProto::DEVICE_POOL_KIND_LOCAL)
        })};

        SetUpRuntime(TTestRuntimeBuilder()
            .With([]{
                auto config = CreateDefaultStorageConfig();
                config.SetNonReplicatedDontSuspendDevices(false);
                config.SetAllocationUnitNonReplicatedSSD(93);

                return config;
            }())
            .WithAgents(agents)
            .Build());

        DiskRegistry->SetWritableState(true);

        {
            auto config = CreateRegistryConfig(0, agents);
            auto& pool = *config.AddDevicePoolConfigs();
            pool.SetName("local");
            pool.SetAllocationUnit(93_GB);
            pool.SetKind(NProto::DEVICE_POOL_KIND_LOCAL);
            DiskRegistry->UpdateConfig(std::move(config));
        }

        RegisterAgents(*Runtime, 1);
        WaitForAgents(*Runtime, 1);

        DiskRegistry->ResumeDevice(
            agents[0].GetAgentId(),
            agents[0].GetDevices(1).GetDeviceName());

        WaitForSecureErase(*Runtime, agents);

        {
            auto response = DiskRegistry->AllocateDisk("nrd", 93_GB);
            UNIT_ASSERT_VALUES_EQUAL(1, response->Record.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-1",
                response->Record.GetDevices(0).GetDeviceUUID());
        }

        {
            auto response = DiskRegistry->AllocateDisk(
                "local",
                93_GB,     // diskSize
                512,       // blockSize
                TString{}, // placementGroupId
                0,         // placementPartitionIndex
                "nbs",
                "nbs.tests",
                0,         // replicaCount
                NProto::STORAGE_MEDIA_SSD_LOCAL);

            UNIT_ASSERT_VALUES_EQUAL(1, response->Record.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-2",
                response->Record.GetDevices(0).GetDeviceUUID());
        }

        // try to remove host
        {
            auto [error, timeout] = RemoveHost(agents[0].GetAgentId());

            // no way, we have disks on the host
            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);
        }

        TAutoPtr<IEventHandle> secureErase;
        Runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            switch (event->GetTypeRewrite()) {
                case TEvDiskRegistryPrivate::EvSecureEraseRequest: {
                    auto& msg = *event->Get<
                        TEvDiskRegistryPrivate::TEvSecureEraseRequest>();

                    UNIT_ASSERT_VALUES_EQUAL(1, msg.DirtyDevices.size());
                    UNIT_ASSERT_VALUES_EQUAL(
                        "uuid-1",
                        msg.DirtyDevices[0].GetDeviceUUID());

                    UNIT_ASSERT(!secureErase);
                    secureErase.Swap(event);
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        // deallocate 'nrd' so we trigger the secure erase operation
        DiskRegistry->MarkDiskForCleanup("nrd");
        DiskRegistry->DeallocateDisk("nrd");

        Runtime->DispatchEvents({
            .CustomFinalCondition = [&] {
                return !!secureErase;
            }
        }, 15s);
        UNIT_ASSERT(secureErase);
        Runtime->SetObserverFunc(&TTestActorRuntime::DefaultObserverFunc);

        DiskRegistry->MarkDiskForCleanup("local");
        DiskRegistry->SendDeallocateDiskRequest(
            "local",
            true // sync
        );

        // try to remove host
        {
            auto [error, timeout] = RemoveHost(agents[0].GetAgentId());

            // no way, we have a dirty local device on the host
            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(0, timeout);
        }

        // Complete the secure erase operation
        Runtime->Send(secureErase);
        UNIT_ASSERT(!secureErase);

        // Wait for the next secure erase to clear the local device (uuid-2)
        Runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            switch (event->GetTypeRewrite()) {
                case TEvDiskRegistryPrivate::EvSecureEraseRequest: {
                    auto& msg = *event->Get<
                        TEvDiskRegistryPrivate::TEvSecureEraseRequest>();

                    UNIT_ASSERT_VALUES_EQUAL(1, msg.DirtyDevices.size());
                    UNIT_ASSERT_VALUES_EQUAL(
                        "uuid-2",
                        msg.DirtyDevices[0].GetDeviceUUID());
                }
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        Runtime->DispatchEvents(
            {.FinalEvents = {TDispatchOptions::TFinalEventCondition(
                 TEvDiskRegistryPrivate::EvSecureEraseResponse)}},
            15s);

        Runtime->DispatchEvents(
            {.FinalEvents = {TDispatchOptions::TFinalEventCondition(
                 TEvDiskRegistry::EvDeallocateDiskResponse)}},
            15s);

        {
            auto response = DiskRegistry->RecvDeallocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        // now we ready to remove host
        {
            auto [error, timeout] = RemoveHost(agents[0].GetAgentId());

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, timeout);
        }
    }

    Y_UNIT_TEST_F(ShouldUpdateAllLogicalDevices, TFixture)
    {
        // two logical devices on the same path
        const auto agent = CreateAgentConfig("agent-1", {
            Device("path", "uuid-1", "rack-1", 10_GB),
            Device("path", "uuid-2", "rack-1", 10_GB)
        });

        auto config = CreateDefaultStorageConfig();

        SetUpRuntime(TTestRuntimeBuilder()
            .WithAgents({agent})
            .With(config)
            .Build());

        DiskRegistry->SetWritableState(true);
        DiskRegistry->UpdateConfig(CreateRegistryConfig(0, {agent}));
        RegisterAndWaitForAgents(*Runtime, {agent});

        // allocate vol0 on one of the two logical devices
        {
            auto response = DiskRegistry->AllocateDisk("vol0", 10_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, msg.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(0, msg.MigrationsSize());
        }

        {
            auto [error, timeout] = RemoveDevice(agent.GetAgentId(), "path");

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                config.GetNonReplicatedInfraTimeout(),
                timeout * 1000);
        }

        // check that vol0 hasn't started migration to a logical device with the
        // same path
        {
            auto response = DiskRegistry->AllocateDisk("vol0", 10_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, msg.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(0, msg.MigrationsSize());
        }

        // check that the second logical device is not available for allocation
        {
            DiskRegistry->SendAllocateDiskRequest("vol1", 10_GB);
            auto response = DiskRegistry->RecvAllocateDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                E_BS_DISK_ALLOCATION_FAILED,
                response->GetStatus());
        }
    }

    Y_UNIT_TEST_F(
        ShouldNotScheduleSwitchAgentDiskToReadOnlyAfterAgentRemoving,
        TFixture)
    {
        const auto agent = CreateAgentConfig("agent-1", {});

        SetUpRuntime(TTestRuntimeBuilder().WithAgents({agent}).Build());

        DiskRegistry->SetWritableState(true);

        DiskRegistry->UpdateConfig(CreateRegistryConfig(0, {agent}));

        RegisterAndWaitForAgents(*Runtime, {agent});

        {
            DiskRegistry->ChangeAgentState(
                "agent-1",
                NProto::AGENT_STATE_UNAVAILABLE);
        }

        // Check that no scheduled switch events.
        auto scheduledEvents = Runtime->CaptureScheduledEvents();
        for (auto event: scheduledEvents) {
            UNIT_ASSERT(
                event.Event->GetTypeRewrite() !=
                TEvDiskRegistryPrivate::EvSwitchAgentDisksToReadOnlyRequest);
        }

        // Sending Disable agent because it checks that agent exists, and if it
        // is not true, response with E_NOT_FOUND.
        DiskRegistry->SendDisableAgentRequest("agent-1");
        auto response = DiskRegistry->RecvDisableAgentResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response->GetError().GetCode());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
