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

#include <atomic>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NDiskRegistryTest;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryTest)
{
    Y_UNIT_TEST(ShouldPassAllParamsInAcquireDevicesRequest)
    {
        const auto agentConfig = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB),
            Device("dev-3", "uuid-3", "rack-1", 10_GB),
            Device("dev-4", "uuid-4", "rack-1", 10_GB),
            Device("dev-5", "uuid-5", "rack-1", 10_GB)
        });

        THashSet<TString> acquiredDevices;

        auto agent = CreateTestDiskAgent(agentConfig);

        NProto::EVolumeAccessMode accessMode = NProto::VOLUME_ACCESS_READ_WRITE;
        ui64 mountSeqNumber = 0;
        TString diskId;
        ui32 volumeGeneration = 0;

        agent->HandleAcquireDevicesImpl = [&] (
            const TEvDiskAgent::TEvAcquireDevicesRequest::TPtr& ev,
            const TActorContext& ctx)
        {
            const auto& record = ev->Get()->Record;
            accessMode = record.GetAccessMode();
            mountSeqNumber = record.GetMountSeqNumber();
            diskId = record.GetDiskId();
            volumeGeneration = record.GetVolumeGeneration();

            NCloud::Reply(
                ctx,
                *ev,
                std::make_unique<TEvDiskAgent::TEvAcquireDevicesResponse>()
            );

            return true;
        };

        agent->HandleReleaseDevicesImpl = [&] (
            const TEvDiskAgent::TEvReleaseDevicesRequest::TPtr& ev,
            const TActorContext& ctx)
        {
            Y_UNUSED(ctx);

            const auto& record = ev->Get()->Record;
            diskId = record.GetDiskId();
            volumeGeneration = record.GetVolumeGeneration();
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agentConfig}));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agentConfig});

        diskRegistry.AllocateDisk("disk-1", 10_GB);

        diskRegistry.AcquireDisk(
            "disk-1",
            "session-1",
            NProto::VOLUME_ACCESS_READ_ONLY,
            1,
            11
        );

        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::VOLUME_ACCESS_READ_ONLY),
            static_cast<int>(accessMode)
        );
        UNIT_ASSERT_VALUES_EQUAL(1, mountSeqNumber);
        UNIT_ASSERT_VALUES_EQUAL("disk-1", diskId);
        UNIT_ASSERT_VALUES_EQUAL(11, volumeGeneration);

        accessMode = {};
        mountSeqNumber = 0;
        diskId = {};
        volumeGeneration = 0;

        diskRegistry.AcquireDisk(
            "disk-1",
            "session-1",
            NProto::VOLUME_ACCESS_READ_WRITE,
            2,
            12
        );

        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::VOLUME_ACCESS_READ_WRITE),
            static_cast<int>(accessMode)
        );
        UNIT_ASSERT_VALUES_EQUAL(2, mountSeqNumber);
        UNIT_ASSERT_VALUES_EQUAL("disk-1", diskId);
        UNIT_ASSERT_VALUES_EQUAL(12, volumeGeneration);

        accessMode = {};
        mountSeqNumber = 0;
        diskId = {};
        volumeGeneration = 0;

        diskRegistry.ReleaseDisk("disk-1", "session-1", 12);

        UNIT_ASSERT_VALUES_EQUAL("disk-1", diskId);
        UNIT_ASSERT_VALUES_EQUAL(12, volumeGeneration);
    }

    Y_UNIT_TEST(ShouldCancelAcquireDisk)
    {
        const auto agent1 = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        const auto agent2 = CreateAgentConfig("agent-2", {
            Device("dev-1", "uuid-3", "rack-1", 10_GB),
            Device("dev-2", "uuid-4", "rack-1", 10_GB)
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({
                CreateTestDiskAgent(agent1),
                CreateBrokenTestDiskAgent(agent2)
             })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent1, agent2}));

        RegisterAgents(*runtime, 2);
        WaitForAgents(*runtime, 2);
        diskRegistry.CleanupDevices(TVector<TString>{
            "uuid-1", "uuid-2",
            "uuid-3", "uuid-4"
        });

        diskRegistry.AllocateDisk("disk-1", 40_GB);

        diskRegistry.SendAcquireDiskRequest("disk-1", "session-1");
        auto response = diskRegistry.RecvAcquireDiskResponse();
        UNIT_ASSERT_VALUES_EQUAL(response->GetStatus(), E_BS_INVALID_SESSION);
    }

    Y_UNIT_TEST(ShouldRespectAgentRequestTimeoutDuringAcquire)
    {
        const auto agent1 = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        const auto agent2 = CreateAgentConfig("agent-2", {
            Device("dev-1", "uuid-3", "rack-1", 10_GB),
            Device("dev-2", "uuid-4", "rack-1", 10_GB)
        });

        auto active = std::make_shared<std::atomic<bool>>();
        active->store(true);

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({
                CreateTestDiskAgent(agent1),
                CreateSuspendedTestDiskAgent(agent2, active),
             })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent1, agent2}));

        RegisterAgents(*runtime, 2);
        WaitForAgents(*runtime, 2);
        WaitForSecureErase(*runtime, {agent1, agent2});

        diskRegistry.AllocateDisk("disk-1", 40_GB);

        active->store(false);
        diskRegistry.SendAcquireDiskRequest("disk-1", "session-1");
        {
            auto response = diskRegistry.RecvAcquireDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(response->GetStatus(), E_REJECTED);
        }

        active->store(true);
        diskRegistry.SendAcquireDiskRequest("disk-1", "session-1");
        {
            auto response = diskRegistry.RecvAcquireDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(response->GetStatus(), S_OK);
        }
    }

    Y_UNIT_TEST(ShouldCancelPendingSessionsOnReboot)
    {
        const auto agent1 = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        auto active = std::make_shared<std::atomic<bool>>();

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({
                CreateSuspendedTestDiskAgent(agent1, active)
             })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);
        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent1}));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agent1});

        diskRegistry.AllocateDisk("disk-1", 20_GB);
        diskRegistry.SendAcquireDiskRequest("disk-1", "session-1");

        runtime->AdvanceCurrentTime(TDuration::Seconds(1));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        diskRegistry.RebootTablet();
        diskRegistry.WaitReady();

        runtime->AdvanceCurrentTime(TDuration::Seconds(1));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));
        *active = 1;

        auto response = diskRegistry.RecvAcquireDiskResponse();
        UNIT_ASSERT_VALUES_EQUAL(
            response->GetStatus(),
            E_REJECTED);

        diskRegistry.AcquireDisk("disk-1", "session-1");
    }

    Y_UNIT_TEST(ShouldCancelSessionOnReRegisterAgent)
    {
        const auto agent1 = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB)
        });

        const auto agent2 = CreateAgentConfig("agent-2", {
            Device("dev-1", "uuid-2", "rack-1", 10_GB)
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent1, agent2 })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent1, agent2}));

        RegisterAgents(*runtime, 2);
        WaitForAgents(*runtime, 2);
        WaitForSecureErase(*runtime, {agent1, agent2});

        diskRegistry.AllocateDisk("disk-1", 20_GB);
        diskRegistry.AcquireDisk("disk-1", "session-1");

        RegisterAgent(*runtime, 0);

        diskRegistry.AcquireDisk("disk-1", "session-2"); // OK: session-1 has been released
    }

    Y_UNIT_TEST(ShouldSendAcquireDiskOnRegisterAgent)
    {
        const auto agentConfig1 = CreateAgentConfig(
            "agent-1",
            {Device("dev-1", "uuid-1", "rack-1", 10_GB)});
        const auto agentConfig2 = CreateAgentConfig(
            "agent-2",
            {Device("dev-1", "uuid-2", "rack-1", 10_GB)});

        auto* agent1 = CreateTestDiskAgent(agentConfig1);
        auto* agent2 = CreateTestDiskAgent(agentConfig2);

        int agent1AcquireCallCount = 0;
        int agent2AcquireCallCount = 0;

        auto agent1HandleAcquireDevices =
            [&](const TEvDiskAgent::TEvAcquireDevicesRequest::TPtr& ev,
                const TActorContext& ctx)
        {
            Y_UNUSED(ctx);
            agent1AcquireCallCount++;
            const auto& record = ev->Get()->Record;

            UNIT_ASSERT_VALUES_EQUAL(77, record.GetMountSeqNumber());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", record.GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL(88, record.GetVolumeGeneration());
            return false;
        };
        auto agent2HandleAcquireDevices =
            [&](const TEvDiskAgent::TEvAcquireDevicesRequest::TPtr& ev,
                const TActorContext& ctx)
        {
            agent2AcquireCallCount++;
            Y_UNUSED(ctx);
            Y_UNUSED(ev);
            return false;
        };
        auto handleReleaseDevicesImpl =
            [&](const TEvDiskAgent::TEvReleaseDevicesRequest::TPtr& ev,
                const TActorContext& ctx)
        {
            Y_UNUSED(ev);
            Y_UNUSED(ctx);
            UNIT_ASSERT(false);
        };

        auto runtime =
            TTestRuntimeBuilder().WithAgents({agent1, agent2}).Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(
            CreateRegistryConfig(0, {agentConfig1, agentConfig2}));

        RegisterAgents(*runtime, 2);
        WaitForAgents(*runtime, 2);
        WaitForSecureErase(*runtime, {agentConfig1, agentConfig2});

        diskRegistry.AllocateDisk("disk-1", 20_GB);
        diskRegistry.AcquireDisk(
            "disk-1",
            "session-1",
            NProto::VOLUME_ACCESS_READ_WRITE,
            77,
            88);
        runtime->AdvanceCurrentTime(UpdateCountersInterval);
        runtime->DispatchEvents(
            TDispatchOptions(),
            TDuration::MilliSeconds(10));

        auto cachedAcquireDevicesRequestAmount =
            runtime->GetAppData(0)
                .Counters->GetSubgroup("counters", "blockstore")
                ->GetSubgroup("component", "disk_registry")
                ->GetCounter("CachedAcquireDevicesRequestAmount");
        UNIT_ASSERT_VALUES_EQUAL(2, cachedAcquireDevicesRequestAmount->Val());

        agent1->HandleAcquireDevicesImpl = agent1HandleAcquireDevices;
        agent2->HandleAcquireDevicesImpl = agent2HandleAcquireDevices;
        agent1->HandleReleaseDevicesImpl = handleReleaseDevicesImpl;
        agent2->HandleReleaseDevicesImpl = handleReleaseDevicesImpl;

        // Register agent should send cached acquire requests.
        RegisterAgent(*runtime, 0);
        runtime->AdvanceCurrentTime(UpdateCountersInterval);
        runtime->DispatchEvents(
            TDispatchOptions(),
            TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(1, agent1AcquireCallCount);
        UNIT_ASSERT_VALUES_EQUAL(0, agent2AcquireCallCount);
        UNIT_ASSERT_VALUES_EQUAL(1, cachedAcquireDevicesRequestAmount->Val());

        // Folowing register shouldn't send acquire requests since we just sent
        // them and didn't cache fresh ones yet.
        RegisterAgent(*runtime, 0);
        runtime->AdvanceCurrentTime(UpdateCountersInterval);
        runtime->DispatchEvents(
            TDispatchOptions(),
            TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(1, agent1AcquireCallCount);
        UNIT_ASSERT_VALUES_EQUAL(0, agent2AcquireCallCount);
        UNIT_ASSERT_VALUES_EQUAL(1, cachedAcquireDevicesRequestAmount->Val());

        diskRegistry.AcquireDisk(
            "disk-1",
            "session-1",
            NProto::VOLUME_ACCESS_READ_WRITE,
            77,
            88);
        runtime->AdvanceCurrentTime(UpdateCountersInterval);
        runtime->DispatchEvents(
            TDispatchOptions(),
            TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(2, agent1AcquireCallCount);
        UNIT_ASSERT_VALUES_EQUAL(1, agent2AcquireCallCount);
        UNIT_ASSERT_VALUES_EQUAL(2, cachedAcquireDevicesRequestAmount->Val());

        const NProto::TStorageServiceConfig storageConfig =
            CreateDefaultStorageConfig();
        runtime->AdvanceCurrentTime(TDuration::MilliSeconds(
            storageConfig.GetCachedAcquireRequestLifetime() * 2));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));
        // This register shouldn't send acquire requests because too much time
        // passed since the last acquire.
        RegisterAgent(*runtime, 0);
        runtime->AdvanceCurrentTime(UpdateCountersInterval);
        runtime->DispatchEvents(
            TDispatchOptions(),
            TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(2, agent1AcquireCallCount);
        UNIT_ASSERT_VALUES_EQUAL(1, agent2AcquireCallCount);
        UNIT_ASSERT_VALUES_EQUAL(1, cachedAcquireDevicesRequestAmount->Val());

        diskRegistry.AcquireDisk(
            "disk-1",
            "session-1",
            NProto::VOLUME_ACCESS_READ_WRITE,
            77,
            88);
        runtime->AdvanceCurrentTime(UpdateCountersInterval);
        runtime->DispatchEvents(
            TDispatchOptions(),
            TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(3, agent1AcquireCallCount);
        UNIT_ASSERT_VALUES_EQUAL(2, agent2AcquireCallCount);
        UNIT_ASSERT_VALUES_EQUAL(2, cachedAcquireDevicesRequestAmount->Val());

        // Although a release event wasn't sent, agent registering shouldn't
        // send acquire because the disk is destroyed.
        diskRegistry.MarkDiskForCleanup("disk-1");
        diskRegistry.DeallocateDisk("disk-1");
        runtime->AdvanceCurrentTime(UpdateCountersInterval);
        runtime->DispatchEvents(
            TDispatchOptions(),
            TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(0, cachedAcquireDevicesRequestAmount->Val());

        RegisterAgent(*runtime, 0);
        runtime->AdvanceCurrentTime(UpdateCountersInterval);
        runtime->DispatchEvents(
            TDispatchOptions(),
            TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(3, agent1AcquireCallCount);
        UNIT_ASSERT_VALUES_EQUAL(2, agent2AcquireCallCount);
    }

    Y_UNIT_TEST(ShouldAcquireDisk)
    {
        const auto agent1 = CreateAgentConfig("agent-1", {
            Device("test", "uuid-1", "rack-1", 10_GB)
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent1 })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);
        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent1}));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agent1});

        {
            auto response = diskRegistry.AllocateDisk("disk-1", 10_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(msg.DevicesSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(
                msg.GetDevices(0).GetNodeId(),
                runtime->GetNodeId(0));
        }

        bool finished = false;

        runtime->SetObserverFunc( [&] (TAutoPtr<IEventHandle>& event) {
            switch (event->GetTypeRewrite()) {
                case TEvDiskRegistryPrivate::EvFinishAcquireDiskResponse: {
                    finished = true;
                    break;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        {
            auto response = diskRegistry.AcquireDisk("disk-1", "session-1");
            const auto& msg = response->Record;

            UNIT_ASSERT_VALUES_EQUAL(1, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                msg.GetDevices(0).GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultLogicalBlockSize,
                msg.GetDevices(0).GetBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(
                msg.GetDevices(0).GetNodeId(),
                runtime->GetNodeId(0));

            UNIT_ASSERT(finished);
        }

        diskRegistry.ReleaseDisk("disk-1", "session-1");
    }


    Y_UNIT_TEST(ShouldNotSendAcquireReleaseRequestsToUnavailableAgents)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("test", "uuid-1", "rack-1", 10_GB)
            }),
            CreateAgentConfig("agent-2", {
                Device("test", "uuid-2", "rack-2", 10_GB)
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
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(msg.DevicesSize(), 2);
            UNIT_ASSERT_VALUES_EQUAL(
                msg.GetDevices(0).GetNodeId(),
                runtime->GetNodeId(0));

            UNIT_ASSERT_VALUES_EQUAL(
                msg.GetDevices(1).GetNodeId(),
                runtime->GetNodeId(1));
        }

        auto breakAgent = [&] (const TString& agentId) {
            diskRegistry.ChangeAgentState(
                agentId,
                NProto::AGENT_STATE_UNAVAILABLE
            );
            runtime->AdvanceCurrentTime(TDuration::Seconds(20));
            runtime->DispatchEvents({}, TDuration::MilliSeconds(10));
        };

        breakAgent("agent-1");

        {
            diskRegistry.SendAcquireDiskRequest("disk-1", "session-1");
            auto response = diskRegistry.RecvAcquireDiskResponse();

            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            const auto& msg = response->Record;

            UNIT_ASSERT_VALUES_EQUAL(1, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                msg.GetDevices(0).GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultLogicalBlockSize,
                msg.GetDevices(0).GetBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(
                msg.GetDevices(0).GetNodeId(),
                runtime->GetNodeId(1));
        }

        diskRegistry.ReleaseDisk("disk-1", "session-1");

        breakAgent("agent-2");

        // It is OK to successfully acquire empty device list
        {
            diskRegistry.SendAcquireDiskRequest("disk-1", "session-1");
            auto response = diskRegistry.RecvAcquireDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            UNIT_ASSERT_VALUES_EQUAL(0, response->Record.DevicesSize());
        }

        // Let's acquire one more session
        {
            diskRegistry.SendAcquireDiskRequest(
                "disk-1",
                "session-2",
                NProto::VOLUME_ACCESS_READ_ONLY);

            auto response = diskRegistry.RecvAcquireDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            UNIT_ASSERT_VALUES_EQUAL(0, response->Record.DevicesSize());
        }

        diskRegistry.ReleaseDisk("disk-1", "session-1");
        diskRegistry.ReleaseDisk("disk-1", "session-2");

        diskRegistry.ChangeAgentState("agent-1", NProto::AGENT_STATE_WARNING);
        {
            auto response = diskRegistry.AcquireDisk("disk-1", "session-1");
            UNIT_ASSERT_VALUES_EQUAL(1, response->Record.DevicesSize());
        }
    }

    Y_UNIT_TEST(ShouldNotSendAcquireReleaseRequestsToBrokenDevices)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("test", "uuid-1", "rack-1", 10_GB)
            }),
            CreateAgentConfig("agent-2", {
                Device("test", "uuid-2", "rack-2", 10_GB)
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
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);
            const auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(msg.DevicesSize(), 2);
            UNIT_ASSERT_VALUES_EQUAL(
                msg.GetDevices(0).GetNodeId(),
                runtime->GetNodeId(0));

            UNIT_ASSERT_VALUES_EQUAL(
                msg.GetDevices(1).GetNodeId(),
                runtime->GetNodeId(1));
        }

        auto breakDevice = [&](const TString& deviceUUID)
        {
            diskRegistry.ChangeDeviceState(
                deviceUUID,
                NProto::DEVICE_STATE_ERROR);
            runtime->AdvanceCurrentTime(TDuration::Seconds(20));
            runtime->DispatchEvents({}, TDuration::MilliSeconds(10));
        };

        breakDevice("uuid-1");

        {
            diskRegistry.SendAcquireDiskRequest("disk-1", "session-1");
            auto response = diskRegistry.RecvAcquireDiskResponse();

            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            const auto& msg = response->Record;

            UNIT_ASSERT_VALUES_EQUAL(1, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-2",
                msg.GetDevices(0).GetDeviceUUID());
        }

        diskRegistry.ReleaseDisk("disk-1", "session-1");
    }

    Y_UNIT_TEST(ShouldAcquireReleaseSession)
    {
        const auto agent1 = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent1 })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, { agent1 }));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agent1});

        diskRegistry.AllocateDisk("disk-1", 20_GB);

        {
            auto response = diskRegistry.AcquireDisk("disk-1", "session-1");
            UNIT_ASSERT(!HasError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(2, response->Record.DevicesSize());
        }

        {
            auto response = diskRegistry.RemoveDiskSession(
                "disk-1",
                "session-1",
                TVector<TAgentReleaseDevicesCachedRequest>());
            UNIT_ASSERT(!HasError(response->GetError()));
        }
    }

    Y_UNIT_TEST(ShouldHandleUndeliveredAcquire)
    {
        auto agent = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB)
        });
        agent.SetNodeId(42);

        auto runtime = TTestRuntimeBuilder().Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig({agent}));

        diskRegistry.RegisterAgent(agent);
        diskRegistry.CleanupDevices(TVector<TString>{"uuid-1"});

        diskRegistry.AllocateDisk("disk-1", 10_GB);
        diskRegistry.SendAcquireDiskRequest("disk-1", "session-1");

        auto response = diskRegistry.RecvAcquireDiskResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
    }

    Y_UNIT_TEST(ShouldFailReleaseSessionIfDiskRegistryRestarts)
    {
        const auto agent1 = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB)
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent1 })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, { agent1 }));

        RegisterAgents(*runtime, 1);
        WaitForAgents(*runtime, 1);
        WaitForSecureErase(*runtime, {agent1});

        diskRegistry.AllocateDisk("disk-1", 20_GB);

        {
            auto response = diskRegistry.AcquireDisk("disk-1", "session-1");
            UNIT_ASSERT(!HasError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(2, response->Record.DevicesSize());
        }

        auto observerFunc = runtime->SetObserverFunc( [&] (TAutoPtr<IEventHandle>& event) {
            switch (event->GetTypeRewrite()) {
                case TEvDiskAgent::EvReleaseDevicesRequest: {
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        diskRegistry.SendReleaseDiskRequest("disk-1", "session-1");

        runtime->AdvanceCurrentTime(TDuration::Seconds(1));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));
        diskRegistry.RebootTablet();
        diskRegistry.WaitReady();

        auto response = diskRegistry.RecvReleaseDiskResponse();
        UNIT_ASSERT_VALUES_EQUAL(
            response->GetStatus(),
            E_REJECTED);
    }

    Y_UNIT_TEST(ShouldRejectTimedOutReleaseDisk)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1.1", "rack-1", 10_GB),
                Device("dev-2", "uuid-1.2", "rack-1", 10_GB)
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-1", "uuid-2.1", "rack-1", 10_GB),
                Device("dev-2", "uuid-2.2", "rack-1", 10_GB)
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

        diskRegistry.AllocateDisk("disk-1", 40_GB);
        diskRegistry.AcquireDisk("disk-1", "session-1");

        auto observerFunc = runtime->SetObserverFunc( [&] (TAutoPtr<IEventHandle>& event) {
            switch (event->GetTypeRewrite()) {
                case TEvDiskAgent::EvReleaseDevicesRequest: {
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        diskRegistry.SendReleaseDiskRequest("disk-1", "session-1");

        runtime->AdvanceCurrentTime(TDuration::Seconds(5));
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        auto response = diskRegistry.RecvReleaseDiskResponse();
        UNIT_ASSERT_VALUES_EQUAL(
            response->GetStatus(),
            E_TIMEOUT);

        runtime->SetObserverFunc(observerFunc);
        diskRegistry.ReleaseDisk("disk-1", "session-1");
    }

    Y_UNIT_TEST(ShouldIgnoreRetriableErrorsOnAcquireSession)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-1", "uuid-2", "rack-1", 10_GB),
            })
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, agents));

        RegisterAgents(*runtime, 2);
        WaitForAgents(*runtime, 2);

        WaitForSecureErase(*runtime, agents);

        {
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        {
            auto response = diskRegistry.AcquireDisk("disk-1", "session-1");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(2, response->Record.DevicesSize());
        }

        // reject acquire requests to uuid-1
        auto observerFunc = runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
            switch (event->GetTypeRewrite()) {
                case TEvDiskAgent::EvAcquireDevicesRequest: {
                    auto& msg = *event->Get<TEvDiskAgent::TEvAcquireDevicesRequest>();
                    if (msg.Record.GetDeviceUUIDs(0) == agents[0].GetDevices(0).GetDeviceUUID()) {
                        auto response = std::make_unique<TEvDiskAgent::TEvAcquireDevicesResponse>(
                            MakeError(E_REJECTED));

                        runtime->Send(
                            new IEventHandle(
                                event->Sender,
                                event->Recipient,
                                response.release(),
                                0, // flags
                                event->Cookie));

                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        {
            diskRegistry.SendAcquireDiskRequest("disk-1", "session-1");
            auto response = diskRegistry.RecvAcquireDiskResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(0, response->Record.DevicesSize());
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
