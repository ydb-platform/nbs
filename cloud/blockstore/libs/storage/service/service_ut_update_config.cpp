#include "service_ut.h"

#include <cloud/blockstore/public/api/protos/disk.pb.h>

#include <algorithm>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

auto CreateKnownAgentOld(
    const TString& agentId,
    std::initializer_list<TString> uuids)
{
    NProto::TKnownDiskAgent agent;

    agent.SetAgentId(agentId);

    for (auto& uuid: uuids) {
        agent.AddDevices(uuid);
    }

    return agent;
}

auto CreateKnownAgent(
    const TString& agentId,
    std::initializer_list<TString> uuids)
{
    NProto::TKnownDiskAgent agent;

    agent.SetAgentId(agentId);

    for (auto& uuid: uuids) {
        auto& device = *agent.AddKnownDevices();
        device.SetDeviceUUID(uuid);
        device.SetSerialNumber("SN-" + uuid);
    }

    return agent;
}

auto CreateDeviceOverride(
    const TString& diskId,
    const TString& uuid,
    ui32 blockCount)
{
    NProto::TDeviceOverride d;
    d.SetDiskId(diskId);
    d.SetDevice(uuid);
    d.SetBlocksCount(blockCount);

    return d;
}

auto CreateDevicePoolConfig(
    const TString& name,
    NProto::EDevicePoolKind kind,
    ui64 allocationUnit)
{
    NProto::TKnownDevicePool config;

    config.SetName(name);
    config.SetKind(kind);
    config.SetAllocationUnit(allocationUnit);

    return config;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceUpdateDiskRegistryConfigTest)
{
    Y_UNIT_TEST(ShouldUpdateDiskRegistryConfig)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);

        constexpr int configVersion = 0;

        const TVector agents{
            CreateKnownAgentOld(
                "agent-0002",
                {"uuid-2.2", "uuid-2.1", "uuid-2.3"}),
            CreateKnownAgent("agent-0001", {"uuid-1.1", "uuid-1.2"}),
            CreateKnownAgent(
                "agent-0003",
                {"uuid-3.1", "uuid-3.3", "uuid-3.2"})};

        const TVector deviceOverrides{
            CreateDeviceOverride("foo", "uuid-1", 42),
            CreateDeviceOverride("bar", "uuid-2", 100),
            CreateDeviceOverride("baz", "uuid-3", 200),
            CreateDeviceOverride("fuz", "uuid-4", 300)};

        const TVector devicePools{
            CreateDevicePoolConfig(
                "ssd-v1",
                NProto::DEVICE_POOL_KIND_LOCAL,
                100_GB),
            CreateDevicePoolConfig(
                "ssd-v2",
                NProto::DEVICE_POOL_KIND_LOCAL,
                368_GB)};

        service.UpdateDiskRegistryConfig(
            configVersion,
            agents,
            deviceOverrides,
            devicePools,
            false   // ignoreVersion
        );

        auto response = service.DescribeDiskRegistryConfig();
        const auto& r = response->Record;
        UNIT_ASSERT_VALUES_EQUAL(1, r.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(agents.size(), r.KnownAgentsSize());
        UNIT_ASSERT_VALUES_EQUAL(
            deviceOverrides.size(),
            r.DeviceOverridesSize());
        UNIT_ASSERT_VALUES_EQUAL(devicePools.size(), r.KnownDevicePoolsSize());

        {
            const auto& agent = r.GetKnownAgents(0);
            UNIT_ASSERT_VALUES_EQUAL("agent-0001", agent.GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(2, agent.KnownDevicesSize());

            auto& device0 = agent.GetKnownDevices(0);
            auto& device1 = agent.GetKnownDevices(1);

            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", device0.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("SN-uuid-1.1", device0.GetSerialNumber());

            UNIT_ASSERT_VALUES_EQUAL("uuid-1.2", device1.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("SN-uuid-1.2", device1.GetSerialNumber());
        }

        {
            const auto& agent = r.GetKnownAgents(1);
            UNIT_ASSERT_VALUES_EQUAL("agent-0002", agent.GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(3, agent.KnownDevicesSize());

            auto& device0 = agent.GetKnownDevices(0);
            auto& device1 = agent.GetKnownDevices(1);
            auto& device2 = agent.GetKnownDevices(2);

            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", device0.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("", device0.GetSerialNumber());

            UNIT_ASSERT_VALUES_EQUAL("uuid-2.2", device1.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("", device1.GetSerialNumber());

            UNIT_ASSERT_VALUES_EQUAL("uuid-2.3", device2.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("", device2.GetSerialNumber());
        }

        {
            const auto& agent = r.GetKnownAgents(2);
            UNIT_ASSERT_VALUES_EQUAL("agent-0003", agent.GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(3, agent.KnownDevicesSize());

            auto& device0 = agent.GetKnownDevices(0);
            auto& device1 = agent.GetKnownDevices(1);
            auto& device2 = agent.GetKnownDevices(2);

            UNIT_ASSERT_VALUES_EQUAL("uuid-3.1", device0.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("SN-uuid-3.1", device0.GetSerialNumber());

            UNIT_ASSERT_VALUES_EQUAL("uuid-3.2", device1.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("SN-uuid-3.2", device1.GetSerialNumber());

            UNIT_ASSERT_VALUES_EQUAL("uuid-3.3", device2.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("SN-uuid-3.3", device2.GetSerialNumber());
        }
    }

    Y_UNIT_TEST(ShouldRejectUpdateDiskRegistryConfigWithWrongVersion)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);

        const TVector agents1{
            CreateKnownAgent("agent-0001", {"uuid-1", "uuid-2"})};

        const TVector agents2{CreateKnownAgent("agent-0001", {"uuid-1"})};

        service.UpdateDiskRegistryConfig(0, agents1);
        service.SendUpdateDiskRegistryConfigRequest(10, agents2);
        auto response = service.RecvUpdateDiskRegistryConfigResponse();
        UNIT_ASSERT(FAILED(response->GetStatus()));
    }

    Y_UNIT_TEST(ShouldForceUpdateDiskRegistryConfig)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);

        const TVector agents1{
            CreateKnownAgent("agent-0001", {"uuid-1", "uuid-2"})};

        const TVector agents2{
            CreateKnownAgent("agent-0001", {"uuid-1", "uuid-2"}),
            CreateKnownAgent("agent-0002", {"uuid-1"})};

        service.UpdateDiskRegistryConfig(0, agents1);

        {
            auto response = service.DescribeDiskRegistryConfig();
            const auto& r = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, r.GetVersion());
            UNIT_ASSERT_VALUES_EQUAL(1, r.KnownAgentsSize());
        }

        service.UpdateDiskRegistryConfig(
            10,
            agents2,
            TVector<NProto::TDeviceOverride>{},
            TVector<NProto::TKnownDevicePool>{},
            true   // ignoreVersion
        );

        {
            auto response = service.DescribeDiskRegistryConfig();
            const auto& r = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(2, r.GetVersion());
            UNIT_ASSERT_VALUES_EQUAL(2, r.KnownAgentsSize());

            TVector ids{
                r.GetKnownAgents(0).GetAgentId(),
                r.GetKnownAgents(1).GetAgentId()};

            Sort(ids);

            UNIT_ASSERT_VALUES_EQUAL("agent-0001", ids[0]);
            UNIT_ASSERT_VALUES_EQUAL("agent-0002", ids[1]);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
