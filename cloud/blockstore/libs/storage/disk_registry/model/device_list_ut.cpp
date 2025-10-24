#include "device_list.h"

#include "agent_list.h"

#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/iterator_range.h>
#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 DefaultBlockSize = 4_KB;
constexpr ui64 DefaultBlockCount = 93_GB / DefaultBlockSize;
const TString DefaultPoolName;

////////////////////////////////////////////////////////////////////////////////

auto CreateAgentConfig(
    const TString& id,
    ui32 nodeId,
    const TString& rack,
    const TString& poolName = "",
    const TVector<TString>& deviceNames = {"/some/device"},
    int deviceCount = 15)
{
    NProto::TAgentConfig agent;
    agent.SetAgentId(id);
    agent.SetNodeId(nodeId);

    for (int i = 0; i != deviceCount; ++i) {
        auto* device = agent.AddDevices();
        device->SetBlockSize(DefaultBlockSize);
        device->SetBlocksCount(DefaultBlockCount);
        device->SetUnadjustedBlockCount(DefaultBlockCount);
        device->SetRack(rack);
        device->SetNodeId(agent.GetNodeId());
        device->SetDeviceUUID(id + "-" + ToString(i + 1));
        device->SetDeviceName(deviceNames[i % deviceNames.size()]);
        device->SetAgentId(id);
        if (poolName) {
            device->SetPoolKind(NProto::DEVICE_POOL_KIND_GLOBAL);
            device->SetPoolName(poolName);
        }
    }

    return agent;
}

template <typename C>
ui64 CalcTotalSize(const C& devices)
{
    return Accumulate(devices, ui64{}, [] (ui64 val, auto& x) {
        return val + x.GetBlockSize() * x.GetBlocksCount();
    });
}

////////////////////////////////////////////////////////////////////////////////

TDevicePoolConfigs CreateDevicePoolConfigs(
    std::initializer_list<std::tuple<TString, ui64, NProto::EDevicePoolKind>>
        pools)
{
    NProto::TDevicePoolConfig nonrepl;
    nonrepl.SetAllocationUnit(DefaultBlockCount * DefaultBlockSize);

    TDevicePoolConfigs result{{TString{}, nonrepl}};

    for (auto [name, size, kind]: pools) {
        NProto::TDevicePoolConfig config;
        config.SetAllocationUnit(size);
        config.SetName(name);
        config.SetKind(kind);
        result.emplace(name, std::move(config));
    }

    return result;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDeviceListTest)
{
    Y_UNIT_TEST(ShouldAllocateSingleDevice)
    {
        const auto poolConfigs = CreateDevicePoolConfigs({});

        TDeviceList deviceList;

        auto allocate = [&] (THashSet<TString> racks) {
            return deviceList.AllocateDevice(
                "disk",
                {
                    .ForbiddenRacks = std::move(racks),
                    .LogicalBlockSize = DefaultBlockSize,
                    .BlockCount = DefaultBlockCount
                }
            ).GetDeviceUUID();
        };

        UNIT_ASSERT(allocate({}).empty());

        NProto::TAgentConfig foo = CreateAgentConfig("foo", 1000, "rack-1");
        NProto::TAgentConfig bar = CreateAgentConfig("bar", 2000, "rack-2");
        NProto::TAgentConfig baz = CreateAgentConfig("baz", 3000, "rack-3");

        foo.MutableDevices(6)->SetState(NProto::DEVICE_STATE_WARNING);
        bar.MutableDevices(6)->SetState(NProto::DEVICE_STATE_WARNING);
        baz.MutableDevices(6)->SetState(NProto::DEVICE_STATE_WARNING);
        baz.MutableDevices(9)->SetState(NProto::DEVICE_STATE_ERROR);

        deviceList.UpdateDevices(foo, poolConfigs);
        deviceList.UpdateDevices(bar, poolConfigs);
        deviceList.UpdateDevices(baz, poolConfigs);

        deviceList.MarkDeviceAsDirty("foo-13");
        deviceList.MarkDeviceAsDirty("bar-13");
        deviceList.MarkDeviceAsDirty("baz-13");

        UNIT_ASSERT_VALUES_EQUAL(3, deviceList.GetDirtyDevices().size());

        UNIT_ASSERT(!allocate({}).empty());
        UNIT_ASSERT(allocate({"rack-1", "rack-2", "rack-3"}).empty());
        UNIT_ASSERT(allocate({"rack-2", "rack-3"}).StartsWith("foo-"));
        UNIT_ASSERT(allocate({"rack-1", "rack-3"}).StartsWith("bar-"));
        UNIT_ASSERT(allocate({"rack-1", "rack-2"}).StartsWith("baz-"));

        // total = 15 * 3; dirty = 3; allocated = 4; warn = 3; error = 1
        for (int i = 15 * 3 - 3 - 4 - 3 - 1; i; --i) {
            UNIT_ASSERT(!allocate({}).empty());
        }

        UNIT_ASSERT(allocate({}).empty());
        UNIT_ASSERT_VALUES_EQUAL(3, deviceList.GetDirtyDevices().size());

        UNIT_ASSERT(!deviceList.ReleaseDevice("unknown"));
        UNIT_ASSERT(deviceList.ReleaseDevice("foo-1"));
        UNIT_ASSERT_VALUES_EQUAL(4, deviceList.GetDirtyDevices().size());
        UNIT_ASSERT(allocate({}).empty());

        UNIT_ASSERT(deviceList.MarkDeviceAsClean("foo-1"));
        deviceList.UpdateDevices(foo, poolConfigs);

        UNIT_ASSERT(allocate({"rack-1"}).empty());
        UNIT_ASSERT_VALUES_EQUAL("foo-1", allocate({}));
    }

    Y_UNIT_TEST(ShouldAllocateDevices)
    {
        const auto poolConfigs = CreateDevicePoolConfigs({});

        TDeviceList deviceList;

        auto allocate = [&] (ui32 n, THashSet<TString> racks) {
            return deviceList.AllocateDevices(
                "disk",
                {
                    .ForbiddenRacks = std::move(racks),
                    .LogicalBlockSize = DefaultBlockSize,
                    .BlockCount = n * DefaultBlockCount
                }
            );
        };

        UNIT_ASSERT_VALUES_EQUAL(0, deviceList.Size());
        UNIT_ASSERT(allocate(1, {}).empty());
        UNIT_ASSERT(allocate(8, {}).empty());

        NProto::TAgentConfig foo = CreateAgentConfig("foo", 1000, "rack-1");
        NProto::TAgentConfig bar = CreateAgentConfig("bar", 2000, "rack-2");
        NProto::TAgentConfig baz = CreateAgentConfig("baz", 3000, "rack-3");

        foo.MutableDevices(6)->SetState(NProto::DEVICE_STATE_WARNING);
        bar.MutableDevices(6)->SetState(NProto::DEVICE_STATE_WARNING);
        baz.MutableDevices(6)->SetState(NProto::DEVICE_STATE_WARNING);
        baz.MutableDevices(9)->SetState(NProto::DEVICE_STATE_ERROR);

        deviceList.UpdateDevices(foo, poolConfigs);
        UNIT_ASSERT_VALUES_EQUAL(15, deviceList.Size());

        deviceList.UpdateDevices(bar, poolConfigs);
        UNIT_ASSERT_VALUES_EQUAL(30, deviceList.Size());

        deviceList.UpdateDevices(baz, poolConfigs);
        UNIT_ASSERT_VALUES_EQUAL(45, deviceList.Size());

        UNIT_ASSERT_VALUES_EQUAL(0, deviceList.GetDirtyDevices().size());

        UNIT_ASSERT(allocate(1, {"rack-1", "rack-2", "rack-3"}).empty());
        UNIT_ASSERT(allocate(15, {"rack-2", "rack-3"}).empty());
        UNIT_ASSERT(allocate(15, {"rack-1", "rack-3"}).empty());
        UNIT_ASSERT(allocate(15, {"rack-1", "rack-2"}).empty());

        deviceList.MarkDeviceAsDirty("foo-13");
        deviceList.MarkDeviceAsDirty("bar-13");
        deviceList.MarkDeviceAsDirty("baz-13");

        UNIT_ASSERT_VALUES_EQUAL(3, deviceList.GetDirtyDevices().size());

        {
            auto devices = allocate(1, {"rack-2", "rack-3"});
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT(devices[0].GetDeviceUUID().StartsWith("foo-"));
        }

        {
            auto devices = allocate(1, {"rack-1", "rack-3"});
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT(devices[0].GetDeviceUUID().StartsWith("bar-"));
        }

        {
            auto devices = allocate(1, {"rack-1", "rack-2"});
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT(devices[0].GetDeviceUUID().StartsWith("baz-"));
        }

        // total = 45; warn = 3; error = 1; allocated = 3

        {
            const ui64 n = 7;

            auto devices = allocate(n, {});
            UNIT_ASSERT_VALUES_EQUAL(n, devices.size());

            const ui64 size = CalcTotalSize(devices);
            UNIT_ASSERT_VALUES_EQUAL(
                n * DefaultBlockSize * DefaultBlockCount,
                size);
        }

        // total = 45; dirty = 3; warn = 3; error = 1; allocated = 10

        {
            const ui64 n = 10;
            auto devices = allocate(n, {"rack-1", "rack-3"});
            UNIT_ASSERT_VALUES_EQUAL(n, devices.size());

            const ui64 size = CalcTotalSize(devices);
            UNIT_ASSERT_VALUES_EQUAL(
                n * DefaultBlockSize * DefaultBlockCount,
                size);
        }

        // total = 45; dirty = 3; warn = 3; error = 1; allocated = 20

        {
            const ui64 n = 45 - 3 - 3 - 1 - 20;
            auto devices = allocate(n, {});
            UNIT_ASSERT_VALUES_EQUAL(n, devices.size());

            const ui64 size = CalcTotalSize(devices);
            UNIT_ASSERT_VALUES_EQUAL(
                n * DefaultBlockSize * DefaultBlockCount,
                size);
        }

        UNIT_ASSERT_VALUES_EQUAL(0, allocate(1, {}).size());

        THashSet<TString> discardedDevices {
            "foo-7", "bar-7", "baz-7", "baz-10", "foo-13", "bar-13", "baz-13"
        };

        for (auto* agent: {&foo, &bar, &baz}) {
            for (auto& device: agent->GetDevices()) {
                const auto& uuid = device.GetDeviceUUID();
                auto diskId = deviceList.FindDiskId(uuid);

                if (discardedDevices.contains(uuid)) {
                    UNIT_ASSERT(diskId.empty());
                } else {
                    UNIT_ASSERT_VALUES_EQUAL("disk", diskId);
                }
            }
        }

        UNIT_ASSERT(deviceList.MarkDeviceAsClean("foo-13"));
        deviceList.UpdateDevices(foo, poolConfigs);
        UNIT_ASSERT(deviceList.MarkDeviceAsClean("bar-13"));
        deviceList.UpdateDevices(bar, poolConfigs);
        UNIT_ASSERT(deviceList.MarkDeviceAsClean("baz-13"));
        deviceList.UpdateDevices(baz, poolConfigs);

        UNIT_ASSERT_VALUES_EQUAL(0, allocate(3, {"rack-1"}).size());
        UNIT_ASSERT_VALUES_EQUAL(0, allocate(3, {"rack-2"}).size());
        UNIT_ASSERT_VALUES_EQUAL(0, allocate(3, {"rack-3"}).size());

        {
            const ui64 n = 3;
            auto devices = allocate(n, {});
            UNIT_ASSERT_VALUES_EQUAL(n, devices.size());

            const ui64 size = CalcTotalSize(devices);
            UNIT_ASSERT_VALUES_EQUAL(
                n * DefaultBlockSize * DefaultBlockCount,
                size);
        }
    }

    Y_UNIT_TEST(ShouldMinimizeRackCountUponDeviceAllocation)
    {
        const auto poolConfigs = CreateDevicePoolConfigs({});

        TDeviceList deviceList;

        auto allocate = [&] (ui32 n, THashSet<TString> racks) {
            return deviceList.AllocateDevices(
                "disk",
                {
                    .ForbiddenRacks = std::move(racks),
                    .LogicalBlockSize = DefaultBlockSize,
                    .BlockCount = n * DefaultBlockCount
                }
            );
        };

        UNIT_ASSERT_VALUES_EQUAL(0, deviceList.Size());
        UNIT_ASSERT(allocate(1, {}).empty());
        UNIT_ASSERT(allocate(8, {}).empty());

        // 10 agents per rack => 150 devices per rack
        for (ui32 rack = 0; rack < 5; ++rack) {
            for (ui32 agent = 0; agent < 10; ++agent) {
                NProto::TAgentConfig config = CreateAgentConfig(
                    Sprintf("r%ua%u", rack, agent),
                    rack * 100 + agent,
                    Sprintf("rack-%u", rack));

                deviceList.UpdateDevices(config, poolConfigs);
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(5 * 10 * 15, deviceList.Size());

        UNIT_ASSERT_VALUES_EQUAL(0, deviceList.GetDirtyDevices().size());

        auto devices1 = allocate(100, {});
        UNIT_ASSERT_VALUES_EQUAL(100, devices1.size());
        const auto& rack1 = devices1[0].GetRack();

        for (const auto& device: devices1) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                rack1,
                device.GetRack(),
                device.GetAgentId());
        }

        auto devices2 = allocate(100, {});
        UNIT_ASSERT_VALUES_EQUAL(100, devices2.size());
        const auto& rack2 = devices2[0].GetRack();

        UNIT_ASSERT_VALUES_UNEQUAL(rack1, rack2);

        for (const auto& device: devices2) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                rack2,
                device.GetRack(),
                device.GetAgentId());
        }
    }

    Y_UNIT_TEST(ShouldMinimizeDeviceNameCountUponDeviceAllocation)
    {
        const auto poolConfigs = CreateDevicePoolConfigs({});

        TDeviceList deviceList;

        auto allocate = [&] (ui32 n) {
            return deviceList.AllocateDevices(
                "disk",
                {
                    .LogicalBlockSize = DefaultBlockSize,
                    .BlockCount = n * DefaultBlockCount
                }
            );
        };

        NProto::TAgentConfig config = CreateAgentConfig(
            "agent-id",
            1000, // nodeId
            "rack",
            "", // poolName
            {"/dev/1", "/dev/2", "/dev/3", "/dev/4"},
            400);

        deviceList.UpdateDevices(config, poolConfigs);

        UNIT_ASSERT_VALUES_EQUAL(400, deviceList.Size());

        UNIT_ASSERT_VALUES_EQUAL(0, deviceList.GetDirtyDevices().size());

        auto devices1 = allocate(100);
        UNIT_ASSERT_VALUES_EQUAL(100, devices1.size());
        const auto& deviceName1 = devices1[0].GetDeviceName();

        for (const auto& device: devices1) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                deviceName1,
                device.GetDeviceName(),
                device.GetDeviceUUID());
        }

        auto devices2 = allocate(100);
        UNIT_ASSERT_VALUES_EQUAL(100, devices2.size());
        const auto& deviceName2 = devices2[0].GetDeviceName();

        UNIT_ASSERT_VALUES_UNEQUAL(deviceName1, deviceName2);

        for (const auto& device: devices2) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                deviceName2,
                device.GetDeviceName(),
                device.GetDeviceUUID());
        }
    }

    Y_UNIT_TEST(ShouldSelectRackWithTheMostFreeSpaceUponDeviceAllocation)
    {
        const auto poolConfigs = CreateDevicePoolConfigs({});

        TDeviceList deviceList;

        auto allocate = [&](ui32 n, THashSet<ui32> nodeIds = {})
        {
            return deviceList.AllocateDevices(
                "disk",
                {
                    .LogicalBlockSize = DefaultBlockSize,
                    .BlockCount = n * DefaultBlockCount,
                    .NodeIds = std::move(nodeIds),
                }
            );
        };

        NProto::TAgentConfig agent1 = CreateAgentConfig("agent1", 1, "rack1");
        NProto::TAgentConfig agent2 = CreateAgentConfig("agent2", 2, "rack1");
        NProto::TAgentConfig agent3 = CreateAgentConfig("agent3", 3, "rack2");
        NProto::TAgentConfig agent4 = CreateAgentConfig("agent4", 4, "rack2");
        deviceList.UpdateDevices(agent1, poolConfigs);
        deviceList.UpdateDevices(agent2, poolConfigs);
        deviceList.UpdateDevices(agent3, poolConfigs);
        deviceList.UpdateDevices(agent4, poolConfigs);

        UNIT_ASSERT_VALUES_EQUAL(60, deviceList.Size());

        // using up some space in rack1 (on agent1)
        auto devices0 = allocate(15, {agent1.GetNodeId()});
        UNIT_ASSERT_VALUES_EQUAL(15, devices0.size());
        for (const auto& d: devices0) {
            UNIT_ASSERT_VALUES_EQUAL("rack1", d.GetRack());
            UNIT_ASSERT_VALUES_EQUAL(agent1.GetAgentId(), d.GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(agent1.GetNodeId(), d.GetNodeId());
        }

        // now rack2 has less occupied space
        auto devices1 = allocate(10);
        UNIT_ASSERT_VALUES_EQUAL(10, devices1.size());
        for (const auto& d: devices1) {
            UNIT_ASSERT_VALUES_EQUAL("rack2", d.GetRack());
            UNIT_ASSERT_VALUES_EQUAL(devices1[0].GetNodeId(), d.GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL(devices1[0].GetAgentId(), d.GetAgentId());
        }

        // rack2 still has less occupied space
        auto devices2 = allocate(10);
        UNIT_ASSERT_VALUES_EQUAL(10, devices2.size());

        // it must be another node, the least occupied one
        UNIT_ASSERT_VALUES_UNEQUAL(
            devices1[0].GetNodeId(),
            devices2[0].GetNodeId());
        UNIT_ASSERT_VALUES_UNEQUAL(
            devices1[0].GetAgentId(),
            devices2[0].GetAgentId());

        for (const auto& d: devices2) {
            UNIT_ASSERT_VALUES_EQUAL("rack2", d.GetRack());
            UNIT_ASSERT_VALUES_EQUAL(devices2[0].GetNodeId(), d.GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL(devices2[0].GetAgentId(), d.GetAgentId());
        }

        // now rack1 has less occupied space
        auto devices3 = allocate(1);
        UNIT_ASSERT_VALUES_EQUAL(1, devices3.size());
        UNIT_ASSERT_VALUES_EQUAL("rack1", devices3[0].GetRack());
        UNIT_ASSERT_VALUES_EQUAL(agent2.GetAgentId(), devices3[0].GetAgentId());
        UNIT_ASSERT_VALUES_EQUAL(agent2.GetNodeId(), devices3[0].GetNodeId());
    }

    Y_UNIT_TEST(ShouldDownrankNodes)
    {
        const auto poolConfigs = CreateDevicePoolConfigs({});

        TDeviceList deviceList;

        auto allocate = [&](ui32 n,
                            THashSet<ui32> nodeIds = {},
                            THashSet<ui32> downrankedNodeIds = {})
        {
            return deviceList.AllocateDevices(
                "disk",
                {
                    .DownrankedNodeIds = std::move(downrankedNodeIds),
                    .LogicalBlockSize = DefaultBlockSize,
                    .BlockCount = n * DefaultBlockCount,
                    .NodeIds = std::move(nodeIds),
                });
        };

        NProto::TAgentConfig agent1 = CreateAgentConfig("agent1", 1, "rack1");
        NProto::TAgentConfig agent2 = CreateAgentConfig("agent2", 2, "rack1");
        NProto::TAgentConfig agent3 = CreateAgentConfig("agent3", 3, "rack2");
        NProto::TAgentConfig agent4 = CreateAgentConfig("agent4", 4, "rack2");
        NProto::TAgentConfig agent5 = CreateAgentConfig("agent5", 5, "rack3");
        NProto::TAgentConfig agent6 = CreateAgentConfig("agent6", 6, "rack3");

        for (auto& agent: {agent1, agent2, agent3, agent4, agent5, agent6}) {
            deviceList.UpdateDevices(agent, poolConfigs);
        }

        allocate(12, {agent2.GetNodeId()});

        allocate(6, {agent3.GetNodeId()});
        allocate(12, {agent4.GetNodeId()});

        allocate(9, {agent5.GetNodeId()});
        allocate(12, {agent6.GetNodeId()});

        {
            // rack1 *agent1: 15, agent2: 3  18
            // rack2  agent3:  9, agent4: 3  12
            // rack3  agent5:  6, agent6: 3   9

            auto devices = allocate(1);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("rack1", devices[0].GetRack());
            UNIT_ASSERT_VALUES_EQUAL(agent1.GetAgentId(), devices[0].GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(agent1.GetNodeId(), devices[0].GetNodeId());
        }

        const THashSet<ui32> downrankedNodes = {
            agent1.GetNodeId(),
            agent5.GetNodeId()
        };

        {
            // rack1 [agent1: 14], *agent2: 3  17
            // rack2  agent3:  9,   agent4: 3  12
            // rack3 [agent5:  6],  agent6: 3   9

            auto devices = allocate(3, {}, downrankedNodes);
            UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
            for (const auto& d: devices) {
                UNIT_ASSERT_VALUES_EQUAL("rack1", d.GetRack());
                UNIT_ASSERT_VALUES_EQUAL(agent2.GetAgentId(), d.GetAgentId());
                UNIT_ASSERT_VALUES_EQUAL(agent2.GetNodeId(), d.GetNodeId());
            }
        }

        {
            // rack1 [agent1: 14]             14
            // rack2 *agent3:  9,  agent4: 3  12
            // rack3 [agent5:  6], agent6: 3   9

            auto devices = allocate(9, {}, downrankedNodes);
            UNIT_ASSERT_VALUES_EQUAL(9, devices.size());
            for (const auto& d: devices) {
                UNIT_ASSERT_VALUES_EQUAL_C("rack2", d.GetRack(), d);
                UNIT_ASSERT_VALUES_EQUAL(agent3.GetAgentId(), d.GetAgentId());
                UNIT_ASSERT_VALUES_EQUAL(agent3.GetNodeId(), d.GetNodeId());
            }
        }

        {
            // rack1 [agent1: 14]              14
            // rack3 [agent5:  6], *agent6: 3   9
            // rack2                agent4: 3   3

            auto devices = allocate(3, {}, downrankedNodes);
            UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
            for (const auto& d: devices) {
                UNIT_ASSERT_VALUES_EQUAL_C("rack3", d.GetRack(), d);
                UNIT_ASSERT_VALUES_EQUAL(agent6.GetAgentId(), d.GetAgentId());
                UNIT_ASSERT_VALUES_EQUAL(agent6.GetNodeId(), d.GetNodeId());
            }
        }

        {
            // rack1 [agent1: 14]             14
            // rack3 [agent5:  6]              6
            // rack2              *agent4: 3   3

            auto devices = allocate(3, {}, downrankedNodes);
            UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
            for (const auto& d: devices) {
                UNIT_ASSERT_VALUES_EQUAL_C("rack2", d.GetRack(), d);
                UNIT_ASSERT_VALUES_EQUAL(agent4.GetAgentId(), d.GetAgentId());
                UNIT_ASSERT_VALUES_EQUAL(agent4.GetNodeId(), d.GetNodeId());
            }
        }

        // It's time for the downranked nodes.

        {
            // rack1 *[agent1: 14]  14
            // rack3  [agent5:  6]   6

            auto devices = allocate(6, {}, downrankedNodes);
            UNIT_ASSERT_VALUES_EQUAL(6, devices.size());
            for (const auto& d: devices) {
                UNIT_ASSERT_VALUES_EQUAL_C("rack1", d.GetRack(), d);
                UNIT_ASSERT_VALUES_EQUAL(agent1.GetAgentId(), d.GetAgentId());
                UNIT_ASSERT_VALUES_EQUAL(agent1.GetNodeId(), d.GetNodeId());
            }
        }

        {
            // rack1 *[agent1: 8]  8
            // rack3  [agent5: 6]  6

            auto devices = allocate(6, {}, downrankedNodes);
            UNIT_ASSERT_VALUES_EQUAL(6, devices.size());
            for (const auto& d: devices) {
                UNIT_ASSERT_VALUES_EQUAL_C("rack1", d.GetRack(), d);
                UNIT_ASSERT_VALUES_EQUAL(agent1.GetAgentId(), d.GetAgentId());
                UNIT_ASSERT_VALUES_EQUAL(agent1.GetNodeId(), d.GetNodeId());
            }
        }

        {
            // rack3 *[agent5: 6]  6
            // rack1  [agent1: 2]  2

            auto devices = allocate(2, {}, downrankedNodes);
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            for (const auto& d: devices) {
                UNIT_ASSERT_VALUES_EQUAL_C("rack3", d.GetRack(), d);
                UNIT_ASSERT_VALUES_EQUAL(agent5.GetAgentId(), d.GetAgentId());
                UNIT_ASSERT_VALUES_EQUAL(agent5.GetNodeId(), d.GetNodeId());
            }
        }

        {
            // rack3 *[agent5: 4]  4
            // rack1 *[agent1: 2]  2

            auto devices = allocate(6, {}, downrankedNodes);
            UNIT_ASSERT_VALUES_EQUAL(6, devices.size());
        }

        // No space left
        {
            auto devices = allocate(1, {}, downrankedNodes);
            UNIT_ASSERT_VALUES_EQUAL(0, devices.size());
        }
    }

    Y_UNIT_TEST(ShouldSelectPhysicalDeviceWithTheMostSpaceUponDeviceAllocation)
    {
        const auto poolConfigs = CreateDevicePoolConfigs({});

        TDeviceList deviceList;

        auto allocate = [&] (ui32 n) {
            return deviceList.AllocateDevices(
                "disk",
                {
                    .LogicalBlockSize = DefaultBlockSize,
                    .BlockCount = n * DefaultBlockCount
                }
            );
        };

        NProto::TAgentConfig agent1 = CreateAgentConfig(
            "agent1",
            1,
            "rack1",
            "",
            {"/dev1", "/dev2"},
            100);
        NProto::TAgentConfig agent2 = CreateAgentConfig(
            "agent2",
            2,
            "rack2",
            "",
            {"/dev1", "/dev2"},
            100);
        deviceList.UpdateDevices(agent1, poolConfigs);
        deviceList.UpdateDevices(agent2, poolConfigs);

        UNIT_ASSERT_VALUES_EQUAL(200, deviceList.Size());

        auto devices1 = allocate(10);
        UNIT_ASSERT_VALUES_EQUAL(10, devices1.size());
        for (auto& d: devices1) {
            UNIT_ASSERT_VALUES_EQUAL("agent1", d.GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL("/dev1", d.GetDeviceName());
        }

        auto devices2 = allocate(10);
        UNIT_ASSERT_VALUES_EQUAL(10, devices2.size());
        for (auto& d: devices2) {
            UNIT_ASSERT_VALUES_EQUAL("agent2", d.GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL("/dev1", d.GetDeviceName());
        }

        auto devices3 = allocate(10);
        UNIT_ASSERT_VALUES_EQUAL(10, devices3.size());
        for (auto& d: devices3) {
            UNIT_ASSERT_VALUES_EQUAL("agent1", d.GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL("/dev2", d.GetDeviceName());
        }

        auto devices4 = allocate(10);
        UNIT_ASSERT_VALUES_EQUAL(10, devices4.size());
        for (auto& d: devices4) {
            UNIT_ASSERT_VALUES_EQUAL("agent2", d.GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL("/dev2", d.GetDeviceName());
        }

        auto devices5 = allocate(10);
        UNIT_ASSERT_VALUES_EQUAL(10, devices5.size());
        for (auto& d: devices5) {
            UNIT_ASSERT_VALUES_EQUAL("agent1", d.GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL("/dev1", d.GetDeviceName());
        }
    }

    Y_UNIT_TEST(ShouldSelectAgentWithTheLeastOccupiedSpaceUponDeviceAllocation)
    {
        const auto poolConfigs = CreateDevicePoolConfigs({});

        TDeviceList deviceList;

        auto allocate = [&] (ui32 n) {
            return deviceList.AllocateDevices(
                "disk",
                {
                    .LogicalBlockSize = DefaultBlockSize,
                    .BlockCount = n * DefaultBlockCount
                }
            );
        };

        NProto::TAgentConfig agent1 = CreateAgentConfig(
            "agent1",
            1,
            "rack1",
            "",
            {"/dev1"},
            100);
        NProto::TAgentConfig agent2 = CreateAgentConfig(
            "agent2",
            2,
            "rack2",
            "",
            {"/dev1"},
            200);
        deviceList.UpdateDevices(agent1, poolConfigs);
        deviceList.UpdateDevices(agent2, poolConfigs);

        UNIT_ASSERT_VALUES_EQUAL(300, deviceList.Size());

        // allocating from agent2 - occupied space is 0 at both agents but
        // agent2 has more free space
        auto devices1 = allocate(10);
        UNIT_ASSERT_VALUES_EQUAL(10, devices1.size());
        for (auto& d: devices1) {
            UNIT_ASSERT_VALUES_EQUAL("agent2", d.GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL("/dev1", d.GetDeviceName());
        }

        // now we should allocate from agent1 since it has less occupied space
        auto devices2 = allocate(10);
        UNIT_ASSERT_VALUES_EQUAL(10, devices2.size());
        for (auto& d: devices2) {
            UNIT_ASSERT_VALUES_EQUAL("agent1", d.GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL("/dev1", d.GetDeviceName());
        }

        // now the occupied space is again the same for agent1 and agent2 =>
        // allocating from agent2
        auto devices3 = allocate(100);
        UNIT_ASSERT_VALUES_EQUAL(100, devices3.size());
        for (auto& d: devices3) {
            UNIT_ASSERT_VALUES_EQUAL("agent2", d.GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL("/dev1", d.GetDeviceName());
        }

        // allocating from agent1 since it has less occupied space
        auto devices4 = allocate(30);
        UNIT_ASSERT_VALUES_EQUAL(30, devices4.size());
        for (auto& d: devices4) {
            UNIT_ASSERT_VALUES_EQUAL("agent1", d.GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL("/dev1", d.GetDeviceName());
        }

        // allocating from agent1 again since it has less occupied space -
        // even though it has less free space than agent2
        auto devices5 = allocate(60);
        UNIT_ASSERT_VALUES_EQUAL(60, devices5.size());
        for (auto& d: devices5) {
            UNIT_ASSERT_VALUES_EQUAL("agent1", d.GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL("/dev1", d.GetDeviceName());
        }

        // no free space at agent1 - allocating from agent2
        auto devices6 = allocate(10);
        UNIT_ASSERT_VALUES_EQUAL(10, devices6.size());
        for (auto& d: devices6) {
            UNIT_ASSERT_VALUES_EQUAL("agent2", d.GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL("/dev1", d.GetDeviceName());
        }
    }

    Y_UNIT_TEST(ShouldSelectRackAccordingToRestrictions)
    {
        const auto poolConfigs = CreateDevicePoolConfigs({});

        TDeviceList deviceList;

        auto allocate = [&] (
            ui32 n,
            THashSet<TString> forbiddenRacks,
            THashSet<TString> preferredRacks)
        {
            return deviceList.AllocateDevices(
                "disk",
                {
                    .ForbiddenRacks = std::move(forbiddenRacks),
                    .PreferredRacks = std::move(preferredRacks),
                    .LogicalBlockSize = DefaultBlockSize,
                    .BlockCount = n * DefaultBlockCount
                }
            );
        };

        NProto::TAgentConfig agent1 = CreateAgentConfig("agent1", 1, "rack1");
        NProto::TAgentConfig agent2 = CreateAgentConfig("agent2", 2, "rack2");
        NProto::TAgentConfig agent3 = CreateAgentConfig("agent3", 3, "rack2");
        deviceList.UpdateDevices(agent1, poolConfigs);
        deviceList.UpdateDevices(agent2, poolConfigs);
        deviceList.UpdateDevices(agent3, poolConfigs);

        UNIT_ASSERT_VALUES_EQUAL(45, deviceList.Size());

        // Allocate from rack with the most space.
        auto devices1 = allocate(10, {}, {});
        UNIT_ASSERT_VALUES_EQUAL(10, devices1.size());
        UNIT_ASSERT_VALUES_EQUAL("rack2", devices1[0].GetRack());

        // Restrict allocate from rack2. Allocation should occur on rack1.
        auto devices2 = allocate(10, {"rack2"}, {});
        UNIT_ASSERT_VALUES_EQUAL(10, devices2.size());
        UNIT_ASSERT_VALUES_EQUAL("rack1", devices2[0].GetRack());

        // Prefer allocate from rack1. Allocation should occur on rack1 despite
        // there is more space on rack2.
        auto devices3 = allocate(1, {}, {"rack1"});
        UNIT_ASSERT_VALUES_EQUAL(1, devices3.size());
        UNIT_ASSERT_VALUES_EQUAL("rack1", devices3[0].GetRack());

        // Restrict allocate from rack1 and rack2.
        auto devices4 = allocate(1, {"rack1", "rack2"}, {});
        UNIT_ASSERT_VALUES_EQUAL(0, devices4.size());
    }

    Y_UNIT_TEST(ShouldAllocateFromSpecifiedNode)
    {
        const auto poolConfigs = CreateDevicePoolConfigs({});

        TDeviceList deviceList;

        TVector agents {
            CreateAgentConfig("agent1", 1, "rack1"),
            CreateAgentConfig("agent2", 2, "rack2"),
            CreateAgentConfig("agent3", 3, "rack2")
        };

        for (const auto& agent: agents) {
            UNIT_ASSERT_VALUES_EQUAL(15, agent.DevicesSize());
            deviceList.UpdateDevices(agent, poolConfigs);
        }

        auto allocate = [&] (ui32 n, ui32 nodeId) {
            return deviceList.AllocateDevices(
                Sprintf("disk-%d-%d", n, nodeId),
                {
                    .ForbiddenRacks = {},
                    .LogicalBlockSize = DefaultBlockSize,
                    .BlockCount = n * DefaultBlockCount,
                    .NodeIds = { nodeId }
                }
            );
        };

        // allocate one device from each agent
        for (const auto& agent: agents) {
            const auto nodeId = agent.GetNodeId();
            auto devices = allocate(1, nodeId);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL(nodeId, devices[0].GetNodeId());
        }

        for (const auto& agent: agents) {
            auto devices = allocate(15, agent.GetNodeId());
            // can't allocate 15 devices
            UNIT_ASSERT_VALUES_EQUAL(0, devices.size());
        }

        // allocate 14 devices from each agent
        for (const auto& agent: agents) {
            const auto nodeId = agent.GetNodeId();

            auto devices = allocate(14, nodeId);
            UNIT_ASSERT_VALUES_EQUAL(14, devices.size());
            for (auto& d: devices) {
                UNIT_ASSERT_VALUES_EQUAL(nodeId, d.GetNodeId());
            }
        }
    }

    // TODO: allocate by pool name
    /*
    Y_UNIT_TEST(ShouldAllocateFromSpecifiedNodeByTag)
    {
        const auto poolConfigs = CreateDevicePoolConfigs({});

        const TString ssdLocalTag = "ssd_local";

        TDeviceList deviceList;

        TVector agents {
            CreateAgentConfig("agent1", 1, "rack1"),
            CreateAgentConfig("agent2", 2, "rack2"),
            CreateAgentConfig("agent3", 3, "rack2")
        };

        // tag some devices with `ssd_local` (8 in total for each agents)
        for (auto& agent: agents) {
            int i = 0;
            for (auto& device: *agent.MutableDevices()) {
                if (i++ % 2 == 0) {
                    device.SetPoolName(ssdLocalTag);
                }
            }
        }

        for (const auto& agent: agents) {
            UNIT_ASSERT_VALUES_EQUAL(15, agent.DevicesSize());
            deviceList.UpdateDevices(agent, poolConfigs);
        }

        auto allocate = [&] (ui32 n, ui32 nodeId, TString name) {
            return deviceList.AllocateDevices(
                Sprintf("disk-%d-%d-%s", n, nodeId, name.c_str()),
                {
                    .OtherRacks = {},
                    .LogicalBlockSize = DefaultBlockSize,
                    .BlockCount = n * DefaultBlockCount,
                    .PoolName = name,
                    .NodeIds = { nodeId }
                }
            );
        };

        // allocate one device from each agent and default pool
        for (const auto& agent: agents) {
            const auto nodeId = agent.GetNodeId();
            auto devices = allocate(1, nodeId, DefaultPoolName);
            UNIT_ASSERT_VALUES_EQUAL_C(1, devices.size(), agent.GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(nodeId, devices[0].GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL(DefaultPoolName, devices[0].GetPoolName());
        }

        // allocate one device from each agent and ssd_local pool
        for (const auto& agent: agents) {
            const auto nodeId = agent.GetNodeId();
            auto devices = allocate(1, nodeId, ssdLocalTag);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL(nodeId, devices[0].GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL(ssdLocalTag, devices[0].GetPoolName());
        }

        for (const auto& agent: agents) {
            auto devices = allocate(8, agent.GetNodeId(), ssdLocalTag);
            // can't allocate 8 devices
            UNIT_ASSERT_VALUES_EQUAL(0, devices.size());
        }

        for (int n: { 3, 4 }) {
            for (const auto& agent: agents) {
                auto devices = allocate(n, agent.GetNodeId(), ssdLocalTag);
                UNIT_ASSERT_VALUES_EQUAL(n, devices.size());
                for (auto& d: devices) {
                    UNIT_ASSERT_VALUES_EQUAL(agent.GetNodeId(), d.GetNodeId());
                    UNIT_ASSERT_VALUES_EQUAL(ssdLocalTag, d.GetPoolName());
                }
            }
        }
    }*/

    Y_UNIT_TEST(ShouldAllocateFromLocalPoolOnlyAtSingleHost)
    {
        const TString localPoolName = "local-ssd";

        const auto poolConfigs = CreateDevicePoolConfigs({{
            localPoolName, 93_GB, NProto::DEVICE_POOL_KIND_LOCAL
        }});

        TDeviceList deviceList;

        TVector agents {
            CreateAgentConfig("agent1", 1, "rack1"),
            CreateAgentConfig("agent2", 2, "rack2"),
            CreateAgentConfig("agent3", 3, "rack2")
        };

        for (auto& agent: agents) {
            auto& device = *agent.MutableDevices(0);
            device.SetPoolName(localPoolName);
            device.SetPoolKind(NProto::DEVICE_POOL_KIND_LOCAL);
        }

        for (const auto& agent: agents) {
            UNIT_ASSERT_VALUES_EQUAL(15, agent.DevicesSize());
            deviceList.UpdateDevices(agent, poolConfigs);
        }

        auto allocate = [&] (ui32 n) {
            return deviceList.AllocateDevices(
                "disk-id",
                {
                    .ForbiddenRacks = {},
                    .LogicalBlockSize = DefaultBlockSize,
                    .BlockCount = n * DefaultBlockCount,
                    .PoolKind = NProto::DEVICE_POOL_KIND_LOCAL,
                    .NodeIds = { 1, 2, 3 },
                }
            );
        };

        {
            auto devices = allocate(3);
            UNIT_ASSERT_VALUES_EQUAL(0, devices.size());
        }

        {
            auto devices = allocate(2);
            UNIT_ASSERT_VALUES_EQUAL(0, devices.size());
        }

        {
            auto devices = allocate(1);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL(localPoolName, devices[0].GetPoolName());
        }
    }

    Y_UNIT_TEST(ShouldAllocateFromGlobalPoolByPoolName)
    {
        const TString rotPoolName = "rot";
        const TString otherPoolName = "other";

        const auto poolConfigs = CreateDevicePoolConfigs(
            {{rotPoolName, 93_GB, NProto::DEVICE_POOL_KIND_GLOBAL},
             {otherPoolName, 93_GB, NProto::DEVICE_POOL_KIND_GLOBAL}});

        TDeviceList deviceList;

        TVector agents {
            CreateAgentConfig("agent1", 1, "rack1"),
            CreateAgentConfig("agent2", 2, "rack2"),
            CreateAgentConfig("agent3", 3, "rack2", rotPoolName),
            CreateAgentConfig("agent4", 4, "rack2", otherPoolName),
            CreateAgentConfig("agent5", 5, "rack3", otherPoolName),
            CreateAgentConfig("agent6", 6, "rack3", rotPoolName),
        };

        for (const auto& agent: agents) {
            deviceList.UpdateDevices(agent, poolConfigs);
        }

        auto allocate = [&] (ui32 n) {
            return deviceList.AllocateDevices(
                "disk-id",
                {
                    .ForbiddenRacks = {},
                    .LogicalBlockSize = DefaultBlockSize,
                    .BlockCount = n * DefaultBlockCount,
                    .PoolName = rotPoolName,
                    .PoolKind = NProto::DEVICE_POOL_KIND_GLOBAL,
                }
            );
        };

        auto check = [&] (const TVector<NProto::TDeviceConfig>& devices) {
            for (ui32 i = 0; i < devices.size(); ++i) {
                const auto& device = devices[i];
                UNIT_ASSERT_VALUES_EQUAL_C(
                    rotPoolName,
                    device.GetPoolName(),
                    TStringBuilder() << "device " << i << "/" << devices.size()
                        << ", agent " << device.GetAgentId());
            }
        };

        {
            auto devices = allocate(25);
            UNIT_ASSERT_VALUES_EQUAL(25, devices.size());
            check(devices);
        }

        {
            auto devices = allocate(6);
            UNIT_ASSERT_VALUES_EQUAL(0, devices.size());
        }

        {
            auto devices = allocate(5);
            UNIT_ASSERT_VALUES_EQUAL(5, devices.size());
            check(devices);
        }
    }

    Y_UNIT_TEST(ShouldFilterDevices)
    {
        ui32 i = 1;

        auto makeDevice = [&] (
            const TString& agentId,
            const TString& deviceName,
            const TString& poolName,
            const NProto::EDevicePoolKind poolKind)
        {
            NProto::TDeviceConfig d;
            d.SetAgentId(agentId);
            d.SetDeviceName(deviceName);
            d.SetDeviceUUID(Sprintf("uuid-%u", i));
            d.SetPoolName(poolName);
            d.SetPoolKind(poolKind);
            d.SetBlockSize(DefaultBlockSize);
            d.SetBlocksCount(DefaultBlockCount);
            d.SetUnadjustedBlockCount(DefaultBlockCount);
            ++i;
            return d;
        };

        TVector<NProto::TDeviceConfig> devices = {
            makeDevice("agent-1", "/dev/1", "", NProto::DEVICE_POOL_KIND_DEFAULT),
            makeDevice("agent-1", "/dev/1", "", NProto::DEVICE_POOL_KIND_DEFAULT),
            makeDevice("agent-1", "/dev/1", "", NProto::DEVICE_POOL_KIND_DEFAULT),
            makeDevice("agent-1", "/dev/1", "", NProto::DEVICE_POOL_KIND_DEFAULT),
            makeDevice("agent-1", "/dev/2", "", NProto::DEVICE_POOL_KIND_DEFAULT),
            makeDevice("agent-1", "/dev/2", "", NProto::DEVICE_POOL_KIND_DEFAULT),
            makeDevice("agent-1", "/dev/2", "", NProto::DEVICE_POOL_KIND_DEFAULT),
            makeDevice("agent-2", "/dev/1", "", NProto::DEVICE_POOL_KIND_DEFAULT),
            makeDevice("agent-2", "/dev/1", "", NProto::DEVICE_POOL_KIND_DEFAULT),
            makeDevice("agent-2", "/dev/1", "", NProto::DEVICE_POOL_KIND_DEFAULT),
            makeDevice("agent-2", "/dev/1", "", NProto::DEVICE_POOL_KIND_DEFAULT),
            makeDevice("agent-2", "/dev/1", "", NProto::DEVICE_POOL_KIND_DEFAULT),

            makeDevice("agent-3", "/dev/1", "local", NProto::DEVICE_POOL_KIND_LOCAL),
            makeDevice("agent-3", "/dev/1", "local", NProto::DEVICE_POOL_KIND_LOCAL),
            makeDevice("agent-3", "/dev/1", "local", NProto::DEVICE_POOL_KIND_LOCAL),
            makeDevice("agent-3", "/dev/1", "local", NProto::DEVICE_POOL_KIND_LOCAL),
            makeDevice("agent-3", "/dev/2", "local", NProto::DEVICE_POOL_KIND_LOCAL),
            makeDevice("agent-3", "/dev/2", "local", NProto::DEVICE_POOL_KIND_LOCAL),
            makeDevice("agent-3", "/dev/2", "local", NProto::DEVICE_POOL_KIND_LOCAL),
            makeDevice("agent-4", "/dev/1", "local", NProto::DEVICE_POOL_KIND_LOCAL),
            makeDevice("agent-4", "/dev/1", "local", NProto::DEVICE_POOL_KIND_LOCAL),
            makeDevice("agent-4", "/dev/1", "local", NProto::DEVICE_POOL_KIND_LOCAL),
            makeDevice("agent-4", "/dev/1", "local", NProto::DEVICE_POOL_KIND_LOCAL),
            makeDevice("agent-4", "/dev/1", "local", NProto::DEVICE_POOL_KIND_LOCAL),

            makeDevice("agent-5", "/dev/1", "rot", NProto::DEVICE_POOL_KIND_GLOBAL),
            makeDevice("agent-5", "/dev/1", "rot", NProto::DEVICE_POOL_KIND_GLOBAL),
            makeDevice("agent-5", "/dev/1", "rot", NProto::DEVICE_POOL_KIND_GLOBAL),
            makeDevice("agent-5", "/dev/1", "rot", NProto::DEVICE_POOL_KIND_GLOBAL),
            makeDevice("agent-5", "/dev/2", "rot", NProto::DEVICE_POOL_KIND_GLOBAL),
            makeDevice("agent-5", "/dev/2", "rot", NProto::DEVICE_POOL_KIND_GLOBAL),
            makeDevice("agent-5", "/dev/2", "rot", NProto::DEVICE_POOL_KIND_GLOBAL),
            makeDevice("agent-6", "/dev/1", "rot", NProto::DEVICE_POOL_KIND_GLOBAL),
            makeDevice("agent-6", "/dev/1", "rot", NProto::DEVICE_POOL_KIND_GLOBAL),
            makeDevice("agent-6", "/dev/1", "rot", NProto::DEVICE_POOL_KIND_GLOBAL),
            makeDevice("agent-6", "/dev/1", "rot", NProto::DEVICE_POOL_KIND_GLOBAL),
            makeDevice("agent-6", "/dev/1", "rot", NProto::DEVICE_POOL_KIND_GLOBAL),
        };

        auto filtered = FilterDevices(devices, 10, 2, 1);
        UNIT_ASSERT_VALUES_EQUAL(12 + 6 + 3, filtered.size());

        UNIT_ASSERT_VALUES_EQUAL("uuid-1", filtered[0].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-2", filtered[1].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-3", filtered[2].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-4", filtered[3].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-5", filtered[4].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-6", filtered[5].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-7", filtered[6].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-8", filtered[7].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-9", filtered[8].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-10", filtered[9].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-11", filtered[10].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-12", filtered[11].GetDeviceUUID());

        UNIT_ASSERT_VALUES_EQUAL("uuid-13", filtered[12].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-14", filtered[13].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-17", filtered[14].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-18", filtered[15].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-20", filtered[16].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-21", filtered[17].GetDeviceUUID());

        UNIT_ASSERT_VALUES_EQUAL("uuid-25", filtered[18].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-29", filtered[19].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-32", filtered[20].GetDeviceUUID());
    }

    Y_UNIT_TEST(ShouldForgetDevice)
    {
        TDeviceList deviceList;

        const auto poolConfigs = CreateDevicePoolConfigs({});

        NProto::TAgentConfig foo = CreateAgentConfig("foo", 1000, "rack-1");

        deviceList.UpdateDevices(foo, poolConfigs);
        deviceList.MarkDeviceAsDirty("foo-1");

        deviceList.MarkDeviceAllocated("vol0", "foo-2");
        deviceList.MarkDeviceAsDirty("foo-2");

        deviceList.MarkDeviceAllocated("vol0", "foo-3");

        deviceList.MarkDeviceAsDirty("foo-4");
        deviceList.SuspendDevice("foo-4");

        deviceList.SuspendDevice("foo-5");

        UNIT_ASSERT(deviceList.IsDirtyDevice("foo-1"));

        UNIT_ASSERT(deviceList.IsDirtyDevice("foo-2"));
        UNIT_ASSERT(deviceList.IsAllocatedDevice("foo-2"));

        UNIT_ASSERT(deviceList.IsAllocatedDevice("foo-3"));

        UNIT_ASSERT(deviceList.IsDirtyDevice("foo-4"));
        UNIT_ASSERT(deviceList.IsSuspendedDevice("foo-4"));

        UNIT_ASSERT(deviceList.IsSuspendedDevice("foo-5"));

        UNIT_ASSERT(deviceList.FindDevice("foo-6"));

        deviceList.ForgetDevice("foo-1");

        UNIT_ASSERT(!deviceList.IsDirtyDevice("foo-1"));
        UNIT_ASSERT(!deviceList.FindDevice("foo-1"));

        deviceList.ForgetDevice("foo-2");

        UNIT_ASSERT(!deviceList.IsDirtyDevice("foo-2"));
        UNIT_ASSERT(!deviceList.IsAllocatedDevice("foo-2"));
        UNIT_ASSERT(!deviceList.FindDevice("foo-2"));

        deviceList.ForgetDevice("foo-3");

        UNIT_ASSERT(!deviceList.IsAllocatedDevice("foo-3"));
        UNIT_ASSERT(!deviceList.FindDevice("foo-3"));

        deviceList.ForgetDevice("foo-4");

        UNIT_ASSERT(!deviceList.IsDirtyDevice("foo-4"));
        UNIT_ASSERT(!deviceList.IsSuspendedDevice("foo-4"));
        UNIT_ASSERT(!deviceList.FindDevice("foo-4"));

        deviceList.ForgetDevice("foo-5");

        UNIT_ASSERT(!deviceList.IsSuspendedDevice("foo-5"));
        UNIT_ASSERT(!deviceList.FindDevice("foo-5"));

        deviceList.ForgetDevice("foo-6");

        UNIT_ASSERT(!deviceList.FindDevice("foo-6"));

        UNIT_ASSERT_VALUES_EQUAL(0, deviceList.AllocateDevices("vol1", {
            .LogicalBlockSize = 4_KB,
            .BlockCount = 10 * DefaultBlockCount * DefaultBlockSize / 4_KB
        }).size());
    }

    Y_UNIT_TEST(ShouldTrackDeviceCountPerPool)
    {
        const auto poolConfigs = CreateDevicePoolConfigs(
            {{"rot", 93_GB, NProto::DEVICE_POOL_KIND_GLOBAL}});

        TDeviceList deviceList;

        auto a1 = CreateAgentConfig("a1", 1000, "rack-1");
        auto a2 = CreateAgentConfig(
            "a2",
            2000,
            "rack-2",
            "rot",
            {"/some/device"},
            100);

        deviceList.UpdateDevices(a1, poolConfigs);
        deviceList.UpdateDevices(a2, poolConfigs);

        auto devCounts = deviceList.GetPoolName2DeviceCount();
        UNIT_ASSERT_VALUES_EQUAL(2, devCounts.size());
        UNIT_ASSERT_VALUES_EQUAL(15, devCounts[""]);
        UNIT_ASSERT_VALUES_EQUAL(100, devCounts["rot"]);

        deviceList.ForgetDevice("a1-1");
        deviceList.ForgetDevice("a2-1");
        deviceList.ForgetDevice("a2-2");

        devCounts = deviceList.GetPoolName2DeviceCount();
        UNIT_ASSERT_VALUES_EQUAL(2, devCounts.size());
        UNIT_ASSERT_VALUES_EQUAL(14, devCounts[""]);
        UNIT_ASSERT_VALUES_EQUAL(98, devCounts["rot"]);

        deviceList.RemoveDevices(a2);

        devCounts = deviceList.GetPoolName2DeviceCount();
        UNIT_ASSERT_VALUES_EQUAL(2, devCounts.size());
        UNIT_ASSERT_VALUES_EQUAL(14, devCounts[""]);
        UNIT_ASSERT_VALUES_EQUAL(0, devCounts["rot"]);
    }

    Y_UNIT_TEST(ShouldFilterOutDeviceWithWrongSize)
    {
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        InitCriticalEventsCounter(counters);
        auto configMismatch = counters->GetCounter(
            "AppCriticalEvents/DiskRegistryAgentDevicePoolConfigMismatch",
            true);
        UNIT_ASSERT_VALUES_EQUAL(0, configMismatch->Val());

        const auto poolConfigs = CreateDevicePoolConfigs({});
        const ui64 hugeDeviceBlockCount = 10 * DefaultBlockCount;

        auto createAgentConfig = [] (bool withHugeDevice) {
            NProto::TAgentConfig agentConfig;
            agentConfig.SetAgentId("node-id");
            agentConfig.SetNodeId(42);

            for (int i = 0; i != 3; ++i) {
                auto* device = agentConfig.AddDevices();
                device->SetBlockSize(DefaultBlockSize);
                if (withHugeDevice && i == 1) {
                    device->SetBlocksCount(hugeDeviceBlockCount);
                    device->SetUnadjustedBlockCount(hugeDeviceBlockCount + 5);
                } else {
                    device->SetBlocksCount(DefaultBlockCount);
                    device->SetUnadjustedBlockCount(DefaultBlockCount + 5);
                }
                device->SetRack("rack");
                device->SetNodeId(agentConfig.GetNodeId());
                device->SetDeviceUUID("uuid-" + ToString(i + 1));
                device->SetDeviceName("path-" + device->GetDeviceUUID());
                device->SetAgentId(agentConfig.GetAgentId());
            }

            return agentConfig;
        };

        const ui32 expectedDeviceCount = 3;

        TDeviceList deviceList;

        {
            deviceList.UpdateDevices(createAgentConfig(true), poolConfigs);

            const auto devices = deviceList.AllocateDevices("disk-id", {
                .LogicalBlockSize = DefaultBlockSize,
                .BlockCount = expectedDeviceCount * DefaultBlockCount,
            });
            UNIT_ASSERT_VALUES_EQUAL(0, devices.size());
        }

        UNIT_ASSERT_VALUES_EQUAL(1, configMismatch->Val());

        {

            deviceList.UpdateDevices(createAgentConfig(false), poolConfigs);

            const auto devices = deviceList.AllocateDevices("disk-id", {
                .LogicalBlockSize = DefaultBlockSize,
                .BlockCount = expectedDeviceCount * DefaultBlockCount,
            });

            UNIT_ASSERT_VALUES_EQUAL(expectedDeviceCount, devices.size());
            for (const auto& device: devices) {
                const ui64 size =
                    device.GetBlockSize() * device.GetBlocksCount();

                UNIT_ASSERT_VALUES_EQUAL_C(
                    DefaultBlockCount * DefaultBlockSize,
                    size,
                    device);

                UNIT_ASSERT_LT_C(
                    device.GetBlocksCount(),
                    device.GetUnadjustedBlockCount(),
                    device);
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(1, configMismatch->Val());
    }

    Y_UNIT_TEST(ShouldNotTriggerDevicePoolConfigMismatchOnAllocatedDevices)
    {
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        InitCriticalEventsCounter(counters);
        auto configMismatch = counters->GetCounter(
            "AppCriticalEvents/DiskRegistryAgentDevicePoolConfigMismatch",
            true);
        UNIT_ASSERT_VALUES_EQUAL(0, configMismatch->Val());

        const TString diskId = "vol0";

        NProto::TAgentConfig agent;
        agent.SetAgentId("agent-id");
        agent.SetNodeId(42);

        auto& device = *agent.AddDevices();
        device.SetBlockSize(4_KB);
        device.SetUnadjustedBlockCount(93_GB / 4_KB + 1);
        device.SetBlocksCount(device.GetUnadjustedBlockCount());
        device.SetNodeId(42);
        device.SetDeviceName("path");
        device.SetDeviceUUID("uuid");

        TDevicePoolConfigs poolConfigs;
        auto& defaultPool = poolConfigs[""];
        defaultPool.SetAllocationUnit(93_GB);

        {
            TDeviceList deviceList{
                {},   // dirtyDevices
                {},   // suspendedDevices
                {{device.GetDeviceUUID(), diskId}},
                false   // alwaysAllocateLocalDisks
            };

            deviceList.UpdateDevices(agent, poolConfigs);
        }

        UNIT_ASSERT_VALUES_EQUAL(0, configMismatch->Val());

        {
            TDeviceList deviceList{
                {},     // dirtyDevices
                {},     // suspendedDevices
                {},     // allocatedDevices
                false   // alwaysAllocateLocalDisks
            };

            deviceList.UpdateDevices(agent, poolConfigs);
        }

        UNIT_ASSERT_VALUES_EQUAL(1, configMismatch->Val());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
