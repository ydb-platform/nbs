#include "replica_table.h"

#include <cloud/blockstore/libs/storage/disk_registry/model/device_list.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

auto CreateDevice(TString id)
{
    NProto::TDeviceConfig config;
    config.SetDeviceUUID(std::move(id));
    return config;
}

void AddDevicesToAgent(
    NProto::TAgentConfig& agentConfig,
    const TVector<TVector<TReplicaTable::TDeviceInfo>>& deviceMatrix)
{
    for (const auto& cell: deviceMatrix) {
        for (const auto& device: cell) {
            *agentConfig.AddDevices() = CreateDevice(device.Id);
        }
    }
}

void SetDeviceState(
    NProto::TAgentConfig& agentConfig,
    const TString& deviceId,
    NProto::EDeviceState deviceState)
{
    for (size_t i = 0; i < agentConfig.DevicesSize(); ++i) {
        if (agentConfig.GetDevices(i).GetDeviceUUID() == deviceId) {
            agentConfig.MutableDevices(i)->SetState(deviceState);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TDevicePoolConfigs CreateDevicePoolConfigs(
    std::initializer_list<std::tuple<TString, ui64, NProto::EDevicePoolKind>>
        pools)
{
    NProto::TDevicePoolConfig nonrepl;
    nonrepl.SetAllocationUnit(93_GB);

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

Y_UNIT_TEST_SUITE(TReplicaTableTest)
{
    Y_UNIT_TEST(ShouldTryReplaceDevice)
    {
        const auto poolConfigs = CreateDevicePoolConfigs({});

        TDeviceList deviceList;
        TReplicaTable t(&deviceList);
        t.AddReplica("disk-1", {"d1-1", "d1-2", "d1-3", "d1-4"});
        t.AddReplica("disk-1", {"d2-1", "d2-2", "d2-3", "d2-4"});
        t.AddReplica("disk-1", {"d3-1", "d3-2", "d3-3", "d3-4"});

        NProto::TAgentConfig agentConfig;
        agentConfig.SetAgentId("foo");
        AddDevicesToAgent(agentConfig, t.AsMatrix("disk-1"));
        *agentConfig.AddDevices() = CreateDevice("d1-2-new");
        *agentConfig.AddDevices() = CreateDevice("d1-2-new-new");
        *agentConfig.AddDevices() = CreateDevice("d2-2-new");
        *agentConfig.AddDevices() = CreateDevice("d2-2-new");
        deviceList.UpdateDevices(agentConfig, poolConfigs);

        // We have mirror-3 disk.
        // Mark device "d2-2" as replacement. We still have two good devices in
        // the cell.
        t.MarkReplacementDevice("disk-1", "d2-2", true);
        // Check that we have one device in the replacement state.
        UNIT_ASSERT_VALUES_EQUAL(1, t.GetDevicesReplacementsCount("disk-1"));
        // Check that the device "d1-2" is ready for replacement.
        UNIT_ASSERT(t.IsReplacementAllowed("disk-1", "d1-2"));
        // Replace it. Now we have only one good device in the cell.
        UNIT_ASSERT(t.ReplaceDevice("disk-1", "d1-2", "d1-2-new"));
        // We cannot replace a device that no longer belongs to the disk.
        UNIT_ASSERT(!t.IsReplacementAllowed("disk-1", "d1-2"));
        UNIT_ASSERT(!t.ReplaceDevice("disk-1", "d1-2", "d1-2-new"));

        // Cannot replace last good device in cell.
        UNIT_ASSERT(!t.IsReplacementAllowed("disk-1", "d3-2"));
        // Can replace a device that is already in the replacement state.
        UNIT_ASSERT(t.IsReplacementAllowed("disk-1", "d1-2-new"));
        UNIT_ASSERT(t.ReplaceDevice("disk-1", "d1-2-new", "d1-2-new-new"));

        // Can replace a device that is already in the replacement state.
        UNIT_ASSERT(t.IsReplacementAllowed("disk-1", "d2-2"));
        UNIT_ASSERT(t.ReplaceDevice("disk-1", "d2-2", "d2-2-new"));

        // Cannot replace last good device in cell.
        UNIT_ASSERT(!t.IsReplacementAllowed("disk-1", "d3-2"));

        // Finish replacement for one device. Now we have two good devices in
        // the cell.
        t.MarkReplacementDevice("disk-1", "d2-2-new", false);

        // Now we can replace the device. Since it is not the last good one in
        // the cell
        UNIT_ASSERT(t.IsReplacementAllowed("disk-1", "d3-2"));

        // Transfer one of the good devices to the error state.
        SetDeviceState(
            agentConfig,
            "d2-2-new",
            NProto::EDeviceState::DEVICE_STATE_ERROR);
        deviceList.UpdateDevices(agentConfig, poolConfigs);
        // Cannot replace last good device in cell (one broken, one in
        // replacement state).
        UNIT_ASSERT(!t.IsReplacementAllowed("disk-1", "d3-2"));

        // Transfer device back to the online state.
        SetDeviceState(
            agentConfig,
            "d2-2-new",
            NProto::EDeviceState::DEVICE_STATE_ONLINE);
        deviceList.UpdateDevices(agentConfig, poolConfigs);

        // Transfer replacement device to the error state.
        SetDeviceState(
            agentConfig,
            "d1-2-new-new",
            NProto::EDeviceState::DEVICE_STATE_ERROR);
        deviceList.UpdateDevices(agentConfig, poolConfigs);

        // Now we can replace the device. Since it is not the last good one in
        // the cell.
        UNIT_ASSERT(t.IsReplacementAllowed("disk-1", "d3-2"));
        UNIT_ASSERT(t.ReplaceDevice("disk-1", "d3-2", "d3-2-new"));

        UNIT_ASSERT(t.RemoveMirroredDisk("disk-1"));
        UNIT_ASSERT(!t.RemoveMirroredDisk("disk-1"));
    }

    Y_UNIT_TEST(ShouldCalculateReplicaStats)
    {
        const auto poolConfigs = CreateDevicePoolConfigs({});

        TDeviceList deviceList;
        TReplicaTable t(&deviceList);
        t.AddReplica("disk-1", {"d1-1-1", "d1-1-2", "d1-1-3", "d1-1-4"});
        t.AddReplica("disk-1", {"d1-2-1", "d1-2-2", "d1-2-3", "d1-2-4"});
        t.AddReplica("disk-1", {"d1-3-1", "d1-3-2", "d1-3-3", "d1-3-4"});
        t.AddReplica("disk-2", {"d2-1-1", "d2-1-2", "d2-1-3", "d2-1-4"});
        t.AddReplica("disk-2", {"d2-2-1", "d2-2-2", "d2-2-3", "d2-2-4"});

        NProto::TAgentConfig agentConfig;
        agentConfig.SetAgentId("foo");

        AddDevicesToAgent(agentConfig, t.AsMatrix("disk-1"));
        AddDevicesToAgent(agentConfig, t.AsMatrix("disk-2"));
        deviceList.UpdateDevices(agentConfig, poolConfigs);

        auto replicaCountStats = t.CalculateReplicaCountStats();
        UNIT_ASSERT_VALUES_EQUAL(1, replicaCountStats.Mirror3DiskMinus0);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats.Mirror3DiskMinus1);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats.Mirror3DiskMinus2);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats.Mirror3DiskMinus3);
        UNIT_ASSERT_VALUES_EQUAL(1, replicaCountStats.Mirror2DiskMinus0);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats.Mirror2DiskMinus1);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats.Mirror2DiskMinus2);

        t.MarkReplacementDevice("disk-1", "d1-2-2", true);

        UNIT_ASSERT_VALUES_EQUAL(1, t.GetDevicesReplacementsCount("disk-1"));

        replicaCountStats = t.CalculateReplicaCountStats();
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats.Mirror3DiskMinus0);
        UNIT_ASSERT_VALUES_EQUAL(1, replicaCountStats.Mirror3DiskMinus1);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats.Mirror3DiskMinus2);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats.Mirror3DiskMinus3);
        UNIT_ASSERT_VALUES_EQUAL(1, replicaCountStats.Mirror2DiskMinus0);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats.Mirror2DiskMinus1);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats.Mirror2DiskMinus2);

        t.MarkReplacementDevice("disk-1", "d1-1-1", true);
        t.MarkReplacementDevice("disk-1", "d1-1-2", true);
        t.MarkReplacementDevice("disk-1", "d1-2-3", true);
        t.MarkReplacementDevice("disk-1", "d1-3-1", true);

        UNIT_ASSERT_VALUES_EQUAL(5, t.GetDevicesReplacementsCount("disk-1"));

        t.MarkReplacementDevice("disk-2", "d2-2-1", true);

        UNIT_ASSERT_VALUES_EQUAL(1, t.GetDevicesReplacementsCount("disk-2"));

        replicaCountStats = t.CalculateReplicaCountStats();
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats.Mirror3DiskMinus0);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats.Mirror3DiskMinus1);
        UNIT_ASSERT_VALUES_EQUAL(1, replicaCountStats.Mirror3DiskMinus2);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats.Mirror3DiskMinus3);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats.Mirror2DiskMinus0);
        UNIT_ASSERT_VALUES_EQUAL(1, replicaCountStats.Mirror2DiskMinus1);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats.Mirror2DiskMinus2);

        t.MarkReplacementDevice("disk-1", "d1-2-1", true);

        UNIT_ASSERT_VALUES_EQUAL(6, t.GetDevicesReplacementsCount("disk-1"));

        replicaCountStats = t.CalculateReplicaCountStats();
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats.Mirror3DiskMinus0);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats.Mirror3DiskMinus1);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats.Mirror3DiskMinus2);
        UNIT_ASSERT_VALUES_EQUAL(1, replicaCountStats.Mirror3DiskMinus3);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats.Mirror2DiskMinus0);
        UNIT_ASSERT_VALUES_EQUAL(1, replicaCountStats.Mirror2DiskMinus1);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats.Mirror2DiskMinus2);

        // Remove replacement state from mirror-2 device d2-2-1
        t.MarkReplacementDevice("disk-2", "d2-2-1", false);

        UNIT_ASSERT_VALUES_EQUAL(0, t.GetDevicesReplacementsCount("disk-2"));

        replicaCountStats = t.CalculateReplicaCountStats();
        UNIT_ASSERT_VALUES_EQUAL(1, replicaCountStats.Mirror2DiskMinus0);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats.Mirror2DiskMinus1);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats.Mirror2DiskMinus2);

        // Transferring into error state device for mirror-2 disk in the
        // first cell.
        SetDeviceState(
            agentConfig,
            "d2-1-1",
            NProto::EDeviceState::DEVICE_STATE_ERROR);
        deviceList.UpdateDevices(agentConfig, poolConfigs);

        replicaCountStats = t.CalculateReplicaCountStats();
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats.Mirror2DiskMinus0);
        UNIT_ASSERT_VALUES_EQUAL(1, replicaCountStats.Mirror2DiskMinus1);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats.Mirror2DiskMinus2);

        // Set replacement state to mirror-2 device d2-2-1.
        // N.B. second device "d2-1-1" in this cell is in an ERROR state.
        t.MarkReplacementDevice("disk-2", "d2-2-1", true);
        replicaCountStats = t.CalculateReplicaCountStats();
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats.Mirror2DiskMinus0);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats.Mirror2DiskMinus1);
        UNIT_ASSERT_VALUES_EQUAL(1, replicaCountStats.Mirror2DiskMinus2);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
