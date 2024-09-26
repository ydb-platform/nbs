#include "disk_registry_database.h"

#include <cloud/blockstore/libs/storage/testlib/test_executor.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TByDiskId
{
    template <typename T>
    bool operator () (const T& lhs, const T& rhs) const
    {
        return lhs.GetDiskId() < rhs.GetDiskId();
    }
};

struct TByNodeId
{
    template <typename T>
    bool operator () (const T& lhs, const T& rhs) const
    {
        return lhs.GetNodeId() < rhs.GetNodeId();
    }
};

struct TByAgentId
{
    template <typename T>
    bool operator () (const T& lhs, const T& rhs) const
    {
        return lhs.GetAgentId() < rhs.GetAgentId();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryDatabaseTest)
{
    Y_UNIT_TEST(ShouldStoreAgents)
    {
        TTestExecutor executor;

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            NProto::TAgentConfig config;
            config.SetNodeId(42);
            auto& device = *config.AddDevices();
            device.SetDeviceName("dev-1");

            db.UpdateAgent(config);
        });

        executor.ReadTx([&] (TDiskRegistryDatabase db) {
            TVector<NProto::TAgentConfig> items;
            db.ReadAgents(items);

            UNIT_ASSERT_VALUES_EQUAL(items.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(items[0].GetNodeId(), 42);
            UNIT_ASSERT_VALUES_EQUAL(items[0].DevicesSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(items[0].GetDevices(0).GetDeviceName(),
                "dev-1");
        });
    }

    Y_UNIT_TEST(ShouldStoreDisks)
    {
        TTestExecutor executor;

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            NProto::TDiskConfig config;
            config.SetDiskId("disk-1");

            *config.AddDeviceUUIDs() = "dev-uuid-1";
            *config.AddDeviceUUIDs() = "dev-uuid-2";

            db.UpdateDisk(config);
        });

        executor.ReadTx([&] (TDiskRegistryDatabase db) {
            TVector<NProto::TDiskConfig> items;
            db.ReadDisks(items);

            UNIT_ASSERT_VALUES_EQUAL(1, items.size());

            const auto& disk = items[0];

            UNIT_ASSERT_VALUES_EQUAL("disk-1", disk.GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL(2, disk.DeviceUUIDsSize());
            UNIT_ASSERT_VALUES_EQUAL("dev-uuid-1", disk.GetDeviceUUIDs(0));
            UNIT_ASSERT_VALUES_EQUAL("dev-uuid-2", disk.GetDeviceUUIDs(1));
            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ONLINE, disk.GetState());
        });
    }

    Y_UNIT_TEST(ShouldStoreKnownAgents)
    {
        TTestExecutor executor;

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            NProto::TDiskRegistryConfig config;

            auto& agent = *config.AddKnownAgents();
            agent.SetAgentId("host-123");
            agent.AddDevices()->SetDeviceUUID("id-1");

            db.WriteDiskRegistryConfig(config);
        });

        executor.ReadTx([&] (TDiskRegistryDatabase db) {
            NProto::TDiskRegistryConfig config;
            db.ReadDiskRegistryConfig(config);

            TVector<NProto::TAgentConfig> items(
                config.GetKnownAgents().begin(),
                config.GetKnownAgents().end());

            UNIT_ASSERT_VALUES_EQUAL(items.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(items[0].GetAgentId(), "host-123");
            UNIT_ASSERT_VALUES_EQUAL(items[0].DevicesSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(items[0].GetDevices(0).GetDeviceUUID(), "id-1");
        });
    }

    Y_UNIT_TEST(ShouldStorePlacementGroups)
    {
        TTestExecutor executor;

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            NProto::TPlacementGroupConfig config;
            config.SetGroupId("group-1");
            auto& disk = *config.AddDisks();
            *disk.AddDeviceRacks() = "rack-1";
            *disk.AddDeviceRacks() = "rack-2";
            disk.SetDiskId("disk-1");

            db.UpdatePlacementGroup(config);
        });

        executor.ReadTx([&] (TDiskRegistryDatabase db) {
            TVector<NProto::TPlacementGroupConfig> items;
            db.ReadPlacementGroups(items);

            UNIT_ASSERT_VALUES_EQUAL(items.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(items[0].GetGroupId(), "group-1");
            UNIT_ASSERT_VALUES_EQUAL(items[0].DisksSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(items[0].GetDisks(0).DeviceRacksSize(), 2);
            UNIT_ASSERT_VALUES_EQUAL(items[0].GetDisks(0).GetDeviceRacks(0), "rack-1");
            UNIT_ASSERT_VALUES_EQUAL(items[0].GetDisks(0).GetDeviceRacks(1), "rack-2");
            UNIT_ASSERT_VALUES_EQUAL(items[0].GetDisks(0).GetDiskId(), "disk-1");
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.DeletePlacementGroup("group-1");
        });

        executor.ReadTx([&] (TDiskRegistryDatabase db) {
            TVector<NProto::TPlacementGroupConfig> items;
            db.ReadPlacementGroups(items);

            UNIT_ASSERT_VALUES_EQUAL(items.size(), 0);
        });
    }

    Y_UNIT_TEST(ShouldDeleteAgent)
    {
        TTestExecutor executor;

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        for (int i = 0; i != 3; ++i) {
            executor.WriteTx([&] (TDiskRegistryDatabase db) {
                NProto::TAgentConfig config;
                config.SetNodeId(i + 1);
                config.SetAgentId(ToString(i + 1));

                db.UpdateAgent(config);
            });
        }

        executor.ReadTx([&] (TDiskRegistryDatabase db) {
            TVector<NProto::TAgentConfig> items;
            db.ReadAgents(items);

            UNIT_ASSERT_VALUES_EQUAL(items.size(), 3);

            Sort(items, TByNodeId());

            for (int i = 0; i != 3; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(items[i].GetNodeId(), i + 1);
            }
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.DeleteAgent("2");
        });

        executor.ReadTx([&] (TDiskRegistryDatabase db) {
            TVector<NProto::TAgentConfig> items;
            db.ReadAgents(items);

            UNIT_ASSERT_VALUES_EQUAL(items.size(), 2);

            Sort(items, TByNodeId());

            UNIT_ASSERT_VALUES_EQUAL(1, items[0].GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL("1", items[0].GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(3, items[1].GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL("3", items[1].GetAgentId());
        });
    }

    Y_UNIT_TEST(ShouldDeleteDisk)
    {
        TTestExecutor executor;

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        for (int i = 0; i != 3; ++i) {
            executor.WriteTx([&] (TDiskRegistryDatabase db) {
                NProto::TDiskConfig config;
                config.SetDiskId(ToString(i + 1));
                db.UpdateDisk(config);
            });
        }

        executor.ReadTx([&] (TDiskRegistryDatabase db) {
            TVector<NProto::TDiskConfig> items;
            db.ReadDisks(items);

            UNIT_ASSERT_VALUES_EQUAL(items.size(), 3);

            Sort(items, TByDiskId());

            for (int i = 0; i != 3; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(items[i].GetDiskId(), ToString(i + 1));
            }
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.DeleteDisk("2");
        });

        executor.ReadTx([&] (TDiskRegistryDatabase db) {
            TVector<NProto::TDiskConfig> items;
            db.ReadDisks(items);

            UNIT_ASSERT_VALUES_EQUAL(items.size(), 2);

            Sort(items, TByDiskId());

            UNIT_ASSERT_VALUES_EQUAL(items[0].GetDiskId(), "1");
            UNIT_ASSERT_VALUES_EQUAL(items[1].GetDiskId(), "3");
        });
    }

    Y_UNIT_TEST(ShouldUpdateKnownAgents)
    {
        TTestExecutor executor;

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            NProto::TDiskRegistryConfig config;
            for (int i = 0; i != 3; ++i) {
                auto& agent = *config.AddKnownAgents();
                agent.SetAgentId(ToString(i + 1));
                agent.AddDevices()->SetDeviceUUID("id-1");
            }

            db.WriteDiskRegistryConfig(config);
        });

        executor.ReadTx([&] (TDiskRegistryDatabase db) {
            NProto::TDiskRegistryConfig config;
            db.ReadDiskRegistryConfig(config);

            TVector<NProto::TAgentConfig> items(
                config.GetKnownAgents().begin(),
                config.GetKnownAgents().end());

            UNIT_ASSERT_VALUES_EQUAL(items.size(), 3);

            Sort(items, TByAgentId());

            for (int i = 0; i != 3; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(items[i].GetAgentId(), ToString(i + 1));
            }
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            NProto::TDiskRegistryConfig config;
            for (int i = 0; i != 3; ++i) {
                if (i == 1) {
                    continue;
                }

                auto& agent = *config.AddKnownAgents();
                agent.SetAgentId(ToString(i + 1));
                agent.AddDevices()->SetDeviceUUID("id-1");
            }

            db.WriteDiskRegistryConfig(config);
        });

        executor.ReadTx([&] (TDiskRegistryDatabase db) {
            NProto::TDiskRegistryConfig config;
            db.ReadDiskRegistryConfig(config);

            TVector<NProto::TAgentConfig> items(
                config.GetKnownAgents().begin(),
                config.GetKnownAgents().end());

            UNIT_ASSERT_VALUES_EQUAL(items.size(), 2);

            Sort(items, TByAgentId());

            UNIT_ASSERT_VALUES_EQUAL(items[0].GetAgentId(), "1");
            UNIT_ASSERT_VALUES_EQUAL(items[1].GetAgentId(), "3");
        });
    }

    Y_UNIT_TEST(ShouldStoreDirtyDevices)
    {
        TTestExecutor executor;

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.UpdateDirtyDevice("uuid-1", {});
            db.UpdateDirtyDevice("uuid-2", "foo");
            db.UpdateDirtyDevice("uuid-3", "bar");
        });

        executor.ReadTx([&] (TDiskRegistryDatabase db) {
            TVector<TDirtyDevice> items;
            db.ReadDirtyDevices(items);
            SortBy(items, [] (const auto& x) { return x.Id; });

            UNIT_ASSERT_VALUES_EQUAL(items.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", items[0].Id);
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", items[1].Id);
            UNIT_ASSERT_VALUES_EQUAL("foo", items[1].DiskId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-3", items[2].Id);
            UNIT_ASSERT_VALUES_EQUAL("bar", items[2].DiskId);
        });
    }

    Y_UNIT_TEST(ShouldDeleteDirtyDevices)
    {
        TTestExecutor executor;

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.UpdateDirtyDevice("uuid-1", {});
            db.UpdateDirtyDevice("uuid-2", "foo");
            db.UpdateDirtyDevice("uuid-3", "bar");
        });

        executor.ReadTx([&] (TDiskRegistryDatabase db) {
            TVector<TDirtyDevice> items;
            db.ReadDirtyDevices(items);
            SortBy(items, [] (const auto& x) { return x.Id; });

            UNIT_ASSERT_VALUES_EQUAL(items.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", items[0].Id);
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", items[1].Id);
            UNIT_ASSERT_VALUES_EQUAL("foo", items[1].DiskId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-3", items[2].Id);
            UNIT_ASSERT_VALUES_EQUAL("bar", items[2].DiskId);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.DeleteDirtyDevice("uuid-2");
        });

        executor.ReadTx([&] (TDiskRegistryDatabase db) {
            TVector<TDirtyDevice> items;
            db.ReadDirtyDevices(items);
            SortBy(items, [] (const auto& x) { return x.Id; });

            UNIT_ASSERT_VALUES_EQUAL(items.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", items[0].Id);
            UNIT_ASSERT_VALUES_EQUAL("", items[0].DiskId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-3", items[1].Id);
            UNIT_ASSERT_VALUES_EQUAL("bar", items[1].DiskId);
        });
    }

    Y_UNIT_TEST(ShouldStoreDiskState)
    {
        TTestExecutor executor;

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const ui64 SeqNoOffset = 10;

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            db.WriteLastDiskStateSeqNo(SeqNoOffset);
        });

        ui64 seqNo = 0;

        executor.ReadTx([&] (TDiskRegistryDatabase db) mutable {
            db.ReadLastDiskStateSeqNo(seqNo);
        });

        for (int i = 0; i != 3; ++i) {
            executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
                NProto::TDiskState state;

                state.SetDiskId("disk-" + ToString(i));
                state.SetState(NProto::DISK_STATE_WARNING);
                state.SetStateMessage("msg-" + ToString(i));
                db.UpdateDiskState(state, seqNo++);
                db.WriteLastDiskStateSeqNo(seqNo);
            });
        }

        executor.ReadTx([&] (TDiskRegistryDatabase db) {
            TVector<TDiskStateUpdate> changes;
            db.ReadDiskStateChanges(changes);

            UNIT_ASSERT_VALUES_EQUAL(3, changes.size());
            for (int i = 0; i != 3; ++i) {
                auto& [state, seqNo] = changes[i];
                UNIT_ASSERT_VALUES_EQUAL(SeqNoOffset + i, seqNo);
                UNIT_ASSERT_VALUES_EQUAL("disk-" + ToString(i), state.GetDiskId());
                UNIT_ASSERT_VALUES_EQUAL("msg-" + ToString(i), state.GetStateMessage());
                UNIT_ASSERT_EQUAL(
                    NProto::DISK_STATE_WARNING,
                    state.GetState());
            }
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.DeleteDiskStateChanges("disk-1", SeqNoOffset + 1);
        });

        executor.ReadTx([&] (TDiskRegistryDatabase db) {
            TVector<TDiskStateUpdate> changes;
            db.ReadDiskStateChanges(changes);

            UNIT_ASSERT_VALUES_EQUAL(2, changes.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-0", changes[0].State.GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL("disk-2", changes[1].State.GetDiskId());
        });
    }

    Y_UNIT_TEST(ShouldStoreBrokenDisks)
    {
        TTestExecutor executor;

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            db.AddBrokenDisk({"disk-1", TInstant::Seconds(100)});
            db.AddBrokenDisk({"disk-2", TInstant::Seconds(200)});
            db.AddBrokenDisk({"disk-0", TInstant::Seconds(300)});
        });

        executor.ReadTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TBrokenDiskInfo> diskInfos;
            UNIT_ASSERT(db.ReadBrokenDisks(diskInfos));
            UNIT_ASSERT_VALUES_EQUAL(3, diskInfos.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", diskInfos[0].DiskId);
            UNIT_ASSERT_VALUES_EQUAL(
                TInstant::Seconds(100),
                diskInfos[0].TsToDestroy
            );
            UNIT_ASSERT_VALUES_EQUAL("disk-2", diskInfos[1].DiskId);
            UNIT_ASSERT_VALUES_EQUAL(
                TInstant::Seconds(200),
                diskInfos[1].TsToDestroy
            );
            UNIT_ASSERT_VALUES_EQUAL("disk-0", diskInfos[2].DiskId);
            UNIT_ASSERT_VALUES_EQUAL(
                TInstant::Seconds(300),
                diskInfos[2].TsToDestroy
            );
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            db.DeleteBrokenDisk("disk-1");
            db.DeleteBrokenDisk("disk-0");
        });

        executor.ReadTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TBrokenDiskInfo> diskInfos;
            UNIT_ASSERT(db.ReadBrokenDisks(diskInfos));
            UNIT_ASSERT_VALUES_EQUAL(1, diskInfos.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-2", diskInfos[0].DiskId);
            UNIT_ASSERT_VALUES_EQUAL(
                TInstant::Seconds(200),
                diskInfos[0].TsToDestroy
            );
        });
    }

    Y_UNIT_TEST(ShouldStoreDisksToNotify)
    {
        TTestExecutor executor;

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            db.AddDiskToReallocate("disk-1");
            db.AddDiskToReallocate("disk-2");
            db.AddDiskToReallocate("disk-3");
        });

        executor.ReadTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<NProto::TDiskNotification> notifications;
            UNIT_ASSERT(db.ReadDisksToNotify(notifications));
            UNIT_ASSERT_VALUES_EQUAL(3, notifications.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", notifications[0].GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL("disk-2", notifications[1].GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL("disk-3", notifications[2].GetDiskId());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            db.DeleteDiskToReallocate("disk-1");
            db.DeleteDiskToReallocate("disk-3");
        });

        executor.ReadTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<NProto::TDiskNotification> notifications;
            UNIT_ASSERT(db.ReadDisksToNotify(notifications));
            UNIT_ASSERT_VALUES_EQUAL(1, notifications.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-2", notifications[0].GetDiskId());
        });
    }

    Y_UNIT_TEST(ShouldStoreAutomaticallyReplacedDevices)
    {
        TTestExecutor executor;

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            db.AddAutomaticallyReplacedDevice(
                {"device-1", TInstant::Seconds(100)});
            db.AddAutomaticallyReplacedDevice(
                {"device-2", TInstant::Seconds(200)});
            db.AddAutomaticallyReplacedDevice(
                {"device-0", TInstant::Seconds(300)});
        });

        executor.ReadTx([&] (TDiskRegistryDatabase db) mutable {
            TDeque<TAutomaticallyReplacedDeviceInfo> deviceInfos;
            UNIT_ASSERT(db.ReadAutomaticallyReplacedDevices(deviceInfos));
            UNIT_ASSERT_VALUES_EQUAL(3, deviceInfos.size());
            UNIT_ASSERT_VALUES_EQUAL("device-1", deviceInfos[0].DeviceId);
            UNIT_ASSERT_VALUES_EQUAL(
                TInstant::Seconds(100),
                deviceInfos[0].ReplacementTs
            );
            UNIT_ASSERT_VALUES_EQUAL("device-2", deviceInfos[1].DeviceId);
            UNIT_ASSERT_VALUES_EQUAL(
                TInstant::Seconds(200),
                deviceInfos[1].ReplacementTs
            );
            UNIT_ASSERT_VALUES_EQUAL("device-0", deviceInfos[2].DeviceId);
            UNIT_ASSERT_VALUES_EQUAL(
                TInstant::Seconds(300),
                deviceInfos[2].ReplacementTs
            );
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            db.DeleteAutomaticallyReplacedDevice("device-1");
            db.DeleteAutomaticallyReplacedDevice("device-0");
        });

        executor.ReadTx([&] (TDiskRegistryDatabase db) mutable {
            TDeque<TAutomaticallyReplacedDeviceInfo> deviceInfos;
            UNIT_ASSERT(db.ReadAutomaticallyReplacedDevices(deviceInfos));
            UNIT_ASSERT_VALUES_EQUAL(1, deviceInfos.size());
            UNIT_ASSERT_VALUES_EQUAL("device-2", deviceInfos[0].DeviceId);
            UNIT_ASSERT_VALUES_EQUAL(
                TInstant::Seconds(200),
                deviceInfos[0].ReplacementTs
            );
        });
    }
}

}   // namespace NCloud::NBlockStore::NStorage
