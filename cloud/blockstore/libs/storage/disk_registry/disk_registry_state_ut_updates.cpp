#include "disk_registry_state.h"

#include "disk_registry_database.h"

#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_state.h>
#include <cloud/blockstore/libs/storage/testlib/test_executor.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/guid.h>
#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NDiskRegistryStateTest;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TChangeNonreplDiskDevice
    : public NUnitTest::TBaseFixture
{
    TTestExecutor Executor;
    TVector<NProto::TAgentConfig> Agents;
    std::unique_ptr<TDiskRegistryState> State;

    void SetUp(NUnitTest::TTestContext&) override
    {
        Executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });
        const ui32 nativeBlockSize = 512;
        const ui32 adjustedBlockCount = 93_GB / nativeBlockSize;
        const ui32 unAdjustedBlockCount = adjustedBlockCount + 100;

        auto deviceConfig = [&] (auto name, auto uuid) {
            NProto::TDeviceConfig config;

            config.SetDeviceName(name);
            config.SetDeviceUUID(uuid);
            config.SetBlockSize(nativeBlockSize);
            config.SetBlocksCount(adjustedBlockCount);
            config.SetUnadjustedBlockCount(unAdjustedBlockCount);

            return config;
        };

        Agents = {
            AgentConfig(1, {
                deviceConfig("dev-1", "uuid-1.1"),
                deviceConfig("dev-2", "uuid-1.2"),
                deviceConfig("dev-3", "uuid-1.3"),
            })};

        State = TDiskRegistryStateBuilder()
                    .WithKnownAgents(Agents)
                    .WithDisks({Disk("disk-1", {"uuid-1.1", "uuid-1.2"})})
                    .Build();
    }
};

struct TChangeMirrorDiskDevice
    : public NUnitTest::TBaseFixture
{
    TTestExecutor Executor;
    TVector<NProto::TAgentConfig> Agents;
    std::unique_ptr<TDiskRegistryState> State;

    void SetUp(NUnitTest::TTestContext&) override
    {
        Executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const ui32 nativeBlockSize = 512;
        const ui32 adjustedBlockCount = 93_GB / nativeBlockSize;
        const ui32 unAdjustedBlockCount = adjustedBlockCount + 100;

        auto deviceConfig = [&] (auto name, auto uuid) {
            NProto::TDeviceConfig config;

            config.SetDeviceName(name);
            config.SetDeviceUUID(uuid);
            config.SetBlockSize(nativeBlockSize);
            config.SetBlocksCount(adjustedBlockCount);
            config.SetUnadjustedBlockCount(unAdjustedBlockCount);

            return config;
        };

        Agents = {
            AgentConfig(1, {
                deviceConfig("dev-1", "uuid-1.1"),
                deviceConfig("dev-2", "uuid-1.2"),
                deviceConfig("dev-3", "uuid-1.3"),
            }),
            AgentConfig(2, {
                deviceConfig("dev-1", "uuid-2.1"),
                deviceConfig("dev-2", "uuid-2.2"),
                deviceConfig("dev-3", "uuid-2.3"),
            })};

        State = TDiskRegistryStateBuilder()
                    .WithKnownAgents(Agents)
                    .WithDisks({MirrorDisk(
                        "disk-1",
                        {{"uuid-1.1", "uuid-1.2"}, {"uuid-2.1", "uuid-2.2"}})})
                    .Build();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryStateUpdatesTest)
{
    Y_UNIT_TEST(ShouldUpdateDiskBlockSize)
    {
        const TVector agents {
            AgentConfig(1, {
                Device("dev-1", "uuid-1.1", "", DefaultBlockSize, 10_GB),
            }),
        };

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto diskId = "disk-1";

        auto statePtr = TDiskRegistryStateBuilder()
                            .WithKnownAgents(agents)
                            .WithDisks({Disk(diskId, {"uuid-1.1"})})
                            .Build();
        TDiskRegistryState& state = *statePtr;

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto error = state.UpdateDiskBlockSize(Now(), db, "", 1_KB, false);
            UNIT_ASSERT_VALUES_EQUAL_C(E_ARGUMENT, error.GetCode(),
                "should reject a request with empty disk id");

            error = state.UpdateDiskBlockSize(Now(), db, "nonexistent", 1_KB, false);
            UNIT_ASSERT_VALUES_EQUAL_C(E_NOT_FOUND, error.GetCode(),
                "should reject a request with nonexistent disk id");

            error = state.UpdateDiskBlockSize(Now(), db, diskId, 1_KB, false);
            UNIT_ASSERT_VALUES_EQUAL_C(E_ARGUMENT, error.GetCode(),
                "should reject a request with too small block size");

            error = state.UpdateDiskBlockSize(Now(), db, diskId, 256_KB, false);
            UNIT_ASSERT_VALUES_EQUAL_C(E_ARGUMENT, error.GetCode(),
                "should reject a request with too big block size");

            error = state.UpdateDiskBlockSize(Now(), db, diskId, 64_KB, false);
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(),
                "should complete a request with good block size");

            error = state.UpdateDiskBlockSize(Now(), db, diskId, 64_KB, false);
            UNIT_ASSERT_VALUES_EQUAL_C(S_FALSE, error.GetCode(),
                "should complete a request with S_FALSE to reset the "
                "exact same block size as it is right now");

            error = state.UpdateDiskBlockSize(Now(), db, diskId, 1_KB, true);
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(),
                "should complete a request with too small block size "
                "if force flag is given");
        });
    }

    Y_UNIT_TEST(ShouldUpdateDiskBlockSizeWhenDeviceResizesBadly)
    {
        const auto deviceId = "uuid-1.1";
        const auto diskId = "disk-1";

        const TVector agents {
            AgentConfig(1, {
                Device("dev-1", deviceId, "", DefaultBlockSize, 10000000000),
            })
        };

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto statePtr = TDiskRegistryStateBuilder()
                            .WithKnownAgents(agents)
                            .WithDisks({Disk(diskId, {deviceId})})
                            .Build();
        TDiskRegistryState& state = *statePtr;

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto error = state.UpdateDiskBlockSize(Now(), db, diskId, 128_KB, false);
            UNIT_ASSERT_VALUES_EQUAL_C(E_ARGUMENT, error.GetCode(),
                "should reject a request where device will be badly resized");

            const auto blocksCountBeforeUpdate = state.GetDevice(deviceId)
                .GetBlocksCount();

            error = state.UpdateDiskBlockSize(Now(), db, diskId, 128_KB, true);
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(),
                "should complete a request where device will be badly resized "
                "with force");
            UNIT_ASSERT_VALUES_UNEQUAL_C(blocksCountBeforeUpdate,
                state.GetDevice(deviceId).GetBlocksCount(),
                "should update blocks count for badly resized disk"
            );
        });
    }
}

Y_UNIT_TEST_SUITE(TDiskRegistryChangeNonreplDiskTest)
{
    Y_UNIT_TEST_F(ShouldFailByDiskId, TChangeNonreplDiskDevice)
    {
        Executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto error = State->ChangeDiskDevice(
                TInstant::FromValue(1000),
                db,
                "disk-2",
                "uuid-1.1",
                "uuid-1.3");

            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT, error.GetCode(), error.GetMessage());
        });
    }

    Y_UNIT_TEST_F(ShouldFailBySourceDeviceId, TChangeNonreplDiskDevice)
    {
        Executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto error = State->ChangeDiskDevice(
                TInstant::FromValue(1000),
                db,
                "disk-2",
                "uuid-1.4",
                "uuid-1.3");

            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT, error.GetCode(), error.GetMessage());
        });
    }

    Y_UNIT_TEST_F(ShouldFailByTargetDeviceId, TChangeNonreplDiskDevice)
    {
        Executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto error = State->ChangeDiskDevice(
                TInstant::FromValue(1000),
                db,
                "disk-2",
                "uuid-1.1",
                "uuid-1.4");

            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT, error.GetCode(), error.GetMessage());
        });
    }

    Y_UNIT_TEST_F(ShouldSuccess, TChangeNonreplDiskDevice)
    {
        Executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto error = State->ChangeDiskDevice(
                TInstant::FromValue(1000),
                db,
                "disk-1",
                "uuid-1.1",
                "uuid-1.3");

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                error.GetMessage());

            TDiskInfo info;
            UNIT_ASSERT_SUCCESS(State->GetDiskInfo("disk-1", info));

            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ERROR, info.State);
            UNIT_ASSERT_VALUES_EQUAL(2, info.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-1.3",
                info.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-1.2",
                info.Devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1000, info.StateTs.GetValue());
        });
    }
}

Y_UNIT_TEST_SUITE(TDiskRegistryChangeMirrorDiskTest)
{
    Y_UNIT_TEST_F(ShouldFailByDiskId, TChangeMirrorDiskDevice)
    {
        Executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto error = State->ChangeDiskDevice(
                TInstant::FromValue(1000),
                db,
                "disk-2",
                "uuid-1.1",
                "uuid-1.3");

            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT, error.GetCode(), error.GetMessage());
        });
    }

    Y_UNIT_TEST_F(ShouldFailBySourceDeviceId, TChangeMirrorDiskDevice)
    {
        Executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto error = State->ChangeDiskDevice(
                TInstant::FromValue(1000),
                db,
                "disk-2",
                "uuid-1.4",
                "uuid-1.3");

            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT, error.GetCode(), error.GetMessage());
        });
    }

    Y_UNIT_TEST_F(ShouldFailByTargetDeviceId, TChangeMirrorDiskDevice)
    {
        Executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto error = State->ChangeDiskDevice(
                TInstant::FromValue(1000),
                db,
                "disk-2",
                "uuid-1.1",
                "uuid-1.4");

            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT, error.GetCode(), error.GetMessage());
        });
    }

    Y_UNIT_TEST_F(ShouldSuccess, TChangeMirrorDiskDevice)
    {
        Executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto error = State->ChangeDiskDevice(
                TInstant::FromValue(1000),
                db,
                "disk-1/0",
                "uuid-1.1",
                "uuid-1.3");

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                error.GetMessage());

            TDiskInfo info;
            UNIT_ASSERT_SUCCESS(State->GetDiskInfo("disk-1/0", info));

            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ERROR, info.State);
            UNIT_ASSERT_VALUES_EQUAL(2, info.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-1.3",
                info.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-1.2",
                info.Devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1000, info.StateTs.GetValue());
        });

        Executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto error = State->ChangeDiskDevice(
                TInstant::FromValue(1000),
                db,
                "disk-1/1",
                "uuid-2.1",
                "uuid-2.3");

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                error.GetMessage());

            TDiskInfo info;
            UNIT_ASSERT_SUCCESS(State->GetDiskInfo("disk-1/1", info));

            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ERROR, info.State);
            UNIT_ASSERT_VALUES_EQUAL(2, info.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-2.3",
                info.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-2.2",
                info.Devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1000, info.StateTs.GetValue());
        });
    }
}

}   // namespace NCloud::NBlockStore::NStorage
