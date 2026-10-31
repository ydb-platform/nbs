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

TDiskRegistryState::TCreateDiskFromDevicesParams MakeMirror2Params(
    bool force = true,
    bool keepBlockCount = true)
{
    return TDiskRegistryState::TCreateDiskFromDevicesParams(
        DefaultLogicalBlockSize,
        NProto::STORAGE_MEDIA_SSD_MIRROR2,
        force,
        keepBlockCount);
}

TVector<TVector<TString>> MakeMirror2ReplicaUuids()
{
    return {
        {"uuid-1", "uuid-2"},
        {"uuid-3", "uuid-4"},
    };
}

std::unique_ptr<TDiskRegistryState> MakeDefaultState()
{
    return TDiskRegistryStateBuilder()
        .WithKnownAgents({
            AgentConfig(
                1,
                {
                    Device("dev-1", "uuid-1", "rack-1"),
                    Device("dev-2", "uuid-2", "rack-1"),
                }),
            AgentConfig(
                2,
                {
                    Device("dev-1", "uuid-3", "rack-2"),
                    Device("dev-2", "uuid-4", "rack-2"),
                }),
        })
        .Build();
}

NProto::TError CreateMirroredDiskFromDevices(
    TDiskRegistryState& state,
    TDiskRegistryDatabase& db,
    const TString& diskId,
    const TDiskRegistryState::TCreateDiskFromDevicesParams& params,
    const TVector<TVector<TString>>& replicas)
{
    TDiskRegistryState::TAllocateDiskResult result;
    return state.CreateDiskFromDevices(
        Now(),
        db,
        diskId,
        params,
        {},
        replicas,
        &result);
}

void AssertDevicesNotAttachedToDiskIfCreateFailed(
    const TDiskRegistryState& state,
    const TString& diskId,
    const TVector<TString>& deviceUuids)
{
    const TString prefix = diskId + "/";
    for (const auto& uuid : deviceUuids) {
        if (uuid.empty()) {
            continue;
        }
        const auto owner = state.FindDisk(uuid);
        UNIT_ASSERT(owner.empty() || owner != diskId);
    }
}

void AssertSimpleCreateFromDevicesFailedClean(
    const TDiskRegistryState& state,
    const TString& diskId,
    const TVector<TString>& deviceUuids)
{
    UNIT_ASSERT(!state.FindDisk(diskId));
    AssertDevicesNotAttachedToDiskIfCreateFailed(state, diskId, deviceUuids);
}

void AssertMirrorCreateFromDevicesFailedClean(
    const TDiskRegistryState& state,
    const TString& masterId,
    const TVector<TVector<TString>>& replicas)
{
    UNIT_ASSERT(!state.FindDisk(masterId));
    UNIT_ASSERT(!state.FindPlacementGroup(masterId + "/g"));
    for (ui32 i = 0; i < replicas.size(); ++i) {
        UNIT_ASSERT(!state.FindDisk(masterId + "/" + ToString(i)));
    }
    for (const auto& replica : replicas) {
        AssertDevicesNotAttachedToDiskIfCreateFailed(state, masterId, replica);
    }
}

void AssertNoMirror2DiskArtifactsForMaster(
    const TDiskRegistryState& state,
    const TString& masterId)
{
    UNIT_ASSERT(!state.FindPlacementGroup(masterId + "/g"));
    UNIT_ASSERT(!state.FindDisk(masterId + "/0"));
    UNIT_ASSERT(!state.FindDisk(masterId + "/1"));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryStateCreateTest)
{
    Y_UNIT_TEST(ShouldCreateDiskFromDevices)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const ui32 nativeBlockSize = 512;
        const ui64 adjustedBlockCount = 10_GB / nativeBlockSize;
        const ui64 unAdjustedBlockCount = adjustedBlockCount + 100;

        auto deviceConfig = [&] (auto name, auto uuid, bool local) {
            NProto::TDeviceConfig config;

            config.SetDeviceName(name);
            config.SetDeviceUUID(uuid);
            config.SetBlockSize(nativeBlockSize);
            config.SetBlocksCount(adjustedBlockCount);
            config.SetUnadjustedBlockCount(unAdjustedBlockCount);
            if (local) {
                config.SetPoolKind(NProto::DEVICE_POOL_KIND_LOCAL);
                config.SetPoolName("local-ssd");
            }

            return config;
        };

        TVector agents {
            AgentConfig(1, {
                deviceConfig("dev-1", "uuid-1.1", false),    // disk-1
                deviceConfig("dev-2", "uuid-1.2", false),
                deviceConfig("dev-3", "uuid-1.3", false),
                deviceConfig("dev-4", "uuid-1.4", true),
                deviceConfig("dev-5", "uuid-1.5", true),
            }),
            AgentConfig(2, {
                deviceConfig("dev-1", "uuid-2.1", false),    // dirty
                deviceConfig("dev-2", "uuid-2.2", false),
                deviceConfig("dev-3", "uuid-2.3", false),
                deviceConfig("dev-4", "uuid-2.4", false),
                deviceConfig("dev-5", "uuid-2.5", true),
            })
        };

        auto statePtr =
            TDiskRegistryStateBuilder()
                .WithAgents(agents)
                .WithConfig(
                    [&]
                    {
                        auto config = MakeConfig(agents);
                        auto* pool = config.AddDevicePoolConfigs();
                        pool->SetName("local-ssd");
                        pool->SetKind(NProto::DEVICE_POOL_KIND_LOCAL);
                        pool->SetAllocationUnit(
                            adjustedBlockCount * nativeBlockSize);

                        return config;
                    }())
                .WithDisks({Disk("disk-1", {"uuid-1.1"})})
                .WithDirtyDevices({TDirtyDevice{"uuid-2.1", {}}})
                .Build();
        TDiskRegistryState& state = *statePtr;

        auto deviceByName = [] (auto agentId, auto name) {
            NProto::TDeviceConfig config;
            config.SetAgentId(agentId);
            config.SetDeviceName(name);
            return config;
        };

        auto deviceByUUID = [] (auto uuid) {
            NProto::TDeviceConfig config;
            config.SetDeviceUUID(uuid);
            return config;
        };

        // unknown device
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result {};
            auto error = state.CreateDiskFromDevices(
                Now(),
                db,
                "foo",
                TDiskRegistryState::TCreateDiskFromDevicesParams(
                    4_KB,
                    NProto::STORAGE_MEDIA_SSD_LOCAL,
                    /*force=*/false,
                    /*keepBlockCount=*/false),
                {
                    deviceByName("agent-2", "dev-3"),
                    deviceByName("agent-3", "foo-2"),
                },
                {},
                &result);

            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, error.GetCode());
            UNIT_ASSERT(error.GetMessage().Contains("not found"));

            AssertSimpleCreateFromDevicesFailedClean(state, "foo", {});
            UNIT_ASSERT(state.FindDisk("uuid-2.3").empty());
            UNIT_ASSERT(state.FindDisk("foo-2").empty());
        });

        // allocated device
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result {};
            auto error = state.CreateDiskFromDevices(
                Now(),
                db,
                "foo",
                TDiskRegistryState::TCreateDiskFromDevicesParams(
                    nativeBlockSize,
                    NProto::STORAGE_MEDIA_SSD_LOCAL,
                    /*force=*/true,
                    /*keepBlockCount=*/false),
                {deviceByName("agent-1", "dev-1")},
                {},
                &result);

            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
            UNIT_ASSERT(error.GetMessage().Contains("is allocated for"));

            AssertSimpleCreateFromDevicesFailedClean(state, "foo", {"uuid-1.1"});
        });

        // dirty device (!force)
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result {};
            auto error = state.CreateDiskFromDevices(
                Now(),
                db,
                "foo",
                TDiskRegistryState::TCreateDiskFromDevicesParams(
                    nativeBlockSize,
                    NProto::STORAGE_MEDIA_SSD_LOCAL,
                    /*force=*/false,
                    /*keepBlockCount=*/false),
                {deviceByName("agent-2", "dev-1")},
                {},
                &result);

            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
            UNIT_ASSERT(error.GetMessage().Contains("is dirty"));

            AssertSimpleCreateFromDevicesFailedClean(state, "foo", {"uuid-2.1"});
        });

        // disk already exists
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result {};
            auto error = state.CreateDiskFromDevices(
                Now(),
                db,
                "disk-1",
                TDiskRegistryState::TCreateDiskFromDevicesParams(
                    nativeBlockSize,
                    NProto::STORAGE_MEDIA_SSD_LOCAL,
                    /*force=*/true,
                    /*keepBlockCount=*/false),
                {deviceByName("agent-2", "dev-2")},
                {},
                &result);

            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
            UNIT_ASSERT(error.GetMessage().Contains("already exists"));

            TDiskInfo info;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1", info));
            UNIT_ASSERT_VALUES_EQUAL(1, info.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", info.Devices[0].GetDeviceUUID());
            AssertDevicesNotAttachedToDiskIfCreateFailed(state, "disk-1", {"uuid-2.2"});
        });

        // dirty device (force)
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result {};
            auto error = state.CreateDiskFromDevices(
                TInstant::FromValue(1000),
                db,
                "bar",
                TDiskRegistryState::TCreateDiskFromDevicesParams(
                    nativeBlockSize,
                    NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                    /*force=*/true,
                    /*keepBlockCount=*/false),
                {deviceByName("agent-2", "dev-1")},
                {},
                &result);

            UNIT_ASSERT_VALUES_EQUAL(1, result.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", result.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(unAdjustedBlockCount, result.Devices[0].GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetDirtyDevices().size());

            TDiskInfo info;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("bar", info));

            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ONLINE, info.State);
            UNIT_ASSERT_EQUAL(
                NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                info.MediaKind);
            UNIT_ASSERT_VALUES_EQUAL(1, info.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL(unAdjustedBlockCount, info.Devices[0].GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", info.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(nativeBlockSize, info.LogicalBlockSize);
            UNIT_ASSERT_VALUES_EQUAL(1000, info.StateTs.GetValue());
        });

        // regular
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result {};
            auto error = state.CreateDiskFromDevices(
                TInstant::FromValue(1000),
                db,
                "baz",
                TDiskRegistryState::TCreateDiskFromDevicesParams(
                    nativeBlockSize,
                    NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                    /*force=*/false,
                    /*keepBlockCount=*/false),
                {
                    deviceByName("agent-1", "dev-2"),
                    deviceByName("agent-2", "dev-2")
                },
                {},
                &result);

            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());

            UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.2", result.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.2", result.Devices[1].GetDeviceUUID());

            TDiskInfo info;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("baz", info));

            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ONLINE, info.State);
            UNIT_ASSERT_EQUAL(
                NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                info.MediaKind);
            UNIT_ASSERT_VALUES_EQUAL(2, info.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.2", info.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.2", info.Devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(nativeBlockSize, info.LogicalBlockSize);
            UNIT_ASSERT_VALUES_EQUAL(1000, info.StateTs.GetValue());
        });

        // regular
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result {};
            auto error = state.CreateDiskFromDevices(
                TInstant::FromValue(1000),
                db,
                "nonrepl",
                TDiskRegistryState::TCreateDiskFromDevicesParams(
                    nativeBlockSize,
                    NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                    /*force=*/false,
                    /*keepBlockCount=*/false),
                {
                    deviceByUUID("uuid-2.3"),
                    deviceByUUID("uuid-1.3")
                },
                {},
                &result);

            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());

            UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.3", result.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.3", result.Devices[1].GetDeviceUUID());

            TDiskInfo info;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("nonrepl", info));

            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ONLINE, info.State);
            UNIT_ASSERT_EQUAL(
                NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                info.MediaKind);
            UNIT_ASSERT_VALUES_EQUAL(2, info.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.3", info.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.3", info.Devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(nativeBlockSize, info.LogicalBlockSize);
            UNIT_ASSERT_VALUES_EQUAL(1000, info.StateTs.GetValue());
        });

        // bs=4K
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result {};
            auto error = state.CreateDiskFromDevices(
                TInstant::FromValue(2000),
                db,
                "nonrepl-4K",
                TDiskRegistryState::TCreateDiskFromDevicesParams(
                    4_KB,
                    NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                    /*force=*/false,
                    /*keepBlockCount=*/false),
                { deviceByUUID("uuid-2.4") },
                {},
                &result);

            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());

            UNIT_ASSERT_VALUES_EQUAL(1, result.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.4", result.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(unAdjustedBlockCount, result.Devices[0].GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(nativeBlockSize, result.Devices[0].GetBlockSize());
            UNIT_ASSERT_VALUES_EQUAL(
                result.Devices[0].GetBlocksCount(),
                result.Devices[0].GetUnadjustedBlockCount());

            TDiskInfo info;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("nonrepl-4K", info));

            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ONLINE, info.State);
            UNIT_ASSERT_EQUAL(
                NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                info.MediaKind);
            UNIT_ASSERT_VALUES_EQUAL(1, info.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.4", info.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                nativeBlockSize,
                info.Devices[0].GetBlockSize());
            UNIT_ASSERT_VALUES_EQUAL(
                unAdjustedBlockCount,
                info.Devices[0].GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                info.Devices[0].GetBlocksCount(),
                info.Devices[0].GetUnadjustedBlockCount());
            UNIT_ASSERT_VALUES_EQUAL(4_KB, info.LogicalBlockSize);
            UNIT_ASSERT_VALUES_EQUAL(2000, info.StateTs.GetValue());
        });

        // local
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result {};
            auto error = state.CreateDiskFromDevices(
                TInstant::FromValue(1000),
                db,
                "local",
                TDiskRegistryState::TCreateDiskFromDevicesParams(
                    nativeBlockSize,
                    NProto::STORAGE_MEDIA_SSD_LOCAL,
                    /*force=*/false,
                    /*keepBlockCount=*/false),
                {
                    deviceByUUID("uuid-1.4"),
                    deviceByUUID("uuid-1.5")
                },
                {},
                &result);

            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());

            UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.4", result.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.5", result.Devices[1].GetDeviceUUID());

            TDiskInfo info;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("local", info));

            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ONLINE, info.State);
            UNIT_ASSERT_EQUAL(NProto::STORAGE_MEDIA_SSD_LOCAL, info.MediaKind);
            UNIT_ASSERT_VALUES_EQUAL(2, info.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.4", info.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.5", info.Devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(nativeBlockSize, info.LogicalBlockSize);
            UNIT_ASSERT_VALUES_EQUAL(1000, info.StateTs.GetValue());
        });
    }

    Y_UNIT_TEST(ShouldCollectMTBF)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        NProto::TAgentConfig agent1;
        agent1.SetAgentId("agent-1");
        agent1.MutableTimeBetweenFailures()->SetWorkTime(40);
        agent1.MutableTimeBetweenFailures()->SetBrokenCount(1);
        NProto::TAgentConfig agent2;
        agent2.SetAgentId("agent-2");
        agent2.MutableTimeBetweenFailures()->SetWorkTime(100);
        agent2.MutableTimeBetweenFailures()->SetBrokenCount(2);
        NProto::TAgentConfig agent3;
        agent3.SetAgentId("agent-3");

        const TVector agents{agent1, agent2, agent3};
        auto monitoring = CreateMonitoringServiceStub();
        auto diskRegistryGroup = monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "disk_registry");

        auto statePtr = TDiskRegistryStateBuilder()
                            .With(diskRegistryGroup)
                            .WithKnownAgents(agents)
                            .Build();
        TDiskRegistryState& state = *statePtr;

        state.PublishCounters(TInstant::Zero());

        UNIT_ASSERT_VALUES_EQUAL(
            46,
            diskRegistryGroup->GetCounter("MeanTimeBetweenFailures")->Val());
    }

    Y_UNIT_TEST(ShouldRetrieveCorrectStorageInfo)
    {
        const ui32 nativeBlockSize = 512;
        const ui64 logicalDeviceSize = 100_GB;
        const ui64 adjustedBlockCount = logicalDeviceSize / nativeBlockSize;
        const ui64 unAdjustedBlockCount = adjustedBlockCount + 100;
        const TString poolName = "test";

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto deviceConfig = [&] (auto name, auto uuid) {
            NProto::TDeviceConfig config;

            config.SetDeviceName(name);
            config.SetDeviceUUID(uuid);
            config.SetBlockSize(nativeBlockSize);
            config.SetBlocksCount(adjustedBlockCount);
            config.SetUnadjustedBlockCount(unAdjustedBlockCount);
            config.SetPoolName(poolName);
            config.SetPoolKind(NProto::DEVICE_POOL_KIND_LOCAL);

            return config;
        };

        const auto agent = AgentConfig(1, {
            deviceConfig("dev-1", "uuid-1.1"),
            deviceConfig("dev-2", "uuid-1.2"),
            deviceConfig("dev-3", "uuid-1.3"),
            deviceConfig("dev-4", "uuid-1.4"),
        });

        auto statePtr =
            TDiskRegistryStateBuilder()
                .WithConfig(
                    [&]
                    {
                        auto config = MakeConfig(0, {agent});
                        auto* pool = config.AddDevicePoolConfigs();
                        pool->SetName(poolName);
                        pool->SetKind(NProto::DEVICE_POOL_KIND_LOCAL);
                        pool->SetAllocationUnit(logicalDeviceSize);

                        return config;
                    }())
                .WithAgents({agent})
                .Build();
        TDiskRegistryState& state = *statePtr;

        {
            auto [infos, error] = state.QueryAvailableStorage(
                agent.GetAgentId(), TString {}, NProto::DEVICE_POOL_KIND_LOCAL);
            UNIT_ASSERT_C(!HasError(error), error);

            UNIT_ASSERT_VALUES_EQUAL(1, infos.size());
            const auto& info = infos[0];

            UNIT_ASSERT_VALUES_EQUAL(agent.DevicesSize(), info.ChunkCount);
            UNIT_ASSERT_VALUES_EQUAL(logicalDeviceSize, info.ChunkSize);
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result;
            const auto error = state.CreateDiskFromDevices(
                Now(),
                db,
                "vol0",
                TDiskRegistryState::TCreateDiskFromDevicesParams(
                    nativeBlockSize,
                    NProto::STORAGE_MEDIA_SSD_LOCAL,
                    /*force=*/false,
                    /*keepBlockCount=*/false),
                {agent.GetDevices(0)},
                {},
                &result);

            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
            UNIT_ASSERT_VALUES_EQUAL(1, result.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL(
                agent.GetDevices(0).GetDeviceUUID(),
                result.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                unAdjustedBlockCount,
                result.Devices[0].GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                unAdjustedBlockCount,
                result.Devices[0].GetUnadjustedBlockCount());
            UNIT_ASSERT_VALUES_EQUAL(
                nativeBlockSize,
                result.Devices[0].GetBlockSize());
        });

        {
            auto [infos, error] = state.QueryAvailableStorage(
                agent.GetAgentId(), TString {}, NProto::DEVICE_POOL_KIND_LOCAL);
            UNIT_ASSERT(!HasError(error));

            UNIT_ASSERT_VALUES_EQUAL(1, infos.size());

            UNIT_ASSERT_VALUES_EQUAL(agent.DevicesSize(), infos[0].ChunkCount);
            UNIT_ASSERT_VALUES_EQUAL(logicalDeviceSize, infos[0].ChunkSize);
        }
    }

    Y_UNIT_TEST(ShouldAllocateDiskWithAdjustedBlockCount)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const TString testDeviceName = "/dev/disk/by-partlabel/NVMENBS01";
        const TString testDeviceId = "uuid-1";
        const ui64 testDeviceSizeInBytes = 99998498816; // 93.1 GiB
        const ui64 testUnadjustedBlockCount = testDeviceSizeInBytes / DefaultLogicalBlockSize;
        const ui64 testBlockCount = 93_GB / DefaultLogicalBlockSize;

        const NProto::TAgentConfig agentConfig = AgentConfig(1, {
            Device(
                testDeviceName,
                testDeviceId,
                "rack-1",
                DefaultLogicalBlockSize,
                testDeviceSizeInBytes)
        });

        auto statePtr =
            TDiskRegistryStateBuilder().WithStorageConfig({}).Build();
        TDiskRegistryState& state = *statePtr;

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(
                state.RegisterAgent(db, agentConfig, Now()).GetError());

            const auto d = state.GetDevice(testDeviceId);
            UNIT_ASSERT_C(d.GetDeviceUUID().empty(), d);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto r = state.UpdateCmsDeviceState(
                db,
                agentConfig.GetAgentId(),
                testDeviceName,
                NProto::DEVICE_STATE_ONLINE,
                /*customMessage=*/TString(),
                Now(),
                false,  // shouldResumeDevice
                false); // dryRun

            UNIT_ASSERT_SUCCESS(r.Error);

            UNIT_ASSERT_VALUES_EQUAL(0, r.AffectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Zero(), r.Timeout);

            const auto d = state.GetDevice(testDeviceId);

            UNIT_ASSERT_VALUES_EQUAL(testBlockCount, d.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(DefaultLogicalBlockSize, d.GetBlockSize());
            UNIT_ASSERT_VALUES_EQUAL(testUnadjustedBlockCount, d.GetUnadjustedBlockCount());

            UNIT_ASSERT(state.IsDirtyDevice(testDeviceId));
            state.MarkDeviceAsClean(Now(), db, testDeviceId);
            UNIT_ASSERT(!state.IsDirtyDevice(testDeviceId));
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result;
            UNIT_ASSERT_SUCCESS(state.AllocateDisk(
                Now(),
                db,
                TDiskRegistryState::TAllocateDiskParams {
                    .DiskId = "nrd0",
                    .BlockSize = DefaultLogicalBlockSize,
                    .BlocksCount = testBlockCount
                },
                &result));

            UNIT_ASSERT_VALUES_EQUAL(1, result.Devices.size());

            const auto& d = result.Devices[0];

            UNIT_ASSERT_VALUES_EQUAL(DefaultLogicalBlockSize, d.GetBlockSize());
            UNIT_ASSERT_VALUES_EQUAL(testBlockCount, d.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(testUnadjustedBlockCount, d.GetUnadjustedBlockCount());
            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ONLINE, state.GetDiskState("nrd0"));
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto [r, error] = state.RegisterAgent(db, agentConfig, Now());
            UNIT_ASSERT_SUCCESS(error);

            UNIT_ASSERT_VALUES_EQUAL_C(
                0,
                r.AffectedDisks.size(),
                r.AffectedDisks[0]);
            UNIT_ASSERT_VALUES_EQUAL_C(
                0,
                r.DisksToReallocate.size(),
                r.DisksToReallocate[0]);

            UNIT_ASSERT_EQUAL(
                NProto::DISK_STATE_ONLINE,
                state.GetDiskState("nrd0"));
        });
    }

    Y_UNIT_TEST(ShouldRejectInvalidCreateMirroredDiskFromDevices)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        auto statePtr = MakeDefaultState();
        TDiskRegistryState& state = *statePtr;

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                const auto params =
                    TDiskRegistryState::TCreateDiskFromDevicesParams(
                        DefaultLogicalBlockSize,
                        NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                        /*force=*/false,
                        /*keepBlockCount=*/false);

                const auto error = CreateMirroredDiskFromDevices(
                    state,
                    db,
                    "disk-1",
                    params,
                    MakeMirror2ReplicaUuids());

                UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
                UNIT_ASSERT(error.GetMessage().Contains(
                    "Replicas count is inconsistent with media kind"));

                AssertMirrorCreateFromDevicesFailedClean(
                    state,
                    "disk-1",
                    MakeMirror2ReplicaUuids());
            });

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                const auto error = CreateMirroredDiskFromDevices(
                    state,
                    db,
                    "disk-1",
                    MakeMirror2Params(),
                    {MakeMirror2ReplicaUuids()[0]});

                UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
                UNIT_ASSERT(error.GetMessage().Contains("expects 2 replicas"));

                AssertMirrorCreateFromDevicesFailedClean(
                    state,
                    "disk-1",
                    {MakeMirror2ReplicaUuids()[0]});
            });

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                const TVector<TVector<TString>> replicasWithGap = {
                    {"uuid-1", ""},
                    {"uuid-3", ""},
                };

                const auto error = CreateMirroredDiskFromDevices(
                    state,
                    db,
                    "disk-1",
                    MakeMirror2Params(),
                    replicasWithGap);

                UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
                UNIT_ASSERT(error.GetMessage().Contains("no data for device"));

                AssertMirrorCreateFromDevicesFailedClean(
                    state,
                    "disk-1",
                    replicasWithGap);
            });

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                const TVector<TVector<TString>> duplicateReplicas = {
                    {"uuid-1"},
                    {"uuid-1"},
                };

                const auto error = CreateMirroredDiskFromDevices(
                    state,
                    db,
                    "disk-1",
                    MakeMirror2Params(),
                    duplicateReplicas);

                UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
                UNIT_ASSERT(error.GetMessage().Contains("is allocated for disk"));

                AssertMirrorCreateFromDevicesFailedClean(
                    state,
                    "disk-1",
                    duplicateReplicas);
            });

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto masterStatePtr =
                    TDiskRegistryStateBuilder()
                        .WithKnownAgents({
                            AgentConfig(
                                1,
                                {
                                    Device("dev-1", "uuid-1", "rack-1"),
                                    Device("dev-2", "uuid-2", "rack-1"),
                                }),
                            AgentConfig(
                                2,
                                {
                                    Device("dev-1", "uuid-3", "rack-2"),
                                    Device("dev-2", "uuid-4", "rack-2"),
                                }),
                        })
                        .WithDisks({Disk("disk-1", {"uuid-1"})})
                        .Build();

                const auto error = CreateMirroredDiskFromDevices(
                    *masterStatePtr,
                    db,
                    "disk-1",
                    MakeMirror2Params(),
                    MakeMirror2ReplicaUuids());

                UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
                UNIT_ASSERT(error.GetMessage().Contains("invalid master disk"));

                AssertNoMirror2DiskArtifactsForMaster(*masterStatePtr, "disk-1");
                TDiskInfo info;
                UNIT_ASSERT_SUCCESS(masterStatePtr->GetDiskInfo("disk-1", info));
                UNIT_ASSERT_VALUES_EQUAL(1, info.Devices.size());
            });

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto replicaState =
                    Disk("disk-1/1", {"uuid-3"}, NProto::DISK_STATE_ONLINE);
                replicaState.SetMasterDiskId("other-disk");

                auto boundStatePtr =
                    TDiskRegistryStateBuilder()
                        .WithKnownAgents({
                            AgentConfig(
                                1,
                                {
                                    Device("dev-1", "uuid-1", "rack-1"),
                                    Device("dev-2", "uuid-2", "rack-1"),
                                }),
                            AgentConfig(
                                2,
                                {
                                    Device("dev-1", "uuid-3", "rack-2"),
                                    Device("dev-2", "uuid-4", "rack-2"),
                                }),
                        })
                        .WithDisks({std::move(replicaState)})
                        .Build();

                const auto error = CreateMirroredDiskFromDevices(
                    *boundStatePtr,
                    db,
                    "disk-1",
                    MakeMirror2Params(),
                    MakeMirror2ReplicaUuids());

                UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
                UNIT_ASSERT(
                    error.GetMessage().Contains("already attached to other disk"));

                AssertNoMirror2DiskArtifactsForMaster(*boundStatePtr, "disk-1");
                TDiskInfo info;
                UNIT_ASSERT_SUCCESS(boundStatePtr->GetDiskInfo("disk-1/1", info));
                UNIT_ASSERT_VALUES_EQUAL("other-disk", info.MasterDiskId);
            });

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                const TVector<TVector<TString>> replicas = {
                    {"uuid-1", "missing"},
                    {"uuid-3"},
                };

                const auto error = CreateMirroredDiskFromDevices(
                    state,
                    db,
                    "disk-1",
                    MakeMirror2Params(),
                    replicas);

                UNIT_ASSERT(error.GetCode() != S_OK);

                AssertMirrorCreateFromDevicesFailedClean(state, "disk-1", replicas);
            });
    }

    Y_UNIT_TEST(ShouldCreateMirroredDiskFromDevices)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        auto statePtr = MakeDefaultState();
        TDiskRegistryState& state = *statePtr;

        const auto replicaUuids = MakeMirror2ReplicaUuids();

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TDiskRegistryState::TAllocateDiskResult result;
                const auto error = state.CreateDiskFromDevices(
                    TInstant::FromValue(1000),
                    db,
                    "disk-1",
                    MakeMirror2Params(),
                    {},
                    replicaUuids,
                    &result);

                UNIT_ASSERT_VALUES_EQUAL_C(
                    S_OK,
                    error.GetCode(),
                    error.GetMessage());

                UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
                UNIT_ASSERT_VALUES_EQUAL(
                    "uuid-1",
                    result.Devices[0].GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL(
                    "uuid-2",
                    result.Devices[1].GetDeviceUUID());
            });

        {
            TDiskInfo masterInfo;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1", masterInfo));

            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ONLINE, masterInfo.State);
            UNIT_ASSERT_EQUAL(
                NProto::STORAGE_MEDIA_SSD_MIRROR2,
                masterInfo.MediaKind);
            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                masterInfo.LogicalBlockSize);
            UNIT_ASSERT_VALUES_EQUAL(2, masterInfo.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-1",
                masterInfo.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-2",
                masterInfo.Devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1, masterInfo.Replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(2, masterInfo.Replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-3",
                masterInfo.Replicas[0][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-4",
                masterInfo.Replicas[0][1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1000, masterInfo.StateTs.GetValue());
        }

        {
            TDiskInfo replicaInfo;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1/0", replicaInfo));
            UNIT_ASSERT_VALUES_EQUAL("disk-1", replicaInfo.MasterDiskId);
        }

        {
            TDiskInfo replicaInfo;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1/1", replicaInfo));
            UNIT_ASSERT_VALUES_EQUAL("disk-1", replicaInfo.MasterDiskId);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            "|uuid-1|uuid-3|"
            "|uuid-2|uuid-4|",
            GetReplicaTableRepr(state, "disk-1"));

        const auto* group = state.FindPlacementGroup("disk-1/g");
        UNIT_ASSERT(group);
        UNIT_ASSERT_VALUES_EQUAL(2, group->DisksSize());
        UNIT_ASSERT_VALUES_EQUAL("disk-1/0", group->GetDisks(0).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-1/1", group->GetDisks(1).GetDiskId());
    }

    Y_UNIT_TEST(ShouldRejectMirroredDiskFromDevicesWithIncompleteData)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        auto statePtr = MakeDefaultState();
        TDiskRegistryState& state = *statePtr;

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                const TVector<TVector<TString>> replicasWithGap = {
                    {"uuid-1", ""},
                    {"uuid-3", ""},
                };

                const auto error = CreateMirroredDiskFromDevices(
                    state,
                    db,
                    "disk-1",
                    MakeMirror2Params(),
                    replicasWithGap);

                UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
                UNIT_ASSERT(error.GetMessage().Contains("no data for device"));

                AssertMirrorCreateFromDevicesFailedClean(
                    state,
                    "disk-1",
                    replicasWithGap);
            });
    }

    Y_UNIT_TEST(ShouldCreateMirroredDiskFromDevicesWithFreshDeviceFirst)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        auto statePtr = MakeDefaultState();
        TDiskRegistryState& state = *statePtr;

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                const TVector<TVector<TString>> replicasWithFresh = {
                    {"", "uuid-2"},
                    {"uuid-3", "uuid-4"},
                };

                TDiskRegistryState::TAllocateDiskResult result;
                const auto error = state.CreateDiskFromDevices(
                    TInstant::FromValue(1000),
                    db,
                    "disk-1",
                    MakeMirror2Params(),
                    {},
                    replicasWithFresh,
                    &result);

                UNIT_ASSERT_VALUES_EQUAL_C(
                    S_OK,
                    error.GetCode(),
                    error.GetMessage());

                UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
                UNIT_ASSERT_VALUES_EQUAL(
                    "uuid-1",
                    result.Devices[0].GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL(
                    "uuid-2",
                    result.Devices[1].GetDeviceUUID());
            });

        UNIT_ASSERT_VALUES_EQUAL(
            "|uuid-1*|uuid-3|"
            "|uuid-2|uuid-4|",
            GetReplicaTableRepr(state, "disk-1"));
    }

    Y_UNIT_TEST(ShouldCreateMirroredDiskFromDevicesWithFreshDevice)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        auto statePtr = MakeDefaultState();
        TDiskRegistryState& state = *statePtr;

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                const TVector<TVector<TString>> replicasWithFresh = {
                    {"uuid-1", ""},
                    {"uuid-3", "uuid-4"},
                };

                TDiskRegistryState::TAllocateDiskResult result;
                const auto error = state.CreateDiskFromDevices(
                    TInstant::FromValue(1000),
                    db,
                    "disk-1",
                    MakeMirror2Params(),
                    {},
                    replicasWithFresh,
                    &result);

                UNIT_ASSERT_VALUES_EQUAL_C(
                    S_OK,
                    error.GetCode(),
                    error.GetMessage());

                UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
                UNIT_ASSERT_VALUES_EQUAL(
                    "uuid-1",
                    result.Devices[0].GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL(
                    "uuid-2",
                    result.Devices[1].GetDeviceUUID());
            });

        UNIT_ASSERT_VALUES_EQUAL(
            "|uuid-1|uuid-3|"
            "|uuid-2*|uuid-4|",
            GetReplicaTableRepr(state, "disk-1"));
    }

    Y_UNIT_TEST(ShouldCreateMirroredDiskFromDevicesWithFreshDevices)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        auto statePtr = MakeDefaultState();
        TDiskRegistryState& state = *statePtr;

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                const TVector<TVector<TString>> replicas = {
                    {"uuid-1", ""},
                    {"", "uuid-4"},
                };

                TDiskRegistryState::TAllocateDiskResult result;
                const auto error = state.CreateDiskFromDevices(
                    TInstant::FromValue(1000),
                    db,
                    "disk-1",
                    MakeMirror2Params(),
                    {},
                    replicas,
                    &result);

                UNIT_ASSERT_VALUES_EQUAL_C(
                    S_OK,
                    error.GetCode(),
                    error.GetMessage());

                UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
                UNIT_ASSERT_VALUES_EQUAL(
                    "uuid-1",
                    result.Devices[0].GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL(
                    "uuid-2",
                    result.Devices[1].GetDeviceUUID());
            });

        UNIT_ASSERT_VALUES_EQUAL(
            "|uuid-1|uuid-3*|"
            "|uuid-2*|uuid-4|",
            GetReplicaTableRepr(state, "disk-1"));
    }

    Y_UNIT_TEST(ShouldNotTouchDevicesOfOtherDisksWhenMirroredCreationFails)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        auto statePtr = TDiskRegistryStateBuilder()
                            .WithKnownAgents({
                                AgentConfig(
                                    1,
                                    {
                                        Device("dev-1", "uuid-1", "rack-1"),
                                        Device("dev-2", "uuid-2", "rack-1"),
                                    }),
                                AgentConfig(
                                    2,
                                    {
                                        Device("dev-1", "uuid-3", "rack-2"),
                                        Device("dev-2", "uuid-4", "rack-2"),
                                    }),
                            })
                            .WithDisks({Disk("other", {"uuid-2"})})
                            .Build();
        TDiskRegistryState& state = *statePtr;

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                const auto error = CreateMirroredDiskFromDevices(
                    state,
                    db,
                    "disk-1",
                    MakeMirror2Params(/*force=*/false),
                    {
                        {"uuid-1", "uuid-2"},
                        {"uuid-3", "uuid-4"},
                    });

                UNIT_ASSERT_VALUES_EQUAL_C(
                    E_ARGUMENT,
                    error.GetCode(),
                    FormatError(error));
                UNIT_ASSERT(
                    error.GetMessage().Contains("is allocated for disk"));
            });

        // The device of the other disk must stay untouched.
        UNIT_ASSERT_VALUES_EQUAL("other", state.FindDisk("uuid-2"));
        UNIT_ASSERT(!state.IsDirtyDevice("uuid-2"));

        // A failed request must not change the state of its own devices.
        UNIT_ASSERT_VALUES_EQUAL("", state.FindDisk("uuid-1"));
        UNIT_ASSERT(!state.IsDirtyDevice("uuid-1"));
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetBrokenDisks().size());
    }

    Y_UNIT_TEST(ShouldNotEraseDevicesWhenMirroredRequestIsInvalid)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        auto statePtr = MakeDefaultState();
        TDiskRegistryState& state = *statePtr;

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                const auto error = CreateMirroredDiskFromDevices(
                    state,
                    db,
                    "disk-1",
                    MakeMirror2Params(/*force=*/false),
                    {
                        {"uuid-1", ""},
                        {"uuid-3", ""},
                    });

                UNIT_ASSERT_C(HasError(error), FormatError(error));

                UNIT_ASSERT(!state.IsDirtyDevice("uuid-1"));
                UNIT_ASSERT(!state.IsDirtyDevice("uuid-3"));
                UNIT_ASSERT_VALUES_EQUAL(0, state.GetBrokenDisks().size());

                UNIT_ASSERT_VALUES_EQUAL_C(
                    E_ARGUMENT,
                    error.GetCode(),
                    FormatError(error));
            });
    }

    Y_UNIT_TEST(ShouldBeIdempotentWhenCreatingMirroredDiskWithFreshDevices)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        auto statePtr = TDiskRegistryStateBuilder()
                            .WithKnownAgents({
                                AgentConfig(
                                    1,
                                    {
                                        Device("dev-1", "uuid-1", "rack-1"),
                                        Device("dev-2", "uuid-2", "rack-1"),
                                        Device("dev-3", "uuid-5", "rack-1"),
                                    }),
                                AgentConfig(
                                    2,
                                    {
                                        Device("dev-1", "uuid-3", "rack-2"),
                                        Device("dev-2", "uuid-4", "rack-2"),
                                    }),
                            })
                            .Build();
        TDiskRegistryState& state = *statePtr;

        const TVector<TVector<TString>> replicas = {
            {"uuid-1", ""},
            {"uuid-3", "uuid-4"},
        };

        TString freshDeviceId;

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TDiskRegistryState::TAllocateDiskResult result;
                const auto error = state.CreateDiskFromDevices(
                    Now(),
                    db,
                    "disk-1",
                    MakeMirror2Params(/*force=*/false),
                    {},
                    replicas,
                    &result);

                UNIT_ASSERT_VALUES_EQUAL_C(
                    S_OK,
                    error.GetCode(),
                    FormatError(error));
                UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
                freshDeviceId = result.Devices[1].GetDeviceUUID();
            });

        // The same request again, e.g. a retry after a timeout.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                const auto error = CreateMirroredDiskFromDevices(
                    state,
                    db,
                    "disk-1",
                    MakeMirror2Params(/*force=*/false),
                    replicas);

                UNIT_ASSERT_VALUES_EQUAL_C(
                    S_ALREADY,
                    error.GetCode(),
                    FormatError(error));
            });

        TDiskInfo info;
        UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1", info));
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetBrokenDisks().size());

        UNIT_ASSERT_VALUES_EQUAL("disk-1/0", state.FindDisk("uuid-1"));
        UNIT_ASSERT_VALUES_EQUAL("disk-1/0", state.FindDisk(freshDeviceId));
        UNIT_ASSERT_VALUES_EQUAL("disk-1/1", state.FindDisk("uuid-3"));
        UNIT_ASSERT_VALUES_EQUAL("disk-1/1", state.FindDisk("uuid-4"));

        for (const auto* uuid:
             {"uuid-1", "uuid-2", "uuid-3", "uuid-4", "uuid-5"})
        {
            UNIT_ASSERT_C(!state.IsDirtyDevice(uuid), uuid);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
