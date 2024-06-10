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

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryStatePoolsTest)
{
    // TODO
    /*
    Y_UNIT_TEST(ShouldAllocateDiskFromTargetPool)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const TVector agents {
            AgentConfig(1, {
                Device("dev-1", "uuid-1.1"),
                Device("dev-2", "uuid-1.2")
                    | WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL),
                Device("dev-3", "uuid-1.3"),
                Device("dev-4", "uuid-1.4")
            }),
            AgentConfig(2, {
                Device("dev-1", "uuid-2.1")
                    | WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL),
                Device("dev-2", "uuid-2.2"),
                Device("dev-3", "uuid-2.3"),
                Device("dev-4", "uuid-2.4")
            })
        };

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithConfig([&] {
                auto config = MakeConfig(0, agents);

                auto* local = config.AddDevicePoolConfigs();
                local->SetName("local-ssd");
                local->SetKind(NProto::DEVICE_POOL_KIND_LOCAL);
                local->SetAllocationUnit(DefaultDeviceSize);

                return config;
             }())
            .WithAgents(agents)
            .Build();

        auto allocate = [&] (auto db, ui32 deviceCount) {
            TDiskRegistryState::TAllocateDiskResult result;

            auto error = state.AllocateDisk(
                TInstant::Zero(),
                db,
                TDiskRegistryState::TAllocateDiskParams {
                    .DiskId = "foo",
                    .BlockSize = DefaultLogicalBlockSize,
                    .BlocksCount = deviceCount * DefaultDeviceSize / DefaultLogicalBlockSize,
                    .MediaKind = NProto::STORAGE_MEDIA_SSD_LOCAL
                },
                &result);

            return std::make_pair(std::move(result), error);
        };

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto [result, error] = allocate(db, 3);

            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto [result, error] = allocate(db, 2);

            UNIT_ASSERT_VALUES_EQUAL(error.GetCode(), S_OK);
            UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
            Sort(result.Devices, TByDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL("uuid-1.2", result.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", result.Devices[1].GetDeviceUUID());
        });
    }*/

    Y_UNIT_TEST(ShouldAllocateDiskOnTargetNode)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const TVector agents {
            AgentConfig(1, {
                Device("dev-1", "uuid-1.1"),
                Device("dev-2", "uuid-1.2")
                    | WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL),
                Device("dev-3", "uuid-1.3"),
                Device("dev-4", "uuid-1.4")
            }),
            AgentConfig(2, {
                Device("dev-1", "uuid-2.1")
                    | WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL),
                Device("dev-2", "uuid-2.2"),
                Device("dev-3", "uuid-2.3"),
                Device("dev-4", "uuid-2.4")
            })
        };

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithConfig([&] {
                auto config = MakeConfig(0, agents);

                auto* local = config.AddDevicePoolConfigs();
                local->SetName("local-ssd");
                local->SetKind(NProto::DEVICE_POOL_KIND_LOCAL);
                local->SetAllocationUnit(DefaultDeviceSize);

                return config;
             }())
            .WithAgents(agents)
            .Build();

        auto allocate = [&] (auto db, ui32 deviceCount, TString agentId) {
            TDiskRegistryState::TAllocateDiskResult result;

            auto error = state.AllocateDisk(
                TInstant::Zero(),
                db,
                TDiskRegistryState::TAllocateDiskParams {
                    .DiskId = ToString(deviceCount) + "-" + agentId,
                    .BlockSize = DefaultLogicalBlockSize,
                    .BlocksCount = deviceCount * DefaultDeviceSize / DefaultLogicalBlockSize,
                    .AgentIds = { agentId }
                },
                &result);

            return std::make_pair(std::move(result), error);
        };

        for (auto& agent: agents) {
            executor.WriteTx([&] (TDiskRegistryDatabase db) {
                auto [result, error] = allocate(db, 4, agent.GetAgentId());
                UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
            });
        }

        for (auto& agent: agents) {
            executor.WriteTx([&] (TDiskRegistryDatabase db) {
                auto [result, error] = allocate(db, 3, agent.GetAgentId());

                UNIT_ASSERT_VALUES_EQUAL_C(error.GetCode(), S_OK, error);
                UNIT_ASSERT_VALUES_EQUAL(3, result.Devices.size());
                Sort(result.Devices, TByDeviceUUID());

                for (auto& d: result.Devices) {
                    UNIT_ASSERT_VALUES_EQUAL(agent.GetAgentId(), d.GetAgentId());
                    UNIT_ASSERT_VALUES_EQUAL(agent.GetNodeId(), d.GetNodeId());
                    UNIT_ASSERT_VALUES_EQUAL("", d.GetPoolName());
                }
            });
        }
    }

    Y_UNIT_TEST(ShouldAllocateDiskOnTargetNodeWithTargetPool)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const TVector agents {
            AgentConfig(1, {
                Device("dev-1", "uuid-1.1"),
                Device("dev-2", "uuid-1.2")
                    | WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL),
                Device("dev-3", "uuid-1.3"),
                Device("dev-4", "uuid-1.4")
            }),
            AgentConfig(2, {
                Device("dev-1", "uuid-2.1")
                    | WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL),
                Device("dev-2", "uuid-2.2"),
                Device("dev-3", "uuid-2.3"),
                Device("dev-4", "uuid-2.4")
            })
        };

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithConfig([&] {
                auto config = MakeConfig(0, agents);

                auto* local = config.AddDevicePoolConfigs();
                local->SetName("local-ssd");
                local->SetKind(NProto::DEVICE_POOL_KIND_LOCAL);
                local->SetAllocationUnit(DefaultDeviceSize);

                return config;
             }())
            .WithAgents(agents)
            .Build();

        auto allocate = [&] (auto db, ui32 deviceCount, TString agentId) {
            TDiskRegistryState::TAllocateDiskResult result;

            auto error = state.AllocateDisk(
                TInstant::Zero(),
                db,
                TDiskRegistryState::TAllocateDiskParams {
                    .DiskId = ToString(deviceCount) + "-" + agentId,
                    .BlockSize = DefaultLogicalBlockSize,
                    .BlocksCount = deviceCount * DefaultDeviceSize / DefaultLogicalBlockSize,
                    .AgentIds = { agentId },
                    .MediaKind = NProto::STORAGE_MEDIA_SSD_LOCAL
                },
                &result);

            return std::make_pair(std::move(result), error);
        };

        for (auto& agent: agents) {
            executor.WriteTx([&] (TDiskRegistryDatabase db) {
                auto [result, error] = allocate(db, 2, agent.GetAgentId());
                UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
            });
        }

        const TString expectedDevices[] = { "uuid-1.2", "uuid-2.1" };

        for (size_t i = 0; i != agents.size(); ++i) {
            executor.WriteTx([&] (TDiskRegistryDatabase db) {
                const auto& agent = agents[i];
                auto [result, error] = allocate(db, 1, agent.GetAgentId());

                UNIT_ASSERT_VALUES_EQUAL(error.GetCode(), S_OK);
                UNIT_ASSERT_VALUES_EQUAL(1, result.Devices.size());
                auto& device = result.Devices[0];
                UNIT_ASSERT_VALUES_EQUAL(agent.GetAgentId(), device.GetAgentId());
                UNIT_ASSERT_VALUES_EQUAL(agent.GetNodeId(), device.GetNodeId());
                UNIT_ASSERT_VALUES_EQUAL(expectedDevices[i], device.GetDeviceUUID());
            });
        }
    }

    Y_UNIT_TEST(ShouldReplaceDeviceWithPool)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const TVector agents {
            AgentConfig(1, {
                Device("dev-1", "uuid-1.1") | WithPool("pool"),
                Device("dev-2", "uuid-1.2") | WithPool("pool")
            }),
            AgentConfig(2, {
                Device("dev-1", "uuid-2.1"),
                Device("dev-2", "uuid-2.2") | WithPool("pool"),
                Device("dev-2", "uuid-2.3")
            })
        };

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithConfig([&] {
                auto config = MakeConfig(0, agents);

                auto* local = config.AddDevicePoolConfigs();
                local->SetName("pool");
                local->SetAllocationUnit(DefaultDeviceSize);

                return config;
             }())
            .WithAgents(agents)
            .WithDisks({ Disk("disk-1", { "uuid-1.1", "uuid-1.2" }) })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            bool updated = false;
            UNIT_ASSERT_SUCCESS(state.ReplaceDevice(
                db,
                "disk-1",
                "uuid-1.2",
                "",     // no replacement device
                TInstant::Zero(),
                "",     // message
                true,   // manual
                &updated));
        });

        {
            TVector<TDeviceConfig> devices;
            UNIT_ASSERT_SUCCESS(state.GetDiskDevices("disk-1", devices));
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.2", devices[1].GetDeviceUUID());
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            bool updated = false;
            const auto error = state.ReplaceDevice(
                db,
                "disk-1",
                "uuid-1.1",
                "",     // no replacement device
                TInstant::Zero(),
                "",     // message
                true,   // manual
                &updated);
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
        });
    }

    Y_UNIT_TEST(ShouldReplaceDeviceWithLocalPool)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const TVector agents {
            AgentConfig(1, {
                Device("dev-1", "uuid-1.1")
                    | WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL),
                Device("dev-2", "uuid-1.2")
                    | WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL)
            }),
            AgentConfig(2, {
                Device("dev-1", "uuid-2.1"),
                Device("dev-2", "uuid-2.2")
                    | WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL),
                Device("dev-3", "uuid-2.3")
                    | WithPool("bar"),
                Device("dev-4", "uuid-2.4")
                    | WithPool("baz")
            })
        };

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithConfig([&] {
                auto config = MakeConfig(0, agents);

                auto* local = config.AddDevicePoolConfigs();
                local->SetName("local-ssd");
                local->SetKind(NProto::DEVICE_POOL_KIND_LOCAL);
                local->SetAllocationUnit(DefaultDeviceSize);

                return config;
             }())
            .WithAgents(agents)
            .WithDisks({ Disk("disk-1", { "uuid-1.1", "uuid-1.2" }) })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            bool updated = false;
            const auto error = state.ReplaceDevice(
                db,
                "disk-1",
                "uuid-1.1",
                "",     // no replacement device
                TInstant::Zero(),
                "",     // message
                true,   // manual
                &updated);
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
        });
    }

    Y_UNIT_TEST(ShouldAdjustDeviceBlockCount)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto agent = AgentConfig(1, {
            Device("dev-1", "uuid-1")
                | WithPool("local1")
                | WithTotalSize(2_GB + 16_KB, 512),
            Device("dev-2", "uuid-2")
                | WithTotalSize(1_GB + 16_KB),
            Device("dev-3", "uuid-3")
                | WithPool("local1")
                | WithTotalSize(2_GB - 4_KB),
            Device("dev-4", "uuid-4")
                | WithTotalSize(1_GB - 8_KB, 8_KB),
            Device("dev-5", "uuid-5")
                | WithPool("local2")
                | WithTotalSize(512_MB + 4_KB),
            Device("dev-6", "uuid-6")
                | WithPool("local2")
                | WithTotalSize(512_MB),
            Device("dev-7", "uuid-7")
                | WithPool("global1")
                | WithTotalSize(1100_MB),
            Device("dev-8", "uuid-8")
                | WithPool("global1")
                | WithTotalSize(1010_MB),
            Device("dev-9", "uuid-9")
                | WithPool("global2")
                | WithTotalSize(2501_MB)
        });

        THashMap<TString, ui64> pools {
            {"", 1_GB},

            {"local1", 2_GB},
            {"local2", 512_MB},

            {"global1", 1000_MB},
            {"global2", 2500_MB}
        };

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithConfig([&] {
                auto config = MakeConfig(0, {agent});

                auto* nonrepl = config.AddDevicePoolConfigs();
                nonrepl->SetAllocationUnit(pools[""]);

                for (auto* name: {"local1", "local2"}) {
                    auto* local = config.AddDevicePoolConfigs();
                    local->SetName(name);
                    local->SetKind(NProto::DEVICE_POOL_KIND_LOCAL);
                    local->SetAllocationUnit(pools[name]);
                }

                for (auto* name: {"global1", "global2"}) {
                    auto* local = config.AddDevicePoolConfigs();
                    local->SetName(name);
                    local->SetKind(NProto::DEVICE_POOL_KIND_GLOBAL);
                    local->SetAllocationUnit(pools[name]);
                }

                return config;
             }())
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, agent));
        });

        UNIT_ASSERT_EQUAL(
            NProto::DEVICE_STATE_ERROR, state.GetDevice("uuid-3").GetState());
        UNIT_ASSERT_EQUAL(
            NProto::DEVICE_STATE_ERROR, state.GetDevice("uuid-4").GetState());

        // check default pool
        {
            auto [infos, error] = state.QueryAvailableStorage(
                agent.GetAgentId(), TString {}, NProto::DEVICE_POOL_KIND_DEFAULT);
            UNIT_ASSERT(!HasError(error));

            UNIT_ASSERT_VALUES_EQUAL(1, infos.size());

            UNIT_ASSERT_VALUES_EQUAL(1, infos[0].ChunkCount);
            UNIT_ASSERT_VALUES_EQUAL(1_GB, infos[0].ChunkSize);
        }

        // check all local pools
        {
            auto [infos, error] = state.QueryAvailableStorage(
                agent.GetAgentId(), TString {}, NProto::DEVICE_POOL_KIND_LOCAL);
            UNIT_ASSERT(!HasError(error));

            SortBy(infos, [] (auto& info) {
                return info.ChunkSize;
            });

            UNIT_ASSERT_VALUES_EQUAL(0, infos.size());
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            for (auto& d: agent.GetDevices()) {
                state.ResumeDevices(Now(), db, {d.GetDeviceUUID()});
            }
        });

        {
            auto [infos, error] = state.QueryAvailableStorage(
                agent.GetAgentId(), TString {}, NProto::DEVICE_POOL_KIND_LOCAL);
            UNIT_ASSERT(!HasError(error));
            UNIT_ASSERT_VALUES_EQUAL(0, infos.size());
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            for (auto& d: agent.GetDevices()) {
                state.MarkDeviceAsClean(Now(), db, d.GetDeviceUUID());
            }
        });

        {
            auto [infos, error] = state.QueryAvailableStorage(
                agent.GetAgentId(), TString {}, NProto::DEVICE_POOL_KIND_LOCAL);
            UNIT_ASSERT(!HasError(error));

            SortBy(infos, [] (auto& info) {
                return info.ChunkSize;
            });

            UNIT_ASSERT_VALUES_EQUAL(2, infos.size());

            UNIT_ASSERT_VALUES_EQUAL(2, infos[0].ChunkCount);
            UNIT_ASSERT_VALUES_EQUAL(512_MB, infos[0].ChunkSize);

            UNIT_ASSERT_VALUES_EQUAL(1, infos[1].ChunkCount);
            UNIT_ASSERT_VALUES_EQUAL(2_GB, infos[1].ChunkSize);
        }

        // check local1 pool
        {
            auto [infos, error] = state.QueryAvailableStorage(
                agent.GetAgentId(),
                "local1",
                NProto::DEVICE_POOL_KIND_LOCAL);

            UNIT_ASSERT(!HasError(error));

            UNIT_ASSERT_VALUES_EQUAL(1, infos.size());
            UNIT_ASSERT_VALUES_EQUAL(1, infos[0].ChunkCount);
            UNIT_ASSERT_VALUES_EQUAL(2_GB, infos[0].ChunkSize);
        }

        // check all global pools
        {
            auto [infos, error] = state.QueryAvailableStorage(
                agent.GetAgentId(), TString {}, NProto::DEVICE_POOL_KIND_GLOBAL);
            UNIT_ASSERT(!HasError(error));

            SortBy(infos, [] (auto& info) {
                return info.ChunkSize;
            });

            UNIT_ASSERT_VALUES_EQUAL(2, infos.size());

            UNIT_ASSERT_VALUES_EQUAL(2, infos[0].ChunkCount);
            UNIT_ASSERT_VALUES_EQUAL(1000_MB, infos[0].ChunkSize);

            UNIT_ASSERT_VALUES_EQUAL(1, infos[1].ChunkCount);
            UNIT_ASSERT_VALUES_EQUAL(2500_MB, infos[1].ChunkSize);
        }

        UNIT_ASSERT_VALUES_EQUAL(1, state.GetAgents().size());
        for (auto& device: state.GetAgents()[0].GetDevices()) {
            if (device.GetState() != NProto::DEVICE_STATE_ONLINE) {
                continue;
            }

            const ui64 size = device.GetBlockSize() * device.GetBlocksCount();
            const ui64 expected = pools[device.GetPoolName()];

            UNIT_ASSERT_VALUES_EQUAL(expected, size);
        }
    }

    Y_UNIT_TEST(ShouldRejectAllocationForLocalPools)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        constexpr ui64 localDeviceSize = 99999997952; // ~ 93.13 GiB

        auto makeLocalDevice = [] (const auto* name, const auto* uuid) {
            return Device(name, uuid)
                | WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL)
                | WithTotalSize(localDeviceSize);
        };

        const TVector agents {
            AgentConfig(1, {
                makeLocalDevice("dev-1", "uuid-1.1"),
                makeLocalDevice("dev-2", "uuid-1.2"),
                Device("dev-3", "uuid-1.3"),
                Device("dev-4", "uuid-1.4")
            }),
            AgentConfig(2, {
                makeLocalDevice("dev-1", "uuid-2.1"),
                makeLocalDevice("dev-2", "uuid-2.2"),
                makeLocalDevice("dev-3", "uuid-2.3"),
                Device("dev-4", "uuid-2.4")
            })
        };

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithConfig([&] {
                auto config = MakeConfig(0, agents);

                auto* pool = config.AddDevicePoolConfigs();
                pool->SetName("local-ssd");
                pool->SetKind(NProto::DEVICE_POOL_KIND_LOCAL);
                pool->SetAllocationUnit(localDeviceSize);

                return config;
             }())
            .WithAgents(agents)
            .Build();

        auto allocate = [&] (auto db, ui32 deviceCount, TVector<TString> agentIds) {
            TDiskRegistryState::TAllocateDiskResult result;

            auto error = state.AllocateDisk(
                TInstant::Zero(),
                db,
                TDiskRegistryState::TAllocateDiskParams {
                    .DiskId = "local0",
                    .BlockSize = DefaultLogicalBlockSize,
                    .BlocksCount = deviceCount * localDeviceSize / DefaultLogicalBlockSize,
                    .AgentIds = std::move(agentIds),
                    .PoolName = {},
                    .MediaKind = NProto::STORAGE_MEDIA_SSD_LOCAL
                },
                &result);

            return std::make_pair(std::move(result), error);
        };

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto [result, error] = allocate(db, 2, {agents[0].GetAgentId()});

            UNIT_ASSERT_VALUES_EQUAL_C(error.GetCode(), S_OK, error);
            UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
            Sort(result.Devices, TByDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", result.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.2", result.Devices[1].GetDeviceUUID());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto [result, error] = allocate(db, 2, {agents[1].GetAgentId()});

            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto [result, error] = allocate(db, 3, {agents[1].GetAgentId()});

            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto [result, error] = allocate(db, 3, {
                agents[0].GetAgentId(),
                agents[1].GetAgentId()
            });

            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
        });
    }

    Y_UNIT_TEST(ShouldUpdateDevices)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        constexpr ui64 smallPoolUnitSize = 1_GB;
        constexpr ui64 bigPoolUnitSize = 100_GB;

        const auto agentConfig = AgentConfig(1, {
            Device("dev-1", "uuid-1.1")
                | WithPool("small", NProto::DEVICE_POOL_KIND_GLOBAL)
                | WithTotalSize(smallPoolUnitSize, DefaultLogicalBlockSize),
            Device("dev-2", "uuid-1.2")
                | WithPool("big", NProto::DEVICE_POOL_KIND_LOCAL)
                | WithTotalSize(bigPoolUnitSize, DefaultLogicalBlockSize)
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithStorageConfig([]{
                auto config = CreateDefaultStorageConfigProto();
                config.SetAllocationUnitNonReplicatedSSD(93);
                return config;
            }())
            .WithConfig([&] {
                auto config = MakeConfig(0, {agentConfig});

                auto* small = config.AddDevicePoolConfigs();
                small->SetName("small");
                small->SetKind(NProto::DEVICE_POOL_KIND_GLOBAL);
                small->SetAllocationUnit(smallPoolUnitSize);

                auto* big = config.AddDevicePoolConfigs();
                big->SetName("big");
                big->SetKind(NProto::DEVICE_POOL_KIND_LOCAL);
                big->SetAllocationUnit(bigPoolUnitSize);

                return config;
             }())
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, agentConfig));

            const auto* agent = state.FindAgent(agentConfig.GetAgentId());
            UNIT_ASSERT(agent);

            UNIT_ASSERT_VALUES_EQUAL(
                agentConfig.DevicesSize(),
                agent->DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(0, agent->UnknownDevicesSize());

            for (size_t i = 0; i != agent->DevicesSize(); ++i) {
                const auto& expected = agentConfig.GetDevices(i);
                const auto& device = agent->GetDevices(i);

                UNIT_ASSERT_EQUAL_C(
                    NProto::DEVICE_STATE_ONLINE,
                    device.GetState(), device);

                UNIT_ASSERT_EQUAL(
                    expected.GetPoolKind(),
                    device.GetPoolKind());

                UNIT_ASSERT_VALUES_EQUAL(
                    expected.GetDeviceUUID(),
                    device.GetDeviceUUID());

                UNIT_ASSERT_VALUES_EQUAL(
                    expected.GetPoolName(),
                    device.GetPoolName());

                UNIT_ASSERT_VALUES_EQUAL(
                    expected.GetBlockSize(),
                    device.GetBlockSize());

                UNIT_ASSERT_VALUES_EQUAL(
                    expected.GetBlocksCount(),
                    device.GetBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(
                    expected.GetBlocksCount(),
                    device.GetUnadjustedBlockCount());
            }
        });

        // update devices

        const auto newAgentConfig = AgentConfig(
            1,
            {Device("dev-1", "uuid-1.1") |
                 WithTotalSize(93_GB, DefaultLogicalBlockSize),
             Device("dev-2", "uuid-1.2") |
                 WithTotalSize(93_GB, DefaultLogicalBlockSize),
             // new devices
             Device("dev-3", "uuid-1.3") |
                 WithTotalSize(93_GB, DefaultLogicalBlockSize),
             Device("dev-4", "uuid-1.4") |
                 WithTotalSize(93_GB, DefaultLogicalBlockSize)});

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, newAgentConfig));

            const auto* agent = state.FindAgent(newAgentConfig.GetAgentId());
            UNIT_ASSERT(agent);

            UNIT_ASSERT_VALUES_EQUAL(2, agent->DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(2, agent->UnknownDevicesSize());

            for (size_t i = 0; i != newAgentConfig.DevicesSize(); ++i) {
                const auto& expected = newAgentConfig.GetDevices(i);
                const auto& device = i < agent->DevicesSize()
                    ? agent->GetDevices(i)
                    : agent->GetUnknownDevices(i - agent->DevicesSize());

                UNIT_ASSERT_EQUAL_C(
                    NProto::DEVICE_STATE_ONLINE,
                    device.GetState(), device);

                UNIT_ASSERT_EQUAL(
                    expected.GetPoolKind(),
                    device.GetPoolKind());

                UNIT_ASSERT_VALUES_EQUAL(
                    expected.GetDeviceUUID(),
                    device.GetDeviceUUID());

                UNIT_ASSERT_VALUES_EQUAL(
                    expected.GetPoolName(),
                    device.GetPoolName());

                UNIT_ASSERT_VALUES_EQUAL(
                    expected.GetBlockSize(),
                    device.GetBlockSize());

                UNIT_ASSERT_VALUES_EQUAL(
                    expected.GetBlocksCount(),
                    device.GetBlocksCount());

                if (i < agent->DevicesSize()) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        expected.GetBlocksCount(),
                        device.GetUnadjustedBlockCount());
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(
                        0,
                        device.GetUnadjustedBlockCount());
                }
            }
        });
    }
}

}   // namespace NCloud::NBlockStore::NStorage
