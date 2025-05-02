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

struct TFixture
    : public NUnitTest::TBaseFixture
{
    TTestExecutor Executor;

    const TVector<NProto::TAgentConfig> Agents {
        AgentConfig(1, {
            Device("dev-1", "uuid-1.1"),
            Device("dev-2", "uuid-1.2"),
            Device("dev-3", "uuid-1.3"),
            Device("dev-4", "uuid-1.4")
        }),
        AgentConfig(2, {
            Device("dev-1", "uuid-2.1"),
            Device("dev-2", "uuid-2.2"),
            Device("dev-3", "uuid-2.3")
                | WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL),
            Device("dev-4", "uuid-2.4")
                | WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL)
        }),
        AgentConfig(3, {
            Device("dev-1", "uuid-3.1")
                | WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL),
            Device("dev-2", "uuid-3.2")
                | WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL),
            Device("dev-3", "uuid-3.3")
                | WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL),
            Device("dev-4", "uuid-3.4")
                | WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL)
        })
    };

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        TTestExecutor executor;
        WriteTx([&] (auto db) {
            db.InitSchema();
        });
    }

    void WriteTx(auto func)
    {
        Executor.WriteTx([func = std::move(func)] (TDiskRegistryDatabase db) {
            func(db);
        });
    }

    void ReadTx(auto func)
    {
        Executor.ReadTx([func = std::move(func)] (TDiskRegistryDatabase db) {
            func(db);
        });
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryStateSuspendTest)
{
    Y_UNIT_TEST_F(ShouldSuspendDevices, TFixture)
    {
        auto makeConfig = [] (int version, auto agents) {
            return MakeConfig(version, agents)
                | WithPoolConfig(
                    "local-ssd",
                    NProto::DEVICE_POOL_KIND_LOCAL,
                    DefaultDeviceSize);
        };

        auto statePtr = TDiskRegistryStateBuilder()
                            .WithConfig(makeConfig(0, TVector{Agents[0]}))
                            .WithAgents({Agents[0]})
                            .Build();
        TDiskRegistryState& state = *statePtr;

        auto allocateDisk = [&] (auto db, auto* diskId, auto deviceCount, auto kind) {
            TDiskRegistryState::TAllocateDiskResult result;
            auto error = state.AllocateDisk(
                Now(),
                db,
                TDiskRegistryState::TAllocateDiskParams {
                    .DiskId = diskId,
                    .BlockSize = DefaultLogicalBlockSize,
                    .BlocksCount = deviceCount * DefaultDeviceSize / DefaultLogicalBlockSize,
                    .MediaKind = kind
                },
                &result);
            return error.GetCode();
        };

        auto allocNRD = [&] (auto db, auto* diskId, auto deviceCount) {
            return allocateDisk(db, diskId, deviceCount, NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
        };

        auto allocLocalSSD = [&] (auto db, auto* diskId, auto deviceCount) {
            return allocateDisk(db, diskId, deviceCount, NProto::STORAGE_MEDIA_SSD_LOCAL);
        };

        WriteTx([&] (auto db) {
            UNIT_ASSERT_VALUES_EQUAL(S_OK, allocNRD(db, "nrd0", 3));
        });

        WriteTx([&] (auto db) {
            for (auto* uuid: {"uuid-2.1", "uuid-2.2", "uuid-2.3", "uuid-2.4"}) {
                UNIT_ASSERT_SUCCESS(state.SuspendDevice(db, uuid));
            }
        });

        WriteTx([&] (auto db) {
            TVector<TString> affectedDisks;
            UNIT_ASSERT_SUCCESS(state.UpdateConfig(
                db,
                makeConfig(0, Agents),
                false,  // ignoreVersion
                affectedDisks));
            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
        });

        WriteTx([&] (auto db) {
            auto [r, error] = state.RegisterAgent(db, Agents[1], Now());
            UNIT_ASSERT_SUCCESS(error);

            UNIT_ASSERT_VALUES_EQUAL(0, r.AffectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL(0, r.DisksToReallocate.size());
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetDirtyDevices().size());

            for (const auto& d: Agents[0].GetDevices()) {
                UNIT_ASSERT(!state.IsSuspendedDevice(d.GetDeviceUUID()));
            }

            for (const auto& d: Agents[1].GetDevices()) {
                UNIT_ASSERT(state.IsSuspendedDevice(d.GetDeviceUUID()));
            }
        });

        WriteTx([&] (auto db) {
            UNIT_ASSERT_VALUES_EQUAL(
                E_BS_DISK_ALLOCATION_FAILED,
                allocNRD(db, "nrd1", 3)
            );
        });

        ReadTx([&] (auto db) {
            TVector<NProto::TSuspendedDevice> devices;
            UNIT_ASSERT(db.ReadSuspendedDevices(devices));
            UNIT_ASSERT_VALUES_EQUAL(4, devices.size());
        });

        WriteTx([&] (auto db) {
            state.ResumeDevices(Now(), db, {"uuid-2.1"});
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetDirtyDevices().size());

            // Simulate ResumeDevice method call in blockstore-client.
            auto r = state.UpdateCmsDeviceState(
                db,
                Agents[1].GetAgentId(),
                "dev-2",
                NProto::DEVICE_STATE_ONLINE,
                TInstant::FromValue(1),
                true,     // shouldResumeDevice
                false);   // dryRun
            UNIT_ASSERT_VALUES_EQUAL(S_OK, r.Error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(2, state.GetDirtyDevices().size());

            UNIT_ASSERT(state.IsSuspendedDevice("uuid-2.1"));
            UNIT_ASSERT(state.IsSuspendedDevice("uuid-2.2"));

            state.MarkDeviceAsClean(Now(), db, "uuid-2.1");
            state.MarkDeviceAsClean(Now(), db, "uuid-2.2");

            UNIT_ASSERT_VALUES_EQUAL(0, state.GetDirtyDevices().size());

            UNIT_ASSERT(!state.IsSuspendedDevice("uuid-2.1"));
            UNIT_ASSERT(!state.IsSuspendedDevice("uuid-2.2"));
            UNIT_ASSERT(state.IsSuspendedDevice("uuid-2.3"));
            UNIT_ASSERT(state.IsSuspendedDevice("uuid-2.4"));
        });

        ReadTx([&] (auto db) {
            TVector<NProto::TSuspendedDevice> devices;
            UNIT_ASSERT(db.ReadSuspendedDevices(devices));
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
        });

        WriteTx([&] (auto db) {
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                allocNRD(db, "nrd1", 3)
            );
        });

        WriteTx([&] (auto db) {
            UNIT_ASSERT_VALUES_EQUAL(
                E_BS_DISK_ALLOCATION_FAILED,
                allocLocalSSD(db, "local", 2)
            );
        });

        WriteTx([&] (auto db) {
            state.ResumeDevices(Now(), db, {"uuid-2.3", "uuid-2.4"});

            UNIT_ASSERT_VALUES_EQUAL(2, state.GetDirtyDevices().size());

            state.MarkDeviceAsClean(Now(), db, "uuid-2.3");
            state.MarkDeviceAsClean(Now(), db, "uuid-2.4");

            UNIT_ASSERT_VALUES_EQUAL(0, state.GetDirtyDevices().size());

            for (const auto& d: Agents[1].GetDevices()) {
                UNIT_ASSERT(!state.IsSuspendedDevice(d.GetDeviceUUID()));
            }
        });

        ReadTx([&] (auto db) {
            TVector<NProto::TSuspendedDevice> devices;
            UNIT_ASSERT(db.ReadSuspendedDevices(devices));
            UNIT_ASSERT_VALUES_EQUAL(0, devices.size());
        });

        WriteTx([&] (auto db) {
            UNIT_ASSERT_VALUES_EQUAL(S_OK, allocLocalSSD(db, "local", 2));
        });
    }

    Y_UNIT_TEST_F(ShouldSuspendDevicesOnCMSRequest, TFixture)
    {
        auto statePtr =
            TDiskRegistryStateBuilder()
                .WithConfig(
                    MakeConfig(0, Agents) | WithPoolConfig(
                                                "local-ssd",
                                                NProto::DEVICE_POOL_KIND_LOCAL,
                                                DefaultDeviceSize))
                .WithAgents(Agents)
                .Build();
        TDiskRegistryState& state = *statePtr;

        for (auto& agent: Agents) {
            for (const auto& d: agent.GetDevices()) {
                UNIT_ASSERT_C(!state.IsSuspendedDevice(d.GetDeviceUUID()), d);
            }
        }

        WriteTx([&] (auto db) {
            TDiskRegistryState::TAllocateDiskResult result;
            UNIT_ASSERT_SUCCESS(state.AllocateDisk(
                Now(),
                db,
                TDiskRegistryState::TAllocateDiskParams {
                    .DiskId = "local0",
                    .BlockSize = DefaultLogicalBlockSize,
                    .BlocksCount = DefaultDeviceSize / DefaultLogicalBlockSize,
                    .AgentIds = { Agents[1].GetAgentId() },
                    .MediaKind = NProto::STORAGE_MEDIA_SSD_LOCAL
                },
                &result));

            UNIT_ASSERT_VALUES_EQUAL(1, result.Devices.size());

            for (const auto& d: Agents[1].GetDevices()) {
                auto r = state.UpdateCmsDeviceState(
                    db,
                    Agents[1].GetAgentId(),
                    d.GetDeviceName(),
                    NProto::DEVICE_STATE_WARNING,
                    TInstant::FromValue(1),
                    false,  // shouldResumeDevice
                    false); // dryRun

                UNIT_ASSERT(!state.IsSuspendedDevice(d.GetDeviceUUID()));

                if (d.GetDeviceUUID() == result.Devices[0].GetDeviceUUID()) {
                    UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, r.Error.GetCode());
                    ASSERT_VECTORS_EQUAL(TVector{"local0"}, r.AffectedDisks);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(S_OK, r.Error.GetCode());
                }
            }
        });

        WriteTx(
            [&](auto db)
            {
                TVector<TString> affectedDisks;
                auto r = state.PurgeHost(
                    db,
                    Agents[1].GetAgentId(),
                    TInstant::FromValue(2),
                    false,   // dryRun
                    affectedDisks);
                UNIT_ASSERT_VALUES_EQUAL(S_OK, r.GetCode());

                for (const auto& device: Agents[1].GetDevices()) {
                    if (device.GetPoolKind() == NProto::DEVICE_POOL_KIND_LOCAL)
                    {
                        UNIT_ASSERT_C(
                            state.IsSuspendedDevice(device.GetDeviceUUID()),
                            device);
                    }
                }
            });

        WriteTx([&] (auto db) {
            TVector<TString> affectedDisks;
            TDuration timeout;
            UNIT_ASSERT_SUCCESS(state.UpdateCmsHostState(
                db,
                Agents[2].GetAgentId(),
                NProto::AGENT_STATE_WARNING,
                TInstant::FromValue(3),
                false,  // dryRun
                affectedDisks,
                timeout));

            const auto* agent = state.FindAgent(Agents[2].GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(4, agent->GetDevices().size());
        });

        for (const auto& d: Agents[0].GetDevices()) {
            UNIT_ASSERT_C(!state.IsSuspendedDevice(d.GetDeviceUUID()), d);
        }

        WriteTx([&] (auto db) {
            TVector<TString> affectedDisks;
            UNIT_ASSERT_SUCCESS(state.PurgeHost(
                db,
                Agents[2].GetAgentId(),
                TInstant::FromValue(4),
                false,  // dryRun
                affectedDisks));

            const auto* agent = state.FindAgent(Agents[2].GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(0, agent->GetDevices().size());
        });
    }
}

}   // namespace NCloud::NBlockStore::NStorage
