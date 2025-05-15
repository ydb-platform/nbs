#include "disk_registry_database.h"
#include "disk_registry_state.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_state.h>
#include <cloud/blockstore/libs/storage/testlib/test_executor.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/guid.h>
#include <util/generic/size_literals.h>

#include <google/protobuf/util/message_differencer.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NDiskRegistryStateTest;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 AvailableBlockSizes[] =
    {4_KB, 8_KB, 16_KB, 32_KB, 64_KB, 128_KB};

std::unique_ptr<TDiskRegistryState> MakeDiskRegistryState()
{
    auto agentConfig1 = AgentConfig(
        1,
        {
            Device("dev-1", "uuid-1", "rack-1"),
            Device("dev-2", "uuid-2", "rack-1"),
            Device("dev-3", "uuid-3", "rack-1"),
            Device("dev-4", "uuid-4", "rack-1"),
            Device("dev-5", "uuid-5", "rack-1"),
            Device("dev-6", "uuid-6", "rack-1"),
        });

    auto agentConfig2 = AgentConfig(
        2,
        {
            Device("dev-7", "uuid-7", "rack-2"),
            Device("dev-8", "uuid-8", "rack-2"),
            Device("dev-9", "uuid-9", "rack-2"),
        });

    return TDiskRegistryStateBuilder()
        .WithKnownAgents({
            agentConfig1,
            agentConfig2,
        })
        .Build();
}

auto ChangeAgentState(
    TDiskRegistryState& state,
    TDiskRegistryDatabase db,
    const NProto::TAgentConfig& config,
    NProto::EAgentState newState)
{
    TVector<TString> affectedDisks;

    auto error = state.UpdateAgentState(
        db,
        config.GetAgentId(),
        newState,
        TInstant::Now(),
        "test",
        affectedDisks);
    UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

    return affectedDisks;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryStateCheckpointTest)
{
    using TCreateDiskFunc = std::function<
        void(TDiskRegistryState & state, TDiskRegistryDatabase db)>;

    void DoShouldCreateCheckpointForDiskRegistryBasedDisk(
        TCreateDiskFunc createDisk)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        auto statePtr = MakeDiskRegistryState();
        TDiskRegistryState& state = *statePtr;

        // Create source disk
        executor.WriteTx([&](TDiskRegistryDatabase db)
                         { createDisk(state, std::move(db)); });

        // Create checkpoint
        TString shadowDiskId;
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TDeviceConfig> devices;
                auto error = AllocateCheckpoint(
                    Now(),
                    db,
                    state,
                    "disk-1",
                    "checkpoint-1",
                    &shadowDiskId,
                    &devices);
                UNIT_ASSERT_SUCCESS(error);
                UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
                UNIT_ASSERT_UNEQUAL("", shadowDiskId);
                UNIT_ASSERT_EQUAL(
                    NProto::EDiskState::DISK_STATE_ONLINE,
                    state.GetDiskState(shadowDiskId));
            });

        // Validate created checkpoint
        {
            TDiskInfo sourceDiskInfo;
            auto error = state.GetDiskInfo("disk-1", sourceDiskInfo);
            UNIT_ASSERT_SUCCESS(error);

            TDiskInfo checkpointDiskInfo;
            error = state.GetDiskInfo(shadowDiskId, checkpointDiskInfo);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(
                sourceDiskInfo.GetBlocksCount(),
                checkpointDiskInfo.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                sourceDiskInfo.LogicalBlockSize,
                checkpointDiskInfo.LogicalBlockSize);
            UNIT_ASSERT_VALUES_EQUAL(
                sourceDiskInfo.FolderId,
                checkpointDiskInfo.FolderId);
            UNIT_ASSERT_VALUES_EQUAL(
                sourceDiskInfo.CloudId,
                checkpointDiskInfo.CloudId);

            NProto::ECheckpointState checkpointState{};
            error = state.GetCheckpointDataState(
                "disk-1",
                "checkpoint-1",
                &checkpointState);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_EQUAL(
                NProto::ECheckpointState::CHECKPOINT_STATE_CREATING,
                checkpointState);
        }

        // S_ALREADY for same checkpoint.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TDeviceConfig> devices;
                TString shadowDiskId;
                auto error = AllocateCheckpoint(
                    Now(),
                    db,
                    state,
                    "disk-1",
                    "checkpoint-1",
                    &shadowDiskId,
                    &devices);
                UNIT_ASSERT_EQUAL(S_ALREADY, error.GetCode());
            });
    }

    Y_UNIT_TEST(ShouldCreateCheckpointForDiskRegistryBasedDisk)
    {
        for (ui32 blockSize: AvailableBlockSizes) {
            DoShouldCreateCheckpointForDiskRegistryBasedDisk(
                [&](TDiskRegistryState& state, TDiskRegistryDatabase db)
                {
                    TVector<TDeviceConfig> devices;
                    auto error = AllocateDisk(
                        db,
                        state,
                        "disk-1",
                        "",   // placementGroupId
                        0,    // placementPartitionIndex
                        30_GB,
                        devices,
                        TInstant::Seconds(100),
                        NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                        TString(),   // poolName
                        blockSize);
                    UNIT_ASSERT_SUCCESS(error);
                });
        }
    }

    Y_UNIT_TEST(ShouldCreateCheckpointForMirrorDisk8192)
    {
        for (ui32 blockSize: AvailableBlockSizes) {
            DoShouldCreateCheckpointForDiskRegistryBasedDisk(
                [&](TDiskRegistryState& state, TDiskRegistryDatabase db)
                {
                    TVector<TDeviceConfig> devices;
                    TVector<TVector<TDeviceConfig>> replicas;
                    TVector<NProto::TDeviceMigration> migrations;
                    TVector<TString> deviceReplacementIds;
                    auto error = AllocateMirroredDisk(
                        db,
                        state,
                        "disk-1",
                        30_GB,
                        1,
                        devices,
                        replicas,
                        migrations,
                        deviceReplacementIds,
                        TInstant::Seconds(100),
                        NProto::STORAGE_MEDIA_SSD_MIRROR2,
                        blockSize);
                    UNIT_ASSERT_SUCCESS(error);
                });
        }
    }

    Y_UNIT_TEST(ShouldNotCreateWhenNotEnoughFreeSpace)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        auto statePtr = MakeDiskRegistryState();
        TDiskRegistryState& state = *statePtr;

        // Create source disk
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TDeviceConfig> devices;
                TVector<TVector<TDeviceConfig>> replicas;
                TVector<NProto::TDeviceMigration> migrations;
                TVector<TString> deviceReplacementIds;
                auto error = AllocateDisk(
                    db,
                    state,
                    "disk-1",
                    "",   // placementGroupId
                    0,    // placementPartitionIndex
                    60_GB,
                    devices);
                UNIT_ASSERT_SUCCESS(error);
            });

        // Try create checkpoint
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TDeviceConfig> devices;
                TString shadowDiskId;
                auto error = AllocateCheckpoint(
                    Now(),
                    db,
                    state,
                    "disk-1",
                    "checkpoint-1",
                    &shadowDiskId,
                    &devices);
                UNIT_ASSERT_VALUES_EQUAL(
                    E_BS_DISK_ALLOCATION_FAILED,
                    error.GetCode());
            });

        executor.ReadTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                // A shadow disk that could not be created should not be
                // included in the list of broken disks.
                TVector<TBrokenDiskInfo> diskInfos;
                UNIT_ASSERT(db.ReadBrokenDisks(diskInfos));
                UNIT_ASSERT_VALUES_EQUAL(0, diskInfos.size());
            });
    }

    Y_UNIT_TEST(ShouldCreateMultipleCheckpoints)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        auto statePtr = MakeDiskRegistryState();
        TDiskRegistryState& state = *statePtr;

        // Create source disk
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TDeviceConfig> devices;
                TVector<TVector<TDeviceConfig>> replicas;
                TVector<NProto::TDeviceMigration> migrations;
                TVector<TString> deviceReplacementIds;
                auto error = AllocateDisk(
                    db,
                    state,
                    "disk-1",
                    "",   // placementGroupId
                    0,    // placementPartitionIndex
                    10_GB,
                    devices);
                UNIT_ASSERT_SUCCESS(error);
            });

        // create checkpoint #1
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TDeviceConfig> devices;
                TString shadowDiskId;
                auto error = AllocateCheckpoint(
                    Now(),
                    db,
                    state,
                    "disk-1",
                    "checkpoint-1",
                    &shadowDiskId,
                    &devices);
                UNIT_ASSERT_SUCCESS(error);
            });

        // create checkpoint #2
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TDeviceConfig> devices;
                TString shadowDiskId;
                auto error = AllocateCheckpoint(
                    Now(),
                    db,
                    state,
                    "disk-1",
                    "checkpoint-2",
                    &shadowDiskId,
                    &devices);
                UNIT_ASSERT_SUCCESS(error);
            });
    }

    Y_UNIT_TEST(ShouldDeallocateCheckpoints)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        auto statePtr = MakeDiskRegistryState();
        TDiskRegistryState& state = *statePtr;

        // Create source disk
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TDeviceConfig> devices;
                TVector<TVector<TDeviceConfig>> replicas;
                TVector<NProto::TDeviceMigration> migrations;
                TVector<TString> deviceReplacementIds;
                auto error = AllocateDisk(
                    db,
                    state,
                    "disk-1",
                    "",   // placementGroupId
                    0,    // placementPartitionIndex
                    40_GB,
                    devices);
                UNIT_ASSERT_SUCCESS(error);
            });

        // create checkpoint #1
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TDeviceConfig> devices;
                TString shadowDiskId;
                auto error = AllocateCheckpoint(
                    Now(),
                    db,
                    state,
                    "disk-1",
                    "checkpoint-1",
                    &shadowDiskId,
                    &devices);
                UNIT_ASSERT_SUCCESS(error);
            });

        // delete checkpoint #1
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TDeviceConfig> devices;
                TString shadowDiskId;
                auto error = state.DeallocateCheckpoint(
                    db,
                    "disk-1",
                    "checkpoint-1",
                    &shadowDiskId);
                UNIT_ASSERT_SUCCESS(error);
            });

        // double deletion of the checkpoint #1
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TDeviceConfig> devices;
                TString shadowDiskId;
                auto error = state.DeallocateCheckpoint(
                    db,
                    "disk-1",
                    "checkpoint-1",
                    &shadowDiskId);
                UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, error.GetCode());
            });

        // create checkpoint #2
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto dd = state.GetDirtyDevices();
                UNIT_ASSERT_VALUES_EQUAL(4, dd.size());
                for (const auto& device: dd) {
                    state.MarkDeviceAsClean(Now(), db, device.GetDeviceUUID());
                }
            });

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TDeviceConfig> devices;
                TString shadowDiskId;
                auto error = AllocateCheckpoint(
                    Now(),
                    db,
                    state,
                    "disk-1",
                    "checkpoint-2",
                    &shadowDiskId,
                    &devices);
                UNIT_ASSERT_SUCCESS(error);
            });
    }

    Y_UNIT_TEST(ShouldDeallocateCheckpointOnSourceDiskDelete)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        auto statePtr = MakeDiskRegistryState();
        TDiskRegistryState& state = *statePtr;

        // Create source disk
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TDeviceConfig> devices;
                TVector<TVector<TDeviceConfig>> replicas;
                TVector<NProto::TDeviceMigration> migrations;
                TVector<TString> deviceReplacementIds;
                auto error = AllocateDisk(
                    db,
                    state,
                    "disk-1",
                    "",   // placementGroupId
                    0,    // placementPartitionIndex
                    40_GB,
                    devices);
                UNIT_ASSERT_SUCCESS(error);
            });

        // create checkpoint #1
        TString shadowDiskId;
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TDeviceConfig> devices;
                auto error = AllocateCheckpoint(
                    Now(),
                    db,
                    state,
                    "disk-1",
                    "checkpoint-1",
                    &shadowDiskId,
                    &devices);
                UNIT_ASSERT_SUCCESS(error);
            });
        {
            TDiskInfo checkpointDiskInfo;
            auto error =
                state.GetDiskInfo(shadowDiskId, checkpointDiskInfo);
            UNIT_ASSERT_SUCCESS(error);
        }

        // delete source disk
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TDeviceConfig> devices;
                TString shadowDiskId;
                auto error = state.DeallocateCheckpoint(
                    db,
                    "disk-1",
                    "checkpoint-1",
                    &shadowDiskId);
                UNIT_ASSERT_SUCCESS(error);
            });

        // Checkpoints disk deleted.
        {
            TDiskInfo checkpointDiskInfo;
            auto error =
                state.GetDiskInfo(shadowDiskId, checkpointDiskInfo);
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, error.GetCode());
        }
    }

    Y_UNIT_TEST(ShouldChangeStateForCheckpoint)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        auto statePtr = MakeDiskRegistryState();
        TDiskRegistryState& state = *statePtr;

        // Create source disk
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TDeviceConfig> devices;
                TVector<TVector<TDeviceConfig>> replicas;
                TVector<NProto::TDeviceMigration> migrations;
                TVector<TString> deviceReplacementIds;
                auto error = AllocateDisk(
                    db,
                    state,
                    "disk-1",
                    "",   // placementGroupId
                    0,    // placementPartitionIndex
                    40_GB,
                    devices);
                UNIT_ASSERT_SUCCESS(error);
            });

        // create checkpoint
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TString shadowDiskId;
                TVector<TDeviceConfig> devices;
                auto error = AllocateCheckpoint(
                    Now(),
                    db,
                    state,
                    "disk-1",
                    "checkpoint-1",
                    &shadowDiskId,
                    &devices);
                UNIT_ASSERT_SUCCESS(error);
            });

        // Get initial checkpoint state
        {
            NProto::ECheckpointState checkpointState{};
            auto error = state.GetCheckpointDataState(
                "disk-1",
                "checkpoint-1",
                &checkpointState);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_EQUAL(
                NProto::ECheckpointState::CHECKPOINT_STATE_CREATING,
                checkpointState);
        }

        // Change checkpoint state
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto error = state.SetCheckpointDataState(
                    Now(),
                    db,
                    "disk-1",
                    "checkpoint-1",
                    NProto::ECheckpointState::CHECKPOINT_STATE_OK);
                UNIT_ASSERT_SUCCESS(error);
            });

        // Validate checkpoint state changed
        {
            NProto::ECheckpointState checkpointState{};
            auto error = state.GetCheckpointDataState(
                "disk-1",
                "checkpoint-1",
                &checkpointState);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_EQUAL(
                NProto::ECheckpointState::CHECKPOINT_STATE_OK,
                checkpointState);
        }

        // Set same state once again
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto error = state.SetCheckpointDataState(
                    Now(),
                    db,
                    "disk-1",
                    "checkpoint-1",
                    NProto::ECheckpointState::CHECKPOINT_STATE_OK);
                UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, error.GetCode());
            });

        // Get state from unknown checkpoint
        {
            NProto::ECheckpointState checkpointState{};
            auto error = state.GetCheckpointDataState(
                "disk-1",
                "checkpoint-2",
                &checkpointState);
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, error.GetCode());
        }

        // Set state for unknown checkpoint
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto error = state.SetCheckpointDataState(
                    Now(),
                    db,
                    "disk-1",
                    "checkpoint-2",
                    NProto::ECheckpointState::CHECKPOINT_STATE_OK);
                UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, error.GetCode());
            });
    }

    Y_UNIT_TEST(ShouldNotMigrateShadowDiskDevices)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        auto statePtr = MakeDiskRegistryState();
        TDiskRegistryState& state = *statePtr;

        // Create source disk
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TDeviceConfig> devices;
                TVector<TVector<TDeviceConfig>> replicas;
                TVector<NProto::TDeviceMigration> migrations;
                TVector<TString> deviceReplacementIds;
                auto error = AllocateDisk(
                    db,
                    state,
                    "disk-1",
                    "",   // placementGroupId
                    0,    // placementPartitionIndex
                    40_GB,
                    devices);
                UNIT_ASSERT_SUCCESS(error);
            });

        // create checkpoint
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TString shadowDiskId;
                TVector<TDeviceConfig> devices;
                auto error = AllocateCheckpoint(
                    Now(),
                    db,
                    state,
                    "disk-1",
                    "checkpoint-1",
                    &shadowDiskId,
                    &devices);
                UNIT_ASSERT_SUCCESS(error);
            });

        const auto checkpointId =
            TCheckpointInfo::MakeId("disk-1", "checkpoint-1");
        const auto& secondAgent = state.GetAgents()[1];

        // Change state of second agent where shadow disk
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                auto affectedDisks = ChangeAgentState(
                    state,
                    db,
                    secondAgent,
                    NProto::AGENT_STATE_WARNING);

                // State of shadow disk changed to "warning"
                UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
                UNIT_ASSERT_VALUES_EQUAL(checkpointId, affectedDisks[0]);
            });

        // No migrations started
        UNIT_ASSERT(state.IsMigrationListEmpty());

        // Source disk notified.
        UNIT_ASSERT(state.GetDisksToReallocate().FindPtr("disk-1"));
        UNIT_ASSERT(!state.GetDisksToReallocate().FindPtr(checkpointId));
    }

    Y_UNIT_TEST(ShouldNotifySourceDiskWhenNodeIdForShadowDiskChanged)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        auto statePtr = MakeDiskRegistryState();
        TDiskRegistryState& state = *statePtr;

        // Create source disk
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TDeviceConfig> devices;
                TVector<TVector<TDeviceConfig>> replicas;
                TVector<NProto::TDeviceMigration> migrations;
                TVector<TString> deviceReplacementIds;
                auto error = AllocateDisk(
                    db,
                    state,
                    "disk-1",
                    "",   // placementGroupId
                    0,    // placementPartitionIndex
                    40_GB,
                    devices);
                UNIT_ASSERT_SUCCESS(error);
            });

        // create checkpoint
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TString shadowDiskId;
                TVector<TDeviceConfig> devices;
                auto error = AllocateCheckpoint(
                    Now(),
                    db,
                    state,
                    "disk-1",
                    "checkpoint-1",
                    &shadowDiskId,
                    &devices);
                UNIT_ASSERT_SUCCESS(error);
            });
        const auto checkpointId =
            TCheckpointInfo::MakeId("disk-1", "checkpoint-1");

        // Change NodeId for second disk agent
        auto secondAgent = state.GetAgents()[1];
        secondAgent.SetNodeId(42);
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                UNIT_ASSERT_SUCCESS(
                    state.RegisterAgent(db, secondAgent, Now()).GetError());
            });

        // Source disk notified.
        UNIT_ASSERT(state.GetDisksToReallocate().FindPtr("disk-1"));
        UNIT_ASSERT(!state.GetDisksToReallocate().FindPtr(checkpointId));
    }

    Y_UNIT_TEST(ShouldNotifyVolumeOnce)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        auto agentConfig = AgentConfig(
            1,
            {
                Device("dev-1", "uuid-1", "rack-1"),
                Device("dev-2", "uuid-2", "rack-1"),
            });

        auto statePtr =
            TDiskRegistryStateBuilder().WithKnownAgents({agentConfig}).Build();
        TDiskRegistryState& state = *statePtr;

        // Create source disk
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TDeviceConfig> devices;
                TVector<TVector<TDeviceConfig>> replicas;
                TVector<NProto::TDeviceMigration> migrations;
                TVector<TString> deviceReplacementIds;
                auto error = AllocateDisk(
                    db,
                    state,
                    "disk-1",
                    "",   // placementGroupId
                    0,    // placementPartitionIndex
                    10_GB,
                    devices);
                UNIT_ASSERT_SUCCESS(error);
            });

        // create checkpoint
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TString shadowDiskId;
                TVector<TDeviceConfig> devices;
                auto error = AllocateCheckpoint(
                    Now(),
                    db,
                    state,
                    "disk-1",
                    "checkpoint-1",
                    &shadowDiskId,
                    &devices);
                UNIT_ASSERT_SUCCESS(error);
            });
        const auto checkpointId =
            TCheckpointInfo::MakeId("disk-1", "checkpoint-1");

        // Change NodeId for disk agent
        auto agent = state.GetAgents()[0];
        agent.SetNodeId(42);
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable {
                UNIT_ASSERT_SUCCESS(
                    state.RegisterAgent(db, agent, Now()).GetError());
            });

        // Source disk notified.
        UNIT_ASSERT(state.GetDisksToReallocate().FindPtr("disk-1"));
        UNIT_ASSERT(!state.GetDisksToReallocate().FindPtr(checkpointId));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
