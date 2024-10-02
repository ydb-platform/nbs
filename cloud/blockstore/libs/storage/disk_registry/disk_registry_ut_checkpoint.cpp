#include "disk_registry.h"
#include "disk_registry_actor.h"

#include <cloud/blockstore/config/disk.pb.h>

#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_env.h>
#include <cloud/blockstore/libs/storage/testlib/ss_proxy_client.h>

#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/size_literals.h>

#include <chrono>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NDiskRegistryTest;
using namespace std::chrono_literals;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryTest)
{
    Y_UNIT_TEST(ShouldAllocateCheckpointForNonrepl)
    {
        constexpr auto LogicalBlockSize = DefaultLogicalBlockSize * 2;

        const auto agent1 = CreateAgentConfig(
            "agent-1",
            {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
                Device("dev-2", "uuid-2", "rack-1", 10_GB),
                Device("dev-3", "uuid-3", "rack-1", 10_GB),
            });

        const auto agent2 = CreateAgentConfig(
            "agent-2",
            {
                Device("dev-1", "uuid-4", "rack-1", 10_GB),
                Device("dev-2", "uuid-5", "rack-1", 10_GB),
                Device("dev-3", "uuid-6", "rack-1", 10_GB),
            });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent1, agent2 })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(
            CreateRegistryConfig(0, {agent1, agent2}));

        RegisterAgents(*runtime, 2);
        WaitForAgents(*runtime, 2);
        WaitForSecureErase(*runtime, {agent1, agent2});

        // Allocate disk.
        {
            auto response =
                diskRegistry.AllocateDisk("disk-1", 20_GB, LogicalBlockSize);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetError());
        }

        // Failed to create checkpoint from unknown disk.
        {
            diskRegistry.SendAllocateCheckpointRequest("disk-2", "checkpoint-1");
            auto response = diskRegistry.RecvAllocateCheckpointResponse();

            UNIT_ASSERT_VALUES_EQUAL_C(
                E_NOT_FOUND,
                response->GetStatus(),
                response->GetError());
        }

        // Success to create checkpoint #1
        {
            auto response =
                diskRegistry.AllocateCheckpoint("disk-1", "checkpoint-1");

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetError());

            auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(
                "disk-1-checkpoint-1",
                msg.GetShadowDiskId());

            // Check size.
            auto describeResponse =
                diskRegistry.DescribeDisk(msg.GetShadowDiskId());

            auto& describeMsg = describeResponse->Record;

            UNIT_ASSERT_VALUES_EQUAL(
                LogicalBlockSize,
                describeMsg.GetBlockSize());
            UNIT_ASSERT_VALUES_EQUAL(
                20_GB / LogicalBlockSize,
                describeMsg.GetBlocksCount());
        }

        // S_ALREADY for same checkpoint #1
        {
            auto response =
                diskRegistry.AllocateCheckpoint("disk-1", "checkpoint-1");

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_ALREADY,
                response->GetStatus(),
                response->GetError());

            auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(
                "disk-1-checkpoint-1",
                msg.GetShadowDiskId());
        }

        // Failed to create checkpoint from checkpoint.
        {
            diskRegistry.SendAllocateCheckpointRequest(
                "disk-1-checkpoint-1",
                "checkpoint-3");
            auto response = diskRegistry.RecvAllocateCheckpointResponse();

            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                response->GetStatus(),
                response->GetError());
        }

        // Success to create checkpoint #2
        {
            auto response =
                diskRegistry.AllocateCheckpoint("disk-1", "checkpoint-2");

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetError());

            auto& msg = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(
                "disk-1-checkpoint-2",
                msg.GetShadowDiskId());
        }

        // Failed to create checkpoint #3. There is not enough free space.
        {
            diskRegistry.SendAllocateCheckpointRequest(
                "disk-1",
                "checkpoint-3");
            auto response = diskRegistry.RecvAllocateCheckpointResponse();

            UNIT_ASSERT_VALUES_EQUAL_C(
                E_BS_DISK_ALLOCATION_FAILED,
                response->GetStatus(),
                response->GetError());
        }
    }

    Y_UNIT_TEST(ShouldAllocateCheckpointForMirror)
    {
        constexpr auto LogicalBlockSize = DefaultLogicalBlockSize * 2;

        const auto agent1 = CreateAgentConfig(
            "agent-1",
            {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
                Device("dev-2", "uuid-2", "rack-1", 10_GB),
                Device("dev-3", "uuid-3", "rack-1", 10_GB),
            });

        const auto agent2 = CreateAgentConfig(
            "agent-2",
            {
                Device("dev-1", "uuid-4", "rack-2", 10_GB),
                Device("dev-2", "uuid-5", "rack-2", 10_GB),
                Device("dev-3", "uuid-6", "rack-2", 10_GB),
            });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent1, agent2 })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(
            CreateRegistryConfig(0, {agent1, agent2}));

        RegisterAgents(*runtime, 2);
        WaitForAgents(*runtime, 2);
        WaitForSecureErase(*runtime, {agent1, agent2});

        // Allocate mirror disk.
        {
            auto response = diskRegistry.AllocateDisk(
                "disk-1",
                20_GB,
                LogicalBlockSize,
                "",   // placementGroupId
                0,    // placementPartitionIndex
                "",   // cloudId
                "",   // folderId
                1     // replicaCount
            );
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetError());
        }

        // Create checkpoint
        auto response =
            diskRegistry.AllocateCheckpoint("disk-1", "checkpoint-1");

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetStatus(),
            response->GetError());

        auto& msg = response->Record;
        UNIT_ASSERT_VALUES_EQUAL("disk-1-checkpoint-1", msg.GetShadowDiskId());

        // Check size.
        {
            auto response = diskRegistry.DescribeDisk(msg.GetShadowDiskId());

            auto& describeMsg = response->Record;

            UNIT_ASSERT_VALUES_EQUAL(
                LogicalBlockSize,
                describeMsg.GetBlockSize());
            UNIT_ASSERT_VALUES_EQUAL(
                20_GB / LogicalBlockSize,
                describeMsg.GetBlocksCount());
        }
    }

    Y_UNIT_TEST(ShouldDeallocateCheckpoint)
    {
        const auto agent1 = CreateAgentConfig(
            "agent-1",
            {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
                Device("dev-2", "uuid-2", "rack-1", 10_GB),
            });

        const auto agent2 = CreateAgentConfig(
            "agent-2",
            {
                Device("dev-1", "uuid-4", "rack-1", 10_GB),
                Device("dev-2", "uuid-5", "rack-1", 10_GB),
            });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent1, agent2 })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(
            CreateRegistryConfig(0, {agent1, agent2}));

        RegisterAgents(*runtime, 2);
        WaitForAgents(*runtime, 2);
        WaitForSecureErase(*runtime, {agent1, agent2});

        // Allocate disk.
        {
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetError());
        }

        // Create checkpoint
        auto allocateResponse =
            diskRegistry.AllocateCheckpoint("disk-1", "checkpoint-1");

        auto& allocateMsg = allocateResponse->Record;

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            allocateResponse->GetStatus(),
            allocateResponse->GetError());

        // Deallocate checkpoint
        auto deallocateResponse =
            diskRegistry.DeallocateCheckpoint("disk-1", "checkpoint-1");

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            deallocateResponse->GetStatus(),
            deallocateResponse->GetError());

        // Check that the checkpoint disk is destroyed
        diskRegistry.SendDescribeDiskRequest(allocateMsg.GetShadowDiskId());
        auto describeResponse = diskRegistry.RecvDescribeDiskResponse();

        UNIT_ASSERT_VALUES_EQUAL_C(
            E_NOT_FOUND,
            describeResponse->GetStatus(),
            describeResponse->GetError());

        // Check that the released devices have been erased.
        WaitForSecureErase(*runtime, 2);
    }

    Y_UNIT_TEST(ShouldDeallocateCheckpointOnSourceDiskDelete)
    {
        const auto agent1 = CreateAgentConfig(
            "agent-1",
            {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
                Device("dev-2", "uuid-2", "rack-1", 10_GB),
            });

        const auto agent2 = CreateAgentConfig(
            "agent-2",
            {
                Device("dev-1", "uuid-4", "rack-1", 10_GB),
                Device("dev-2", "uuid-5", "rack-1", 10_GB),
            });

        auto runtime =
            TTestRuntimeBuilder().WithAgents({agent1, agent2}).Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent1, agent2}));

        RegisterAgents(*runtime, 2);
        WaitForAgents(*runtime, 2);
        WaitForSecureErase(*runtime, {agent1, agent2});

        // Allocate disk.
        {
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetError());
        }

        TString shadowDiskId;
        // Create checkpoint
        {
            auto response =
                diskRegistry.AllocateCheckpoint("disk-1", "checkpoint-1");

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetError());

            shadowDiskId = response->Record.GetShadowDiskId();
        }

        // Check that the checkpoint disk is exists
        {
            auto response = diskRegistry.DescribeDisk(shadowDiskId);

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetError());
        }

        // Deallocate disk
        {
            diskRegistry.MarkDiskForCleanup("disk-1");
            auto response = diskRegistry.DeallocateDisk("disk-1");

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetError());
        }

        // Check that the checkpoint disk is destroyed along with the source
        // disk
        {
            diskRegistry.SendDescribeDiskRequest(shadowDiskId);
            auto response = diskRegistry.RecvDescribeDiskResponse();

            UNIT_ASSERT_VALUES_EQUAL_C(
                E_NOT_FOUND,
                response->GetStatus(),
                response->GetError());
        }
    }

    Y_UNIT_TEST(ShouldReallocateShadowDiskOnSourceDiskReallocation)
    {
        const auto agent1 = CreateAgentConfig(
            "agent-1",
            {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
                Device("dev-2", "uuid-2", "rack-1", 10_GB),
            });

        const auto agent2 = CreateAgentConfig(
            "agent-2",
            {
                Device("dev-1", "uuid-4", "rack-1", 10_GB),
                Device("dev-2", "uuid-5", "rack-1", 10_GB),
            });

        auto runtime =
            TTestRuntimeBuilder().WithAgents({agent1, agent2}).Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent1, agent2}));

        RegisterAgents(*runtime, 2);
        WaitForAgents(*runtime, 2);
        WaitForSecureErase(*runtime, {agent1, agent2});

        // Allocate disk.
        {
            auto response = diskRegistry.AllocateDisk("disk-1", 10_GB);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetError());
        }

        TString shadowDiskId;
        // Create checkpoint
        {
            auto response =
                diskRegistry.AllocateCheckpoint("disk-1", "checkpoint-1");

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetError());

            shadowDiskId = response->Record.GetShadowDiskId();
        }

        // Check the size of the shadow disk
        {
            auto response = diskRegistry.DescribeDisk(shadowDiskId);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetError());

            auto checkpointSize = response->Record.GetBlocksCount() *
                                  response->Record.GetBlockSize();
            UNIT_ASSERT_VALUES_EQUAL(10_GB, checkpointSize);
        }

        // Reallocate disk
        {
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetError());
        }

        // Check that the size of the shadow disk has also changed
        {
            auto response = diskRegistry.DescribeDisk(shadowDiskId);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetError());

            auto checkpointSize = response->Record.GetBlocksCount() *
                                  response->Record.GetBlockSize();
            UNIT_ASSERT_VALUES_EQUAL(20_GB, checkpointSize);
        }
    }

    Y_UNIT_TEST(ShouldChangeStateForCheckpoint)
    {
        const auto agent1 = CreateAgentConfig(
            "agent-1",
            {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
                Device("dev-2", "uuid-2", "rack-1", 10_GB),
            });

        const auto agent2 = CreateAgentConfig(
            "agent-2",
            {
                Device("dev-1", "uuid-4", "rack-1", 10_GB),
                Device("dev-2", "uuid-5", "rack-1", 10_GB),
            });

        auto runtime =
            TTestRuntimeBuilder().WithAgents({agent1, agent2}).Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, {agent1, agent2}));

        RegisterAgents(*runtime, 2);
        WaitForAgents(*runtime, 2);
        WaitForSecureErase(*runtime, {agent1, agent2});

        // Allocate disk.
        {
            auto response = diskRegistry.AllocateDisk("disk-1", 20_GB);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetError());
        }

        // Create checkpoint
        {
            auto response =
                diskRegistry.AllocateCheckpoint("disk-1", "checkpoint-1");

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetError());
        }

        // Check initial checkpoint data state
        {
            auto response =
                diskRegistry.GetCheckpointDataState("disk-1", "checkpoint-1");

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetError());

            UNIT_ASSERT_EQUAL(
                NProto::ECheckpointState::CHECKPOINT_STATE_CREATING,
                response->Record.GetCheckpointState());
        }

        // Update checkpoint data state to CHECKPOINT_STATE_OK
        {
            auto response = diskRegistry.SetCheckpointDataState(
                "disk-1",
                "checkpoint-1",
                NProto::ECheckpointState::CHECKPOINT_STATE_OK);

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetError());
        }

        // Check checkpoint data state changed
        {
            auto response =
                diskRegistry.GetCheckpointDataState("disk-1", "checkpoint-1");

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetError());

            UNIT_ASSERT_EQUAL(
                NProto::ECheckpointState::CHECKPOINT_STATE_OK,
                response->Record.GetCheckpointState());
        }

        // Update checkpoint data state to CHECKPOINT_STATE_ERROR
        {
            auto response = diskRegistry.SetCheckpointDataState(
                "disk-1",
                "checkpoint-1",
                NProto::ECheckpointState::CHECKPOINT_STATE_ERROR);

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetError());
        }

        // Check checkpoint data state changed
        {
            auto response =
                diskRegistry.GetCheckpointDataState("disk-1", "checkpoint-1");

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetError());

            UNIT_ASSERT_EQUAL(
                NProto::ECheckpointState::CHECKPOINT_STATE_ERROR,
                response->Record.GetCheckpointState());
        }

        // Set same state once again
        {
            auto response = diskRegistry.SetCheckpointDataState(
                "disk-1",
                "checkpoint-1",
                NProto::ECheckpointState::CHECKPOINT_STATE_ERROR);

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_ALREADY,
                response->GetStatus(),
                response->GetError());
        }

        // Get state from unknown checkpoint
        {
            diskRegistry.SendGetCheckpointDataStateRequest(
                "disk-1",
                "checkpoint-x");

            auto response = diskRegistry.RecvGetCheckpointDataStateResponse();

            UNIT_ASSERT_VALUES_EQUAL_C(
                E_NOT_FOUND,
                response->GetStatus(),
                response->GetError());
        }

        // Set state for unknown checkpoint
        {
            diskRegistry.SendSetCheckpointDataStateRequest(
                "disk-1",
                "checkpoint-x",
                NProto::ECheckpointState::CHECKPOINT_STATE_ERROR);

            auto response = diskRegistry.RecvSetCheckpointDataStateResponse();

            UNIT_ASSERT_VALUES_EQUAL_C(
                E_NOT_FOUND,
                response->GetStatus(),
                response->GetError());
        }

    }
}

}   // namespace NCloud::NBlockStore::NStorage
