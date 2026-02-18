#include "tablet.h"

#include <cloud/filestore/libs/storage/testlib/helpers.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_Checkpoints)
{
    Y_UNIT_TEST(ShouldStoreCheckpoints)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        tablet.CreateCheckpoint("test");
        tablet.DestroyCheckpoint("test");
    }

    Y_UNIT_TEST(ShouldReadNodesFromCheckpoint)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session1");

        auto id1 = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test1"));
        auto id2 = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test2"));
        auto id3 = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test3"));

        tablet.AccessNode(id1);
        tablet.AccessNode(id2);
        tablet.AccessNode(id3);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetUsedNodesCount(), 3);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetCheckpointNodesCount(), 0);
        }

        tablet.CreateCheckpoint("checkpoint");
        tablet.AssertUnlinkNodeFailed(RootNodeId, "test2", true);
        tablet.UnlinkNode(RootNodeId, "test2", false);

        tablet.AccessNode(id1);
        tablet.AssertAccessNodeFailed(id2);
        tablet.AccessNode(id3);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetUsedNodesCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetCheckpointNodesCount(), 2);   // TODO
        }

        tablet.InitSession("client", "session2", "checkpoint");

        tablet.AccessNode(id1);
        tablet.AccessNode(id2);
        tablet.AccessNode(id3);
    }

    Y_UNIT_TEST(ShouldReadDataFromCheckpoint)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));

        ui64 handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, 4_KB, 'a');
        tablet.WriteData(handle, 100, 10, 'A');
        tablet.Flush();

        TString expected1;
        expected1.ReserveAndResize(4_KB);
        memset(expected1.begin(), 'a', 4_KB);
        memset(expected1.begin() + 100, 'A', 10);

        {
            auto response = tablet.ReadData(handle, 0, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected1, buffer);
        }

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetCheckpointBlobsCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetCheckpointBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBytesCount(), 10);
        }

        tablet.CreateCheckpoint("checkpoint1");

        tablet.WriteData(handle, 0, 4_KB, 'b');
        tablet.Flush();

        TString expected2;
        expected2.ReserveAndResize(4_KB);
        memset(expected2.begin(), 'b', 4_KB);

        ui32 rangeId = GetMixedRangeIndex(id, 0);
        tablet.Compaction(rangeId);

        {
            auto response = tablet.ReadData(handle, 0, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected2, buffer);
        }

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetCheckpointBlobsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetCheckpointBlocksCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBytesCount(), 10);
        }

        tablet.CreateCheckpoint("checkpoint2");

        tablet.WriteData(handle, 100, 10, 'B');

        TString expected = expected2;
        memset(expected.begin() + 100, 'B', 10);

        tablet.InitSession("client", "session1", "checkpoint1");
        ui64 handle1 = CreateHandle(tablet, id);

        {
            auto response = tablet.ReadData(handle1, 0, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected1, buffer);
        }

        tablet.InitSession("client", "session2", "checkpoint2");
        ui64 handle2 = CreateHandle(tablet, id);

        {
            auto response = tablet.ReadData(handle2, 0, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected2, buffer);
        }

        tablet.InitSession("client", "session");
        handle = CreateHandle(tablet, id);

        {
            auto response = tablet.ReadData(handle, 0, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.DestroyHandle(handle1);
        tablet.DestroyHandle(handle2);

        tablet.DestroyCheckpoint("checkpoint1");
        tablet.DestroyCheckpoint("checkpoint2");

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetCheckpointBlobsCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetCheckpointBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageBlocksCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBytesCount(), 20);
        }
    }

    Y_UNIT_TEST(ShouldStoreAndFlushFreshBytesFromMultipleNodesAndBlocks)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id1 = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test1"));
        auto id2 = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test2"));
        ui64 handle1 = CreateHandle(tablet, id1);
        ui64 handle2 = CreateHandle(tablet, id2);

        // writing fresh blocks, then bytes
        tablet.WriteData(handle1, 4_KB, 4_KB, 'a');
        tablet.WriteData(handle1, 4_KB + 100, 10, 'A');
        tablet.WriteData(handle2, 4_KB, 4_KB, 'b');
        tablet.WriteData(handle2, 4_KB + 100, 10, 'B');
        tablet.FlushBytes();

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBytesCount(), 0);
        }

        TString expected1;
        expected1.ReserveAndResize(4_KB);
        memset(expected1.begin(), 'a', 4_KB);
        memset(expected1.begin() + 100, 'A', 10);

        TString expected2;
        expected2.ReserveAndResize(4_KB);
        memset(expected2.begin(), 'b', 4_KB);
        memset(expected2.begin() + 100, 'B', 10);

        {
            auto response = tablet.ReadData(handle1, 4_KB, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected1, buffer);
        }

        {
            auto response = tablet.ReadData(handle2, 4_KB, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected2, buffer);
        }

        // Flushed blob will contain only garbage blocks and won't be added at all
        tablet.Flush();

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBytesCount(), 0);
        }

        {
            auto response = tablet.ReadData(handle1, 4_KB, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected1, buffer);
        }

        {
            auto response = tablet.ReadData(handle2, 4_KB, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected2, buffer);
        }

        tablet.CreateCheckpoint("checkpoint");

        tablet.WriteData(handle1, 4_KB + 100, 10, 'C');
        tablet.WriteData(handle2, 4_KB + 100, 10, 'D');
        tablet.FlushBytes();

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 4);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBytesCount(), 0);
        }

        TString expected1c = expected1;
        memset(expected1.begin() + 100, 'C', 10);

        TString expected2c = expected2;
        memset(expected2.begin() + 100, 'D', 10);

        {
            auto response = tablet.ReadData(handle1, 4_KB, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected1, buffer);
        }

        {
            auto response = tablet.ReadData(handle2, 4_KB, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected2, buffer);
        }

        tablet.InitSession("client", "sessionc", "checkpoint");
        handle1 = CreateHandle(tablet, id1);
        handle2 = CreateHandle(tablet, id2);

        {
            auto response = tablet.ReadData(handle1, 4_KB, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected1c, buffer);
        }

        {
            auto response = tablet.ReadData(handle2, 4_KB, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected2c, buffer);
        }

        tablet.DestroyHandle(handle1);
        tablet.DestroyHandle(handle2);
    }

    Y_UNIT_TEST(ShouldHandleCommitIdOverflowInCreateCheckpoint)
    {
        const ui32 block = 4_KB;
        const ui32 maxTabletStep = 8;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxTabletStep(maxTabletStep);

        TTestEnv env({}, std::move(storageConfig));

        const ui32 nodeIdx = env.AddDynamicNode();

        TTabletRebootTracker rebootTracker;
        env.GetRuntime().SetEventFilter(rebootTracker.GetEventFilter());

        const ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        const auto id =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        ui32 createCheckpointRejectedCount = 0;

        auto reconnectAndRecreateHandleIfNeeded = [&](bool createHandle = true)
        {
            if (rebootTracker.IsPipeDestroyed()) {
                tablet.ReconnectPipe();
                tablet.WaitReady();
                tablet.RecoverSession();
                if (createHandle) {
                    handle = CreateHandle(tablet, id);
                }
                rebootTracker.ClearPipeDestroyed();
            }
        };

        TVector<std::pair<TString, char>> successfulCheckpoints;
        const int targetSuccessfulCheckpoints = 4;

        for (int i = 0; i < targetSuccessfulCheckpoints;) {
            tablet.SendWriteDataRequest(handle, 0, block, 'a' + i);
            auto writeResponse = tablet.RecvWriteDataResponse();
            reconnectAndRecreateHandleIfNeeded();

            if (HasError(writeResponse->GetError())) {
                UNIT_ASSERT_VALUES_EQUAL(
                    E_REJECTED,
                    writeResponse->GetError().GetCode());
                continue;
            }

            TString checkpointId = TStringBuilder() << "checkpoint_" << i;

            tablet.SendCreateCheckpointRequest(checkpointId);
            auto checkpointResponse = tablet.RecvCreateCheckpointResponse();
            reconnectAndRecreateHandleIfNeeded();

            if (HasError(checkpointResponse->GetError())) {
                UNIT_ASSERT_VALUES_EQUAL(
                    E_REJECTED,
                    checkpointResponse->GetError().GetCode());
                ++createCheckpointRejectedCount;
                continue;
            }

            successfulCheckpoints.push_back(
                std::make_pair(checkpointId, 'a' + i));

            ++i;
        }

        for (int i = 0; i < targetSuccessfulCheckpoints;) {
            tablet.InitSession(
                "client",
                TStringBuilder() << "session_" << i,
                successfulCheckpoints[i].first);
            tablet.SendCreateHandleRequest(id, TCreateHandleArgs::RDWR);
            auto handleResponse = tablet.RecvCreateHandleResponse();
            reconnectAndRecreateHandleIfNeeded(false);

            if (HasError(handleResponse->GetError())) {
                UNIT_ASSERT_VALUES_EQUAL(
                    E_REJECTED,
                    handleResponse->GetError().GetCode());
                continue;
            }

            ui64 checkpointHandle = handleResponse->Record.GetHandle();

            auto response = tablet.ReadData(checkpointHandle, 0, block);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_BUFFER_CONTENTS_EQUAL(
                buffer,
                block,
                successfulCheckpoints[i].second);

            ++i;
        }

        UNIT_ASSERT_C(
            rebootTracker.GetGenerationCount() >= 2,
            "Expected at least 2 different generations due to tablet reboot,"
            "got "
                << rebootTracker.GetGenerationCount());

        UNIT_ASSERT_C(
            createCheckpointRejectedCount >= 1,
            "Expected at least 1 checkpoint to overflow commit id, "
            "got "
                << createCheckpointRejectedCount);
    }
}

}   // namespace NCloud::NFileStore::NStorage
