#include "tablet.h"
#include "tablet_schema.h"

#include <cloud/filestore/libs/storage/tablet/model/block.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_Data)
{
    using namespace NActors;

    using namespace NCloud::NStorage;

    Y_UNIT_TEST(ShouldStoreFreshBytes)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);
        tablet.WriteData(handle, 0, 4_KB, '0');
        tablet.WriteData(handle, 100, 10, 'a');

        TString expected;
        expected.ReserveAndResize(4_KB);
        memset(expected.begin(), '0', 4_KB);
        memset(expected.begin() + 100, 'a', 10);

        {
            auto response = tablet.ReadData(handle, 0, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.Flush();

        {
            auto response = tablet.ReadData(handle, 0, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        ui32 rangeId = GetMixedRangeIndex(id, 0);
        tablet.Compaction(rangeId);

        {
            auto response = tablet.ReadData(handle, 0, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.WriteData(handle, 105, 10, 'b');
        memset(expected.begin() + 105, 'b', 10);

        {
            auto response = tablet.ReadData(handle, 0, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.DestroyHandle(handle);
    }

    Y_UNIT_TEST(ShouldLoadFreshBytesOnStartup)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);
        tablet.WriteData(handle, 0, 4_KB, '0');
        tablet.WriteData(handle, 100, 10, 'a');
        tablet.WriteData(handle, 110, 10, 'c');
        tablet.WriteData(handle, 105, 10, 'b');

        TString expected;
        expected.ReserveAndResize(4_KB);
        memset(expected.begin(), '0', 4_KB);
        memset(expected.begin() + 100, 'a', 10);
        memset(expected.begin() + 110, 'c', 10);
        memset(expected.begin() + 105, 'b', 10);

        {
            auto response = tablet.ReadData(handle, 0, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.RebootTablet();
        tablet.RecoverSession();

        handle = CreateHandle(tablet, id);

        {
            auto response = tablet.ReadData(handle, 0, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }
    }

    Y_UNIT_TEST(ShouldFlushFreshBytes)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        // writing fresh block, then bytes
        tablet.WriteData(handle, 0, 4_KB, '0');
        tablet.WriteData(handle, 100, 10, 'a');
        tablet.FlushBytes();

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBytesCount(), 0);
        }

        TString expected;
        expected.ReserveAndResize(4_KB);
        memset(expected.begin(), '0', 4_KB);
        memset(expected.begin() + 100, 'a', 10);

        {
            auto response = tablet.ReadData(handle, 0, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        // Flushed blob will contain only garbage blocks and won't be added at all
        tablet.Flush();

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBytesCount(), 0);
        }

        {
            auto response = tablet.ReadData(handle, 0, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        // writing fresh bytes, then block
        tablet.WriteData(handle, 100, 10, 'b');
        tablet.WriteData(handle, 0, 4_KB, '1');
        tablet.FlushBytes();

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBytesCount(), 0);
        }

        memset(expected.begin(), '1', 4_KB);

        {
            auto response = tablet.ReadData(handle, 0, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.Flush();

        {
            auto response = tablet.ReadData(handle, 0, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBytesCount(), 0);
        }

        // writing fresh bytes on top of a blob
        tablet.WriteData(handle, 100, 10, 'c');
        tablet.FlushBytes();

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            // could've been 1 if FlushBytes collected all overwritten blobs,
            // but it collects only those blobs that own the most recent block
            // versions (unlike Compaction which collects everything)
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBytesCount(), 0);
        }

        memset(expected.begin() + 100, 'c', 10);

        {
            auto response = tablet.ReadData(handle, 0, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.DestroyHandle(handle);

        // TODO test with intersecting fresh byte ranges
    }

    Y_UNIT_TEST(ShouldFlushFreshBytesByLargeOffset)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        // writing fresh block, then bytes
        tablet.WriteData(handle, 13_GB + 1, 10, 'a');
        tablet.WriteData(handle, 13_GB + 5, 10, 'b');
        tablet.FlushBytes();

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBytesCount(), 0);
        }

        {
            TString expected = TString(1, 0) + TString(4, 'a') + TString(10, 'b');
            auto response = tablet.ReadData(handle, 13_GB, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }
    }

    Y_UNIT_TEST(ShouldFlushFreshBytesWithPartialBlobIntersection)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        // blob
        tablet.WriteData(handle, 0, 12_KB, '0');
        tablet.Flush();

        tablet.WriteData(handle, 4_KB, 10, 'a');
        tablet.FlushBytes();

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 4);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageBlocksCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBytesCount(), 0);
        }

        TString expected;
        expected.ReserveAndResize(12_KB);
        memset(expected.begin(), '0', 12_KB);
        memset(expected.begin() + 4_KB, 'a', 10);

        {
            auto response = tablet.ReadData(handle, 0, 12_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.DestroyHandle(handle);
    }

    Y_UNIT_TEST(ShouldAcceptLargeUnalignedWrites)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetWriteBlobThreshold(8_KB);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);
        tablet.WriteData(handle, 0, 16_KB, '0');    // merged
        tablet.WriteData(handle, 8_KB, 4_KB, '1');  // fresh
        tablet.WriteData(handle, 1_KB, 9_KB, 'a');     // large unaligned write

        TString expected;
        expected.ReserveAndResize(16_KB);
        memset(expected.begin(), '0', 16_KB);
        memset(expected.begin() + 8_KB, '1', 4_KB);
        memset(expected.begin() + 1_KB, 'a', 9_KB);

        {
            auto response = tablet.ReadData(handle, 0, 16_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.Flush();

        {
            auto response = tablet.ReadData(handle, 0, 16_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.FlushBytes();

        {
            auto response = tablet.ReadData(handle, 0, 16_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.DestroyHandle(handle);
    }

    Y_UNIT_TEST(ShouldStoreFreshBlocks)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);
        tablet.WriteData(handle, 0, 4_KB, 'a');

        {
            auto response = tablet.ReadData(handle, 0, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, 4_KB, 'a'));
        }

        tablet.WriteData(handle, 0, 4_KB, 'b');

        {
            auto response = tablet.ReadData(handle, 0, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, 4_KB, 'b'));
        }

        tablet.DestroyHandle(handle);
    }

    Y_UNIT_TEST(ShouldWriteBlobOnFlush)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, 4_KB, 'a');
        tablet.WriteData(handle, 4_KB, 4_KB, 'b');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 0);
        }

        tablet.Flush();

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
        }

        {
            auto response = tablet.ReadData(handle, 0, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, 4_KB, 'a'));
        }

        {
            auto response = tablet.ReadData(handle, 4_KB, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, 4_KB, 'b'));
        }

        tablet.DestroyHandle(handle);
    }

    Y_UNIT_TEST(ShouldWriteBlobForLargeWrite)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, 256_KB, 'a');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 64);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
        }

        {
            auto response = tablet.ReadData(handle, 0, 256_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, 256_KB, 'a'));
        }

        tablet.DestroyHandle(handle);
    }

    Y_UNIT_TEST(ShouldRemoveDeletionMarkersOnCleanup)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, 4_KB, 'a');
        tablet.WriteData(handle, 4_KB, 4_KB, 'a');

        tablet.Flush();

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDeletionMarkersCount(), 2);
        }

        tablet.WriteData(handle, 0, 4_KB, 'b');
        tablet.WriteData(handle, 4_KB, 4_KB, 'b');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDeletionMarkersCount(), 4);
        }

        ui32 rangeId = GetMixedRangeIndex(id, 0);
        tablet.Cleanup(rangeId);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDeletionMarkersCount(), 0);
        }

        {
            auto response = tablet.ReadData(handle, 0, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, 4_KB, 'b'));
        }

        {
            auto response = tablet.ReadData(handle, 4_KB, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, 4_KB, 'b'));
        }

        tablet.DestroyHandle(handle);
    }

    Y_UNIT_TEST(ShouldUpdateCompactionMapUponCleanup)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, 16_KB, 'a');

        tablet.Flush();

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDeletionMarkersCount(), 4);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetUsedCompactionRanges(), 1);
        }

        tablet.WriteData(handle, 0_KB, 16_KB, 'b');
        tablet.Flush();

        ui32 rangeId = GetMixedRangeIndex(id, 0);
        tablet.Compaction(rangeId);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDeletionMarkersCount(), 8);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetUsedCompactionRanges(), 1);
        }

        tablet.DestroyHandle(handle);
        tablet.UnlinkNode(RootNodeId, "test", false);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDeletionMarkersCount(), 12);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetUsedCompactionRanges(), 1);
        }

        // cleanup could start before compaction, then if
        // there are no live blobs left after applying markers
        // it should also update compaction range map
        tablet.Cleanup(rangeId);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDeletionMarkersCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetUsedCompactionRanges(), 0);
        }
    }

    Y_UNIT_TEST(ShouldHandleMultipleBlobsInRangeAndUpdateCompactionMap)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TFileSystemConfig tabletConfig = {
            .BlockSize = 128_KB
        };

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId, tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, 1_MB, 'a');
        tablet.WriteData(handle, 1_MB, 1_MB, 'b');
        tablet.WriteData(handle, 2_MB, 1_MB, 'c');
        tablet.WriteData(handle, 3_MB, 1_MB, 'd');
        tablet.WriteData(handle, 4_MB, 1_MB, 'e');

        tablet.Flush();

        ui32 rangeId = GetMixedRangeIndex(id, 0);
        tablet.Compaction(rangeId);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetUsedCompactionRanges(), 1);
        }

        tablet.DestroyHandle(handle);
        tablet.UnlinkNode(RootNodeId, "test", false);

        // cleanup could start before compaction, then if
        // there are no live blobs left after applying markers
        // it should also update compaction range map
        tablet.Cleanup(rangeId);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDeletionMarkersCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetUsedCompactionRanges(), 0);
        }
    }

    Y_UNIT_TEST(ShouldRewriteBlobsOnCompaction)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, 4_KB, 'a');
        tablet.Flush();

        tablet.WriteData(handle, 4_KB, 4_KB, 'b');
        tablet.Flush();

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 2);
        }

        ui32 rangeId = GetMixedRangeIndex(id, 0);
        tablet.Compaction(rangeId);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
        }

        {
            auto response = tablet.ReadData(handle, 0, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, 4_KB, 'a'));
        }

        {
            auto response = tablet.ReadData(handle, 4_KB, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, 4_KB, 'b'));
        }

        tablet.DestroyHandle(handle);
    }

    Y_UNIT_TEST(ShouldHandleErasedRangeDuringCompaction)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, 4_KB, 'a');
        tablet.Flush();

        const ui32 rangeId = GetMixedRangeIndex(id, 0);

        tablet.DestroyHandle(handle);
        tablet.UnlinkNode(RootNodeId, "test", false);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
        }

        tablet.Compaction(rangeId);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 0);
        }
    }

    Y_UNIT_TEST(ShouldAddGarbageBlobsForRewrittenBlobs)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetCollectGarbageThreshold(1_GB);
        storageConfig.SetFlushBytesThreshold(1_GB);
        storageConfig.SetFlushThreshold(1_GB);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, 4_KB, 'a');
        tablet.Flush();
        tablet.CollectGarbage();

        ui32 rangeId = GetMixedRangeIndex(id, 0);

        tablet.DestroyHandle(handle);
        tablet.UnlinkNode(RootNodeId, "test", false);
        tablet.Cleanup(rangeId);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageQueueSize(), 4_KB);
        }

        id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, 4_KB, 'a');
        tablet.Flush();
        tablet.CollectGarbage();

        tablet.DestroyHandle(handle);
        tablet.UnlinkNode(RootNodeId, "test", false);
        tablet.Compaction(rangeId);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageQueueSize(), 8_KB);
        }

        id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, 3_KB, 'a');

        tablet.DestroyHandle(handle);
        tablet.UnlinkNode(RootNodeId, "test", false);
        tablet.FlushBytes();

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBytesCount(), 0);
            // GarbageQueueSize remains the same since FlushBytes doesn't
            // generate any new blobs after UnlinkNode (leads to Truncate to
            // zero size)
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageQueueSize(), 8_KB);
        }

        tablet.CollectGarbage();

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageQueueSize(), 0);
        }
    }

    Y_UNIT_TEST(ShouldCollectGarbage)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, 4_KB, 'a');
        tablet.Flush();

        tablet.WriteData(handle, 0, 4_KB, 'b');
        tablet.Flush();

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageQueueSize(), 2 * 4_KB);    // new: 2, garbage: 0
        }

        tablet.CollectGarbage();

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageQueueSize(), 0);   // new: 0, garbage: 0
        }

        ui32 rangeId = GetMixedRangeIndex(id, 0);
        tablet.Compaction(rangeId);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(
                stats.GetGarbageQueueSize(),
                2 * 4_KB + 2 * 4_KB
            );  // new: 1 (x 8_KB), garbage: 2 (x 4_KB)
        }

        tablet.CollectGarbage();

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageQueueSize(), 0);   // new: 0, garbage: 0
        }

        {
            auto response = tablet.ReadData(handle, 0, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, 4_KB, 'b'));
        }

        tablet.DestroyHandle(handle);
    }

    Y_UNIT_TEST(ShouldFlushAfterEmptyFlush)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto response = tablet.FlushBytes();
        UNIT_ASSERT(response->Error.GetCode() == S_ALREADY);

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 1, 4_KB, 'a');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBytesCount(), 4_KB - 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 1);
        }

        response = tablet.FlushBytes();
        UNIT_ASSERT(response->Error.GetCode() == S_OK);
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBytesCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 1);
        }
    }

    Y_UNIT_TEST(ShouldReadEmptyFile)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        auto buffer = ReadData(tablet, handle, 4_KB);
        UNIT_ASSERT_VALUES_EQUAL(buffer.size(), 0);

        buffer = ReadData(tablet, handle, 4_KB, 4_KB);
        UNIT_ASSERT_VALUES_EQUAL(buffer.size(), 0);
    }

    Y_UNIT_TEST(ShouldUpdateFileSizeOnWrite)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(),nodeIdx, tabletId, {
            .BlockCount = MaxFileBlocks * 2
        });
        tablet.InitSession("client", "session");

        const auto huge = env.GetStorageConfig()->GetWriteBlobThreshold() + 4_KB;

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, 1, '1');
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 1);

        tablet.WriteData(handle, 1, 1023, '2');
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 1024);

        tablet.WriteData(handle, 0, huge, '3');
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), huge);

        tablet.WriteData(handle, 1_KB, 1_KB, '4');
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), huge);

        tablet.WriteData(handle, (MaxFileBlocks - 1) * DefaultBlockSize, DefaultBlockSize, '5');
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), MaxFileBlocks * DefaultBlockSize);

        tablet.AssertWriteDataFailed(handle, MaxFileBlocks * DefaultBlockSize, 1_KB, '6');
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), MaxFileBlocks * DefaultBlockSize);
    }

    Y_UNIT_TEST(ShouldTrackUsedBlocks)
    {
        constexpr ui64 maxBlocks = 256;

        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId, {
            .BlockCount = maxBlocks
        });
        tablet.InitSession("client", "session");

        const auto huge = env.GetStorageConfig()->GetWriteBlobThreshold() + 4_KB;

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test1"));
        ui64 handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, 1, '1');
        UNIT_ASSERT_VALUES_EQUAL(GetStorageStats(tablet).GetUsedBlocksCount(), 1u);

        tablet.WriteData(handle, 1, 1023, '2');
        UNIT_ASSERT_VALUES_EQUAL(GetStorageStats(tablet).GetUsedBlocksCount(), 1u);

        tablet.WriteData(handle, 0, huge, '3');
        UNIT_ASSERT_VALUES_EQUAL(GetStorageStats(tablet).GetUsedBlocksCount(), huge/DefaultBlockSize);

        tablet.WriteData(handle, 0, maxBlocks * DefaultBlockSize, '4');
        UNIT_ASSERT_VALUES_EQUAL(GetStorageStats(tablet).GetUsedBlocksCount(), maxBlocks);

        auto response = tablet.AssertWriteDataFailed(handle, maxBlocks * DefaultBlockSize, 4_KB, '5');
        auto error = response->GetError();
        UNIT_ASSERT_VALUES_EQUAL(STATUS_FROM_CODE(error.GetCode()), (ui32)NProto::E_FS_NOSPC);

        tablet.UnlinkNode(RootNodeId, "test1", false);
        UNIT_ASSERT_VALUES_EQUAL(GetStorageStats(tablet).GetUsedBlocksCount(), maxBlocks);

        tablet.DestroyHandle(handle);
        UNIT_ASSERT_VALUES_EQUAL(GetStorageStats(tablet).GetUsedBlocksCount(), 0u);
    }

    Y_UNIT_TEST(ShouldUpSizeFiles)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 1_KB, 1_KB, '1');
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 2_KB);

        auto buffer = ReadData(tablet, handle, 16_KB, 0);
        UNIT_ASSERT_VALUES_EQUAL(buffer.size(), 2_KB);
        auto expected = TString(1_KB, 0) + TString(1_KB, '1');
        UNIT_ASSERT_VALUES_EQUAL(buffer, expected);

        tablet.WriteData(handle, 5_KB, 1_KB, '2');
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 6_KB);

        buffer = ReadData(tablet, handle, 4_KB, 4_KB);
        UNIT_ASSERT_VALUES_EQUAL(buffer.size(), 2_KB);
        expected = TString(1_KB, 0) + TString(1_KB, '2');
        UNIT_ASSERT_VALUES_EQUAL(buffer, expected);
    }

    Y_UNIT_TEST(ShouldReadFromBlobsAndBlocksAtTheSameTime)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, 4_KB, 'a');
        tablet.Flush();
        tablet.WriteData(handle, 4_KB, 4_KB, 'b');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
        }

        TString expected;
        expected.ReserveAndResize(8_KB);
        memset(expected.begin(), 'a', 4_KB);
        memset(expected.begin() + 4_KB, 'b', 4_KB);

        {
            auto response = tablet.ReadData(handle, 0, 8_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.DestroyHandle(handle);
    }

    Y_UNIT_TEST(ShouldAutomaticallyRunCompaction)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetCompactionThreshold(5);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetWriteBlobThreshold(8_KB);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        // blob 0
        tablet.WriteData(handle, 0, 4_KB, 'a');
        tablet.Flush();

        // blob 1
        tablet.WriteData(handle, 4_KB, 4_KB, 'b');
        tablet.Flush();

        // blob 2
        tablet.WriteData(handle, 0, 8_KB, 'c');

        // blob 3
        tablet.WriteData(handle, 0, 8_KB, 'd');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 4);
        }

        // blob 4
        tablet.WriteData(handle, 8_KB, 8_KB, 'e');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
        }

        {
            auto response = tablet.ReadData(handle, 0, 8_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, 8_KB, 'd'));
        }

        {
            auto response = tablet.ReadData(handle, 8_KB, 8_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, 8_KB, 'e'));
        }

        tablet.DestroyHandle(handle);
    }

    Y_UNIT_TEST(ShouldAutomaticallyRunCleanup)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(8);
        storageConfig.SetWriteBlobThreshold(8_KB);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        // deletion 0
        tablet.WriteData(handle, 0, 4_KB, 'a');
        tablet.Flush();

        // deletion 1
        tablet.WriteData(handle, 4_KB, 4_KB, 'b');

        // deletion 2
        tablet.WriteData(handle, 0, 8_KB, 'c');

        // deletion 3
        tablet.WriteData(handle, 0, 8_KB, 'd');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 3);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDeletionMarkersCount(), 6);
        }

        // deletion 4
        tablet.WriteData(handle, 8_KB, 8_KB, 'e');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDeletionMarkersCount(), 0);
        }

        {
            auto response = tablet.ReadData(handle, 0, 8_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, 8_KB, 'd'));
        }

        {
            auto response = tablet.ReadData(handle, 8_KB, 8_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, 8_KB, 'e'));
        }

        tablet.DestroyHandle(handle);
    }

    Y_UNIT_TEST(ShouldAutomaticallyRunCollectGarbage)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetWriteBlobThreshold(8_KB);
        storageConfig.SetCollectGarbageThreshold(24_KB);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        // flushed blob
        tablet.WriteData(handle, 0, 4_KB, 'a');
        tablet.WriteData(handle, 4_KB, 4_KB, 'b');
        tablet.Flush();

        // directly written blob
        tablet.WriteData(handle, 0, 12_KB, 'c');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            // 20_KB of new blobs
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageQueueSize(), 20_KB);
        }

        // triggering automatic garbage collection
        tablet.WriteData(handle, 0, 4_KB, 'a');
        tablet.Flush();

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            // last flushed blob remains since Flush acquires a collect
            // barrier
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageQueueSize(), 4_KB);
        }

        tablet.DestroyHandle(handle);
    }

    Y_UNIT_TEST(ShouldCorrectlyHandleConcurrentCompactionAndMergedBlobWrite)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetWriteBlobThreshold(4_KB);

        TTestEnv env({}, std::move(storageConfig));

        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, 4_KB, 'a');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
        }

        TAutoPtr<IEventHandle> readBlob;
        bool intercept = true;

        env.GetRuntime().SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                if (intercept) {
                    switch (event->GetTypeRewrite()) {
                        case TEvIndexTabletPrivate::EvReadBlobRequest: {
                            if (!readBlob) {
                                readBlob = event.Release();
                                return TTestActorRuntime::EEventAction::DROP;
                            }
                        }
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        ui32 rangeId = GetMixedRangeIndex(id, 0);
        tablet.SendCompactionRequest(rangeId);
        env.GetRuntime().DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        UNIT_ASSERT(readBlob);

        tablet.WriteData(handle, 0, 4_KB, 'b');

        // writing some other data elsewhere to increment commit id
        tablet.WriteData(handle, 1_MB, 4_KB, 'c');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 3);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 3);
        }

        env.GetRuntime().Send(readBlob.Release(), 1 /* node index */);

        {
            auto response = tablet.RecvCompactionResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 2);
        }

        intercept = false;

        TString expected;
        expected.ReserveAndResize(4_KB);
        memset(expected.begin(), 'b', 4_KB);

        {
            auto response = tablet.ReadData(handle, 0, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.DestroyHandle(handle);
    }

    Y_UNIT_TEST(ShouldReadFromFreshAndBlobsCorrectly)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetWriteBlobThreshold(1_GB);
        storageConfig.SetFlushThreshold(1_GB);

        TTestEnv env({}, std::move(storageConfig));

        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        const auto freshBlockCount = 2;

        TString expected;
        expected.ReserveAndResize(4_KB * freshBlockCount);

        for (ui32 i = 0; i < freshBlockCount; ++i) {
            for (ui32 j = 0; j < i + 1; ++j) {
                tablet.WriteData(handle, 4_KB * i, 4_KB, 'a' + j);
                memset(expected.begin() + 4_KB * i, 'a' + j, 4_KB);
            }
        }

        {
            auto response = tablet.ReadData(handle, 0, 4_KB * freshBlockCount);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.Flush();

        {
            auto response = tablet.ReadData(handle, 0, 4_KB * freshBlockCount);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.WriteData(handle, 0, 4_KB, 'e');
        memset(expected.begin(), 'e', 4_KB);

        {
            auto response = tablet.ReadData(handle, 0, 4_KB * freshBlockCount);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.Flush();

        {
            auto response = tablet.ReadData(handle, 0, 4_KB * freshBlockCount);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.DestroyHandle(handle);
    }

    Y_UNIT_TEST(ShouldNotWriteToYellowGroups)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetFlushThreshold(1_GB);
        storageConfig.SetFlushBytesThreshold(1_GB);
        storageConfig.SetWriteBlobThreshold(16_KB);

        TTestEnv env({}, std::move(storageConfig));

        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId, {
            .ChannelCount = 4
        });
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        // writing some data so that our Compaction/Flush/FlushBytes requests
        // don't turn into no-ops

        tablet.WriteData(handle, 0, 16_KB, 'a'); // merged blob
        tablet.WriteData(handle, 0, 4_KB, 'a');     // fresh
        tablet.WriteData(handle, 16_KB + 100, 3_KB, 'a'); // fresh bytes

        ui64 reassignedTabletId = 0;
        TVector<ui32> channels;

        // intercepting EvPutResult and setting yellow flag

        env.GetRuntime().SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    using namespace NKikimr;

                    case TEvBlobStorage::EvPutResult: {
                        auto* msg = event->Get<TEvBlobStorage::TEvPutResult>();
                        const auto validFlag = NKikimrBlobStorage::StatusIsValid;
                        const auto yellowFlag = NKikimrBlobStorage::StatusDiskSpaceLightYellowMove |
                            NKikimrBlobStorage::StatusDiskSpaceYellowStop;
                        const_cast<TStorageStatusFlags&>(msg->StatusFlags).Merge(
                            ui32(validFlag) | ui32(yellowFlag));

                        break;
                    }

                    case TEvHiveProxy::EvReassignTabletRequest: {
                        auto* msg = event->Get<TEvHiveProxy::TEvReassignTabletRequest>();
                        reassignedTabletId = msg->TabletId;
                        channels = msg->Channels;

                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        // checking that the request upon which we receive yellow flag
        // actually succeeds

        tablet.SendWriteDataRequest(handle, 0, 16_KB, 'b');
        {
            auto response = tablet.RecvWriteDataResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        TString expected;
        expected.ReserveAndResize(16_KB);
        memset(expected.begin(), 'b', 16_KB);

        // checking that we can read what we have just written

        {
            auto response = tablet.ReadData(handle, 0, 16_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        // checking that a reassign request has been sent

        UNIT_ASSERT_VALUES_EQUAL(tabletId, reassignedTabletId);
        UNIT_ASSERT_VALUES_EQUAL(1, channels.size());
        UNIT_ASSERT_VALUES_EQUAL(3, channels.front());

        reassignedTabletId = 0;
        channels.clear();

        // checking that all requests that attempt to write data will fail

        tablet.SendWriteDataRequest(handle, 0, 16_KB, 'c');
        {
            auto response = tablet.RecvWriteDataResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_FS_OUT_OF_SPACE, response->GetStatus());
        }

        ui32 rangeId = GetMixedRangeIndex(id, 0);
        tablet.SendCompactionRequest(rangeId);
        {
            auto response = tablet.RecvCompactionResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_FS_OUT_OF_SPACE, response->GetStatus());
        }

        tablet.SendFlushRequest();
        {
            auto response = tablet.RecvFlushResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_FS_OUT_OF_SPACE, response->GetStatus());
        }

        tablet.SendFlushBytesRequest();
        {
            auto response = tablet.RecvFlushBytesResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_FS_OUT_OF_SPACE, response->GetStatus());
        }

        // checking that our data can be read

        {
            auto response = tablet.ReadData(handle, 0, 16_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        // checking that another reassign request has not been sent

        UNIT_ASSERT_VALUES_EQUAL(0, reassignedTabletId);
        UNIT_ASSERT_VALUES_EQUAL(0, channels.size());

        // checking that another reassign request is sent after 1min delay

        env.GetRuntime().AdvanceCurrentTime(TDuration::Minutes(1));

        tablet.SendWriteDataRequest(handle, 0, 16_KB, 'c');
        {
            auto response = tablet.RecvWriteDataResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_FS_OUT_OF_SPACE, response->GetStatus());
        }

        UNIT_ASSERT_VALUES_EQUAL(tabletId, reassignedTabletId);
        UNIT_ASSERT_VALUES_EQUAL(1, channels.size());
        UNIT_ASSERT_VALUES_EQUAL(3, channels.front());

        tablet.DestroyHandle(handle);
    }

    Y_UNIT_TEST(ShouldProperlyReadBlobsWithHoles)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetFlushThreshold(999'999);
        storageConfig.SetWriteBlobThreshold(1_GB);

        TTestEnv env({}, std::move(storageConfig));

        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, 4_KB, 'a');
        tablet.WriteData(handle, 8_KB, 4_KB, 'b');

        tablet.Flush();

        TString expected;
        expected.ReserveAndResize(12_KB);
        memset(expected.begin(), 'a', 4_KB);
        memset(expected.begin() + 4_KB, 0, 4_KB);
        memset(expected.begin() + 8_KB, 'b', 4_KB);

        {
            auto response = tablet.ReadData(handle, 0, 12_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.DestroyHandle(handle);
    }

    Y_UNIT_TEST(ShouldNotDeleteBlobsMarkedAsGarbageByUncommittedTransactions)
    {
        using namespace NKikimr;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetWriteBlobThreshold(4_KB);

        TTestEnv env({}, std::move(storageConfig));

        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        // forcing hard gc to make sure that our next gc is soft
        TVector<TPartialBlobId> blobIds;

        env.GetRuntime().SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvPut: {
                        auto* msg = event->Get<TEvBlobStorage::TEvPut>();
                        if (msg->Id.Channel() >= TIndexTabletSchema::DataChannel) {
                            blobIds.push_back(TPartialBlobId(
                                msg->Id.Generation(),
                                msg->Id.Step(),
                                msg->Id.Channel(),
                                msg->Id.BlobSize(),
                                msg->Id.Cookie(),
                                msg->Id.PartId()));
                        }

                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        // blob1
        tablet.WriteData(handle, 0, 4_KB, 'a');

        UNIT_ASSERT_VALUES_EQUAL(1, blobIds.size());

        // setting keep flags
        tablet.CollectGarbage();

        // blob2 (making sure that our next CollectGarbage call does something)
        tablet.WriteData(handle, 0, 4_KB, 'b');

        bool evPutObserved = false;
        env.GetRuntime().SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvPut: {
                        auto* msg = event->Get<TEvBlobStorage::TEvPut>();
                        // Cerr << "EvPut (Cleanup): " << msg->Id << Endl;
                        Y_UNUSED(msg);
                        evPutObserved = true;

                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        // blob1 should be added to GarbageQueue
        ui32 rangeId = GetMixedRangeIndex(id, 0);
        tablet.SendCleanupRequest(rangeId);

        env.GetRuntime().DispatchEvents(
            TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT(evPutObserved);

        bool collectGarbageResultObserved = false;
        env.GetRuntime().SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvPut: {
                        auto* msg = event->Get<TEvBlobStorage::TEvPut>();
                        // Cerr << "EvPut (Collect): " << msg->Id << Endl;
                        Y_UNUSED(msg);

                        return TTestActorRuntime::EEventAction::DROP;
                    }

                    case TEvBlobStorage::EvCollectGarbage: {
                        auto* msg = event->Get<TEvBlobStorage::TEvCollectGarbage>();
                        if (msg->DoNotKeep) {
                            UNIT_ASSERT_VALUES_EQUAL(0, msg->DoNotKeep->size());
                        }
                        collectGarbageResultObserved = true;

                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        // CollectGarbage should not delete blob1
        tablet.SendCollectGarbageRequest();

        env.GetRuntime().DispatchEvents(
            TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT(collectGarbageResultObserved);

        // Effectively aborting Cleanup transaction
        tablet.RebootTablet();
        tablet.RecoverSession();

        env.GetRuntime().SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);

        TString expected;
        expected.ReserveAndResize(4_KB);
        memset(expected.begin(), 'b', 4_KB);

        // Ensuring that our index is initialized
        {
            auto response = tablet.ReadData(handle, 0, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        // Running garbage compaction - it should succeed reading blob1
        tablet.Compaction(rangeId);
    }

    Y_UNIT_TEST(ShouldLimitDeleteGarbageBlobsPerTx)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxDeleteGarbageBlobsPerTx(2);
        storageConfig.SetWriteBlobThreshold(4_KB);
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCollectGarbageThreshold(1_GB);

        TTestEnv env({}, std::move(storageConfig));

        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, 4_KB, 'a');
        tablet.WriteData(handle, 0, 4_KB, 'a');
        tablet.WriteData(handle, 0, 4_KB, 'a');
        tablet.WriteData(handle, 0, 4_KB, 'a');
        tablet.WriteData(handle, 0, 4_KB, 'a');

        tablet.CollectGarbage();

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageQueueSize(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTxDeleteGarbageRwCompleted(), 3);
        }
    }

    Y_UNIT_TEST(ShouldRejectWritesDueToBackpressure)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetWriteBlobThreshold(8_KB);
        storageConfig.SetFlushThreshold(1_GB);
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetFlushBytesThreshold(1_GB);
        storageConfig.SetFlushThresholdForBackpressure(8_KB);
        storageConfig.SetCompactionThresholdForBackpressure(3);
        storageConfig.SetCleanupThresholdForBackpressure(10);
        storageConfig.SetFlushBytesThresholdForBackpressure(2_KB);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);
        tablet.WriteData(handle, 0, 4_KB, '0'); // 1 fresh block, 1 marker
        tablet.WriteData(handle, 0, 4_KB, '0'); // 2 fresh blocks, 2 markers

        // backpressure due to FlushThresholdForBackpressure
        tablet.SendWriteDataRequest(handle, 0, 4_KB, '0');
        {
            auto response = tablet.RecvWriteDataResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        tablet.Flush();

        // no backpressure after Flush
        tablet.WriteData(handle, 0, 4_KB, '0'); // 1 blob, 1 fresh block, 3 markers
        tablet.WriteData(handle, 0, 8_KB, '0'); // 2 blobs, 1 fresh block, 5 markers
        tablet.WriteData(handle, 0, 8_KB, '0'); // 3 blobs, 1 fresh block, 7 markers

        // backpressure due to CompactionScoreThresholdForBackpressure
        tablet.SendWriteDataRequest(handle, 0, 8_KB, '0');
        {
            auto response = tablet.RecvWriteDataResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        ui32 rangeId = GetMixedRangeIndex(id, 0);
        tablet.Compaction(rangeId);

        // no backpressure after Compaction
        tablet.WriteData(handle, 0, 8_KB, '0'); // 2 blobs, 1 fresh block, 9 markers

        tablet.WriteData(handle, 0, 4_KB, '0'); // 2 blobs, 2 fresh blocks, 10 markers
        tablet.Flush(); // 3 blobs, 10 markers
        tablet.Compaction(rangeId); // 1 blob, 10 markers

        // backpressure due to CleanupScoreThresholdForBackpressure
        tablet.SendWriteDataRequest(handle, 0, 4_KB, '0');
        {
            auto response = tablet.RecvWriteDataResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        tablet.Cleanup(rangeId); // 1 blob

        // no backpressure after Cleanup
        tablet.WriteData(handle, 0, 1_KB, '0'); // 1 blob, 1_KB fresh bytes
        tablet.WriteData(handle, 0, 1_KB, '0'); // 1 blob, 2_KB fresh bytes

        // backpressure due to FlushBytesThresholdForBackpressure
        tablet.SendWriteDataRequest(handle, 0, 1_KB, '0');
        {
            auto response = tablet.RecvWriteDataResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        tablet.FlushBytes(); // 2 blobs

        // no backpressure after FlushBytes
        tablet.WriteData(handle, 0, 1_KB, '0'); // 2 blobs, 1_KB fresh bytes

        TString expected;
        expected.ReserveAndResize(8_KB);
        memset(expected.begin(), '0', 8_KB);

        {
            auto response = tablet.ReadData(handle, 0, 8_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.DestroyHandle(handle);
    }

    Y_UNIT_TEST(ShouldRecoverAfterFreshBlocksRelatedBackpressure)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetWriteBlobThreshold(1_GB);
        storageConfig.SetFlushThreshold(8_KB);
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetFlushBytesThreshold(1_GB);
        storageConfig.SetFlushThresholdForBackpressure(8_KB);
        storageConfig.SetCompactionThresholdForBackpressure(999);
        storageConfig.SetCleanupThresholdForBackpressure(999);
        storageConfig.SetFlushBytesThresholdForBackpressure(1_GB);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);
        tablet.WriteData(handle, 0, 4_KB, '0'); // 1 fresh block
        tablet.WriteData(handle, 0, 1, '0'); // 1 fresh byte

        TAutoPtr<IEventHandle> completion;

        env.GetRuntime().SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTabletPrivate::EvFlushBytesCompleted: {
                        completion = event.Release();
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        tablet.FlushBytes();

        tablet.WriteData(handle, 4_KB, 4_KB, '0'); // 2 fresh blocks

        // backpressure due to FlushThresholdForBackpressure
        tablet.SendWriteDataRequest(handle, 4_KB, 4_KB, '1');
        {
            auto response = tablet.RecvWriteDataResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        bool flushObserved = false;

        env.GetRuntime().SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTabletPrivate::EvFlushCompleted: {
                        flushObserved = true;
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        env.GetRuntime().Send(completion.Release(), 1 /* node index */);
        env.GetRuntime().DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        UNIT_ASSERT(flushObserved);

        // no backpressure after Flush
        tablet.WriteData(handle, 4_KB, 4_KB, '0');

        TString expected;
        expected.ReserveAndResize(8_KB);
        memset(expected.begin(), '0', 8_KB);

        {
            auto response = tablet.ReadData(handle, 0, 8_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.DestroyHandle(handle);
    }

    Y_UNIT_TEST(ShouldReadUnAligned)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        TVector<char> buffer(8_KB);
        for (ui64 i = 0; i < 8_KB; ++i) {
            buffer[i] = i % 10;
        }

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);
        tablet.WriteData(handle, 0, buffer.size(), buffer.data());

        {
            // [0, 4095)
            TStringBuf expected(buffer.data(), 4_KB - 1);

            auto response = tablet.ReadData(handle, 0, 4_KB - 1);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected.size(), buffer.size());
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        {
            // [0, 4097)
            TStringBuf expected(buffer.data(), 4_KB + 1);

            auto response = tablet.ReadData(handle, 0, 4_KB + 1);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected.size(), buffer.size());
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        {
            // [1, 4096)
            TStringBuf expected(buffer.data() + 1, 4_KB - 1);

            auto response = tablet.ReadData(handle, 1, 4_KB - 1);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected.size(), buffer.size());
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        {
            // [1, 4098)
            TStringBuf expected(buffer.data() + 1, 4_KB + 1);

            auto response = tablet.ReadData(handle, 1, 4_KB + 1);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected.size(), buffer.size());
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        {
            // [1, 1025)
            TStringBuf expected(buffer.data() + 1, 1_KB);

            auto response = tablet.ReadData(handle, 1, 1_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected.size(), buffer.size());
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        {
            // [6KB, 8KB) trim by file end
            TStringBuf expected(buffer.data() + 6_KB, 2_KB);

            auto response = tablet.ReadData(handle, 6_KB, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected.size(), buffer.size());
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }
    }

    Y_UNIT_TEST(ShouldWriteFreshBlocksForUnalignedDataAtTheEnd)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto response = tablet.FlushBytes();
        UNIT_ASSERT(response->Error.GetCode() == S_ALREADY);

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, 3_KB, 'a');
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBytesCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 1);
        }

        tablet.WriteData(handle, 0, 7_KB, 'b');
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBytesCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 3);
        }

        tablet.WriteData(handle, 100, 3_KB, 'a');
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBytesCount(), 3_KB);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 3);
        }
    }

    Y_UNIT_TEST(ShouldCollectTracesForWriteRequests)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        auto request = tablet.CreateWriteDataRequest(handle, 0, 4_KB, 'a');
        request->Record.MutableHeaders()->MutableInternal()->MutableTrace()->SetIsTraced(true);

        tablet.SendRequest(std::move(request));
        auto response = tablet.AssertWriteDataResponse(S_OK);
        CheckForkJoin(response->Record.GetHeaders().GetTrace().GetLWTrace().GetTrace(), false);

        request = tablet.CreateWriteDataRequest(handle, 0, 1_MB, 'b');
        request->Record.MutableHeaders()->MutableInternal()->MutableTrace()->SetIsTraced(true);

        tablet.SendRequest(std::move(request));
        response = tablet.AssertWriteDataResponse(S_OK);
        CheckForkJoin(response->Record.GetHeaders().GetTrace().GetLWTrace().GetTrace(), true);
    }

    Y_UNIT_TEST(ShouldDoForcedCompaction)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        TVector<ui32> ranges;
        env.GetRuntime().SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTabletPrivate::EvCompactionRequest: {
                        auto* msg = event->Get<TEvIndexTabletPrivate::TEvCompactionRequest>();
                        ranges.push_back(msg->RangeId);
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        tablet.ForcedCompaction(::xrange(0, 10, 1));
        UNIT_ASSERT_VALUES_EQUAL(ranges.size(), 10);
        UNIT_ASSERT_VALUES_EQUAL(ranges.front(), 0);
        UNIT_ASSERT_VALUES_EQUAL(ranges.back(), 9);

        tablet.AssertForcedCompactionFailed(TVector<ui32>{});
    }

    Y_UNIT_TEST(ShouldRetryForcedCompaction)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        TVector<ui32> ranges;
        bool dropped = false;
        env.GetRuntime().SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTabletPrivate::EvCompactionRequest: {
                        auto* msg = event->Get<TEvIndexTabletPrivate::TEvCompactionRequest>();
                        ranges.push_back(msg->RangeId);

                        if (!dropped) {
                            dropped = true;
                            auto response = std::make_unique<TEvIndexTabletPrivate::TEvCompactionResponse>(
                                MakeError(E_TRY_AGAIN));

                            env.GetRuntime().Send(new IEventHandle(
                                event->Sender,
                                event->Recipient,
                                response.release(),
                                0, // flags
                                event->Cookie),
                                1);

                            return TTestActorRuntime::EEventAction::DROP;
                        }
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        tablet.ForcedCompaction(::xrange(0, 2, 1));
        UNIT_ASSERT_VALUES_EQUAL(ranges.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ranges[0], 0);
        UNIT_ASSERT_VALUES_EQUAL(ranges[1], 0);
        UNIT_ASSERT_VALUES_EQUAL(ranges[2], 1);
    }

    Y_UNIT_TEST(ShouldEnqueuePendingForcedCompaction)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        bool dropped = false;
        TVector<ui32> ranges;
        TAutoPtr<IEventHandle> request;
        env.GetRuntime().SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTabletPrivate::EvCompactionRequest: {
                        auto* msg = event->Get<TEvIndexTabletPrivate::TEvCompactionRequest>();
                        ranges.push_back(msg->RangeId);
                        if (!dropped) {
                            dropped = true;
                            request = event.Release();
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        tablet.SendForcedCompactionRequest(::xrange(0, 1, 1));
        env.GetRuntime().DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        UNIT_ASSERT(request);

        tablet.SendForcedCompactionRequest(::xrange(1, 2, 1));
        env.GetRuntime().Send(request.Release(), 1 /* node index */);
        env.GetRuntime().DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(ranges.size(), 2);
    }

    Y_UNIT_TEST(ShouldForceCompactRangeAndRemoveStaleNodesData)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        bool changed = false;
        env.GetRuntime().SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTabletPrivate::EvAddBlobRequest: {
                        if (!changed) {
                            auto* msg = event->Get<TEvIndexTabletPrivate::TEvAddBlobRequest>();
                            msg->WriteRanges[0].MaxOffset = 1;
                            changed = true;
                        }
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        // Should remove data for non existing nodes
        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));

        ui64 handle = CreateHandle(tablet, id);
        tablet.WriteData(handle, 0, 128_KB, 'a');

        tablet.UnlinkNode(RootNodeId, "test", false);
        tablet.DestroyHandle(handle);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
        }

        ui32 rangeId = GetMixedRangeIndex(id, 0);
        tablet.Compaction(rangeId);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
        }

        tablet.ForcedCompaction(TVector<ui32>{rangeId});
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 0);
        }

        // Should not remove data for existing nodes
        id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        handle = CreateHandle(tablet, id);
        tablet.WriteData(handle, 0, 128_KB, 'a');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
        }

        rangeId = GetMixedRangeIndex(id, 0);
        tablet.Compaction(rangeId);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
        }

        tablet.ForcedCompaction(TVector<ui32>{rangeId});
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
        }
    }

    Y_UNIT_TEST(ShouldDumpCompactionRangeBlobs)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));

        ui64 handle = CreateHandle(tablet, id);
        tablet.WriteData(handle, 0, 128_KB, 'a');

        ui32 rangeId = GetMixedRangeIndex(id, 0);

        auto response = tablet.DumpCompactionRange(rangeId);
        UNIT_ASSERT_VALUES_EQUAL(response->Blobs.size(), 1);
    }
}

}   // namespace NCloud::NFileStore::NStorage
