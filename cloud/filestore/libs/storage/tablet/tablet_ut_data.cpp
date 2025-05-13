#include "tablet_schema.h"

#include <cloud/filestore/libs/storage/tablet/model/block.h>
#include <cloud/filestore/libs/storage/tablet/model/operation.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>

#include <library/cpp/iterator/enumerate.h>
#include <library/cpp/testing/unittest/registar.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/core/base/logoblob.h>
#include <contrib/ydb/core/testlib/basics/storage.h>

#include <util/generic/size_literals.h>

namespace NCloud::NFileStore::NStorage {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_Data)
{
    using namespace NActors;

    using namespace NCloud::NStorage;

    TABLET_TEST(ShouldStoreFreshBytes)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
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

    TABLET_TEST(ShouldLoadFreshBytesOnStartup)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
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

    TABLET_TEST(ShouldFlushFreshBytes)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        // writing fresh block, then bytes
        tablet.WriteData(handle, 0, 4_KB, '0');
        tablet.WriteData(handle, 100, 10, 'a');
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBytesCount(), 10);
        }

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

    TABLET_TEST(ShouldFlushFreshBytesByLargeOffset)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        // writing fresh bytes
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

    TABLET_TEST(ShouldFlushFreshBytesWithPartialBlobIntersection)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        const auto small = tabletConfig.BlockSize;
        const auto large = 3 * tabletConfig.BlockSize;

        // blob
        tablet.WriteData(handle, 0, large, '0');
        tablet.Flush();

        tablet.WriteData(handle, small, 10, 'a');
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 3);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBytesCount(), 10);
        }

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
        expected.ReserveAndResize(large);
        memset(expected.begin(), '0', large);
        memset(expected.begin() + small, 'a', 10);

        {
            auto response = tablet.ReadData(handle, 0, large);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.DestroyHandle(handle);
    }

    TABLET_TEST(ShouldFlushFreshBytesByItemCountThreshold)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetFlushBytesItemCountThreshold(1000);
        storageConfig.SetFlushBytesByItemCountEnabled(true);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        // Write 1 byte 999 times
        for (int i = 1; i <= 999; i++) {
            tablet.WriteData(handle, i * 2, 2, 'a');
        }

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1998, stats.GetFreshBytesCount());
            UNIT_ASSERT_VALUES_EQUAL(999, stats.GetFreshBytesItemCount());
        }

        // Write 1000th byte - FlushBytes should be triggered
        tablet.WriteData(handle, 2000, 2, 'a');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBytesCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBytesItemCount());
        }
    }

    TABLET_TEST(ShouldAcceptLargeUnalignedWrites)
    {
        const auto rangeSize = 4 * tabletConfig.BlockSize;
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetWriteBlobThreshold(2 * block);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);
        tablet.WriteData(handle, 0, rangeSize, '0');            // merged
        tablet.WriteData(handle, 2 * block, block, '1');        // fresh
        tablet.WriteData(handle, 1_KB, 1_KB + 2 * block, 'a');  // large unaligned write

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 4);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBytesCount(), block + 1_KB);
        }

        TString expected;
        expected.ReserveAndResize(rangeSize);
        memset(expected.begin(), '0', rangeSize);
        memset(expected.begin() + 2 * block, '1', block);
        memset(expected.begin() + 1_KB, 'a', 1_KB + 2 * block);

        {
            auto response = tablet.ReadData(handle, 0, rangeSize);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.Flush();

        {
            auto response = tablet.ReadData(handle, 0, rangeSize);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.FlushBytes();

        {
            auto response = tablet.ReadData(handle, 0, rangeSize);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.DestroyHandle(handle);
    }

    TABLET_TEST(ShouldStoreFreshBlocks)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);
        tablet.WriteData(handle, 0, tabletConfig.BlockSize, 'a');

        {
            auto response = tablet.ReadData(handle, 0, tabletConfig.BlockSize);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, tabletConfig.BlockSize, 'a'));
        }

        tablet.WriteData(handle, 0, tabletConfig.BlockSize, 'b');

        {
            auto response = tablet.ReadData(handle, 0, tabletConfig.BlockSize);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, tabletConfig.BlockSize, 'b'));
        }

        tablet.DestroyHandle(handle);
    }

    TABLET_TEST(ShouldWriteBlobOnFlush)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetWriteBlobThreshold(1_GB);  // no direct blob writes

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, tabletConfig.BlockSize, 'a');
        tablet.WriteData(
            handle,
            tabletConfig.BlockSize,
            tabletConfig.BlockSize,
            'b');

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
            auto response = tablet.ReadData(handle, 0, tabletConfig.BlockSize);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, tabletConfig.BlockSize, 'a'));
        }

        {
            auto response = tablet.ReadData(
                handle,
                tabletConfig.BlockSize,
                tabletConfig.BlockSize);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, tabletConfig.BlockSize, 'b'));
        }

        tablet.DestroyHandle(handle);
    }

    TABLET_TEST(ShouldWriteBlobForLargeWrite)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        const auto sz = 256_KB;

        tablet.WriteData(handle, 0, sz, 'a');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(
                stats.GetMixedBlocksCount(),
                sz / tabletConfig.BlockSize);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
        }

        {
            auto response = tablet.ReadData(handle, 0, sz);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, sz, 'a'));
        }

        tablet.DestroyHandle(handle);
    }

    TABLET_TEST(ShouldRemoveDeletionMarkersOnCleanup)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetWriteBlobThreshold(1_GB);  // no direct blob writes

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, block, 'a');
        tablet.WriteData(handle, block, block, 'a');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDeletionMarkersCount(), 2);
        }

        tablet.Flush();

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDeletionMarkersCount(), 2);
        }

        tablet.WriteData(handle, 0, block, 'b');
        tablet.WriteData(handle, block, block, 'b');

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
            auto response = tablet.ReadData(handle, 0, block);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, block, 'b'));
        }

        {
            auto response = tablet.ReadData(handle, block, block);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, block, 'b'));
        }

        tablet.DestroyHandle(handle);
    }

    TABLET_TEST(ShouldUpdateCompactionMapUponCleanup)
    {
        const auto block = tabletConfig.BlockSize;

        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, 4 * block, 'a');

        tablet.Flush();

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDeletionMarkersCount(), 4);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetUsedCompactionRanges(), 1);
        }

        tablet.WriteData(handle, 0_KB, 4 * block, 'b');
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

    TABLET_TEST(ShouldHandleMultipleBlobsInRangeAndUpdateCompactionMap)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        const auto sz = tabletConfig.BlockSize * 8;

        tablet.WriteData(handle, 0, sz, 'a');
        tablet.WriteData(handle, sz, sz, 'b');
        tablet.WriteData(handle, 2 * sz, sz, 'c');
        tablet.WriteData(handle, 3 * sz, sz, 'd');
        tablet.WriteData(handle, 4 * sz, sz, 'e');

        // needed for small block sizes
        tablet.Flush();

        ui32 rangeId = GetMixedRangeIndex(id, 0);
        tablet.Compaction(rangeId);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                stats.GetMixedBlobsCount(),
                ceil(5. * sz / 4_MB));
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

    TABLET_TEST(ShouldRewriteBlobsOnCompaction)
    {
        const auto block = tabletConfig.BlockSize;

        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, block, 'a');
        tablet.Flush();

        tablet.WriteData(handle, block, block, 'b');
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
            auto response = tablet.ReadData(handle, 0, block);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, block, 'a'));
        }

        {
            auto response = tablet.ReadData(handle, block, block);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, block, 'b'));
        }

        tablet.DestroyHandle(handle);
    }

    TABLET_TEST(ShouldHandleErasedRangeDuringCompaction)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, tabletConfig.BlockSize, 'a');
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

    TABLET_TEST(ShouldAddGarbageBlobsForRewrittenBlobs)
    {
        const auto block = tabletConfig.BlockSize;

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

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, block, 'a');
        tablet.Flush();
        tablet.GenerateCommitId();
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
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageQueueSize(), block);
        }

        id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, block, 'a');
        tablet.Flush();
        tablet.GenerateCommitId();
        tablet.CollectGarbage();

        tablet.DestroyHandle(handle);
        tablet.UnlinkNode(RootNodeId, "test", false);
        tablet.Compaction(rangeId);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 0);
            // still 1 block - Compaction doesn't move unneeded blocks to its
            // dst blobs
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageQueueSize(), block);
        }

        id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, block * 3 / 4, 'a');

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
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageQueueSize(), block);
        }

        tablet.CollectGarbage();

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageQueueSize(), 0);
        }
    }

    TABLET_TEST(ShouldCollectGarbage)
    {
        const auto block = tabletConfig.BlockSize;

        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, block, 'a');
        tablet.Flush();

        tablet.WriteData(handle, 0, block, 'b');
        tablet.Flush();

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageQueueSize(), 2 * block);   // new: 2, garbage: 0
        }

        tablet.GenerateCommitId();
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
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(
                stats.GetGarbageQueueSize(),
                1 * block + 2 * block
            );  // new: 1 (x block), garbage: 2 (x block)
        }

        tablet.GenerateCommitId();
        tablet.CollectGarbage();

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageQueueSize(), 0);   // new: 0, garbage: 0
        }

        {
            auto response = tablet.ReadData(handle, 0, block);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, block, 'b'));
        }

        tablet.WriteData(handle, 0, block, 'c');
        tablet.Flush();
        tablet.Compaction(rangeId);

        tablet.CollectGarbage();

        {
            // default collect garbage should not consider the result of the last compaction
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageQueueSize(), 1 * block);   // new: 1, garbage: 0
        }

        // create a new node to trigger the generation of a new commitId
        id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test2"));
        handle = CreateHandle(tablet, id);

        tablet.CollectGarbage();

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageQueueSize(), 0);
        }

        tablet.DestroyHandle(handle);
    }

    TABLET_TEST(ShouldFlushAfterEmptyFlush)
    {
        const auto block = tabletConfig.BlockSize;

        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto response = tablet.FlushBytes();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_FALSE,
            response->Error.GetCode(),
            response->Error.GetMessage());

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 1, block, 'a');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBytesCount(), block - 1);
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

    TABLET_TEST(ShouldReadEmptyFile)
    {
        const auto block = tabletConfig.BlockSize;

        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        auto buffer = ReadData(tablet, handle, block);
        UNIT_ASSERT_VALUES_EQUAL(buffer.size(), 0);

        buffer = ReadData(tablet, handle, block, block);
        UNIT_ASSERT_VALUES_EQUAL(buffer.size(), 0);
    }

    TABLET_TEST(ShouldUpdateFileSizeOnWrite)
    {
        const auto block = tabletConfig.BlockSize;

        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        tabletConfig.BlockCount = GetDefaultMaxFileBlocks() * 2;
        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        const auto huge = env.GetStorageConfig()->GetWriteBlobThreshold()
            + block;

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

        tablet.WriteData(
            handle,
            (GetDefaultMaxFileBlocks() - 1) * block,
            block,
            '5');
        UNIT_ASSERT_VALUES_EQUAL(
            GetNodeAttrs(tablet, id).GetSize(),
            GetDefaultMaxFileBlocks() * block);

        tablet.AssertWriteDataFailed(
            handle,
            GetDefaultMaxFileBlocks() * block,
            1_KB,
            '6');
        UNIT_ASSERT_VALUES_EQUAL(
            GetNodeAttrs(tablet, id).GetSize(),
            GetDefaultMaxFileBlocks() * block);
    }

    TABLET_TEST(ShouldTrackUsedBlocks)
    {
        constexpr ui64 maxBlocks = 256;
        const auto block = tabletConfig.BlockSize;

        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        tabletConfig.BlockCount = maxBlocks;

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        const auto huge = env.GetStorageConfig()->GetWriteBlobThreshold()
            + block;

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test1"));
        ui64 handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, 1, '1');
        UNIT_ASSERT_VALUES_EQUAL(
            GetStorageStats(tablet).GetUsedBlocksCount(),
            1u);

        tablet.WriteData(handle, 1, 1023, '2');
        UNIT_ASSERT_VALUES_EQUAL(
            GetStorageStats(tablet).GetUsedBlocksCount(),
            1u);

        tablet.WriteData(handle, 0, huge, '3');
        UNIT_ASSERT_VALUES_EQUAL(
            GetStorageStats(tablet).GetUsedBlocksCount(),
            huge / block);

        tablet.WriteData(handle, 0, maxBlocks * block, '4');
        UNIT_ASSERT_VALUES_EQUAL(
            GetStorageStats(tablet).GetUsedBlocksCount(),
            maxBlocks);

        auto response = tablet.AssertWriteDataFailed(
            handle,
            maxBlocks * block,
            block,
            '5');
        auto error = response->GetError();
        UNIT_ASSERT_VALUES_EQUAL(
            STATUS_FROM_CODE(error.GetCode()),
            static_cast<ui32>(NProto::E_FS_NOSPC));

        tablet.UnlinkNode(RootNodeId, "test1", false);
        UNIT_ASSERT_VALUES_EQUAL(
            GetStorageStats(tablet).GetUsedBlocksCount(),
            maxBlocks);

        tablet.DestroyHandle(handle);
        UNIT_ASSERT_VALUES_EQUAL(
            GetStorageStats(tablet).GetUsedBlocksCount(),
            0u);
    }

    TABLET_TEST(ShouldUpSizeFiles)
    {
        const auto block = tabletConfig.BlockSize;

        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 1_KB, 1_KB, '1');
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 2_KB);

        auto buffer = ReadData(tablet, handle, 4 * block, 0);
        UNIT_ASSERT_VALUES_EQUAL(buffer.size(), 2_KB);
        auto expected = TString(1_KB, 0) + TString(1_KB, '1');
        UNIT_ASSERT_VALUES_EQUAL(buffer, expected);

        tablet.WriteData(handle, block + 1_KB, 1_KB, '2');
        UNIT_ASSERT_VALUES_EQUAL(
            GetNodeAttrs(tablet, id).GetSize(),
            block + 2_KB);

        buffer = ReadData(tablet, handle, block, block);
        UNIT_ASSERT_VALUES_EQUAL(buffer.size(), 2_KB);
        expected = TString(1_KB, 0) + TString(1_KB, '2');
        UNIT_ASSERT_VALUES_EQUAL(buffer, expected);
    }

    TABLET_TEST(ShouldReadFromBlobsAndBlocksAtTheSameTime)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetWriteBlobThreshold(1_GB);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, block, 'a');
        tablet.Flush();
        tablet.WriteData(handle, block, block, 'b');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
        }

        TString expected;
        expected.ReserveAndResize(2 * block);
        memset(expected.begin(), 'a', block);
        memset(expected.begin() + block, 'b', block);

        {
            auto response = tablet.ReadData(handle, 0, 2 * block);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.DestroyHandle(handle);
    }

    TABLET_TEST_16K(ShouldAutomaticallyRunCompaction)
    {
        const auto block = tabletConfig.BlockSize;
        const auto threshold = 8 * DefaultBlockSize / block;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetCompactionThreshold(threshold);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetWriteBlobThreshold(2 * block);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        // blob 0
        tablet.WriteData(handle, 0, block, 'a');
        tablet.Flush();

        // blob 1
        tablet.WriteData(handle, block, block, 'b');
        tablet.Flush();

        // blob 2
        tablet.WriteData(handle, 0, 2 * block, 'c');

        // blob 3
        tablet.WriteData(handle, 0, 2 * block, 'd');

        // blob 4
        tablet.WriteData(handle, 0, 2 * block, 'e');

        // blob 5
        tablet.WriteData(handle, 0, 2 * block, 'f');

        // blob 6
        tablet.WriteData(handle, 0, 2 * block, 'g');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 7);
        }

        // blob 7
        tablet.WriteData(handle, 2 * block, 2 * block, 'h');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
        }

        {
            auto response = tablet.ReadData(handle, 0, 2 * block);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, 2 * block, 'g'));
        }

        {
            auto response = tablet.ReadData(handle, 2 * block, 2 * block);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, 2 * block, 'h'));
        }

        tablet.DestroyHandle(handle);
    }

    TABLET_TEST_4K_ONLY(ShouldAutomaticallyRunCompactionDueToCompactionThresholdAverage)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetNewCompactionEnabled(true);
        storageConfig.SetCompactionThresholdAverage(2);
        storageConfig.SetGarbageCompactionThresholdAverage(999'999);
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetWriteBlobThreshold(block);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        // allocating 4 compaction ranges
        TSetNodeAttrArgs args(id);
        args.SetFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE);
        args.SetSize(1_MB);
        tablet.SetNodeAttr(args);

        // 3 blobs in 3 different ranges
        tablet.WriteData(handle, 0, block, 'a');
        tablet.WriteData(handle, 256_KB, block, 'a');
        tablet.WriteData(handle, 512_KB, block, 'a');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1_MB / block, stats.GetUsedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetUsedCompactionRanges());
            // GroupSize (64) * ranges needed for our file (1_MB / 256_KB)
            UNIT_ASSERT_VALUES_EQUAL(256, stats.GetAllocatedCompactionRanges());
        }

        // 3 more blobs in 3 different ranges
        tablet.WriteData(handle, block, block, 'b');
        tablet.WriteData(handle, 256_KB + block, block, 'b');
        tablet.WriteData(handle, 512_KB + block, block, 'b');

        // average CompactionScore per used range became 2 => Compaction
        // should've been triggered for one of the ranges and should've
        // decreased its blob count from 2 to 1 => we should have 5 blobs
        // now
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(5, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(6, stats.GetMixedBlocksCount());
        }

        TString expected(1_MB, 0);
        memset(expected.begin(), 'a', block);
        memset(expected.begin() + block, 'b', block);
        memset(expected.begin() + 256_KB, 'a', block);
        memset(expected.begin() + 256_KB + block, 'b', block);
        memset(expected.begin() + 512_KB, 'a', block);
        memset(expected.begin() + 512_KB + block, 'b', block);

        {
            auto response = tablet.ReadData(handle, 0, 1_MB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_EQUAL(expected, buffer);
        }

        tablet.DestroyHandle(handle);
    }

    TABLET_TEST_4K_ONLY(ShouldAutomaticallyRunCompactionDueToGarbageCompactionThresholdAverage)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetNewCompactionEnabled(true);
        storageConfig.SetCompactionThresholdAverage(999'999);
        storageConfig.SetGarbageCompactionThresholdAverage(20);
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetUseMixedBlocksInsteadOfAliveBlocksInCompaction(true);
        storageConfig.SetWriteBlobThreshold(block);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        // allocating 4 compaction ranges
        TSetNodeAttrArgs args(id);
        args.SetFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE);
        args.SetSize(1_MB);
        tablet.SetNodeAttr(args);

        // 768_KB in 3 different ranges
        tablet.WriteData(handle, 0, 256_KB, 'a');
        tablet.WriteData(handle, 256_KB, 256_KB, 'a');
        tablet.WriteData(handle, 512_KB, 256_KB, 'a');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(768_KB / block, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1_MB / block, stats.GetUsedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetGarbageBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetUsedCompactionRanges());
            // GroupSize (64) * ranges needed for our file (1_MB / 256_KB)
            UNIT_ASSERT_VALUES_EQUAL(256, stats.GetAllocatedCompactionRanges());
        }

        // Write 228_KB in the first range and 232_KB in the second range
        tablet.WriteData(handle, 0, 76_KB, 'b');
        tablet.WriteData(handle, 76_KB, 76_KB, 'b');
        tablet.WriteData(handle, 152_KB, 76_KB, 'b');
        tablet.WriteData(handle, 256_KB, 116_KB, 'b');
        tablet.WriteData(handle, 260_KB, 116_KB, 'b');

        // Cleanup the deletion markers and mark the blocks as garbage
        tablet.Cleanup(GetMixedRangeIndex(id, 0));
        tablet.Cleanup(GetMixedRangeIndex(id, BlockGroupSize));
        tablet.Cleanup(GetMixedRangeIndex(id, BlockGroupSize * 2));

        // Garbage fraction is 1228_KB / 1024_KB < 1.2 => Compaction
        // shouldn't have been triggered
        {
            auto response = tablet.GetStorageStats(3);
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(8, stats.GetMixedBlobsCount());
            // now we have 1228_KB of data
            UNIT_ASSERT_VALUES_EQUAL(
                1228_KB / block,
                stats.GetMixedBlocksCount());
            // 115 garbage blocks
            UNIT_ASSERT_VALUES_EQUAL(115, stats.GetGarbageBlocksCount());
            // 1228_KB new (written by user)
            UNIT_ASSERT_VALUES_EQUAL(1228_KB, stats.GetGarbageQueueSize());

            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetUsedCompactionRanges());
            UNIT_ASSERT_VALUES_EQUAL(
                256,
                stats.GetAllocatedCompactionRanges());
            UNIT_ASSERT_VALUES_EQUAL(3, stats.CompactionRangeStatsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "r=1177944064 b=4 d=0 g=57",
                CompactionRangeToString(stats.GetCompactionRangeStats(0)));
            UNIT_ASSERT_VALUES_EQUAL(
                "r=1177944065 b=3 d=0 g=58",
                CompactionRangeToString(stats.GetCompactionRangeStats(1)));
            UNIT_ASSERT_VALUES_EQUAL(
                "r=1177944066 b=1 d=0 g=0",
                CompactionRangeToString(stats.GetCompactionRangeStats(2)));
        }

        // Write 4_KB more data in the third range
        tablet.WriteData(handle, 512_KB, 4_KB, 'b');
        tablet.Cleanup(GetMixedRangeIndex(id, BlockGroupSize * 2));

        // Garbage fraction became 1232_KB / 1024_KB > 1.2 => Compaction
        // should've been triggered for the range with the highest amount
        // of garbage blocks [256, 512_KB), the number of blobs in this range
        // should've been decreased from 3 to 1, and the number of bytes
        // from 356_KB to 256_KB
        {
            auto response = tablet.GetStorageStats(3);
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(7, stats.GetMixedBlobsCount());
            // now we have 1000_KB of data
            UNIT_ASSERT_VALUES_EQUAL(
                1000_KB / block,
                stats.GetMixedBlocksCount());
            // 57 garbage blocks in the first range and 1 in the third
            UNIT_ASSERT_VALUES_EQUAL(58, stats.GetGarbageBlocksCount());
            // 1228_KB previous
            // 4_KB new (written by user)
            // 256_KB new (rewritten by Compaction)
            // 488_KB garbage (marked after Compaction)
            UNIT_ASSERT_VALUES_EQUAL(1976_KB, stats.GetGarbageQueueSize());

            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetUsedCompactionRanges());
            UNIT_ASSERT_VALUES_EQUAL(
                256,
                stats.GetAllocatedCompactionRanges());
            // only two ranges are returned since other one is marked with the
            // 'compacted' flag and thus skipped during top ranges gathering
            UNIT_ASSERT_VALUES_EQUAL(2, stats.CompactionRangeStatsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "r=1177944064 b=4 d=0 g=57",
                CompactionRangeToString(stats.GetCompactionRangeStats(0)));
            UNIT_ASSERT_VALUES_EQUAL(
                "r=1177944066 b=2 d=0 g=1",
                CompactionRangeToString(stats.GetCompactionRangeStats(1)));
        }

        // Writing more data to trigger garbage-based compaction once again
        // Need to write 232_KB of data in total

        // [0, 256_KB) will contain 64_KB more garbage
        tablet.WriteData(handle, 0, 64_KB, 'c');

        // [256_KB, 512_KB) will contain more blobs
        const ui64 bytesInSecondRange = 168_KB;
        for (ui64 off = 0; off < bytesInSecondRange; off += 4_KB) {
            tablet.WriteData(handle, 256_KB + off, 4_KB, 'c');
        }

        tablet.Cleanup(GetMixedRangeIndex(id, 0));
        tablet.Cleanup(GetMixedRangeIndex(id, BlockGroupSize));

        // Garbage fraction became 1232_KB / 1024_KB > 1.2 => Compaction
        // should've been triggered for the range [0, 256_KB) and should've
        // decreased its blob count to 1
        // Byte count in that range should've been decreased to 256_KB
        {
            auto response = tablet.GetStorageStats(3);
            const auto& stats = response->Record.GetStats();
            // a lot of blobs are stored in the range [256_KB, 512_KB)
            UNIT_ASSERT_VALUES_EQUAL(46, stats.GetMixedBlobsCount());
            // now we have 768_KB + bytesInSecondRange + 8_KB of data
            UNIT_ASSERT_VALUES_EQUAL(
                (768_KB + bytesInSecondRange + 4_KB) / block,
                stats.GetMixedBlocksCount());
            // 42 garbage blocks in the second range and 1 in the third
            UNIT_ASSERT_VALUES_EQUAL(43, stats.GetGarbageBlocksCount());
            // calculating GarbageQueueSize and comparing it to some expected
            // value once again isn't really needed for this test

            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetUsedCompactionRanges());
            UNIT_ASSERT_VALUES_EQUAL(
                256,
                stats.GetAllocatedCompactionRanges());

            UNIT_ASSERT_VALUES_EQUAL(3, stats.CompactionRangeStatsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "r=1177944065 b=43 d=0 g=42",
                CompactionRangeToString(stats.GetCompactionRangeStats(0)));
            UNIT_ASSERT_VALUES_EQUAL(
                "r=1177944066 b=2 d=0 g=1",
                CompactionRangeToString(stats.GetCompactionRangeStats(1)));
            UNIT_ASSERT_VALUES_EQUAL(
                "r=1177944064 b=1 d=0 g=0",
                CompactionRangeToString(stats.GetCompactionRangeStats(2)));
        }

        TString expected(1_MB, 0);
        memset(expected.begin(), 'a', 768_KB);
        memset(expected.begin(), 'b', 228_KB);
        memset(expected.begin() + 256_KB, 'b', 120_KB);
        memset(expected.begin() + 512_KB, 'b', 4_KB);
        memset(expected.begin(), 'c', 64_KB);
        memset(expected.begin() + 256_KB, 'c', bytesInSecondRange);

        {
            auto response = tablet.ReadData(handle, 0, 1_MB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_EQUAL(expected, buffer);
        }

        tablet.DestroyHandle(handle);
    }

    TABLET_TEST(ShouldAutomaticallyRunCleanup)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(8);
        storageConfig.SetWriteBlobThreshold(2 * block);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        // deletion 0
        tablet.WriteData(handle, 0, block, 'a');
        tablet.Flush();

        // deletion 1
        tablet.WriteData(handle, block, block, 'b');

        // deletion 2
        tablet.WriteData(handle, 0, 2 * block, 'c');

        // deletion 3
        tablet.WriteData(handle, 0, 2 * block, 'd');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 3);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDeletionMarkersCount(), 6);
        }

        // deletion 4
        tablet.WriteData(handle, 2 * block, 2 * block, 'e');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDeletionMarkersCount(), 0);
        }

        {
            auto response = tablet.ReadData(handle, 0, 2 * block);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, 2 * block, 'd'));
        }

        {
            auto response = tablet.ReadData(handle, 2 * block, 2 * block);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, 2 * block, 'e'));
        }

        tablet.DestroyHandle(handle);
    }

    TABLET_TEST_4K_ONLY(ShouldAutomaticallyRunCleanupDueToCleanupThresholdAverage)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetNewCleanupEnabled(true);
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetCleanupThresholdAverage(3);
        storageConfig.SetWriteBlobThreshold(block);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        // allocating 4 compaction ranges
        TSetNodeAttrArgs args(id);
        args.SetFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE);
        args.SetSize(1_MB);
        tablet.SetNodeAttr(args);

        tablet.WriteData(handle, 0, 2 * block, 'a');
        tablet.WriteData(handle, 256_KB, 3 * block, 'b');
        tablet.WriteData(handle, 512_KB, 3 * block, 'c');
        tablet.WriteData(handle, 768_KB, 3 * block, 'd');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 4);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDeletionMarkersCount(), 11);
        }

        // Cleanup should've been triggered when the number of deletion markers
        // hits 12 (an average of 3 markers per range)
        tablet.WriteData(handle, 2 * block, 2 * block, 'e');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 5);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDeletionMarkersCount(), 9);
        }

        {
            auto response = tablet.ReadData(handle, 0, 2 * block);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, 2 * block, 'a'));
        }

        {
            auto response = tablet.ReadData(handle, 2 * block, 2 * block);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, 2 * block, 'e'));
        }

        {
            auto response = tablet.ReadData(handle, 256_KB, 3 * block);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, 3 * block, 'b'));
        }

        {
            auto response = tablet.ReadData(handle, 512_KB, 3 * block);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, 3 * block, 'c'));
        }

        {
            auto response = tablet.ReadData(handle, 768_KB, 3 * block);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, 3 * block, 'd'));
        }

        tablet.DestroyHandle(handle);
    }

    TABLET_TEST(ShouldAutomaticallyRunCollectGarbage)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetWriteBlobThreshold(2 * block);
        storageConfig.SetCollectGarbageThreshold(6 * block);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        // flushed blob
        tablet.WriteData(handle, 0, block, 'a');
        tablet.WriteData(handle, block, block, 'b');
        tablet.Flush();

        // directly written blob
        tablet.WriteData(handle, 0, 3 * block, 'c');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            // 20_KB of new blobs
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageQueueSize(), 5 * block);
        }

        // triggering automatic garbage collection
        tablet.WriteData(handle, 0, block, 'a');
        tablet.Flush();

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            // last flushed blob remains since Flush acquires a collect
            // barrier
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageQueueSize(), block);
        }

        tablet.DestroyHandle(handle);
    }

    TABLET_TEST(ShouldCorrectlyHandleConcurrentCompactionAndMergedBlobWrite)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetWriteBlobThreshold(block);

        TTestEnv env({}, std::move(storageConfig));

        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, block, 'a');

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
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
        UNIT_ASSERT(readBlob);

        tablet.WriteData(handle, 0, block, 'b');

        // writing some other data elsewhere to increment commit id
        tablet.WriteData(handle, 1_MB, block, 'c');

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
        expected.ReserveAndResize(block);
        memset(expected.begin(), 'b', block);

        {
            auto response = tablet.ReadData(handle, 0, block);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.DestroyHandle(handle);
    }

    TABLET_TEST(ShouldReadFromFreshAndBlobsCorrectly)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetWriteBlobThreshold(1_GB);
        storageConfig.SetFlushThreshold(1_GB);

        TTestEnv env({}, std::move(storageConfig));

        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        const auto freshBlockCount = 2;

        TString expected;
        expected.ReserveAndResize(block * freshBlockCount);

        for (ui32 i = 0; i < freshBlockCount; ++i) {
            for (ui32 j = 0; j < i + 1; ++j) {
                tablet.WriteData(handle, block * i, block, 'a' + j);
                memset(expected.begin() + block * i, 'a' + j, block);
            }
        }

        {
            auto response = tablet.ReadData(handle, 0, block * freshBlockCount);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.Flush();

        {
            auto response = tablet.ReadData(handle, 0, block * freshBlockCount);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.WriteData(handle, 0, block, 'e');
        memset(expected.begin(), 'e', block);

        {
            auto response = tablet.ReadData(handle, 0, block * freshBlockCount);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.Flush();

        {
            auto response = tablet.ReadData(handle, 0, block * freshBlockCount);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.DestroyHandle(handle);
    }

    TABLET_TEST(ShouldNotWriteToYellowGroups)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetFlushThreshold(1_GB);
        storageConfig.SetFlushBytesThreshold(1_GB);
        storageConfig.SetWriteBlobThreshold(4 * block);

        TTestEnv env({}, std::move(storageConfig));
        auto registry = env.GetRegistry();

        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        tabletConfig.ChannelCount = 4;
        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        // writing some data so that our Compaction/Flush/FlushBytes requests
        // don't turn into no-ops

        tablet.WriteData(handle, 0, 4 * block, 'a'); // merged blob
        tablet.WriteData(handle, 0, block, 'a');     // fresh
        tablet.WriteData(handle, 4 * block + 100, block * 3 / 4, 'a'); // fresh bytes

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

        tablet.SendWriteDataRequest(handle, 0, 4 * block, 'b');
        {
            auto response = tablet.RecvWriteDataResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        TString expected;
        expected.ReserveAndResize(4 * block);
        memset(expected.begin(), 'b', 4 * block);

        // checking that we can read what we have just written

        {
            auto response = tablet.ReadData(handle, 0, 4 * block);
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

        tablet.SendWriteDataRequest(handle, 0, 4 * block, 'c');
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
            auto response = tablet.ReadData(handle, 0, 4 * block);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        // checking that another reassign request has not been sent

        UNIT_ASSERT_VALUES_EQUAL(0, reassignedTabletId);
        UNIT_ASSERT_VALUES_EQUAL(0, channels.size());

        // checking that another reassign request is sent after 1min delay

        env.GetRuntime().AdvanceCurrentTime(TDuration::Minutes(1));

        tablet.SendWriteDataRequest(handle, 0, 4 * block, 'c');
        {
            auto response = tablet.RecvWriteDataResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_FS_OUT_OF_SPACE, response->GetStatus());
        }

        UNIT_ASSERT_VALUES_EQUAL(tabletId, reassignedTabletId);
        UNIT_ASSERT_VALUES_EQUAL(1, channels.size());
        UNIT_ASSERT_VALUES_EQUAL(3, channels.front());

        tablet.AdvanceTime(TDuration::Seconds(15));
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(5));

        TTestRegistryVisitor visitor;
        // clang-format off
        registry->Visit(TInstant::Zero(), visitor);
        visitor.ValidateExpectedCounters({
            {{{"sensor", "ReassignCount"}, {"filesystem", "test"}}, 2},
            {{{"sensor", "WritableChannelCount"}, {"filesystem", "test"}}, 3},
            {{{"sensor", "UnwritableChannelCount"}, {"filesystem", "test"}}, 1},
            {{{"sensor", "ChannelsToMoveCount"}, {"filesystem", "test"}}, 1},
        });
        // clang-format on

        tablet.DestroyHandle(handle);
    }

    TABLET_TEST(ShouldProperlyReadBlobsWithHoles)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetFlushThreshold(999'999);
        storageConfig.SetWriteBlobThreshold(1_GB);

        TTestEnv env({}, std::move(storageConfig));

        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, block, 'a');
        tablet.WriteData(handle, 2 * block, block, 'b');

        tablet.Flush();

        TString expected;
        expected.ReserveAndResize(3 * block);
        memset(expected.begin(), 'a', block);
        memset(expected.begin() + block, 0, block);
        memset(expected.begin() + 2 * block, 'b', block);

        {
            auto response = tablet.ReadData(handle, 0, 3 * block);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.DestroyHandle(handle);
    }

    TABLET_TEST(ShouldNotDeleteBlobsMarkedAsGarbageByUncommittedTransactions)
    {
        using namespace NKikimr;

        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetWriteBlobThreshold(block);

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
        tablet.WriteData(handle, 0, block, 'a');

        UNIT_ASSERT_VALUES_EQUAL(1, blobIds.size());

        // setting keep flags
        tablet.CollectGarbage();

        // blob2 (making sure that our next CollectGarbage call does something)
        tablet.WriteData(handle, 0, block, 'b');

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
        expected.ReserveAndResize(block);
        memset(expected.begin(), 'b', block);

        // Ensuring that our index is initialized
        {
            auto response = tablet.ReadData(handle, 0, block);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        // Running garbage compaction - it should succeed reading blob1
        tablet.Compaction(rangeId);
    }

    TABLET_TEST(ShouldLimitDeleteGarbageBlobsPerTx)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxDeleteGarbageBlobsPerTx(2);
        storageConfig.SetWriteBlobThreshold(block);
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

        tablet.WriteData(handle, 0, block, 'a');
        tablet.WriteData(handle, 0, block, 'a');
        tablet.WriteData(handle, 0, block, 'a');
        tablet.WriteData(handle, 0, block, 'a');
        tablet.WriteData(handle, 0, block, 'a');

        tablet.CollectGarbage();

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageQueueSize(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTxDeleteGarbageRwCompleted(), 3);
        }
    }

    TABLET_TEST_16K(ShouldRejectWritesDueToBackpressure)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetWriteBlobThreshold(2 * block);
        storageConfig.SetFlushThreshold(1_GB);
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetFlushBytesThreshold(1_GB);
        storageConfig.SetFlushThresholdForBackpressure(2 * block);
        storageConfig.SetCompactionThresholdForBackpressure(
            8 * DefaultBlockSize / block);
        storageConfig.SetCleanupThresholdForBackpressure(20);
        storageConfig.SetFlushBytesThresholdForBackpressure(block / 2);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto checkIsWriteAllowed = [&](bool expected)
        {
            TTestRegistryVisitor visitor;
            tablet.SendRequest(tablet.CreateUpdateCounters());
            env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
            env.GetRegistry()->Visit(TInstant::Zero(), visitor);
            visitor.ValidateExpectedCounters({
                {{{"sensor", "IsWriteAllowed"}, {"filesystem", "test"}},
                 expected},
            });
        };

        checkIsWriteAllowed(true);

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);
        tablet.WriteData(handle, 0, block, '0'); // 1 fresh block, 1 marker
        tablet.WriteData(handle, 0, block, '0'); // 2 fresh blocks, 2 markers

        // backpressure due to FlushThresholdForBackpressure
        tablet.SendWriteDataRequest(handle, 0, block, '0');
        {
            auto response = tablet.RecvWriteDataResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());

            checkIsWriteAllowed(false);
        }

        tablet.Flush();

        // no backpressure after Flush
        tablet.WriteData(handle, 0, block, '0'); // 1 blob, 1 fresh block, 3 markers
        tablet.WriteData(handle, 0, 2 * block, '0'); // 2 blobs, 1 fresh block, 5 markers
        tablet.WriteData(handle, 0, 2 * block, '0'); // 3 blobs, 1 fresh block, 7 markers
        tablet.WriteData(handle, 2 * block, 2 * block, '0'); // 4 blobs, 1 fresh block, 9 markers
        tablet.WriteData(handle, 4 * block, 2 * block, '0'); // 5 blobs, 1 fresh block, 11 markers
        tablet.WriteData(handle, 6 * block, 2 * block, '0'); // 6 blobs, 1 fresh block, 13 markers
        tablet.WriteData(handle, 8 * block, 2 * block, '0'); // 7 blobs, 1 fresh block, 15 markers
        tablet.WriteData(handle, 10 * block, 2 * block, '0'); // 8 blobs, 1 fresh block, 17 markers

        // backpressure due to CompactionScoreThresholdForBackpressure
        tablet.SendWriteDataRequest(handle, 0, 2 * block, '0');
        {
            auto response = tablet.RecvWriteDataResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        ui32 rangeId = GetMixedRangeIndex(id, 0);
        tablet.Compaction(rangeId);

        // no backpressure after Compaction
        tablet.WriteData(handle, 0, 2 * block, '0'); // 2 blobs, 1 fresh block, 19 markers

        tablet.WriteData(handle, 0, block, '0'); // 2 blobs, 2 fresh blocks, 20 markers
        tablet.Flush(); // 3 blobs, 10 markers
        tablet.Compaction(rangeId); // 1 blob, 10 markers

        // backpressure due to CleanupScoreThresholdForBackpressure
        tablet.SendWriteDataRequest(handle, 0, block, '0');
        {
            auto response = tablet.RecvWriteDataResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        tablet.Cleanup(rangeId); // 1 blob

        // no backpressure after Cleanup
        tablet.WriteData(handle, 0, block / 4, '0'); // 1 blob, block / 4 fresh bytes
        tablet.WriteData(handle, 0, block / 4, '0'); // 1 blob, block / 2 fresh bytes

        // backpressure due to FlushBytesThresholdForBackpressure
        tablet.SendWriteDataRequest(handle, 0, block / 4, '0');
        {
            auto response = tablet.RecvWriteDataResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        tablet.FlushBytes(); // 2 blobs

        // no backpressure after FlushBytes
        tablet.WriteData(handle, 0, block / 4, '0'); // 2 blobs, block / 4 fresh bytes

        TString expected;
        expected.ReserveAndResize(2 * block);
        memset(expected.begin(), '0', 2 * block);

        {
            auto response = tablet.ReadData(handle, 0, 2 * block);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.DestroyHandle(handle);
    }

    TABLET_TEST(ShouldRecoverAfterFreshBlocksRelatedBackpressure)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetWriteBlobThreshold(1_GB);
        storageConfig.SetFlushThreshold(2 * block);
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetFlushBytesThreshold(1_GB);
        storageConfig.SetFlushThresholdForBackpressure(2 * block);
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
        tablet.WriteData(handle, 0, block, '0'); // 1 fresh block
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

        tablet.WriteData(handle, block, block, '0'); // 2 fresh blocks

        // backpressure due to FlushThresholdForBackpressure
        tablet.SendWriteDataRequest(handle, block, block, '1');
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
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
        UNIT_ASSERT(flushObserved);

        // no backpressure after Flush
        tablet.WriteData(handle, block, block, '0');

        TString expected;
        expected.ReserveAndResize(2 * block);
        memset(expected.begin(), '0', 2 * block);

        {
            auto response = tablet.ReadData(handle, 0, 2 * block);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.DestroyHandle(handle);
    }

    TABLET_TEST(ShouldSuicideAfterBackpressureErrors)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetWriteBlobThreshold(2 * block);
        storageConfig.SetFlushThreshold(1_GB);
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetFlushBytesThreshold(1_GB);
        storageConfig.SetFlushThresholdForBackpressure(2 * block);
        storageConfig.SetCompactionThresholdForBackpressure(999'999);
        storageConfig.SetCleanupThresholdForBackpressure(999'999);
        storageConfig.SetFlushBytesThresholdForBackpressure(1_GB);
        storageConfig.SetFlushThresholdForBackpressure(block);
        storageConfig.SetMaxBackpressureErrorsBeforeSuicide(999999);
        storageConfig.SetMaxBackpressurePeriodBeforeSuicide(60'000); // 1m

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);
        tablet.WriteData(handle, 0, block, '0'); // 1 fresh block, 1 marker

        // backpressure due to FlushThresholdForBackpressure
        tablet.SendWriteDataRequest(handle, 0, block, '0');
        {
            auto response = tablet.RecvWriteDataResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        // this filter is rather coarse so we need to enable it as late as
        // possible - as close to the poison pill "trigger point" as possible
        bool poisonPillObserved = false;
        env.GetRuntime().SetEventFilter([&] (auto& runtime, auto& event) {
            Y_UNUSED(runtime);

            switch (event->GetTypeRewrite()) {
                case TEvents::TSystem::Poison: {
                    poisonPillObserved = true;
                }
            }

            return false;
        });

        // backpressure due to FlushThresholdForBackpressure
        // should not cause tablet reboot since the backpressure period hasn't
        // passed yet
        tablet.SendWriteDataRequest(handle, 0, block, '0');
        {
            auto response = tablet.RecvWriteDataResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
        UNIT_ASSERT(!poisonPillObserved);

        env.GetRuntime().AdvanceCurrentTime(TDuration::Minutes(1));

        // backpressure due to FlushThresholdForBackpressure
        // should cause tablet reboot
        tablet.SendWriteDataRequest(handle, 0, block, '0');
        {
            auto response = tablet.RecvWriteDataResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
        UNIT_ASSERT(poisonPillObserved);
    }

    TABLET_TEST(FlushBytesShouldReenqueueAfterAttemptToEnqueueItDuringFlush)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetWriteBlobThreshold(1_GB);
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetFlushThreshold(2 * block);
        storageConfig.SetFlushBytesThreshold(block);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        TAutoPtr<IEventHandle> flush;
        TAutoPtr<IEventHandle> flushBytesCompletion;

        env.GetRuntime().SetEventFilter(
            [&] (auto& runtime, TAutoPtr<IEventHandle>& event)
        {
            Y_UNUSED(runtime);

            switch (event->GetTypeRewrite()) {
                case TEvIndexTabletPrivate::EvFlushRequest: {
                    UNIT_ASSERT(!flush);
                    flush = event.Release();
                    return true;
                }

                case TEvIndexTabletPrivate::EvFlushBytesCompleted: {
                    UNIT_ASSERT(!flushBytesCompletion);
                    flushBytesCompletion = event.Release();
                    return true;
                }
            }

            return false;
        });

        // triggering Flush
        tablet.WriteData(handle, 0, 2 * block, '0'); // 2 fresh blocks
        env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(100));
        // Flush caught
        UNIT_ASSERT(flush);

        // triggering FlushBytes
        // FlushBytes shouldn't get triggered since Flush is already enqueued
        tablet.WriteData(handle, 0, block / 2, '0'); // fresh bytes
        tablet.WriteData(handle, 0, block / 2, '0'); // fresh bytes
        env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(100));

        // releasing Flush
        env.GetRuntime().Send(flush.Release(), 1 /* node index */);
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));

        // FlushBytes should get enqueued and should successfully run and finish
        UNIT_ASSERT(flushBytesCompletion);
    }

    TABLET_TEST(ShouldReadUnAligned)
    {
        const auto block = tabletConfig.BlockSize;

        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        TVector<char> buffer(2 * block);
        for (ui64 i = 0; i < 2 * block; ++i) {
            buffer[i] = i % 10;
        }

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);
        tablet.WriteData(handle, 0, buffer.size(), buffer.data());

        {
            // [0, block - 1)
            TStringBuf expected(buffer.data(), block - 1);

            auto response = tablet.ReadData(handle, 0, block - 1);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected.size(), buffer.size());
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        {
            // [0, block + 1)
            TStringBuf expected(buffer.data(), block + 1);

            auto response = tablet.ReadData(handle, 0, block + 1);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected.size(), buffer.size());
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        {
            // [1, block)
            TStringBuf expected(buffer.data() + 1, block - 1);

            auto response = tablet.ReadData(handle, 1, block - 1);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected.size(), buffer.size());
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        {
            // [1, block + 2)
            TStringBuf expected(buffer.data() + 1, block + 1);

            auto response = tablet.ReadData(handle, 1, block + 1);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected.size(), buffer.size());
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        {
            // [1, block / 4 + 1)
            TStringBuf expected(buffer.data() + 1, block / 4);

            auto response = tablet.ReadData(handle, 1, block / 4);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected.size(), buffer.size());
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        {
            // [block + block / 2, block + block) trim by file end
            TStringBuf expected(buffer.data() + block * 3 / 2, block / 2);

            auto response = tablet.ReadData(handle, block * 3 / 2, block);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected.size(), buffer.size());
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }
    }

    TABLET_TEST(ShouldWriteFreshBlocksForUnalignedDataAtTheEnd)
    {
        const auto block = tabletConfig.BlockSize;

        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto response = tablet.FlushBytes();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_FALSE,
            response->Error.GetCode(),
            response->Error.GetMessage());

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, block * 3 / 4, 'a');
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBytesCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 1);
        }

        tablet.WriteData(handle, 0, block * 7 / 4, 'b');
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBytesCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 3);
        }

        tablet.WriteData(handle, 100, block * 3 / 4, 'a');
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBytesCount(), block * 3 / 4);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 3);
        }
    }

    TABLET_TEST(ShouldCollectTracesForWriteRequests)
    {
        const auto block = tabletConfig.BlockSize;

        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        auto request = tablet.CreateWriteDataRequest(handle, 0, block, 'a');
        request->Record.MutableHeaders()->MutableInternal()->MutableTrace()
            ->SetIsTraced(true);

        tablet.SendRequest(std::move(request));
        auto response = tablet.AssertWriteDataResponse(S_OK);
        CheckForkJoin(
            response->Record.GetHeaders().GetTrace().GetLWTrace().GetTrace(),
            false);

        request = tablet.CreateWriteDataRequest(handle, 0, 1_MB, 'b');
        request->Record.MutableHeaders()->MutableInternal()->MutableTrace()
            ->SetIsTraced(true);

        tablet.SendRequest(std::move(request));
        response = tablet.AssertWriteDataResponse(S_OK);
        CheckForkJoin(
            response->Record.GetHeaders().GetTrace().GetLWTrace().GetTrace(),
            true);
    }

    TABLET_TEST(ShouldDoForcedCompaction)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
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

        tablet.ForcedRangeOperation(
            ::xrange(0, 10, 1),
            TEvIndexTabletPrivate::EForcedRangeOperationMode::Compaction);
        UNIT_ASSERT_VALUES_EQUAL(ranges.size(), 10);
        UNIT_ASSERT_VALUES_EQUAL(ranges.front(), 0);
        UNIT_ASSERT_VALUES_EQUAL(ranges.back(), 9);

        tablet.AssertForcedRangeOperationFailed(
            TVector<ui32>{},
            TEvIndexTabletPrivate::EForcedRangeOperationMode::Compaction);
    }

    TABLET_TEST(ShouldRetryForcedCompaction)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
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

        tablet.ForcedRangeOperation(
            ::xrange(0, 2, 1),
            TEvIndexTabletPrivate::EForcedRangeOperationMode::Compaction);
        UNIT_ASSERT_VALUES_EQUAL(ranges.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ranges[0], 0);
        UNIT_ASSERT_VALUES_EQUAL(ranges[1], 0);
        UNIT_ASSERT_VALUES_EQUAL(ranges[2], 1);
    }

    TABLET_TEST(ShouldEnqueuePendingForcedCompaction)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
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

        tablet.SendForcedRangeOperationRequest(
            ::xrange(0, 1, 1),
            TEvIndexTabletPrivate::EForcedRangeOperationMode::Compaction);
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
        UNIT_ASSERT(request);

        tablet.SendForcedRangeOperationRequest(
            ::xrange(1, 2, 1),
            TEvIndexTabletPrivate::EForcedRangeOperationMode::Compaction);
        env.GetRuntime().Send(request.Release(), 1 /* node index */);
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(ranges.size(), 2);
    }

    TABLET_TEST(ShouldForceCompactAndCleanup)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        TVector<ui32> compactionRanges;
        TVector<ui32> cleanupRanges;
        env.GetRuntime().SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTabletPrivate::EvCompactionRequest: {
                        auto* msg = event->Get<
                            TEvIndexTabletPrivate::TEvCompactionRequest>();
                        compactionRanges.push_back(msg->RangeId);
                        break;
                    }
                    case TEvIndexTabletPrivate::EvCleanupRequest: {
                        auto* msg = event->Get<
                            TEvIndexTabletPrivate::TEvCleanupRequest>();
                        cleanupRanges.push_back(msg->RangeId);
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        tablet.ForcedRangeOperation(
            ::xrange(0, 10, 1),
            TEvIndexTabletPrivate::EForcedRangeOperationMode::Compaction);
        UNIT_ASSERT_VALUES_EQUAL(compactionRanges.size(), 10);
        UNIT_ASSERT_VALUES_EQUAL(compactionRanges.front(), 0);
        UNIT_ASSERT_VALUES_EQUAL(compactionRanges.back(), 9);

        tablet.ForcedRangeOperation(
            ::xrange(5, 15, 1),
            TEvIndexTabletPrivate::EForcedRangeOperationMode::Cleanup);
        UNIT_ASSERT_VALUES_EQUAL(cleanupRanges.size(), 10);
        UNIT_ASSERT_VALUES_EQUAL(cleanupRanges.front(), 5);
        UNIT_ASSERT_VALUES_EQUAL(cleanupRanges.back(), 14);
    }

    TABLET_TEST(ForcedCleanupShouldRemoveStaleData)
    {
        const auto block = tabletConfig.BlockSize;
        const auto rangesCount = 13;
        const auto dataSize = BlockGroupSize * block * rangesCount;

        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        ui32 cleanupCount = 0;
        env.GetRuntime().SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTabletPrivate::EvCleanupRequest: {
                        ++cleanupCount;
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));

        TVector<ui32> rangesSubjectToCleanup;
        for (ui32 i = 0; i < rangesCount * BlockGroupSize; ++i) {
            rangesSubjectToCleanup.push_back(GetMixedRangeIndex(id, i));
        }
        UNIT_ASSERT_VALUES_EQUAL(
            THashSet<ui32>(
                rangesSubjectToCleanup.begin(),
                rangesSubjectToCleanup.end())
                .size(),
            rangesCount);

        ui64 handle = CreateHandle(tablet, id);
        tablet.WriteData(handle, 0, dataSize, 'a');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), rangesCount);
            UNIT_ASSERT_VALUES_EQUAL(
                stats.GetMixedBlocksCount(),
                rangesCount * BlockGroupSize);
        }

        tablet.UnlinkNode(RootNodeId, "test", false);
        tablet.DestroyHandle(handle);

        ui32 id2 =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test2"));
        UNIT_ASSERT_VALUES_EQUAL(
            GetMixedRangeIndex(id, 0),
            GetMixedRangeIndex(id2, 0));

        handle = CreateHandle(tablet, id2);
        tablet.WriteData(handle, 0, dataSize, 'a');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                stats.GetMixedBlobsCount(),
                rangesCount * 2);
        }

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                stats.GetMixedBlobsCount(),
                rangesCount * 2);
            UNIT_ASSERT_VALUES_EQUAL(
                stats.GetMixedBlocksCount(),
                rangesCount * 2 * BlockGroupSize);
        }

        tablet.ForcedRangeOperation(
            rangesSubjectToCleanup,
            TEvIndexTabletPrivate::EForcedRangeOperationMode::Cleanup);

        UNIT_ASSERT_VALUES_EQUAL(cleanupCount, rangesCount * BlockGroupSize);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), rangesCount);
            UNIT_ASSERT_VALUES_EQUAL(
                stats.GetMixedBlocksCount(),
                rangesCount * BlockGroupSize);
        }
    }

    TABLET_TEST(ShouldForceCompactRangeAndRemoveStaleNodesData)
    {
        const auto block = tabletConfig.BlockSize;
        const auto dataSize = 32 * block;

        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        bool changed = false;
        env.GetRuntime().SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    using TAddBlob = TEvIndexTabletPrivate::TEvAddBlobRequest;
                    case TEvIndexTabletPrivate::EvAddBlobRequest: {
                        if (!changed) {
                            auto* msg = event->Get<TAddBlob>();
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
        tablet.WriteData(handle, 0, dataSize, 'a');

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

        tablet.ForcedRangeOperation(
            TVector<ui32>{rangeId},
            TEvIndexTabletPrivate::EForcedRangeOperationMode::Compaction);
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 0);
        }

        // Should not remove data for existing nodes
        id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        handle = CreateHandle(tablet, id);
        tablet.WriteData(handle, 0, dataSize, 'a');

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

        tablet.ForcedRangeOperation(
            TVector<ui32>{rangeId},
            TEvIndexTabletPrivate::EForcedRangeOperationMode::Compaction);
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
        }
    }

    TABLET_TEST(ShouldDoForcedDeletionOfZeroCompactionRanges)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxZeroCompactionRangesToDeletePerTx(10);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 requests = 0;
        ui32 lastCompactionMapRangeId = 0;
        env.GetRuntime().SetEventFilter(
            [&] (auto& runtime, TAutoPtr<IEventHandle>& event)
        {
            Y_UNUSED(runtime);

            switch (event->GetTypeRewrite()) {
                case TEvIndexTabletPrivate
                    ::EvDeleteZeroCompactionRangesRequest:
                {
                    ++requests;
                    break;
                }
                case TEvIndexTabletPrivate::EvLoadCompactionMapChunkResponse: {
                    lastCompactionMapRangeId = Max(
                        event->Get<
                            TEvIndexTabletPrivate
                            ::TEvLoadCompactionMapChunkResponse>()->LastRangeId,
                        lastCompactionMapRangeId);
                    break;
                }
            }
            return false;
        });

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");
        tablet.RebootTablet();

        TVector<NProtoPrivate::TCompactionRangeStats> ranges;
        for (ui32 i = 0; i < 50; ++i) {
            NProtoPrivate::TCompactionRangeStats range;
            range.SetRangeId(i);
            range.SetBlobCount(0);
            range.SetDeletionCount(0);
            ranges.push_back(range);
        }
        for (ui32 i = 100; i < 200; ++i) {
            NProtoPrivate::TCompactionRangeStats range;
            range.SetRangeId(i);
            range.SetBlobCount(0);
            range.SetDeletionCount(0);
            ranges.push_back(range);
        }
        for (ui32 i = 60; i < 80; ++i) {
            NProtoPrivate::TCompactionRangeStats range;
            range.SetRangeId(i);
            range.SetBlobCount(1);
            range.SetDeletionCount(2);
            ranges.push_back(range);
        }
        tablet.WriteCompactionMap(ranges);
        tablet.RebootTablet();

        tablet.ForcedOperation(
            NProtoPrivate::TForcedOperationRequest::E_DELETE_EMPTY_RANGES);

        UNIT_ASSERT_VALUES_EQUAL(requests, 15);
        UNIT_ASSERT_VALUES_EQUAL(lastCompactionMapRangeId, 199);
        tablet.AssertForcedRangeOperationFailed(
            TVector<ui32>{},
            TEvIndexTabletPrivate
                ::EForcedRangeOperationMode::DeleteZeroCompactionRanges);

        lastCompactionMapRangeId = 0;
        tablet.RebootTablet();
        UNIT_ASSERT_VALUES_EQUAL(lastCompactionMapRangeId, 79);
    }

    TABLET_TEST(ShouldDumpCompactionRangeBlobs)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));

        ui64 handle = CreateHandle(tablet, id);
        tablet.WriteData(handle, 0, 128_KB, 'a');

        ui32 rangeId = GetMixedRangeIndex(id, 0);

        auto response = tablet.DumpCompactionRange(rangeId);
        UNIT_ASSERT_VALUES_EQUAL(response->Blobs.size(), 1);
    }

    TABLET_TEST(ShouldLoadCompactionMapInBackground)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetLoadedCompactionRangesPerTx(2);
        storageConfig.SetWriteBlobThreshold(block);

        TTestEnv env({}, std::move(storageConfig));

        env.CreateSubDomain("nfs");

        TAutoPtr<IEventHandle> loadChunk;
        ui32 loadChunkCount = 0;
        env.GetRuntime().SetEventFilter([&] (auto& runtime, auto& event) {
            Y_UNUSED(runtime);

            switch (event->GetTypeRewrite()) {
                case TEvIndexTabletPrivate::EvLoadCompactionMapChunkRequest: {
                    ++loadChunkCount;

                    if (loadChunkCount == 1) {
                        loadChunk = event.Release();
                        return true;
                    }
                }
            }

            return false;
        });

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.SendWriteDataRequest(handle, 0, block, 'a');
        {
            auto response = tablet.RecvWriteDataResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        UNIT_ASSERT(loadChunk);
        UNIT_ASSERT_VALUES_EQUAL(1, loadChunkCount);

        ui32 rangeId = GetMixedRangeIndex(id, 0);
        tablet.SendCompactionRequest(rangeId);
        {
            auto response = tablet.RecvCompactionResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, response->GetStatus());
        }

        UNIT_ASSERT_VALUES_EQUAL(1, loadChunkCount);

        env.GetRuntime().Send(loadChunk.Release(), nodeIdx);
        tablet.SendWriteDataRequest(handle, 0, block, 'a');
        {
            auto response = tablet.RecvWriteDataResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }
        tablet.SendCompactionRequest(rangeId);
        {
            auto response = tablet.RecvCompactionResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        UNIT_ASSERT_VALUES_EQUAL(2, loadChunkCount);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
        }

        // write some more data
        for (ui32 fileNo = 0; fileNo < 3 * NodeGroupSize; ++fileNo) {
            auto id1 = CreateNode(
                tablet,
                TCreateNodeArgs::File(RootNodeId, Sprintf("test%u", fileNo)));
            auto handle1 = CreateHandle(tablet, id1);
            tablet.WriteData(handle1, 0, 2 * BlockGroupSize * block, 'x');
        }

        auto checkCompactionMap = [&] () {
            auto response = tablet.GetStorageStats(10);
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(8, stats.GetUsedCompactionRanges());
            UNIT_ASSERT_VALUES_EQUAL(
                1024,
                stats.GetAllocatedCompactionRanges());
            UNIT_ASSERT_VALUES_EQUAL(8, stats.CompactionRangeStatsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "r=1656356864 b=16 d=1024 g=0",
                CompactionRangeToString(stats.GetCompactionRangeStats(0)));
            UNIT_ASSERT_VALUES_EQUAL(
                "r=1656356865 b=16 d=1024 g=0",
                CompactionRangeToString(stats.GetCompactionRangeStats(1)));
            UNIT_ASSERT_VALUES_EQUAL(
                "r=4283236352 b=16 d=1024 g=0",
                CompactionRangeToString(stats.GetCompactionRangeStats(2)));
            UNIT_ASSERT_VALUES_EQUAL(
                "r=4283236353 b=16 d=1024 g=0",
                CompactionRangeToString(stats.GetCompactionRangeStats(3)));
            UNIT_ASSERT_VALUES_EQUAL(
                "r=1177944064 b=14 d=833 g=0",
                CompactionRangeToString(stats.GetCompactionRangeStats(4)));
            UNIT_ASSERT_VALUES_EQUAL(
                "r=1177944065 b=13 d=832 g=0",
                CompactionRangeToString(stats.GetCompactionRangeStats(5)));
            UNIT_ASSERT_VALUES_EQUAL(
                "r=737148928 b=3 d=192 g=0",
                CompactionRangeToString(stats.GetCompactionRangeStats(6)));
            UNIT_ASSERT_VALUES_EQUAL(
                "r=737148929 b=3 d=192 g=0",
                CompactionRangeToString(stats.GetCompactionRangeStats(7)));
        };

        checkCompactionMap();

        tablet.RebootTablet();
        tablet.RecoverSession();

        handle = CreateHandle(tablet, id);

        {
            TString expected;
            expected.ReserveAndResize(block);
            memset(expected.begin(), 'a', block);

            auto response = tablet.ReadData(handle, 0, block);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        UNIT_ASSERT_VALUES_EQUAL(7, loadChunkCount);

        checkCompactionMap();

        tablet.DestroyHandle(handle);
    }

    TABLET_TEST(ShouldNotAllowTruncateDuringCompactionMapLoading)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetLoadedCompactionRangesPerTx(2);
        storageConfig.SetWriteBlobThreshold(block);

        TTestEnv env({}, std::move(storageConfig));

        env.CreateSubDomain("nfs");

        bool intercepted = false;
        TAutoPtr<IEventHandle> loadChunk;
        env.GetRuntime().SetEventFilter([&] (auto& runtime, auto& event) {
            Y_UNUSED(runtime);

            switch (event->GetTypeRewrite()) {
                case TEvIndexTabletPrivate::EvLoadCompactionMapChunkRequest: {
                    if (!intercepted) {
                        intercepted = true;
                        loadChunk = event.Release();
                        return true;
                    }
                }
            }

            return false;
        });

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));

        // testing file size change - may trigger compaction map update via a
        // truncate call
        TSetNodeAttrArgs args(id);
        args.SetFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE);
        args.SetSize(4 * block);
        tablet.SendSetNodeAttrRequest(args);
        {
            auto response = tablet.RecvSetNodeAttrResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        // testing some other requests that can potentially trigger compaction
        // map update
        tablet.SendAllocateDataRequest(0, 0, 0, 0);
        {
            auto response = tablet.RecvAllocateDataResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        tablet.SendDestroyCheckpointRequest("c");
        {
            auto response = tablet.RecvDestroyCheckpointResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        tablet.SendDestroyHandleRequest(0);
        {
            auto response = tablet.RecvDestroyHandleResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        tablet.SendDestroySessionRequest(0);
        {
            auto response = tablet.RecvDestroySessionResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        tablet.SendTruncateRequest(0, 0);
        {
            auto response = tablet.RecvTruncateResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        tablet.SendUnlinkNodeRequest(0, "", false);
        {
            auto response = tablet.RecvUnlinkNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        tablet.SendZeroRangeRequest(0, 0, 0);
        {
            auto response = tablet.RecvZeroRangeResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        // checking that our state wasn't changed
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDeletionMarkersCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetUsedBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetUsedCompactionRanges(), 0);
        }

        UNIT_ASSERT(loadChunk);

        env.GetRuntime().Send(loadChunk.Release(), nodeIdx);

        // compaction map should be loaded now - SetNodeAttr should succeed
        tablet.SendSetNodeAttrRequest(args);
        {
            auto response = tablet.RecvSetNodeAttrResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        // we have some used blocks now
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDeletionMarkersCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetUsedBlocksCount(), 4);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetUsedCompactionRanges(), 0);
        }

        intercepted = false;

        tablet.RebootTablet();
        tablet.RecoverSession();

        // truncate to 0 size should not succeed before compaction map gets
        // loaded as well
        args.SetSize(0);
        tablet.SendSetNodeAttrRequest(args);
        {
            auto response = tablet.RecvSetNodeAttrResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        // state not changed
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDeletionMarkersCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetUsedBlocksCount(), 4);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetUsedCompactionRanges(), 0);
        }

        env.GetRuntime().Send(loadChunk.Release(), nodeIdx);
        // compaction map loaded - truncate request allowed
        tablet.SendSetNodeAttrRequest(args);
        {
            auto response = tablet.RecvSetNodeAttrResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        // we should see some deletion markers now
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDeletionMarkersCount(), 4);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetUsedBlocksCount(), 0);
            // we have some deletion markers in one of the ranges now
            UNIT_ASSERT_VALUES_EQUAL(stats.GetUsedCompactionRanges(), 1);
        }
    }

    TABLET_TEST(ShouldDeduplicateOutOfOrderCompactionMapChunkLoads)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetLoadedCompactionRangesPerTx(2);
        storageConfig.SetWriteBlobThreshold(block);

        TTestEnv env({}, std::move(storageConfig));

        env.CreateSubDomain("nfs");

        TVector<TEvIndexTabletPrivate::TLoadCompactionMapChunkRequest> requests;
        TAutoPtr<IEventHandle> loadChunk;
        env.GetRuntime().SetEventFilter([&] (auto& runtime, TAutoPtr<IEventHandle>& event) {
            Y_UNUSED(runtime);

            switch (event->GetTypeRewrite()) {
                case TEvIndexTabletPrivate::EvLoadCompactionMapChunkRequest: {
                    using TEv =
                        TEvIndexTabletPrivate::TEvLoadCompactionMapChunkRequest;
                    requests.push_back(*event->Get<TEv>());

                    if (requests.size() == 1) {
                        loadChunk = event.Release();
                        return true;
                    }
                }
            }

            return false;
        });

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        for (ui32 i = 0; i < 10; ++i) {
            tablet.SendWriteDataRequest(handle, 0, block, 'a');
            {
                auto response = tablet.RecvWriteDataResponse();
                UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
            }
        }

        UNIT_ASSERT(loadChunk);
        UNIT_ASSERT_VALUES_EQUAL(1, requests.size());

        env.GetRuntime().Send(loadChunk.Release(), nodeIdx);
        tablet.SendWriteDataRequest(handle, 0, block, 'a');
        {
            auto response = tablet.RecvWriteDataResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        UNIT_ASSERT_VALUES_EQUAL(2, requests.size());
    }

    TABLET_TEST_16K(BackgroundOperationsShouldNotGetStuckForeverDuringCompactionMapLoading)
    {
        const auto block = tabletConfig.BlockSize;
        const auto thresholdInBlobs = 8;
        const auto threshold = thresholdInBlobs * DefaultBlockSize / block;

        NProto::TStorageConfig storageConfig;
        // hard to test anything apart from Compaction - it shares
        // EOperationState with Cleanup and FlushBytes
        storageConfig.SetCompactionThreshold(threshold);
        // Flush has a separate EOperationState
        storageConfig.SetFlushThreshold(1);
        storageConfig.SetLoadedCompactionRangesPerTx(2);
        storageConfig.SetWriteBlobThreshold(2 * block);

        TTestEnv env({}, std::move(storageConfig));

        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        // generating at least one compaction range
        tablet.WriteData(handle, 0, block, 'a');

        TAutoPtr<IEventHandle> loadChunk;
        ui32 loadChunkCount = 0;
        ui32 flushCount = 0;
        ui32 compactionCount = 0;
        env.GetRuntime().SetEventFilter([&] (auto& runtime, auto& event) {
            Y_UNUSED(runtime);

            switch (event->GetTypeRewrite()) {
                case TEvIndexTabletPrivate::EvFlushRequest: {
                    ++flushCount;
                    break;
                }

                case TEvIndexTabletPrivate::EvCompactionRequest: {
                    ++compactionCount;
                    break;
                }

                case TEvIndexTabletPrivate::EvLoadCompactionMapChunkRequest: {
                    ++loadChunkCount;

                    // catching the second chunk - first one should be loaded
                    // so that we are able to write (and thus trigger our
                    // background ops)
                    if (loadChunkCount == 2) {
                        loadChunk = event.Release();
                        return true;
                    }
                }
            }

            return false;
        });

        // rebooting to trigger compaction map reloading
        tablet.RebootTablet();
        tablet.RecoverSession();

        handle = CreateHandle(tablet, id);

        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
        UNIT_ASSERT(loadChunk);
        UNIT_ASSERT_VALUES_EQUAL(2, loadChunkCount);

        // this write should succeed - it targets the range that should be
        // loaded at this point of time
        tablet.SendWriteDataRequest(handle, 0, block, 'a');
        {
            auto response = tablet.RecvWriteDataResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        // Flush should've been triggered and its operation state should've
        // been reset to Idle
        UNIT_ASSERT_VALUES_EQUAL(1, flushCount);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(EOperationState::Idle),
                static_cast<ui32>(stats.GetFlushState()));
        }

        // this write should succeed - it targets the range that should be
        // loaded at this point of time
        for (ui32 i = 0; i < thresholdInBlobs - 1; ++i) {
            tablet.SendWriteDataRequest(handle, 0, 2 * block, 'a');
            {
                auto response = tablet.RecvWriteDataResponse();
                UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            }
        }

        // Compaction should've been triggered and its operation state should've
        // been reset to Idle
        UNIT_ASSERT_VALUES_EQUAL(1, compactionCount);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(EOperationState::Idle),
                static_cast<ui32>(stats.GetBlobIndexOpState()));
        }

        env.GetRuntime().Send(loadChunk.Release(), nodeIdx);
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));

        tablet.DestroyHandle(handle);
    }

    TABLET_TEST(FlushShouldNotGetStuckBecauseOfFlushBytesDuringCompactionMapLoading)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetFlushThreshold(4_KB);
        storageConfig.SetFlushBytesThreshold(1);
        storageConfig.SetLoadedCompactionRangesPerTx(2);

        TTestEnv env({}, std::move(storageConfig));

        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        // generating at least one compaction range
        tablet.WriteData(handle, 0, block, 'a');

        TAutoPtr<IEventHandle> loadChunk;
        ui32 loadChunkCount = 0;
        ui32 flushBytesCount = 0;
        env.GetRuntime().SetEventFilter([&] (auto& runtime, auto& event) {
            Y_UNUSED(runtime);

            switch (event->GetTypeRewrite()) {
                case TEvIndexTabletPrivate::EvFlushBytesRequest: {
                    ++flushBytesCount;
                    break;
                }

                case TEvIndexTabletPrivate::EvLoadCompactionMapChunkRequest: {
                    ++loadChunkCount;

                    // catching the second chunk - first one should be loaded
                    // so that we are able to write (and thus trigger our
                    // background ops)
                    if (loadChunkCount == 2) {
                        loadChunk = event.Release();
                        return true;
                    }
                }
            }

            return false;
        });

        // rebooting to trigger compaction map reloading
        tablet.RebootTablet();
        tablet.RecoverSession();

        handle = CreateHandle(tablet, id);

        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
        UNIT_ASSERT(loadChunk);
        UNIT_ASSERT_VALUES_EQUAL(2, loadChunkCount);

        // this write should succeed - it targets the range that should be
        // loaded at this point of time
        tablet.SendWriteDataRequest(handle, 0, 1_KB, 'a');
        {
            auto response = tablet.RecvWriteDataResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        // FlushBytes should've been triggered and its operation state should've
        // been reset to Idle, Flush state should also be idle after this
        UNIT_ASSERT_VALUES_EQUAL(1, flushBytesCount);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(EOperationState::Idle),
                static_cast<ui32>(stats.GetBlobIndexOpState()));
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(EOperationState::Idle),
                static_cast<ui32>(stats.GetFlushState()));
        }

        env.GetRuntime().Send(loadChunk.Release(), nodeIdx);
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));

        tablet.DestroyHandle(handle);
    }

    TABLET_TEST_16K(ShouldDescribeData)
    {
        const auto block = tabletConfig.BlockSize;

        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));

        ui64 handle = CreateHandle(tablet, id);
        // blobs
        tablet.WriteData(handle, 2 * block, 512_KB, '0');
        // fresh bytes
        tablet.WriteData(handle, 2_KB, 1_KB, '1');
        tablet.WriteData(handle, 255_KB, 2_KB, '2');
        // fresh blocks
        tablet.WriteData(handle, block, 4 * block, '3');
        tablet.WriteData(handle, 192_KB, 3 * block, '4');
        tablet.WriteData(handle, 193_KB, 1_KB, '5');
        // more blobs
        tablet.WriteData(handle, 3 * block, 128_KB, '6');

        auto response = tablet.DescribeData(handle, 0, 256_KB);
        UNIT_ASSERT_VALUES_EQUAL(
            512_KB + 2 * block,
            response->Record.GetFileSize());

        const auto& freshRanges = response->Record.GetFreshDataRanges();
        UNIT_ASSERT_VALUES_EQUAL(4, freshRanges.size());

        TString expected;
        expected.ReserveAndResize(1_KB);
        memset(expected.begin(), '1', 1_KB);

        UNIT_ASSERT_VALUES_EQUAL(2_KB, freshRanges[0].GetOffset());
        UNIT_ASSERT_VALUES_EQUAL(expected, freshRanges[0].GetContent());

        expected.ReserveAndResize(2 * block);
        memset(expected.begin(), '3', 2 * block);

        UNIT_ASSERT_VALUES_EQUAL(block, freshRanges[1].GetOffset());
        UNIT_ASSERT_VALUES_EQUAL(expected, freshRanges[1].GetContent());

        expected.ReserveAndResize(3 * block);
        memset(expected.begin(), '4', 3 * block);
        memset(expected.begin() + 1_KB, '5', 1_KB);

        UNIT_ASSERT_VALUES_EQUAL(192_KB, freshRanges[2].GetOffset());
        UNIT_ASSERT_VALUES_EQUAL(expected, freshRanges[2].GetContent());

        expected.ReserveAndResize(1_KB);
        memset(expected.begin(), '2', 1_KB);

        UNIT_ASSERT_VALUES_EQUAL(255_KB, freshRanges[3].GetOffset());
        UNIT_ASSERT_VALUES_EQUAL(expected, freshRanges[3].GetContent());

        const auto& blobPieces = response->Record.GetBlobPieces();
        /*
        for (const auto& p: blobPieces) {
            Cerr << NKikimr::LogoBlobIDFromLogoBlobID(p.GetBlobId())
                << " " << p.GetBSGroupId() << Endl;

            for (const auto& r: p.GetRanges()) {
                Cerr << r.GetOffset()
                    << " " << r.GetBlobOffset()
                    << " " << r.GetLength() << Endl;
            }
        }
        */
        UNIT_ASSERT_VALUES_EQUAL(2, blobPieces.size());

        const auto blobId0 =
            NKikimr::LogoBlobIDFromLogoBlobID(blobPieces[0].GetBlobId());
        UNIT_ASSERT_VALUES_EQUAL(tabletId, blobId0.TabletID());
        UNIT_ASSERT_VALUES_EQUAL(2, blobId0.Generation());
        UNIT_ASSERT_VALUES_EQUAL(4, blobId0.Step());
        UNIT_ASSERT_VALUES_EQUAL(3, blobId0.Channel());
        UNIT_ASSERT_VALUES_EQUAL(0, blobId0.Cookie());
        UNIT_ASSERT_VALUES_EQUAL(0, blobId0.PartId());
        UNIT_ASSERT_VALUES_EQUAL(0, blobPieces[0].GetBSGroupId());

        UNIT_ASSERT_VALUES_EQUAL(2, blobPieces[0].RangesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            3 * block + 128_KB,
            blobPieces[0].GetRanges(0).GetOffset());
        UNIT_ASSERT_VALUES_EQUAL(
            block + 128_KB,
            blobPieces[0].GetRanges(0).GetBlobOffset());
        UNIT_ASSERT_VALUES_EQUAL(
            192_KB - 128_KB - 3 * block,
            blobPieces[0].GetRanges(0).GetLength());
        UNIT_ASSERT_VALUES_EQUAL(
            3 * block + 192_KB,
            blobPieces[0].GetRanges(1).GetOffset());
        UNIT_ASSERT_VALUES_EQUAL(
            block + 192_KB,
            blobPieces[0].GetRanges(1).GetBlobOffset());
        // blob ranges and fresh byte ranges may intersect - the client should
        // prioritize fresh byte ranges
        // so ideally the length should be 255_KB - 3 * block - 192_KB, but
        // returning 256_KB instead of 255_KB is also valid
        UNIT_ASSERT_VALUES_EQUAL(
            256_KB - 3 * block - 192_KB,
            blobPieces[0].GetRanges(1).GetLength());

        const auto blobId1 =
            NKikimr::LogoBlobIDFromLogoBlobID(blobPieces[1].GetBlobId());
        UNIT_ASSERT_VALUES_EQUAL(tabletId, blobId1.TabletID());
        UNIT_ASSERT_VALUES_EQUAL(2, blobId1.Generation());
        UNIT_ASSERT_VALUES_EQUAL(11, blobId1.Step());
        // 4_KB block leads to 4 blobs, 16_KB block leads to 2 blobs => this
        // blob becomes blob #4 => we start from channel #3, write our 1st blob
        // to it, next blob goes to channel #4, then #5 and the 4th blob goes
        // to the channel #6
        UNIT_ASSERT_VALUES_EQUAL(block == 4_KB ? 6 : 4, blobId1.Channel());
        UNIT_ASSERT_VALUES_EQUAL(0, blobId1.Cookie());
        UNIT_ASSERT_VALUES_EQUAL(0, blobId1.PartId());
        UNIT_ASSERT_VALUES_EQUAL(0, blobPieces[1].GetBSGroupId());

        UNIT_ASSERT_VALUES_EQUAL(1, blobPieces[1].RangesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            3 * block,
            blobPieces[1].GetRanges(0).GetOffset());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            blobPieces[1].GetRanges(0).GetBlobOffset());
        UNIT_ASSERT_VALUES_EQUAL(
            128_KB,
            blobPieces[1].GetRanges(0).GetLength());

        // TODO: properly test BSGroupId
    }

    TABLET_TEST_16K(ShouldDescribeDataOnlyInTheRequestedRange)
    {
        const auto block = tabletConfig.BlockSize;

        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));

        ui64 handle = CreateHandle(tablet, id);
        // blobs
        tablet.WriteData(handle, 0, 128_KB, '0');               // outside
        tablet.WriteData(handle, 128_KB, 128_KB, '1');          // inside
        tablet.WriteData(handle, 256_KB, 128_KB, '2');          // outside
        tablet.WriteData(handle, 384_KB, 128_KB, '3');          // outside
        // fresh bytes
        tablet.WriteData(handle, 0, 1_KB, '4');                 // outside
        tablet.WriteData(handle, 128_KB, 1_KB, '5');            // inside
        tablet.WriteData(handle, 256_KB, 1_KB, '6');            // outside
        // fresh blocks
        tablet.WriteData(handle, block, block, '7');            // outside
        tablet.WriteData(handle, 128_KB + block, block, '8');   // inside
        tablet.WriteData(handle, 256_KB + block, block, '9');   // outside

        auto response = tablet.DescribeData(handle, 128_KB, 128_KB);
        UNIT_ASSERT_VALUES_EQUAL(512_KB, response->Record.GetFileSize());

        const auto& freshRanges = response->Record.GetFreshDataRanges();
        UNIT_ASSERT_VALUES_EQUAL(2, freshRanges.size());

        TString expected;
        expected.ReserveAndResize(1_KB);
        memset(expected.begin(), '5', 1_KB);

        UNIT_ASSERT_VALUES_EQUAL(128_KB, freshRanges[0].GetOffset());
        UNIT_ASSERT_VALUES_EQUAL(expected, freshRanges[0].GetContent());

        expected.ReserveAndResize(block);
        memset(expected.begin(), '8', block);

        UNIT_ASSERT_VALUES_EQUAL(128_KB + block, freshRanges[1].GetOffset());
        UNIT_ASSERT_VALUES_EQUAL(expected, freshRanges[1].GetContent());

        const auto& blobPieces = response->Record.GetBlobPieces();
        UNIT_ASSERT_VALUES_EQUAL(1, blobPieces.size());

        const auto blobId0 =
            NKikimr::LogoBlobIDFromLogoBlobID(blobPieces[0].GetBlobId());
        UNIT_ASSERT_VALUES_EQUAL(tabletId, blobId0.TabletID());
        UNIT_ASSERT_VALUES_EQUAL(2, blobId0.Generation());
        UNIT_ASSERT_VALUES_EQUAL(6, blobId0.Step());
        UNIT_ASSERT_VALUES_EQUAL(4, blobId0.Channel());
        UNIT_ASSERT_VALUES_EQUAL(0, blobId0.Cookie());
        UNIT_ASSERT_VALUES_EQUAL(0, blobId0.PartId());
        UNIT_ASSERT_VALUES_EQUAL(0, blobPieces[0].GetBSGroupId());

        UNIT_ASSERT_VALUES_EQUAL(2, blobPieces[0].RangesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            128_KB,
            blobPieces[0].GetRanges(0).GetOffset());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            blobPieces[0].GetRanges(0).GetBlobOffset());
        UNIT_ASSERT_VALUES_EQUAL(
            block,
            blobPieces[0].GetRanges(0).GetLength());
        UNIT_ASSERT_VALUES_EQUAL(
            128_KB + 2 * block,
            blobPieces[0].GetRanges(1).GetOffset());
        UNIT_ASSERT_VALUES_EQUAL(
            2 * block,
            blobPieces[0].GetRanges(1).GetBlobOffset());
        UNIT_ASSERT_VALUES_EQUAL(
            128_KB - 2 * block,
            blobPieces[0].GetRanges(1).GetLength());
    }

    TABLET_TEST_16K(ShouldDescribePureFreshBytes)
    {
        const auto block = tabletConfig.BlockSize;

        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));

        ui64 handle = CreateHandle(tablet, id);
        // we need to write at the end of the block since writing at the
        // beginning will allow the tablet to write a whole fresh block
        tablet.WriteData(handle, 128_KB + block - 1_KB, 1_KB, '1');
        // ensuring that we have actually written fresh bytes, not blocks
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBytesCount(), 1_KB);
        }

        auto response = tablet.DescribeData(handle, 128_KB, 128_KB);
        UNIT_ASSERT_VALUES_EQUAL(128_KB + block, response->Record.GetFileSize());

        const auto& freshRanges = response->Record.GetFreshDataRanges();
        UNIT_ASSERT_VALUES_EQUAL(1, freshRanges.size());

        TString expected;
        expected.ReserveAndResize(1_KB);
        memset(expected.begin(), '1', 1_KB);

        UNIT_ASSERT_VALUES_EQUAL(
            128_KB + block - 1_KB,
            freshRanges[0].GetOffset());
        UNIT_ASSERT_VALUES_EQUAL(expected, freshRanges[0].GetContent());

        const auto& blobPieces = response->Record.GetBlobPieces();
        UNIT_ASSERT_VALUES_EQUAL(0, blobPieces.size());
    }

    TABLET_TEST(ShouldHandleErrorDuringDescribeData)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        const ui64 invalidHandle = 111;
        tablet.SendDescribeDataRequest(invalidHandle, 0, 256_KB);
        auto response = tablet.RecvDescribeDataResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            ErrorInvalidHandle(invalidHandle).GetCode(),
            response->GetStatus(),
            response->GetErrorReason());
    }

    TABLET_TEST_16K(ShouldDescribeDataUsingReadAhead)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetReadAheadCacheRangeSize(1_MB);
        storageConfig.SetReadAheadCacheMaxResultsPerNode(32);
        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");
        auto registry = env.GetRegistry();

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));

        ui64 handle = CreateHandle(tablet, id);
        ui32 mbs = 32;
        for (ui32 i = 0; i < mbs; ++i) {
            tablet.WriteData(handle, i * 1_MB, 1_MB, '0');
        }

        for (ui32 i = 0; i < mbs * 8; ++i) {
            const ui64 len = 128_KB;
            const ui64 offset = i * len;
            auto response = tablet.DescribeData(handle, offset, len);
            UNIT_ASSERT_VALUES_EQUAL(
                1_MB * mbs,
                response->Record.GetFileSize());

            const auto& blobPieces = response->Record.GetBlobPieces();
            for (const auto& p: blobPieces) {
                UNIT_ASSERT_VALUES_EQUAL(1, p.RangesSize());
                UNIT_ASSERT_VALUES_EQUAL(offset, p.GetRanges(0).GetOffset());
                UNIT_ASSERT_VALUES_EQUAL(len, p.GetRanges(0).GetLength());
            }
        }

        tablet.SendRequest(tablet.CreateUpdateCounters());
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));

        {
            TTestRegistryVisitor visitor;
            // clang-format off
            registry->Visit(TInstant::Zero(), visitor);
            visitor.ValidateExpectedCounters({
                {{
                    {"sensor", "ReadAheadCacheHitCount"},
                    {"filesystem", "test"}
                // first MB not in cache, first 128_KB of each of the next MBs not
                // in cache as well
                }, (mbs - 1) * 7},
                {{
                    {"sensor", "ReadAheadCacheNodeCount"},
                    {"filesystem", "test"}
                }, 1},
            });
            // clang-format on
        }

        // cache invalidation
        tablet.WriteData(handle, 32_MB, 1_MB, '0');

        tablet.SendRequest(tablet.CreateUpdateCounters());
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));

        {
            TTestRegistryVisitor visitor;
            // clang-format off
            registry->Visit(TInstant::Zero(), visitor);
            visitor.ValidateExpectedCounters({
                {{
                    {"sensor", "ReadAheadCacheNodeCount"},
                    {"filesystem", "test"}
                }, 0},
            });
            // clang-format on
        }
    }

    // See #2737 for more details
    TABLET_TEST_16K(ReadAheadCacheShouldNotBeAffectedByConcurrentModifications)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetReadAheadCacheRangeSize(1_MB);
        storageConfig.SetReadAheadCacheMaxResultsPerNode(32);
        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");
        auto registry = env.GetRegistry();

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));

        ui64 handle = CreateHandle(tablet, id);
        for (ui32 i = 0; i < 2; ++i) {
            tablet.WriteData(handle, i * 1_MB, 1_MB, '0');
        }

        for (ui32 i = 0; i < 7; ++i) {
            const ui64 len = 128_KB;
            const ui64 offset = i * len;
            auto response = tablet.DescribeData(handle, offset, len);
        }
        // The next describe data will trigger ReadAhead cache population

        auto& runtime = env.GetRuntime();
        TAutoPtr<IEventHandle> rwTxPutRequest;

        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvPut:
                        if (!rwTxPutRequest) {
                            rwTxPutRequest = std::move(event);
                            return true;
                        }
                }
                return false;
            });

        // Execute stage of RW tx will produce a TEvPut request, which is
        // dropped to postpone the completion of the transaction
        tablet.SendSetNodeAttrRequest(TSetNodeAttrArgs(RootNodeId).SetUid(2));
        runtime.DispatchEvents(TDispatchOptions{
            .CustomFinalCondition = [&]()
            {
                return rwTxPutRequest != nullptr;
            }});

        // Now the DescribeData operation is supposed to start a new transaction
        // and hang because it accesses the same data as the previous one. This
        // operation has a potential to populate the node attributes cache
        tablet.SendDescribeDataRequest(handle, 1_MB - 128_KB, 128_KB);

        tablet.AssertSetNodeAttrNoResponse();
        tablet.AssertDescribeDataNoResponse();

        // However, Prepare stages are already completed, and GetNodeAttr has
        // stale data in its structure

        runtime.SetEventFilter(TTestActorRuntimeBase::DefaultFilterFunc);

        // Now let's start the new transaction that will clear the file by
        // setting its size to 0
        tablet.SendSetNodeAttrRequest(TSetNodeAttrArgs(id).SetSize(0));

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(2));

        // Let us complete the initial transaction that will release the lock
        // and let the DescribeData operation to proceed
        runtime.Send(rwTxPutRequest.Release(), nodeIdx);

        // Now the initial SetNodeAttr should complete
        tablet.RecvSetNodeAttrResponse();
        // The DescribeData operation should also complete
        tablet.RecvDescribeDataResponse();
        // The SetNodeAttr operation should also complete
        tablet.RecvSetNodeAttrResponse();

        // The ReadAhead cache should not give faulty results. If the cache is
        // not invalidated, the DescribeData operation will return stale data
        auto response = tablet.DescribeData(handle, 1_MB, 128_KB);

        UNIT_ASSERT_VALUES_EQUAL(0, response->Record.GetFileSize());
        UNIT_ASSERT_VALUES_EQUAL(0, response->Record.FreshDataRangesSize());
    }

    void DoTestWriteRequestCancellationOnTabletReboot(
        bool writeBatchEnabled,
        const TFileSystemConfig& tabletConfig)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetWriteBatchEnabled(writeBatchEnabled);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        tablet.SendRequest(
            tablet.CreateWriteDataRequest(handle, 0, tabletConfig.BlockSize, 'a'));

        bool putSeen = false;
        env.GetRuntime().SetEventFilter([&] (auto& runtime, auto& event) {
            Y_UNUSED(runtime);

            switch (event->GetTypeRewrite()) {
                case TEvBlobStorage::EvPutResult: {
                    putSeen = true;
                    return true;
                }
            }

            return false;
        });

        TDispatchOptions options;
        options.CustomFinalCondition = [&] {
            return putSeen;
        };
        env.GetRuntime().DispatchEvents(options);

        tablet.RebootTablet();

        auto response = tablet.RecvWriteDataResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            response->GetStatus(),
            response->GetErrorReason());

        UNIT_ASSERT_VALUES_EQUAL(
            "tablet is shutting down",
            response->GetErrorReason());
    }

    TABLET_TEST(ShouldCancelWriteRequestsIfTabletIsRebooted)
    {
        DoTestWriteRequestCancellationOnTabletReboot(false, tabletConfig);
    }

    TABLET_TEST(ShouldCancelBatchedRequestsIfTabletIsRebooted)
    {
        DoTestWriteRequestCancellationOnTabletReboot(true, tabletConfig);
    }

    TABLET_TEST(ShouldCancelReadRequestsIfTabletIsRebooted)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);
        tablet.WriteData(handle, 0, 1_MB, '0');
        tablet.Flush();

        tablet.SendRequest(
            tablet.CreateReadDataRequest(handle, 0, tabletConfig.BlockSize));

        bool getSeen = false;
        bool failGet = true;
        env.GetRuntime().SetEventFilter([&] (auto& runtime, auto& event) {
            Y_UNUSED(runtime);

            switch (event->GetTypeRewrite()) {
                case TEvBlobStorage::EvGetResult: {
                    if (failGet) {
                        getSeen = true;
                        return true;
                    }
                }
            }

            return false;
        });

        TDispatchOptions options;
        options.CustomFinalCondition = [&] {
            return getSeen;
        };

        env.GetRuntime().DispatchEvents(options);

        failGet = false;
        tablet.RebootTablet();

        auto response = tablet.RecvReadDataResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            response->GetStatus(),
            response->GetErrorReason());

        UNIT_ASSERT_VALUES_EQUAL(
            "tablet is shutting down",
            response->GetErrorReason());
    }

#define CHECK_GENERATED_BLOB(offset, length, expected)                       \
    {                                                                        \
        ui64 currentOffset = offset;                                         \
        TVector<ui64> expectedSizes = expected;                              \
        auto blobs = tablet.GenerateBlobIds(node, handle, offset, length);   \
        auto commitId = blobs->Record.GetCommitId();                         \
        const auto [generation, step] = ParseCommitId(commitId);             \
        UNIT_ASSERT_VALUES_EQUAL(                                            \
            blobs->Record.BlobsSize(),                                       \
            expectedSizes.size());                                           \
        for (size_t i = 0; i < expectedSizes.size(); ++i) {                  \
            auto generatedBlob = blobs->Record.GetBlobs(i);                  \
            auto blob = LogoBlobIDFromLogoBlobID(generatedBlob.GetBlobId()); \
            UNIT_ASSERT_VALUES_EQUAL(blob.BlobSize(), expectedSizes[i]);     \
            UNIT_ASSERT_VALUES_EQUAL(blob.Generation(), generation);         \
            UNIT_ASSERT_VALUES_EQUAL(blob.Step(), step);                     \
            UNIT_ASSERT_VALUES_EQUAL(blob.Cookie(), i);                      \
            UNIT_ASSERT_VALUES_EQUAL(                                        \
                generatedBlob.GetOffset(),                                   \
                currentOffset);                                              \
            currentOffset += expectedSizes[i];                               \
        }                                                                    \
    }

    TABLET_TEST(ShouldGenerateBlobIds)
    {
        auto block = tabletConfig.BlockSize;

        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto node =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, node);

        CHECK_GENERATED_BLOB(0, block, TVector<ui64>{block});
        CHECK_GENERATED_BLOB(block, block, TVector<ui64>{block});
        CHECK_GENERATED_BLOB(3 * block, 2 * block, TVector<ui64>{2 * block});
        CHECK_GENERATED_BLOB(
            BlockGroupSize * block / 2,
            block * BlockGroupSize,
            (TVector<ui64>{
                BlockGroupSize * block / 2,
                BlockGroupSize * block / 2}));
        CHECK_GENERATED_BLOB(
            3 * block * BlockGroupSize,
            3 * block * BlockGroupSize,
            (TVector<ui64>{
                block * BlockGroupSize,
                block * BlockGroupSize,
                block * BlockGroupSize}));
        CHECK_GENERATED_BLOB(
            block,
            2 * block * BlockGroupSize,
            (TVector<ui64>{
                block * (BlockGroupSize - 1),
                block * BlockGroupSize,
                block}));
    }

#undef CHECK_GENERATED_BLOB

    TABLET_TEST(ShouldAcquireLockForCollectGarbageOnGenerateBlobIds)
    {
        auto block = tabletConfig.BlockSize;

        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto moveBarrier = [&tablet, block]
        {
            auto node =
                CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
            ui64 handle = CreateHandle(tablet, node);
            tablet.WriteData(handle, 0, block, 'a');
            tablet.Flush();
            tablet.DestroyHandle(handle);
            tablet.UnlinkNode(RootNodeId, "test", false);
            tablet.CollectGarbage();
        };
        moveBarrier();

        ui64 lastCollectGarbage = 0;
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            lastCollectGarbage = stats.GetLastCollectCommitId();
        }
        UNIT_ASSERT_GT(lastCollectGarbage, 0);

        auto node =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test2"));
        ui64 handle = CreateHandle(tablet, node);

        auto blobs = tablet.GenerateBlobIds(node, handle, 0, block);
        auto commitId = blobs->Record.GetCommitId();

        moveBarrier();
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_LE(stats.GetLastCollectCommitId(), commitId);
        }

        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(15));

        moveBarrier();
        // After the GenerateBlobIdsReleaseCollectBarrierTimeout has passed, we
        // can observe that the last collect garbage has moved beyond the commit
        // id of the generated blob.
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_GT(stats.GetLastCollectCommitId(), commitId);
        }

        // Now we validate that the barrier is released even if the TX fails
        TVector<NKikimr::TLogoBlobID> blobIds;

        NProto::TError error;
        error.SetCode(E_REJECTED);

        auto filter = [&](auto& runtime, auto& event)
        {
            Y_UNUSED(runtime);

            switch (event->GetTypeRewrite()) {
                case TEvBlobStorage::EvPutResult: {
                    using TResponse = TEvBlobStorage::TEvPutResult;
                    auto* msg = event->template Get<TResponse>();
                    if (msg->Id.Channel() >= TIndexTabletSchema::DataChannel) {
                        blobIds.push_back(msg->Id);
                    }
                    return false;
                }
                case TEvIndexTabletPrivate::EvWriteBlobResponse: {
                    using TResponse =
                        TEvIndexTabletPrivate::TEvWriteBlobResponse;
                    auto* msg = event->template Get<TResponse>();
                    auto& e = const_cast<NProto::TError&>(msg->Error);
                    e.SetCode(E_REJECTED);
                    return false;
                }
            }

            return false;
        };

        env.GetRuntime().SetEventFilter(filter);

        auto generateResult =
            tablet.GenerateBlobIds(node, handle, 0, block * BlockGroupSize);
        commitId = generateResult->Record.GetCommitId();

        // intercepted blob was successfully written to BlobStorage, yet the
        // following operation is expected to fail
        tablet.AssertWriteDataFailed(handle, 0, block * BlockGroupSize, 'x');

        env.GetRuntime().SetEventFilter(
            TTestActorRuntimeBase::DefaultFilterFunc);

        // because we use handle + 1 instead of handle, it is expected that the
        // handler will fail will fail
        tablet.AssertAddDataFailed(
            node,
            handle + 1,
            0,
            block * BlockGroupSize,
            blobIds,
            commitId);

        moveBarrier();
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            // We expect that upon failre the barrier was released, thus moving
            // the last collect garbage beyond the commit id of the issued blob
            UNIT_ASSERT_GT(stats.GetLastCollectCommitId(), commitId);
        }

        node =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test3"));
        handle = CreateHandle(tablet, node, "", TCreateHandleArgs::RDNLY);


        // now we do the same thing, but expect HandleAddData to execute tx, yet
        // the tx to fail
        generateResult =
            tablet.GenerateBlobIds(node, handle, 0, block * BlockGroupSize);
        commitId = generateResult->Record.GetCommitId();

        tablet.AssertAddDataFailed(
            node,
            handle,
            0,
            block * BlockGroupSize,
            blobIds,
            commitId);

        moveBarrier();
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            // We expect that upon failre the barrier was released, thus moving
            // the last collect garbage beyond the commit id of the issued blob
            UNIT_ASSERT_GT(stats.GetLastCollectCommitId(), commitId);
        }
    }

    TABLET_TEST(ShouldAddData)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetWriteBlobThreshold(block);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        TVector<NKikimr::TLogoBlobID> blobIds;
        bool shouldDropPutResult = true;

        // We can't make direct writes to BlobStorage, so we store the blob ids
        // from an ordinary write and then use them in AddData
        env.GetRuntime().SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvPutResult: {
                        // We intercept all PutResult events in order for tablet
                        // to consider them as written. Nevertheless, these
                        // blobs are already written and we will use them in
                        // AddData
                        auto* msg = event->Get<TEvBlobStorage::TEvPutResult>();
                        if (msg->Id.Channel() >=
                            TIndexTabletSchema::DataChannel) {
                            blobIds.push_back(msg->Id);
                        }
                        if (shouldDropPutResult) {
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TString data(block * BlockGroupSize * 2, '\0');
        for (size_t i = 0; i < data.size(); ++i) {
            // 77 and 256 are coprimes
            data[i] = static_cast<char>(i % 77);
        }

        tablet.SendWriteDataRequest(handle, 0, data.size(), data.data());
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(blobIds.size(), 2);
        shouldDropPutResult = false;

        // We acquire commitId just so there is something to release on
        // completion
        auto commitId = tablet.GenerateBlobIds(id, handle, 0, block)
                            ->Record.GetCommitId();

        auto id2 =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test2"));
        auto handle2 = CreateHandle(tablet, id2);

        Sort(blobIds.begin(), blobIds.end());

        // Now we try to submit the same blobs for another node
        auto request = tablet.CreateAddDataRequest(
            id2,
            handle2,
            0,
            data.size(),
            blobIds,
            commitId);

        tablet.SendRequest(std::move(request));
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(2));

        auto readData = tablet.ReadData(handle2, 0, data.size());

        // After AddData, we should receive AddDataResponse
        auto response = tablet.RecvAddDataResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

        // AddData should correctly update file size
        auto stat = tablet.GetNodeAttr(id2)->Record.GetNode();
        UNIT_ASSERT_VALUES_EQUAL(data.size(), stat.GetSize());

        // Use DescribeData to check that proper blobs were added
        auto describe = tablet.DescribeData(handle2, 0, data.size());
        UNIT_ASSERT_VALUES_EQUAL(describe->Record.BlobPiecesSize(), 2);
        for (auto [i, blobPiece]: Enumerate(describe->Record.GetBlobPieces())) {
            UNIT_ASSERT_VALUES_EQUAL(1, blobPiece.RangesSize());
            UNIT_ASSERT_VALUES_EQUAL(
                i * (block * BlockGroupSize),
                blobPiece.GetRanges(0).GetOffset());
            UNIT_ASSERT_VALUES_EQUAL(0, blobPiece.GetRanges(0).GetBlobOffset());
            UNIT_ASSERT_VALUES_EQUAL(
                block * BlockGroupSize,
                blobPiece.GetRanges(0).GetLength());

            auto blobId = LogoBlobIDFromLogoBlobID(blobPiece.GetBlobId());
            UNIT_ASSERT_VALUES_EQUAL(blobId, blobIds[i]);
        }

        // validate, that no more BlobStorage requests were made
        UNIT_ASSERT_VALUES_EQUAL(blobIds.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(data, readData->Record.GetBuffer());
    }

    TABLET_TEST(ShouldRejectAddDataIfCollectBarrierIsAlreadyReleased)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetWriteBlobThreshold(block);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        TVector<NKikimr::TLogoBlobID> blobIds;

        env.GetRuntime().SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvPutResult: {
                        auto* msg = event->Get<TEvBlobStorage::TEvPutResult>();
                        if (msg->Id.Channel() >=
                            TIndexTabletSchema::DataChannel) {
                            blobIds.push_back(msg->Id);
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TString data(block * BlockGroupSize * 2, 'x');

        tablet.SendWriteDataRequest(handle, 0, data.size(), data.data());
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(blobIds.size(), 2);

        auto commitId = tablet.GenerateBlobIds(id, handle, 0, block)
                            ->Record.GetCommitId();

        // We wait for the collect barrier lease to expire. We expect that the
        // following AddData request will be rejected
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(15));

        auto id2 =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test2"));
        auto handle2 = CreateHandle(tablet, id2);

        Sort(blobIds.begin(), blobIds.end());

        tablet.SendAddDataRequest(
            id2,
            handle2,
            0,
            data.size(),
            blobIds,
            commitId);

        auto response = tablet.RecvAddDataResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
    }

    TABLET_TEST(ShouldAddDataWithUnalignedDataParts)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetWriteBlobThreshold(block);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        TString unalignedHead(1_KB, 'h');
        TString alignedBody(block, 'b');
        TString unalignedTail(2_KB, 't');

        NKikimr::TLogoBlobID blobId;
        ui64 commitId = 0;

        auto writeBlob = [&] () {
            auto gbi = tablet.GenerateBlobIds(id, handle, block, block)->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, gbi.BlobsSize());

            blobId = LogoBlobIDFromLogoBlobID(gbi.GetBlobs(0).GetBlobId());
            commitId = gbi.GetCommitId();
            auto evPut = std::make_unique<TEvBlobStorage::TEvPut>(
                blobId,
                alignedBody,
                TInstant::Max(),
                NKikimrBlobStorage::UserData);
            NKikimr::TActorId proxy =
                MakeBlobStorageProxyID(gbi.GetBlobs(0).GetBSGroupId());
            auto evPutSender = env.GetRuntime().AllocateEdgeActor(proxy.NodeId());
            env.GetRuntime().Send(CreateEventForBSProxy(
                evPutSender,
                proxy,
                evPut.release(),
                blobId.Cookie()));
        };

        writeBlob();

        TVector<NProtoPrivate::TFreshDataRange> unalignedParts;
        {
            NProtoPrivate::TFreshDataRange part;
            part.SetOffset(block - unalignedHead.size());
            part.SetContent(unalignedHead);
            unalignedParts.push_back(part);
            part.SetOffset(block + alignedBody.size());
            part.ClearContent();
            unalignedParts.push_back(part);
        }

        const ui64 offset = block - unalignedHead.size();
        const ui64 len =
            unalignedHead.size() + alignedBody.size() + unalignedTail.size();

        tablet.SendAddDataRequest(
            id,
            handle,
            offset,
            len,
            TVector<NKikimr::TLogoBlobID>({blobId}),
            commitId,
            unalignedParts);

        // one of the parts is empty
        auto response = tablet.RecvAddDataResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_ARGUMENT,
            response->GetError().GetCode(),
            response->GetErrorReason());

        // setting it
        unalignedParts[1].SetContent(unalignedTail);

        // writing a new blob since prev collect barrier was released after we
        // got the error
        writeBlob();

        // now our request should succeed
        tablet.AddData(
            id,
            handle,
            offset,
            len,
            TVector<NKikimr::TLogoBlobID>({blobId}),
            commitId,
            unalignedParts);

        auto data = tablet.ReadData(handle, offset, len)->Record.GetBuffer();

        // AddData should correctly update file size
        auto stat = tablet.GetNodeAttr(id)->Record.GetNode();
        UNIT_ASSERT_VALUES_EQUAL(
            block + alignedBody.size() + unalignedTail.size(),
            stat.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(
            unalignedHead + alignedBody + unalignedTail,
            data);
    }

    TABLET_TEST(ShouldCollectCountersForBackgroundOps)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetFlushThreshold(1_GB);
        storageConfig.SetFlushBytesThreshold(1_GB);
        storageConfig.SetCollectGarbageThreshold(1_GB);
        storageConfig.SetWriteBlobThreshold(2 * block);

        TTestEnv env({}, std::move(storageConfig));
        auto registry = env.GetRegistry();

        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        using TLabel = TTestRegistryVisitor::TLabel;
        auto makeLabels = [] (const TString& request, const TString& sensor) {
            return TVector<TLabel>({
                {"sensor", request + "." + sensor},
                {"filesystem", "test"},
            });
        };

        auto makeExpectedLabels = [=] (
            const ui32 compactionOps, const ui32 compactionOpBytes,
            const ui32 cleanupOps, const ui32 cleanupOpBytes,
            const ui32 flushOps, const ui32 flushOpBytes,
            const ui32 flushBytesOps, const ui32 flushBytesOpBytes,
            const ui32 trimBytesOps, const ui32 trimBytesOpBytes,
            const ui32 collectGarbageOps, const ui32 collectGarbageOpBytes)
        {
            return TVector<std::pair<TVector<TLabel>, i64>>({
                {makeLabels("Compaction", "Count"), compactionOps},
                {makeLabels("Compaction", "RequestBytes"), compactionOpBytes},
                {makeLabels("Cleanup", "Count"), cleanupOps},
                {makeLabels("Cleanup", "RequestBytes"), cleanupOpBytes},
                {makeLabels("Flush", "Count"), flushOps},
                {makeLabels("Flush", "RequestBytes"), flushOpBytes},
                {makeLabels("FlushBytes", "Count"), flushBytesOps},
                {makeLabels("FlushBytes", "RequestBytes"), flushBytesOpBytes},
                {makeLabels("TrimBytes", "Count"), trimBytesOps},
                {makeLabels("TrimBytes", "RequestBytes"), trimBytesOpBytes},
                {makeLabels("CollectGarbage", "Count"), collectGarbageOps},
                {makeLabels("CollectGarbage", "RequestBytes"), collectGarbageOpBytes},
            });
        };

        {
            tablet.AdvanceTime(TDuration::Seconds(15));
            env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));

            TTestRegistryVisitor visitor;
            registry->Visit(TInstant::Zero(), visitor);
            visitor.ValidateExpectedCounters(makeExpectedLabels(
                0, 0,
                0, 0,
                0, 0,
                0, 0,
                0, 0,
                0, 0
            ));
        }

        tablet.WriteData(handle, 0, block, 'a');
        tablet.Flush();

        {
            tablet.AdvanceTime(TDuration::Seconds(15));
            env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));

            TTestRegistryVisitor visitor;
            registry->Visit(TInstant::Zero(), visitor);
            visitor.ValidateExpectedCounters(makeExpectedLabels(
                0, 0,
                0, 0,
                1, block,
                0, 0,
                0, 0,
                0, 0
            ));
        }

        ui32 rangeId = GetMixedRangeIndex(id, 0);
        tablet.Compaction(rangeId);

        {
            tablet.AdvanceTime(TDuration::Seconds(15));
            env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));

            TTestRegistryVisitor visitor;
            registry->Visit(TInstant::Zero(), visitor);
            visitor.ValidateExpectedCounters(makeExpectedLabels(
                1, block,
                0, 0,
                1, block,
                0, 0,
                0, 0,
                0, 0
            ));
        }

        tablet.Cleanup(rangeId);

        {
            tablet.AdvanceTime(TDuration::Seconds(15));
            env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));

            TTestRegistryVisitor visitor;
            registry->Visit(TInstant::Zero(), visitor);
            visitor.ValidateExpectedCounters(makeExpectedLabels(
                1, block,
                1, block,
                1, block,
                0, 0,
                0, 0,
                0, 0
            ));
        }

        tablet.WriteData(handle, 0, 1_KB, 'a');
        tablet.FlushBytes();

        {
            tablet.AdvanceTime(TDuration::Seconds(15));
            env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));

            TTestRegistryVisitor visitor;
            registry->Visit(TInstant::Zero(), visitor);
            visitor.ValidateExpectedCounters(makeExpectedLabels(
                1, block,
                1, block,
                1, block,
                1, 1_KB,
                1, 1_KB,
                0, 0
            ));
        }

        tablet.GenerateCommitId();
        tablet.CollectGarbage();

        {
            tablet.AdvanceTime(TDuration::Seconds(15));
            env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));

            TTestRegistryVisitor visitor;
            registry->Visit(TInstant::Zero(), visitor);
            visitor.ValidateExpectedCounters(makeExpectedLabels(
                1, block,
                1, block,
                1, block,
                1, 1_KB,
                1, 1_KB,
                // 3 x new blobs (flush+compaction+flushbytes results)
                // 2 x garbage blobs (flush+compaction results)
                1, 5 * block
            ));
        }

        tablet.DestroyHandle(handle);
    }

    TABLET_TEST(ShouldTrimFreshBytesDeletionMarkers)
    {
        const ui64 block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetFlushBytesThreshold(1_GB);

        TTestEnv env({}, std::move(storageConfig));
        auto registry = env.GetRegistry();

        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        // initializing the first block to write fresh bytes then
        // otherwise those 1_KB of fresh bytes would be extended to a whole
        // block
        tablet.WriteData(handle, 0, block, 'a');
        tablet.WriteData(handle, 0, 1_KB, 'a');
        tablet.FlushBytes();
        tablet.DestroyHandle(handle);
        tablet.UnlinkNode(RootNodeId, "test", false);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBytesCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDeletedFreshBytesCount(), block);
        }

        tablet.FlushBytes();

        {
            tablet.AdvanceTime(TDuration::Seconds(15));
            env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));

            TTestRegistryVisitor visitor;
            registry->Visit(TInstant::Zero(), visitor);
            visitor.ValidateExpectedCounters({
                {{
                    {"sensor", "FlushBytes.RequestBytes"},
                    {"filesystem", "test"}}, 1_KB},
                {{
                    {"sensor", "FlushBytes.Count"},
                    {"filesystem", "test"}}, 1},
                {{
                    {"sensor", "TrimBytes.RequestBytes"},
                    {"filesystem", "test"}}, static_cast<i64>(block + 1_KB)},
                {{
                    {"sensor", "TrimBytes.Count"},
                    {"filesystem", "test"}}, 2},
            });
        }

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBytesCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDeletedFreshBytesCount(), 0);
        }
    }

    TABLET_TEST(ShouldTrimFreshBytesDeletionMarkersForLargeFiles)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetFlushBytesThreshold(100_GB + 1);

        TTestEnv env({}, std::move(storageConfig));
        auto registry = env.GetRegistry();

        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.ZeroRange(handle, 0, 100_GB);
        tablet.DestroyHandle(handle);
        tablet.UnlinkNode(RootNodeId, "test", false);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBytesCount());
            UNIT_ASSERT_VALUES_EQUAL(100_GB, stats.GetDeletedFreshBytesCount());
        }

        tablet.FlushBytes();

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBytesCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetDeletedFreshBytesCount());
        }

        {
            tablet.AdvanceTime(TDuration::Seconds(15));
            env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));

            TTestRegistryVisitor visitor;
            registry->Visit(TInstant::Zero(), visitor);
            visitor.ValidateExpectedCounters({
                {{
                    {"sensor", "FlushBytes.RequestBytes"},
                    {"filesystem", "test"}}, 0_KB},
                {{
                    {"sensor", "FlushBytes.Count"},
                    {"filesystem", "test"}}, 0},
                {{
                    {"sensor", "TrimBytes.RequestBytes"},
                    {"filesystem", "test"}}, 100_GB},
                {{
                    {"sensor", "TrimBytes.Count"},
                    {"filesystem", "test"}}, 1},
            });
        }
    }

    TABLET_TEST(ShouldCountDudCompactions)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        ui32 rangeId = GetMixedRangeIndex(id, 0);
        tablet.Compaction(rangeId);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 0);
        }

        tablet.DestroyHandle(handle);

        auto registry = env.GetRegistry();

        TTestRegistryVisitor visitor;
        registry->Visit(TInstant::Zero(), visitor);
        visitor.ValidateExpectedCounters({
            {{
                {"sensor", "Compaction.RequestBytes"},
                {"filesystem", "test"}}, 0},
            {{
                {"sensor", "Compaction.Count"},
                {"filesystem", "test"}}, 1},
            {{
                {"sensor", "Compaction.DudCount"},
                {"filesystem", "test"}}, 1},
        });
    }

    TABLET_TEST(ShouldRejectBlobIndexOpsWithProperErrorCodeIfAnotherOpIsRunning)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetWriteBlobThreshold(2 * block);
        TTestEnv env({}, std::move(storageConfig));

        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);
        tablet.WriteData(handle, 0, 256_KB, '0');

        ui32 rangeId = GetMixedRangeIndex(id, 0);
        tablet.SendCompactionRequest(rangeId);
        tablet.SendCleanupRequest(rangeId);

        {
            auto response = tablet.RecvCompactionResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
        }

        {
            auto response = tablet.RecvCleanupResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_TRY_AGAIN,
                response->GetStatus(),
                response->GetErrorReason());
        }

        tablet.WriteData(handle, 0, 256_KB, '0');
        tablet.SendCleanupRequest(rangeId);
        tablet.SendCompactionRequest(rangeId);

        {
            auto response = tablet.RecvCleanupResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
        }

        {
            auto response = tablet.RecvCompactionResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_TRY_AGAIN,
                response->GetStatus(),
                response->GetErrorReason());
        }

        tablet.WriteData(handle, 0, block, '0');
        tablet.SendFlushRequest();
        tablet.SendFlushBytesRequest();

        {
            auto response = tablet.RecvFlushResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
        }

        {
            auto response = tablet.RecvFlushBytesResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_TRY_AGAIN,
                response->GetStatus(),
                response->GetErrorReason());
        }

        tablet.WriteData(handle, 0, 1_KB, '0');
        tablet.SendFlushBytesRequest();
        tablet.SendCompactionRequest(rangeId);
        tablet.SendCleanupRequest(rangeId);

        {
            auto response = tablet.RecvFlushBytesResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
        }

        {
            auto response = tablet.RecvCleanupResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_TRY_AGAIN,
                response->GetStatus(),
                response->GetErrorReason());
        }

        {
            auto response = tablet.RecvCompactionResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_TRY_AGAIN,
                response->GetStatus(),
                response->GetErrorReason());
        }
    }

    TABLET_TEST(CleanupShouldNotInterfereWithCollectGarbage)
    {
        const auto block = tabletConfig.BlockSize;
        tabletConfig.BlockCount = 1'000'000;

        NProto::TStorageConfig storageConfig;

        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetCollectGarbageThreshold(1_GB);

        TTestEnv env({}, std::move(storageConfig));

        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");
        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));

        auto handle = CreateHandle(tablet, id);

        for (int i = 0; i < 2; i++) {
            tablet.WriteData(handle, 0, BlockGroupSize * block * 3, 'a');
        }

        tablet.Cleanup(GetMixedRangeIndex(id, 0));
        tablet.CollectGarbage();
        tablet.Cleanup(GetMixedRangeIndex(id, BlockGroupSize));

        auto& runtime = env.GetRuntime();

        TAutoPtr<IEventHandle> cleanupCompletion, collectGarbageCompletion;
        // See #652 for more details
        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == TEvTablet::EvCommitResult) {
                    if (!cleanupCompletion) {
                        cleanupCompletion = event.Release();
                    } else if (!collectGarbageCompletion) {
                        collectGarbageCompletion = event.Release();
                    }
                    return true;
                }
                return false;
            });

        tablet.SendCleanupRequest(GetMixedRangeIndex(id, BlockGroupSize * 2));
        tablet.SendCollectGarbageRequest();

        // We wait for both Cleanup and CollectGarbage tx to be completed
        runtime.DispatchEvents(TDispatchOptions{
            .CustomFinalCondition = [&]()
            {
                return cleanupCompletion && collectGarbageCompletion;
            }});
        runtime.SetEventFilter(TTestActorRuntimeBase::DefaultFilterFunc);

        runtime.Send(cleanupCompletion.Release(), nodeIdx);
        {
            auto response = tablet.RecvCleanupResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        runtime.Send(collectGarbageCompletion.Release(), nodeIdx);
        {
            auto response = tablet.RecvCollectGarbageResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }
    }

    TABLET_TEST(ShouldGenerateCommitId)
    {
        const auto block = tabletConfig.BlockSize;

        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        ui64 commitId = InvalidCommitId;
        env.GetRuntime().SetEventFilter(
            [&commitId](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvPutResult: {
                        using TResponse = TEvBlobStorage::TEvPutResult;
                        auto* msg = event->template Get<TResponse>();
                        if (msg->Id.Channel() >=
                            TIndexTabletSchema::DataChannel) {
                            commitId = MakeCommitId(
                                msg->Id.Generation(),
                                msg->Id.Step());
                        }
                    }
                }

                return false;
            });
        tablet.WriteData(handle, 0, block * BlockGroupSize, 'a');

        env.GetRuntime().DispatchEvents(TDispatchOptions{
            .CustomFinalCondition = [&]()
            {
                return commitId != InvalidCommitId;
            }});

        auto response = tablet.GenerateCommitId();
        UNIT_ASSERT_VALUES_EQUAL(commitId + 2, response->CommitId);

        response = tablet.GenerateCommitId();
        UNIT_ASSERT_VALUES_EQUAL(commitId + 3, response->CommitId);

        tablet.DestroyHandle(handle);
    }

    TABLET_TEST(CollectGarbageWithTheSameCollectCommitIdShouldNotFail)
    {
        const auto block = tabletConfig.BlockSize;
        NProto::TStorageConfig storageConfig;

        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetCollectGarbageThreshold(1_GB);
        storageConfig.SetMinChannelCount(1);

        // ensure that all blobs use the same channel
        TTestEnv env(
            {.ChannelCount = TIndexTabletSchema::DataChannel + 1},
            std::move(storageConfig));

        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");
        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));

        auto handle = CreateHandle(tablet, id);

        const int size = BlockGroupSize * block;
        for (int i = 0; i < 2; i++) {
            tablet.WriteData(handle, 0, size, 'a' + i);
        }

        auto& runtime = env.GetRuntime();

        TVector<ui64> collectCommitIds;
        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvCollectGarbage: {
                        auto* msg = event->template Get<
                            TEvBlobStorage::TEvCollectGarbage>();
                        if (msg->Channel >= TIndexTabletSchema::DataChannel) {
                            auto commitId = MakeCommitId(
                                msg->CollectGeneration,
                                msg->CollectStep);
                            collectCommitIds.push_back(commitId);
                        }
                        break;
                    }
                }
                return false;
            });

        {
            auto response = tablet.CollectGarbage();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        UNIT_ASSERT_VALUES_EQUAL(1, collectCommitIds.size());

        {
            tablet.Cleanup(GetMixedRangeIndex(id, 0));
            auto response = tablet.CollectGarbage();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        UNIT_ASSERT_VALUES_EQUAL(2, collectCommitIds.size());
        UNIT_ASSERT_VALUES_EQUAL(collectCommitIds[0], collectCommitIds[1]);

        {
            auto readData = tablet.ReadData(handle, 0, size);
            TString data(size, 'b');
            UNIT_ASSERT_VALUES_EQUAL(data, readData->Record.GetBuffer());
        }
    }

    TTestActorRuntimeBase::TEventFilter MakeCollectGarbageFilter(
        ui64 keepCount,
        ui64 doNotKeepCount)
    {
        return [keepCount, doNotKeepCount](auto& runtime, auto& event)
        {
            Y_UNUSED(runtime);
            switch (event->GetTypeRewrite()) {
                case TEvBlobStorage::EvCollectGarbage: {
                    auto* msg =
                        event
                            ->template Get<TEvBlobStorage::TEvCollectGarbage>();
                    if (msg->Channel >= TIndexTabletSchema::DataChannel) {
                        UNIT_ASSERT_VALUES_EQUAL(keepCount, msg->Keep->size());
                        UNIT_ASSERT_VALUES_EQUAL(
                            doNotKeepCount,
                            msg->DoNotKeep->size());
                    }
                    break;
                }
            }
            return false;
        };
    }

    TABLET_TEST(FlushBytesShouldNotInterfereWithCollectGarbage)
    {
        const auto block = tabletConfig.BlockSize;
        NProto::TStorageConfig storageConfig;

        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetCollectGarbageThreshold(1_GB);
        storageConfig.SetMinChannelCount(1);

        // ensure that all blobs use the same channel
        TTestEnv env(
            {.ChannelCount = TIndexTabletSchema::DataChannel + 1},
            std::move(storageConfig));

        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");
        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));

        auto handle = CreateHandle(tablet, id);

        const int size = BlockGroupSize * block;
        tablet.WriteData(handle, 0 * size, size, 'a');
        tablet.WriteData(handle, 1 * size, size, 'b');
        tablet.WriteData(handle, 2 * size, size, 'c');
        tablet.WriteData(handle, 2 * size, 1_KB, 'f');
        tablet.WriteData(handle, 0, size, 'd');

        // File contents:
        // [dddddd][bbbbbb][fcccccc]

        auto& runtime = env.GetRuntime();

        runtime.SetEventFilter(MakeCollectGarbageFilter(4, 0));
        tablet.CollectGarbage();
        // left to right: growth of commitId
        //     new        new        new               new     |
        // [    a    ][    b    ][    c    ][fresh][    d    ] |
        //                                                     |
        //                                            lastCollectCommitId

        tablet.Cleanup(GetMixedRangeIndex(id, 0));
        //   garbage                                           |
        // [    a    ][    b    ][    c    ][fresh][    d    ] |
        //                                                     |
        //                                            lastCollectCommitId
        // runtime.SetEventFilter(MakeCollectGarbageFilter(0, 1));

        // during the FlushBytes (if the barrier would have been acquired for
        // the fresh bytes commit Id)
        //
        //   garbage                           |               |
        // [    a    ][    b    ][    c    ][fresh][    d    ] |
        //                                     |               |
        //                                  barrier   lastCollectCommitId
        //
        // in order to ensure the acquired barrier is held for long enough time,
        // we postpone the ReadBlob request, that is supposed to be sent during
        // the FlushBytes

        TAutoPtr<IEventHandle> readBlob;
        runtime.SetEventFilter(
            [&readBlob](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTabletPrivate::EvReadBlobRequest: {
                        readBlob = event.Release();
                        return true;
                    }
                }
                return false;
            });
        tablet.SendFlushBytesRequest();
        runtime.DispatchEvents(TDispatchOptions{
            .CustomFinalCondition = [&]() -> bool
            {
                return readBlob.Get();
            }});

        runtime.SetEventFilter(MakeCollectGarbageFilter(0, 1));

        tablet.CollectGarbage();
        env.GetRuntime().Send(readBlob.Release(), nodeIdx);

        auto result = tablet.RecvFlushBytesResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, result->GetStatus());
    }

    TABLET_TEST(ShouldNotCollectGarbageWithPreviousGeneration)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetWriteBlobThreshold(block);
        storageConfig.SetCollectGarbageThreshold(block);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        ui32 barrierGen = 0;
        ui32 perGenerationCounter = 0;
        ui32 recordGeneration = 0;
        ui32 collectStep = 0;
        bool firstMessageSeen = false;
        env.GetRuntime().SetEventFilter([&] (auto& runtime, auto& event) {
            Y_UNUSED(runtime);

            switch (event->GetTypeRewrite()) {
                case TEvBlobStorage::EvCollectGarbage: {
                    const auto* msg =
                        event->template Get<TEvBlobStorage::TEvCollectGarbage>();
                    if (msg->TabletId == tabletId && !firstMessageSeen) {
                        barrierGen = msg->CollectGeneration;
                        perGenerationCounter = msg->PerGenerationCounter;
                        recordGeneration = msg->RecordGeneration;
                        collectStep = msg->CollectStep;
                        firstMessageSeen = true;
                        return true;
                    }
                }
            }

            return false;
        });

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        // directly written blob
        tablet.WriteData(handle, 0, block, 'c');

        TDispatchOptions options;
        options.CustomFinalCondition = [&] {
            return barrierGen;
        };
        env.GetRuntime().DispatchEvents(options);

        UNIT_ASSERT_VALUES_EQUAL(recordGeneration, barrierGen);

        auto oldBarrierGen = barrierGen;
        barrierGen = 0;
        perGenerationCounter = 0;
        recordGeneration = 0;
        collectStep = 0;
        firstMessageSeen = false;

        tablet.RebootTablet();

        env.GetRuntime().DispatchEvents(options);

        UNIT_ASSERT_VALUES_EQUAL(0, collectStep);
        UNIT_ASSERT_VALUES_EQUAL(oldBarrierGen + 1, barrierGen);
        UNIT_ASSERT_VALUES_EQUAL(1, perGenerationCounter);
    }

    TABLET_TEST(ShouldFlushBytesWithLargeDeletionMarkers)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxFileBlocks(2_TB / block);
        storageConfig.SetLargeDeletionMarkersEnabled(true);
        storageConfig.SetLargeDeletionMarkerBlocks(1_GB / block);
        storageConfig.SetLargeDeletionMarkersThreshold(128_GB / block);
        // disabling Cleanup
        storageConfig.SetLargeDeletionMarkersCleanupThreshold(1_PB / block);
        storageConfig.SetLargeDeletionMarkersThresholdForBackpressure(
            1_PB / block);
        // disabling FlushBytes
        storageConfig.SetFlushBytesThreshold(1_PB);
        storageConfig.SetFlushBytesThresholdForBackpressure(1_PB);

        const auto blobSize = 2 * block;
        storageConfig.SetWriteBlobThreshold(blobSize);

        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        tabletConfig.BlockCount = 10_TB / block;

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        // increasing file size to 1TiB
        TSetNodeAttrArgs args(id);
        args.SetFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE);
        args.SetSize(1_TB);
        tablet.SetNodeAttr(args);
        UNIT_ASSERT_VALUES_EQUAL(1_TB, GetNodeAttrs(tablet, id).GetSize());

        // decreasing the size to 1GiB to generate LargeDeletionMarkers
        args.SetSize(1_GB);
        tablet.SetNodeAttr(args);
        UNIT_ASSERT_VALUES_EQUAL(1_GB, GetNodeAttrs(tablet, id).GetSize());

        // resize is needed to disable the fresh bytes to fresh block
        // rounding optimization on the next write
        args.SetSize(10_GB + block);
        tablet.SetNodeAttr(args);
        UNIT_ASSERT_VALUES_EQUAL(
            10_GB + block,
            GetNodeAttrs(tablet, id).GetSize());
        // adding some fresh bytes to make FlushBytes actually do some work
        // offset should be >= 2^32 for this test
        tablet.WriteData(handle, 10_GB, 1_KB, '1');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                (1_TB - 1_GB) / block,
                stats.GetLargeDeletionMarkersCount());
            UNIT_ASSERT_VALUES_EQUAL(1_KB, stats.GetFreshBytesCount());
            UNIT_ASSERT_VALUES_EQUAL(
                1_TB - 1_GB,
                stats.GetDeletedFreshBytesCount());
        }

        // just checking that FlushBytes succeeds
        tablet.FlushBytes();

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                (1_TB - 1_GB) / block,
                stats.GetLargeDeletionMarkersCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBytesCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetDeletedFreshBytesCount());
        }

        {
            TString expected;
            expected.ReserveAndResize(block);
            memset(expected.begin(), 0, block);
            memset(expected.begin(), '1', 1_KB);
            auto response = tablet.ReadData(handle, 10_GB, block);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        tablet.DestroyHandle(handle);
    }

    TABLET_TEST(ShouldRejectLargeFileTruncationIfLargeDeletionMarkerCountIsTooHigh)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxFileBlocks(5_TB / block);
        storageConfig.SetLargeDeletionMarkersEnabled(true);
        storageConfig.SetLargeDeletionMarkerBlocks(1_GB / block);
        storageConfig.SetLargeDeletionMarkersThreshold(128_GB / block);
        storageConfig.SetLargeDeletionMarkersCleanupThreshold(20_TB / block);
        storageConfig.SetLargeDeletionMarkersThresholdForBackpressure(
            4_TB / block);
        const auto blobSize = 2 * block;
        storageConfig.SetWriteBlobThreshold(blobSize);

        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        tabletConfig.BlockCount = 10_TB / block;

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id1 =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test1"));
        auto id2 =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test2"));
        CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test3"));

        for (const auto id: {id1, id2}) {
            TSetNodeAttrArgs args(id);
            args.SetFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE);
            args.SetSize(5_TB);
            tablet.SetNodeAttr(args);
            UNIT_ASSERT_VALUES_EQUAL(5_TB, GetNodeAttrs(tablet, id).GetSize());
        }

        {
            tablet.SendUnlinkNodeRequest(RootNodeId, "test1", false);
            auto response = tablet.RecvUnlinkNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
        }

        {
            tablet.SendUnlinkNodeRequest(RootNodeId, "test2", false);
            auto response = tablet.RecvUnlinkNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                response->GetErrorReason());
        }

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                5_TB / block,
                stats.GetLargeDeletionMarkersCount());
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetUsedNodesCount());
        }

        tablet.AdvanceTime(TDuration::Seconds(15));
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(5));

        auto registry = env.GetRegistry();
        {
            TTestRegistryVisitor visitor;
            registry->Visit(TInstant::Zero(), visitor);
            visitor.ValidateExpectedCounters({
                {{{"sensor", "OrphanNodesCount"}, {"filesystem", "test"}}, 0},
            });
        }

        tablet.RenameNode(RootNodeId, "test3", RootNodeId, "test2");

        tablet.AdvanceTime(TDuration::Seconds(15));
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(5));

        {
            TTestRegistryVisitor visitor;
            registry->Visit(TInstant::Zero(), visitor);
            visitor.ValidateExpectedCounters({
                {{{"sensor", "OrphanNodesCount"}, {"filesystem", "test"}}, 1},
            });
        }

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                5_TB / block,
                stats.GetLargeDeletionMarkersCount());
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetUsedNodesCount());
        }
    }

    TABLET_TEST_4K_ONLY(ShouldEnforceFairBlobIndexOpsSchedulingIfCloseToBackpressureThresholds)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetCompactionThreshold(5);
        storageConfig.SetCleanupThreshold(10);
        storageConfig.SetWriteBlobThreshold(block);
        storageConfig.SetBlobIndexOpsPriority(NProto::BIOP_CLEANUP_FIRST);
        storageConfig.SetCompactionThresholdForBackpressure(50);
        storageConfig.SetCleanupThresholdForBackpressure(100);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        TAutoPtr<IEventHandle> compaction;
        TAutoPtr<IEventHandle> cleanup;
        env.GetRuntime().SetEventFilter([&] (auto& runtime, auto& event) {
            Y_UNUSED(runtime);

            switch (event->GetTypeRewrite()) {
                case TEvIndexTabletPrivate::EvCompactionRequest: {
                    compaction = event.Release();
                    return true;
                }

                case TEvIndexTabletPrivate::EvCleanupRequest: {
                    cleanup = event.Release();
                    return true;
                }
            }

            return false;
        });

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        for (ui32 i = 0; i < 4; ++i) {
            tablet.WriteData(handle, 0, block, 'a');
        }
        UNIT_ASSERT(!compaction);
        UNIT_ASSERT(!cleanup);

        tablet.WriteData(handle, 0, block, 'a');
        UNIT_ASSERT(compaction);
        UNIT_ASSERT(!cleanup);

        tablet.WriteData(handle, 0, block, 'a');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(6, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(6, stats.GetDeletionMarkersCount());
        }

        env.GetRuntime().Send(compaction.Release(), nodeIdx);
        env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(100));

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(6, stats.GetDeletionMarkersCount());
        }

        for (ui32 i = 0; i < 3; ++i) {
            tablet.WriteData(handle, 0, block, 'a');
        }

        UNIT_ASSERT(!compaction);
        UNIT_ASSERT(!cleanup);

        tablet.WriteData(handle, 0, block, 'a');
        UNIT_ASSERT(!compaction);
        // Cleanup has higher priority
        UNIT_ASSERT(cleanup);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(5, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(10, stats.GetDeletionMarkersCount());
        }

        // 10% away from the backpressure threshold
        for (ui32 i = 0; i < 40; ++i) {
            tablet.WriteData(handle, 0, block, 'a');
        }

        // 10% away from the backpressure threshold
        for (ui32 i = 0; i < 45; ++i) {
            tablet.WriteData(handle, BlockGroupSize * block, block, 'a');
        }

        {
            auto response = tablet.GetStorageStats(2);
            auto& stats = *response->Record.MutableStats();
            UNIT_ASSERT_VALUES_EQUAL(90, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(95, stats.GetDeletionMarkersCount());
            auto& rangeStats = *stats.MutableCompactionRangeStats();
            UNIT_ASSERT_VALUES_EQUAL(2, rangeStats.size());
            SortBy(rangeStats.begin(), rangeStats.end(), [] (const auto& s) {
                return std::make_pair(s.GetBlobCount(), s.GetDeletionCount());
            });
            UNIT_ASSERT_VALUES_EQUAL(45, rangeStats[0].GetBlobCount());
            UNIT_ASSERT_VALUES_EQUAL(45, rangeStats[0].GetDeletionCount());
            UNIT_ASSERT_VALUES_EQUAL(45, rangeStats[1].GetBlobCount());
            UNIT_ASSERT_VALUES_EQUAL(50, rangeStats[1].GetDeletionCount());
        }

        env.GetRuntime().Send(cleanup.Release(), nodeIdx);
        env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(100));

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(46, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(45, stats.GetDeletionMarkersCount());
        }

        // Compaction should've been scheduled since it's close to backpressure
        // thresholds
        UNIT_ASSERT(!cleanup);
        UNIT_ASSERT(compaction);

        env.GetRuntime().Send(compaction.Release(), nodeIdx);
        env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(100));

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(45, stats.GetDeletionMarkersCount());
        }

        UNIT_ASSERT(cleanup);
        UNIT_ASSERT(!compaction);

        env.GetRuntime().Send(cleanup.Release(), nodeIdx);
        env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(100));

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetDeletionMarkersCount());
        }

        UNIT_ASSERT(!cleanup);
        UNIT_ASSERT(!compaction);
    }

    TABLET_TEST_16K(ShouldSkipAlmostCompactRanges)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetGarbageCompactionThresholdAverage(999'999);
        storageConfig.SetGarbageCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetNewCompactionEnabled(true);
        storageConfig.SetWriteBlobThreshold(block);
        storageConfig.SetCompactRangeGarbagePercentageThreshold(99);
        storageConfig.SetCompactRangeAverageBlobSizeThreshold(256_KB);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        ui32 rangeId = GetMixedRangeIndex(id, 0);
        tablet.WriteData(handle, 0, 256_KB, 'a');
        {
            auto response = tablet.GetStorageStats(1);
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.CompactionRangeStatsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                stats.GetCompactionRangeStats(0).GetBlobCount());
        }

        tablet.Compaction(rangeId);

        // no compaction should've happened since our range is almost 'compact'
        {
            auto response = tablet.GetStorageStats(1);
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(256_KB, stats.GetGarbageQueueSize());
            // the range should be marked as 'compacted' and should be
            // pessimized in the compaction map
            UNIT_ASSERT_VALUES_EQUAL(0, stats.CompactionRangeStatsSize());
        }

        tablet.WriteData(handle, 0, 256_KB, 'a');
        {
            auto response = tablet.GetStorageStats(1);
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.CompactionRangeStatsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                2,
                stats.GetCompactionRangeStats(0).GetBlobCount());
        }

        tablet.Compaction(rangeId);
        // compaction should happen now - we have enough garbage
        // blob size is good but it's not enough to skip compaction
        {
            auto response = tablet.GetStorageStats(1);
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
            // 3 x new blobs + 2 x garbage blobs
            UNIT_ASSERT_VALUES_EQUAL(5 * 256_KB, stats.GetGarbageQueueSize());
            // the range should be marked as 'compacted' and should be
            // pessimized in the compaction map
            UNIT_ASSERT_VALUES_EQUAL(0, stats.CompactionRangeStatsSize());
        }
    }

    TABLET_TEST_4K_ONLY(ShouldIncrementGarbageBlockCountOnBlockDeletion)
    {
        const auto block = tabletConfig.BlockSize;
        const auto nodeCount = NodeGroupSize - 2;

        // Disable conditions for automatic compaction and cleanup triggering
        NProto::TStorageConfig storageConfig;
        storageConfig.SetNewCompactionEnabled(true);
        storageConfig.SetCompactionThresholdAverage(999'999);
        storageConfig.SetGarbageCompactionThresholdAverage(999'999);
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetUseMixedBlocksInsteadOfAliveBlocksInCompaction(true);
        storageConfig.SetWriteBlobThreshold(block);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        // The node values should be in [2, NodeGroupSize) range - their data
        // should be written into the same range

        std::array<ui64, nodeCount> ids;
        for (ui32 i = 0; i < nodeCount; i++) {
            auto name = Sprintf("test%u", i);
            ids[i] =
                CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, name));
            UNIT_ASSERT_VALUES_EQUAL(ids[i], i + 2);
        }

        ui32 rangeId = GetMixedRangeIndex(ids[0], 0);

        // Check that all the nodes share the same range
        for (ui32 i = 1; i < nodeCount; i++) {
            UNIT_ASSERT_VALUES_EQUAL(
                rangeId,
                GetMixedRangeIndex(ids[i], 0));
        }

        // Write data to each node then run compaction manually and ensure
        // that the data has been consolidated to a single blob by calling
        // compaction manually.

        for (ui32 i = 0; i < nodeCount; i++) {
            auto handle = CreateHandle(tablet, ids[i]);
            tablet.WriteData(handle, 0, block * BlockGroupSize, 'a');
            tablet.DestroyHandle(handle);
        }

        // Before compaction
        {
            auto response = tablet.GetStorageStats(1);
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetGarbageBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.CompactionRangeStatsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                14,
                stats.GetCompactionRangeStats(0).GetBlobCount());
        }

        tablet.Compaction(rangeId);
        {
            auto response = tablet.GetStorageStats(1);
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetGarbageBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.CompactionRangeStatsSize());
        }

        // Truncate files to 1 block.
        // The amount of garbage blocks should be equal to the number of
        // deleted blocks in both filesystem and compaction statistics.
        // If the number of garbage blocks is zero in the compaction statistics,
        // it means that the range will be skipped from compaction.

        for (ui32 i = 0; i < nodeCount; i++) {
            TSetNodeAttrArgs args(ids[i]);
            args.SetFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE);
            args.SetSize(block);
            tablet.SetNodeAttr(args);
        }

        tablet.Cleanup(rangeId);
        {
            auto response = tablet.GetStorageStats(1);
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                nodeCount * (BlockGroupSize - 1),
                stats.GetGarbageBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.CompactionRangeStatsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                stats.GetCompactionRangeStats(0).GetBlobCount());
            UNIT_ASSERT_VALUES_EQUAL(
                nodeCount * (BlockGroupSize - 1),
                stats.GetCompactionRangeStats(0).GetGarbageBlockCount());
        }

        // Delete all files but the first one
        for (ui32 i = 1; i < nodeCount; i++) {
            auto name = Sprintf("test%u", i);
            tablet.UnlinkNode(RootNodeId, name, false);
        }

        tablet.Cleanup(rangeId);
        {
            auto response = tablet.GetStorageStats(1);
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                nodeCount * BlockGroupSize - 1,
                stats.GetGarbageBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.CompactionRangeStatsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                stats.GetCompactionRangeStats(0).GetBlobCount());
            UNIT_ASSERT_VALUES_EQUAL(
                nodeCount * BlockGroupSize - 1,
                stats.GetCompactionRangeStats(0).GetGarbageBlockCount());
        }
    }

    TABLET_TEST_4K_ONLY(ShouldAutomaticallyRunCompactionDueToGarbageForSingleBlobs)
    {
        const auto block = tabletConfig.BlockSize;

        // Disable conditions for automatic compaction and cleanup triggering
        // except 20% garbage threshold
        NProto::TStorageConfig storageConfig;
        storageConfig.SetNewCompactionEnabled(true);
        storageConfig.SetCompactionThresholdAverage(999'999);
        storageConfig.SetGarbageCompactionThresholdAverage(20);
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetUseMixedBlocksInsteadOfAliveBlocksInCompaction(true);
        storageConfig.SetWriteBlobThreshold(block);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, block * BlockGroupSize, 'a');

        int blocksCountToTriggerCompaction =
            static_cast<int>(BlockGroupSize / 1.2);

        // Truncate the file and run cleanup manually.
        // The garbage fraction should be below the threshold.
        // Automatic compaction shouldn't have place.
        {
            TSetNodeAttrArgs args(id);
            args.SetFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE);
            args.SetSize(block * (blocksCountToTriggerCompaction + 1));
            tablet.SetNodeAttr(args);
        }

        ui32 rangeId = GetMixedRangeIndex(id, 0);
        tablet.Cleanup(rangeId);
        {
            auto response = tablet.GetStorageStats(1);
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                BlockGroupSize - blocksCountToTriggerCompaction - 1,
                stats.GetGarbageBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.CompactionRangeStatsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                stats.GetCompactionRangeStats(0).GetBlobCount());
            UNIT_ASSERT_VALUES_EQUAL(
                BlockGroupSize - blocksCountToTriggerCompaction - 1,
                stats.GetCompactionRangeStats(0).GetGarbageBlockCount());
        }

        // Truncate the file a bit more and run cleanup manually.
        // The garbage fraction should be above the threshold.
        // Automatic compaction should've been triggered.
        {
            TSetNodeAttrArgs args(id);
            args.SetFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE);
            args.SetSize(block * blocksCountToTriggerCompaction);
            tablet.SetNodeAttr(args);
        }

        {
            auto response = tablet.GetStorageStats(1);
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetGarbageBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.CompactionRangeStatsSize());
        }

        tablet.DestroyHandle(handle);
    }

    TABLET_TEST(ShouldAutomaticallyRunCleanupForSparselyPopulatedRanges)
    {
        const auto block = tabletConfig.BlockSize;
        const int ignoredNodeCount = NodeGroupSize - 2;
        const int groupCount = 8;

        // Disable conditions for automatic compaction and configure cleanup
        NProto::TStorageConfig storageConfig;
        storageConfig.SetNewCleanupEnabled(true);
        storageConfig.SetCleanupThresholdAverage(64);
        storageConfig.SetCompactionThresholdAverage(999'999);
        storageConfig.SetGarbageCompactionThresholdAverage(999'999);
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetUseMixedBlocksInsteadOfAliveBlocksInCompaction(true);
        storageConfig.SetWriteBlobThreshold(block);
        storageConfig.SetCalculateCleanupScoreBasedOnUsedBlocksCount(true);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        // Create 14 ignored nodes in the first range
        std::array<ui64, ignoredNodeCount> ignoredIds;
        for (ui32 i = 0; i < ignoredNodeCount; i++) {
            auto name = Sprintf("ignored%u", i);
            ignoredIds[i] =
                CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, name));
        }

        // Create 8 groups of 16 files, 4 blocks each
        std::array<ui64, NodeGroupSize * groupCount> ids;
        for (ui32 i = 0; i < NodeGroupSize * groupCount; i++) {
            auto name = Sprintf("test%u", i);
            ids[i] = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, name));
            auto handle = CreateHandle(tablet, ids[i]);
            tablet.WriteData(handle, 0, 4 * block, 'a');
            tablet.DestroyHandle(handle);
        }

        // Ensure that all 16-node groups share the same range
        for (ui32 i = 0; i < groupCount; i++) {
            for (ui32 j = 1; j < NodeGroupSize; j++) {
                UNIT_ASSERT_VALUES_EQUAL(
                    GetMixedRangeIndex(ids[i * NodeGroupSize + j], 0),
                    GetMixedRangeIndex(ids[i * NodeGroupSize], 0));
            }
        }

        // Force run cleanup and compaction
        for (ui32 i = 0; i < groupCount; i++) {
            tablet.Cleanup(GetMixedRangeIndex(ids[i * NodeGroupSize], 0));
            tablet.Compaction(GetMixedRangeIndex(ids[i * NodeGroupSize], 0));
        }

        // Check the statistics
        // Each range should contain 16 * 4 = 64 blocks
        {
            auto response = tablet.GetStorageStats(1);
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(8 * 64, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(8 * 64, stats.GetUsedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetDeletionMarkersCount());
        }

        // Truncate one file in each group to 1 block
        for (ui32 i = 0; i < groupCount; i++) {
            TSetNodeAttrArgs args(ids[i * NodeGroupSize]);
            args.SetFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE);
            args.SetSize(block);
            tablet.SetNodeAttr(args);
        }

        // There should be 3 deletion markers and 61 used blocks in each range
        // 3/61 scales to ~50/1024 < 64 deletion markers per fully filled range
        // Cleanup shouldn't have been run
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(8 * 64, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(8 * 61, stats.GetUsedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(8 * 3, stats.GetDeletionMarkersCount());
        }

        // Truncate another file in each group to 3 blocks
        for (ui32 i = 0; i < groupCount; i++) {
            TSetNodeAttrArgs args(ids[i * NodeGroupSize + 1]);
            args.SetFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE);
            args.SetSize(3 * block);
            tablet.SetNodeAttr(args);
        }

        // There should be 4 deletion markers and 60 used blocks in each range
        // 4/60 scales to ~68/1024 > 64 deletion markers per fully filled range
        // Cleanup should've been run automatically for a single range
        // After cleanup, there should remain 28 deletion markers in total
        // 28/480 scales to ~59/1024 => no more cleanup operations
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(8 * 64, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(8 * 60, stats.GetUsedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(28, stats.GetDeletionMarkersCount());
        }

        // Delete 15 files in each group
        for (ui32 i = 0; i < groupCount; i++) {
            for (ui32 j = 1; j < NodeGroupSize; j++) {
                auto name = Sprintf("test%u", i * NodeGroupSize + j);
                tablet.UnlinkNode(RootNodeId, name, false);
            }
        }

        // Cleanup should've been triggered automatically
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(8, stats.GetUsedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetDeletionMarkersCount());
        }
    }

    TABLET_TEST(CheckCompactionStatsAddBlobWriteBatch)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetWriteBatchEnabled(true);
        storageConfig.SetWriteBatchTimeout(1); // 1 ms
        storageConfig.SetWriteBlobThreshold(block);
        storageConfig.SetMaxBlobSize(2 * block);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        for (ui32 i = 0; i < 4; i++) {
            auto request =
                tablet.CreateWriteDataRequest(handle, i * 2 * block, block, 'a');
            tablet.SendRequest(std::move(request));
        }

        TDispatchOptions options;
        env.GetRuntime().DispatchEvents(options);

        {
            auto response = tablet.GetStorageStats(1);
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                4,
                stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.CompactionRangeStatsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                2,
                stats.GetCompactionRangeStats(0).GetBlobCount());
            UNIT_ASSERT_VALUES_EQUAL(
                4,
                stats.GetCompactionRangeStats(0).GetDeletionCount());
        }

        tablet.DestroyHandle(handle);
    }

    TABLET_TEST(ShouldRunCleanupForEmptyFilesystem)
    {
        const auto block = tabletConfig.BlockSize;

        // Disable conditions for automatic compaction and configure cleanup
        NProto::TStorageConfig storageConfig;
        storageConfig.SetNewCleanupEnabled(true);
        storageConfig.SetCleanupThresholdAverage(64);
        storageConfig.SetCompactionThresholdAverage(999'999);
        storageConfig.SetGarbageCompactionThresholdAverage(999'999);
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetUseMixedBlocksInsteadOfAliveBlocksInCompaction(true);
        storageConfig.SetWriteBlobThreshold(block);
        storageConfig.SetCalculateCleanupScoreBasedOnUsedBlocksCount(true);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);
        tablet.WriteData(handle, 0, block, 'a');
        tablet.DestroyHandle(handle);
        tablet.UnlinkNode(RootNodeId, "test", false);

        // There should be 1 deletion marker and 0 used blocks
        // Cleanup should've been triggered
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetDeletionMarkersCount());
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
