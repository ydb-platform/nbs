#include "tablet_schema.h"

#include <cloud/filestore/libs/storage/tablet/model/block.h>
#include <cloud/filestore/libs/storage/tablet/model/operation.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/base/logoblob.h>

#include <util/generic/size_literals.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_Data)
{
    using namespace NActors;

    using namespace NCloud::NStorage;

#define TABLET_TEST_IMPL(name, largeBS)                                        \
    void TestImpl##name(TFileSystemConfig tabletConfig);                       \
    Y_UNIT_TEST(name)                                                          \
    {                                                                          \
        TestImpl##name(TFileSystemConfig{.BlockSize = 4_KB});                  \
    }                                                                          \
    Y_UNIT_TEST(name##largeBS)                                                 \
    {                                                                          \
        TestImpl##name(TFileSystemConfig{.BlockSize = largeBS});               \
    }                                                                          \
    void TestImpl##name(TFileSystemConfig tabletConfig)                        \
// TABLET_TEST_IMPL

#define TABLET_TEST(name)                                                      \
    TABLET_TEST_IMPL(name, 128_KB)                                             \
// TABLET_TEST

#define TABLET_TEST_16K(name)                                                  \
    TABLET_TEST_IMPL(name, 16_KB)                                              \
// TABLET_TEST_16K

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
        tablet.CollectGarbage();

        tablet.DestroyHandle(handle);
        tablet.UnlinkNode(RootNodeId, "test", false);
        tablet.Compaction(rangeId);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageQueueSize(), 2 * block);
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
            UNIT_ASSERT_VALUES_EQUAL(stats.GetGarbageQueueSize(), 2 * block);
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
                2 * block + 2 * block
            );  // new: 1 (x 2 blocks), garbage: 2 (x block)
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
            auto response = tablet.ReadData(handle, 0, block);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, block, 'b'));
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
        UNIT_ASSERT(response->Error.GetCode() == S_ALREADY);

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

        tabletConfig.BlockCount = MaxFileBlocks * 2;
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
            (MaxFileBlocks - 1) * block,
            block,
            '5');
        UNIT_ASSERT_VALUES_EQUAL(
            GetNodeAttrs(tablet, id).GetSize(),
            MaxFileBlocks * block);

        tablet.AssertWriteDataFailed(
            handle,
            MaxFileBlocks * block,
            1_KB,
            '6');
        UNIT_ASSERT_VALUES_EQUAL(
            GetNodeAttrs(tablet, id).GetSize(),
            MaxFileBlocks * block);
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

    TABLET_TEST(ShouldAutomaticallyRunCompaction)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetCompactionThreshold(5);
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

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 4);
        }

        // blob 4
        tablet.WriteData(handle, 2 * block, 2 * block, 'e');

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
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

        env.GetRuntime().SetObserverFunc([&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
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

                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
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

        env.GetRuntime().SetObserverFunc([&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
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

                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
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

        env.GetRuntime().SetObserverFunc([&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
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

                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
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
        env.GetRuntime().SetObserverFunc([&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvPut: {
                        auto* msg = event->Get<TEvBlobStorage::TEvPut>();
                        // Cerr << "EvPut (Cleanup): " << msg->Id << Endl;
                        Y_UNUSED(msg);
                        evPutObserved = true;

                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
            }
        );

        // blob1 should be added to GarbageQueue
        ui32 rangeId = GetMixedRangeIndex(id, 0);
        tablet.SendCleanupRequest(rangeId);

        env.GetRuntime().DispatchEvents(
            TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT(evPutObserved);

        bool collectGarbageResultObserved = false;
        env.GetRuntime().SetObserverFunc([&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
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

                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
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

    TABLET_TEST(ShouldRejectWritesDueToBackpressure)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetWriteBlobThreshold(2 * block);
        storageConfig.SetFlushThreshold(1_GB);
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetFlushBytesThreshold(1_GB);
        storageConfig.SetFlushThresholdForBackpressure(2 * block);
        storageConfig.SetCompactionThresholdForBackpressure(3);
        storageConfig.SetCleanupThresholdForBackpressure(10);
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

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);
        tablet.WriteData(handle, 0, block, '0'); // 1 fresh block, 1 marker
        tablet.WriteData(handle, 0, block, '0'); // 2 fresh blocks, 2 markers

        // backpressure due to FlushThresholdForBackpressure
        tablet.SendWriteDataRequest(handle, 0, block, '0');
        {
            auto response = tablet.RecvWriteDataResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        tablet.Flush();

        // no backpressure after Flush
        tablet.WriteData(handle, 0, block, '0'); // 1 blob, 1 fresh block, 3 markers
        tablet.WriteData(handle, 0, 2 * block, '0'); // 2 blobs, 1 fresh block, 5 markers
        tablet.WriteData(handle, 0, 2 * block, '0'); // 3 blobs, 1 fresh block, 7 markers

        // backpressure due to CompactionScoreThresholdForBackpressure
        tablet.SendWriteDataRequest(handle, 0, 2 * block, '0');
        {
            auto response = tablet.RecvWriteDataResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        ui32 rangeId = GetMixedRangeIndex(id, 0);
        tablet.Compaction(rangeId);

        // no backpressure after Compaction
        tablet.WriteData(handle, 0, 2 * block, '0'); // 2 blobs, 1 fresh block, 9 markers

        tablet.WriteData(handle, 0, block, '0'); // 2 blobs, 2 fresh blocks, 10 markers
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

        env.GetRuntime().SetObserverFunc([&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTabletPrivate::EvFlushBytesCompleted: {
                        completion = event.Release();
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
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

        env.GetRuntime().SetObserverFunc([&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTabletPrivate::EvFlushCompleted: {
                        flushObserved = true;
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
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
        UNIT_ASSERT(response->Error.GetCode() == S_ALREADY);

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
        env.GetRuntime().SetObserverFunc([&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTabletPrivate::EvCompactionRequest: {
                        auto* msg = event->Get<TEvIndexTabletPrivate::TEvCompactionRequest>();
                        ranges.push_back(msg->RangeId);
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
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
        env.GetRuntime().SetObserverFunc([&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
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

                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
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
        env.GetRuntime().SetObserverFunc([&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
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

                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
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
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
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

                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
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
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTabletPrivate::EvCleanupRequest: {
                        ++cleanupCount;
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
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
        env.GetRuntime().SetObserverFunc([&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
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

                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
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
            UNIT_ASSERT_VALUES_EQUAL(1024, stats.GetAllocatedCompactionRanges());
            UNIT_ASSERT_VALUES_EQUAL(8, stats.CompactionRangeStatsSize());
            auto rangeToString = [] (const NProtoPrivate::TCompactionRangeStats& rs) {
                return Sprintf(
                    "r=%u b=%u d=%u",
                    rs.GetRangeId(),
                    rs.GetBlobCount(),
                    rs.GetDeletionCount());
            };
            UNIT_ASSERT_VALUES_EQUAL(
                "r=1656356864 b=16 d=1024",
                rangeToString(stats.GetCompactionRangeStats(0)));
            UNIT_ASSERT_VALUES_EQUAL(
                "r=1656356865 b=16 d=1024",
                rangeToString(stats.GetCompactionRangeStats(1)));
            UNIT_ASSERT_VALUES_EQUAL(
                "r=4283236352 b=16 d=1024",
                rangeToString(stats.GetCompactionRangeStats(2)));
            UNIT_ASSERT_VALUES_EQUAL(
                "r=4283236353 b=16 d=1024",
                rangeToString(stats.GetCompactionRangeStats(3)));
            UNIT_ASSERT_VALUES_EQUAL(
                "r=1177944064 b=14 d=833",
                rangeToString(stats.GetCompactionRangeStats(4)));
            UNIT_ASSERT_VALUES_EQUAL(
                "r=1177944065 b=13 d=832",
                rangeToString(stats.GetCompactionRangeStats(5)));
            UNIT_ASSERT_VALUES_EQUAL(
                "r=737148928 b=3 d=192",
                rangeToString(stats.GetCompactionRangeStats(6)));
            UNIT_ASSERT_VALUES_EQUAL(
                "r=737148929 b=3 d=192",
                rangeToString(stats.GetCompactionRangeStats(7)));
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

    TABLET_TEST(BackgroundOperationsShouldNotGetStuckForeverDuringCompactionMapLoading)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        // hard to test anything apart from Compaction - it shares
        // EOperationState with Cleanup and FlushBytes
        storageConfig.SetCompactionThreshold(2);
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
        tablet.SendWriteDataRequest(handle, 0, 2 * block, 'a');
        {
            auto response = tablet.RecvWriteDataResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
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
        UNIT_ASSERT_VALUES_EQUAL(3, blobId0.Step());
        UNIT_ASSERT_VALUES_EQUAL(3, blobId0.Channel());
        UNIT_ASSERT_VALUES_EQUAL(0, blobId0.Cookie());
        UNIT_ASSERT_VALUES_EQUAL(0, blobId0.PartId());
        UNIT_ASSERT_VALUES_EQUAL(0, blobPieces[0].GetBSGroupId());

        UNIT_ASSERT_VALUES_EQUAL(2, blobPieces[0].RangesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            3 * block + 128_KB,
            blobPieces[0].GetRanges(0).GetOffset());
        UNIT_ASSERT_VALUES_EQUAL(
            1 + 128_KB / block,
            blobPieces[0].GetRanges(0).GetBlobOffset());
        UNIT_ASSERT_VALUES_EQUAL(
            192_KB - 128_KB - 3 * block,
            blobPieces[0].GetRanges(0).GetLength());
        UNIT_ASSERT_VALUES_EQUAL(
            3 * block + 192_KB,
            blobPieces[0].GetRanges(1).GetOffset());
        UNIT_ASSERT_VALUES_EQUAL(
            1 + 192_KB / block,
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
        UNIT_ASSERT_VALUES_EQUAL(10, blobId1.Step());
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

#undef TABLET_TEST
}

}   // namespace NCloud::NFileStore::NStorage
