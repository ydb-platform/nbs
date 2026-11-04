#include "tablet.h"

#include <cloud/filestore/libs/storage/tablet/model/block.h>
#include <cloud/filestore/libs/storage/tablet/model/split_range.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash_set.h>
#include <util/generic/size_literals.h>
#include <util/system/env.h>

#include <random>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString GenerateValidateData(ui32 size, ui32 seed = 0)
{
    TString data(size, 0);
    for (ui32 i = 0; i < size; ++i) {
        data[i] = 'A' + ((i + seed) % ('Z' - 'A' + 1));
    }
    return data;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_Data_Stress)
{
    TABLET_TEST_4K_ONLY(ShouldHandleRangeIdCollisionsInCompactionMapStats)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        const auto sanitizerType = GetEnv("SANITIZER_TYPE");
        const THashSet<TString> slowSanitizers({"thread"});
        const ui32 compactionThreshold =
            slowSanitizers.contains(sanitizerType) ? 2 : 5;
        storageConfig.SetCompactionThreshold(compactionThreshold);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetLoadedCompactionRangesPerTx(2);
        storageConfig.SetWriteBlobThreshold(block);

        TTestEnv env(testEnvConfig, std::move(storageConfig));


        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        // more than enough space
        tabletConfig.BlockCount = 30_TB / block;

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        // RootNodeId is 1, so we will create nodes 2 - 15 and all of them
        // will end up in a single NodeGroup and, thus, will go to the same
        // compaction ranges
        const ui32 nodeCount = 14;
        TVector<ui64> nodes(nodeCount);
        const ui32 collisions = 2 * compactionThreshold;
        const ui32 deletionMarkers = nodeCount * collisions * BlockGroupSize;
        TVector<ui32> collidingBlocks;
        for (ui32 i = 0; i < collisions; ++i) {
            // see TBlockLocalityHasher implementation
            collidingBlocks.push_back(i * (BlockGroupSize << 16));
        }

        const auto expectedWriteCount = nodeCount * collisions;
        // should be 140 x 64 x 4KiB == 35MiB for builds without slow sanitizers
        const auto expectedBlockCount = expectedWriteCount * BlockGroupSize;
        const auto expectedBlobCount = static_cast<ui32>(ceil(
            static_cast<double>(expectedBlockCount) / (4_MB / block)));
        UNIT_ASSERT_C(
            compactionThreshold < expectedBlobCount,
            TStringBuilder() << "expectedBlobCount: " << expectedBlobCount);
        for (ui32 i = 0; i < nodeCount; ++i) {
            const auto id = CreateNode(
                tablet,
                TCreateNodeArgs::File(RootNodeId, Sprintf("test_%u", i)));

            nodes[i] = id;
        }

        ui32 rangeId = GetMixedRangeIndex(nodes[0], collidingBlocks[0]);
        for (auto nodeId: nodes) {
            for (auto blockIndex: collidingBlocks) {
                UNIT_ASSERT_VALUES_EQUAL(
                    rangeId,
                    GetMixedRangeIndex(nodeId, blockIndex));
                UNIT_ASSERT_VALUES_EQUAL(
                    rangeId,
                    GetMixedRangeIndex(nodeId, blockIndex));
            }
        }

        for (auto nodeId: nodes) {
            auto handle = CreateHandle(tablet, nodeId);
            for (auto blockIndex: collidingBlocks) {
                tablet.WriteData(
                    handle,
                    static_cast<ui64>(block) * blockIndex,
                    block * BlockGroupSize,
                    'a');
            }
        }

        // Compactions should've happened automatically

        {
            auto response = tablet.GetStorageStats(0, 1);
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                expectedBlockCount,
                stats.GetMixedBlocksCount());
            const auto expectedExcessBlobCount = compactionThreshold - 2;
            UNIT_ASSERT_VALUES_EQUAL(
                expectedBlobCount + expectedExcessBlobCount,
                stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetUsedCompactionRanges());
            UNIT_ASSERT_VALUES_EQUAL(
                256,
                stats.GetAllocatedCompactionRanges());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.CompactionRangeStatsSize());
            // The amount of GarbageBlocksCount is 0 since the blocks
            // were not overwritten
            UNIT_ASSERT_VALUES_EQUAL(
                Sprintf(
                    "r=1177944064 b=%u d=%u g=0",
                    (compactionThreshold - 1),
                    deletionMarkers),
                CompactionRangeToString(stats.GetCompactionRangeStats(0)));
        }
    }

    TABLET_TEST(ShouldTruncateLargeFiles)
    {
        const auto block = tabletConfig.BlockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxFileBlocks(2_TB / block);
        storageConfig.SetLargeDeletionMarkersEnabled(true);
        storageConfig.SetLargeDeletionMarkerBlocks(1_GB / block);
        storageConfig.SetLargeDeletionMarkersThreshold(128_GB / block);
        storageConfig.SetLargeDeletionMarkersCleanupThreshold(3_TB / block);
        storageConfig.SetLargeDeletionMarkersThresholdForBackpressure(
            10_TB / block);
        const auto blobSize = 2 * block;
        storageConfig.SetWriteBlobThreshold(blobSize);

        TTestEnv env(testEnvConfig, storageConfig);

        ui32 nodeIdx = env.AddDynamicNode();
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

        tablet.WriteData(handle, 0, block, '1');
        UNIT_ASSERT_VALUES_EQUAL(block, GetNodeAttrs(tablet, id).GetSize());

        TSetNodeAttrArgs args(id);
        args.SetFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE);
        args.SetSize(1_TB);
        tablet.SetNodeAttr(args);
        UNIT_ASSERT_VALUES_EQUAL(1_TB, GetNodeAttrs(tablet, id).GetSize());

        // writing some data at the beginning of the file
        tablet.WriteData(handle, blobSize, blobSize, '2');
        UNIT_ASSERT_VALUES_EQUAL(
            TString(blobSize, '2'),
            ReadData(tablet, handle, blobSize, blobSize));

        // writing some data at the end of the file
        tablet.WriteData(handle, 1_TB - blobSize, blobSize, '3');
        UNIT_ASSERT_VALUES_EQUAL(
            TString(blobSize, '3'),
            ReadData(tablet, handle, blobSize, 1_TB - blobSize));

        // downsizing the file and increasing its size back to 1_TB again
        args.SetSize(512_GB);
        tablet.SetNodeAttr(args);
        UNIT_ASSERT_VALUES_EQUAL(512_GB, GetNodeAttrs(tablet, id).GetSize());

        args.SetSize(1_TB);
        tablet.SetNodeAttr(args);
        UNIT_ASSERT_VALUES_EQUAL(1_TB, GetNodeAttrs(tablet, id).GetSize());

        // data at the end of the file should've been erased
        UNIT_ASSERT_VALUES_EQUAL(
            TString(blobSize, 0),
            ReadData(tablet, handle, blobSize, 1_TB - blobSize));

        // data at the beginning should still be present
        UNIT_ASSERT_VALUES_EQUAL(
            TString(block, '1'),
            ReadData(tablet, handle, block));
        UNIT_ASSERT_VALUES_EQUAL(
            TString(blobSize, '2'),
            ReadData(tablet, handle, blobSize, blobSize));

        tablet.DestroyHandle(handle);

        // deleting the file
        // after this point we should have 512_GB + 1_TB of deletion markers
        tablet.UnlinkNode(RootNodeId, "test", false);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(5, stats.GetDeletionMarkersCount());
            UNIT_ASSERT_VALUES_EQUAL(
                (1_TB + 512_GB) / block,
                stats.GetLargeDeletionMarkersCount());
            UNIT_ASSERT_VALUES_EQUAL(
                2 * blobSize / block,
                stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetFreshBlocksCount());
        }

        // let's create a new file
        auto id2 =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        TSetNodeAttrArgs args2(id2);
        args2.SetFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE);
        args2.SetSize(1_TB + 512_GB);
        tablet.SetNodeAttr(args2);
        UNIT_ASSERT_VALUES_EQUAL(
            1_TB + 512_GB,
            GetNodeAttrs(tablet, id2).GetSize());

        // deletion marker-related stats shouldn't have changed
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(5, stats.GetDeletionMarkersCount());
            UNIT_ASSERT_VALUES_EQUAL(
                (1_TB + 512_GB) / block,
                stats.GetLargeDeletionMarkersCount());
            UNIT_ASSERT_VALUES_EQUAL(
                2 * blobSize / block,
                stats.GetMixedBlocksCount());
            // 2 new blobs
            UNIT_ASSERT_VALUES_EQUAL(2 * blobSize, stats.GetGarbageQueueSize());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetFreshBlocksCount());
        }

        // but after unlinking the file Cleanup op should start running
        tablet.UnlinkNode(RootNodeId, "test", false);

        // so here all large deletion markers should've been cleaned up and
        // the corresponding mixed blobs should've been deleted
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetDeletionMarkersCount());
            UNIT_ASSERT_VALUES_EQUAL(
                (3_TB - 1_GB) / block,
                stats.GetLargeDeletionMarkersCount());
            UNIT_ASSERT_VALUES_EQUAL(
                blobSize / block,
                stats.GetMixedBlocksCount());
            // 2 new blobs + 1 garbage blob
            UNIT_ASSERT_VALUES_EQUAL(3 * blobSize, stats.GetGarbageQueueSize());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetFreshBlocksCount());
        }
    }

    TABLET_TEST(StressTestForWriteFlushCompactionCleanup)
    {
        enum EStepType : ui32
        {
            Flush = 0,
            FlushBytes,
            Compaction,
            Cleanup,
            Write,
            MAX = 10,   // Keep the gap between Write and MAX to increase
                        // probability of Write
        };

        NProto::TStorageConfig storageConfig;
        TTestEnv env(testEnvConfig, storageConfig);

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        const ui64 maxOffset = 1024 * tabletConfig.BlockSize;
        const ui64 maxNumBlocksInRequest = 8;
        const size_t iterationsLimit = 1000;

        std::uniform_int_distribution<ui32> dist(0, EStepType::MAX - 1);
        std::uniform_int_distribution<ui64> opTypeDist(0, EStepType::MAX);
        std::uniform_int_distribution<ui64> blockDist(1, maxNumBlocksInRequest);
        std::uniform_int_distribution<ui64> offsetDist(0, maxOffset);
        std::uniform_int_distribution<ui64> seedDist(0, 1000000);

        const auto seedValue = time(0);
        std::mt19937_64 engine;
        engine.seed(seedValue);

        TLog Log = env.CreateLog();
        STORAGE_DEBUG("Seed: " << seedValue);

        TString data(
            maxOffset + maxNumBlocksInRequest * tabletConfig.BlockSize,
            0);

        ui64 currentFileSize = 0;

        for (size_t i = 0; i < iterationsLimit; ++i) {
            const ui32 type = opTypeDist(engine);
            switch (type) {
                case EStepType::Flush:
                    tablet.Flush();
                    break;
                case EStepType::FlushBytes:
                    tablet.FlushBytes();
                    break;
                case EStepType::Compaction:
                    tablet.Compaction(GetMixedRangeIndex(id, 0));
                    break;
                case EStepType::Cleanup:
                    tablet.Cleanup(GetMixedRangeIndex(id, 0));
                    break;
                default: {
                    const auto& offset = offsetDist(engine);
                    const auto& blocksCount = blockDist(engine);
                    const auto& request = GenerateValidateData(
                        blocksCount * tabletConfig.BlockSize,
                        seedDist(engine));
                    tablet.WriteData(
                        handle,
                        offset,
                        request.size(),
                        request.data(),
                        id);
                    memcpy(&data[offset], request.data(), request.size());
                    currentFileSize =
                        std::max(currentFileSize, offset + request.size());
                    break;
                }
            }
        }

        for (size_t offset = 0; offset < currentFileSize;
             offset += tabletConfig.BlockSize)
        {
            auto requestSize = std::min(
                static_cast<size_t>(tabletConfig.BlockSize),
                currentFileSize - offset);
            auto response = tablet.ReadData(handle, offset, requestSize);
            const auto& buffer = response->Record.GetBuffer();
            auto dataBlock = TString(data.data() + offset, requestSize);
            UNIT_ASSERT_EQUAL_C(dataBlock, buffer, offset);
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
