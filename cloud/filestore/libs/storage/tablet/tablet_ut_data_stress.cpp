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

enum EStepType : ui32
{
    Truncate = 0,
    AllocateDefault,
    AllocateKeepSize,
    AllocatePunch,
    AllocateZero,
    AllocateZeroKeepSize,
    AllocateUnshare,

    // Must be the last one.
    MAX
};

struct TEnvironment
    : public NUnitTest::TBaseFixture
{
    TTestEnv Env;
    std::unique_ptr<TIndexTabletClient> Tablet;

    TLog Log;

    ui64 Id = 0;
    ui64 Handle = 0;

    ui64 MaxBlocks = 0;
    ui64 BlockSize = 4_KB;
    std::mt19937_64 Engine;

    TString Data;
    char Fill = 'a';
    ui64 MaxSize = 0;

    TEnvironment() : Log(Env.CreateLog())
    {}

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        Env.CreateSubDomain("nfs");

        const ui32 nodeIdx = Env.CreateNode("nfs");
        const ui64 tabletId = Env.BootIndexTablet(nodeIdx);

        TLog log = Env.CreateLog();
        {
            std::random_device rd;
            const auto seedValue = rd();
            Engine.seed(seedValue);
            STORAGE_DEBUG("Seed: " << seedValue);
        }
        MaxBlocks = Engine() % 64 + 1;
        STORAGE_DEBUG("Max blocks: " << MaxBlocks);

        Tablet = std::make_unique<TIndexTabletClient>(
            Env.GetRuntime(),
            nodeIdx,
            tabletId,
            TFileSystemConfig{
                .BlockCount = MaxBlocks
            });
        Tablet->InitSession("client", "session");

        Id = CreateNode(*Tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        Handle = CreateHandle(*Tablet, Id);
    }

    char Next(char letter)
    {
        if (letter >= 'a' && letter <= 'z') {
            return letter == 'z' ? 'A' : letter + 1;
        }
        if (letter >= 'A' && letter <= 'Z') {
            return letter == 'Z' ? '0' : letter + 1;
        }
        if (letter >= '0' && letter <= '9') {
            return letter == '9' ? 'a' : letter + 1;
        }
        UNIT_ASSERT_C(false, "Unreachable code");
        return 'a';
    }

    void WriteData(ui64 offset, ui64 length)
    {
        Tablet->WriteData(Handle, offset, length, Fill);
        Fill = Next(Fill);
    }

    TString ReadData(ui64 size, ui64 offset = 0)
    {
        return Tablet->ReadData(Handle, offset, size)->Record.GetBuffer();
    }

    void AllocateData(ui64 offset, ui64 length, ui32 flags)
    {
        Tablet->AllocateData(Handle, offset, length, flags);
    }

    NProto::TNodeAttr GetNodeAttrs()
    {
        return Tablet->GetNodeAttr(Id)->Record.GetNode();
    }

    NProtoPrivate::TStorageStats GetStorageStats()
    {
        return Tablet->GetStorageStats()->Record.GetStats();
    }

    void Resize(ui64 targetSize)
    {
        TSetNodeAttrArgs args(Id);
        args.SetFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE);
        args.SetSize(targetSize);
        Tablet->SetNodeAttr(std::move(args));
    }

    TString ReadFullData()
    {
        return ReadData(MaxBlocks * BlockSize);
    }

    void PerformAllocate(ui32 flags)
    {
        const ui64 currentSize = GetNodeAttrs().GetSize();

        std::uniform_int_distribution<ui64> offsetDist(0, MaxBlocks * BlockSize);
        std::uniform_int_distribution<ui64> sizeDist(1, MaxBlocks * BlockSize);

        ui64 allocOffset = 0;
        ui64 allocSize = 0;
        do {
            allocOffset = offsetDist(Engine);
            allocSize = sizeDist(Engine);
        } while (allocOffset + allocSize > MaxBlocks * BlockSize);

        AllocateData(allocOffset, allocSize, flags);

        const ui64 newSize = GetNodeAttrs().GetSize();
        MaxSize = Max<ui64>(MaxSize, newSize);

        if (HasFlag(flags, NProto::TAllocateDataRequest::F_KEEP_SIZE)) {
            UNIT_ASSERT_VALUES_EQUAL(currentSize, newSize);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(
                Max<ui64>(currentSize, allocOffset + allocSize),
                newSize);
        }

        const auto data = ReadFullData();
        if (const ui64 diff = Min<ui64>(allocOffset, currentSize)) {
            UNIT_ASSERT_VALUES_EQUAL(Data.substr(0, diff), data.substr(0, diff));
        }

        if (allocOffset < currentSize) {
            const ui64 rightBorder = Min<ui64>(currentSize, allocOffset + allocSize);
            if (const ui64 diff = rightBorder - allocOffset;
                HasFlag(flags, NProto::TAllocateDataRequest::F_PUNCH_HOLE) ||
                HasFlag(flags, NProto::TAllocateDataRequest::F_ZERO_RANGE))
            {
                TString expected;
                expected.resize(diff, 0);
                UNIT_ASSERT_VALUES_EQUAL(expected, data.substr(allocOffset, diff));

                WriteData(allocOffset, diff);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(
                    Data.substr(allocOffset, diff),
                    data.substr(allocOffset, diff));
            }

            if (rightBorder != currentSize) {
                const ui64 diff = currentSize - rightBorder;
                UNIT_ASSERT_VALUES_EQUAL(
                    Data.substr(rightBorder, diff),
                    data.substr(rightBorder, diff));
            } else {
                if (const ui64 diff = newSize - currentSize;
                    !HasFlag(flags, NProto::TAllocateDataRequest::F_KEEP_SIZE) && diff)
                {
                    TString expected(diff, '\0');
                    UNIT_ASSERT_VALUES_EQUAL(expected, data.substr(currentSize, diff));

                    WriteData(currentSize, diff);
                }
            }
        } else {
            if (const ui64 diff = newSize - currentSize;
                !HasFlag(flags, NProto::TAllocateDataRequest::F_KEEP_SIZE))
            {
                TString expected(diff, '\0');
                UNIT_ASSERT_VALUES_EQUAL(expected, data.substr(currentSize, diff));

                WriteData(currentSize, diff);
            }
        }

        Data = ReadFullData();
    }

    void PerformTruncate()
    {
        const ui64 currentSize = GetNodeAttrs().GetSize();

        std::uniform_int_distribution<ui64> sizeDist(0, MaxBlocks * BlockSize);
        const ui64 newSize = sizeDist(Engine);
        MaxSize = Max<ui64>(MaxSize, newSize);

        Resize(newSize);
        UNIT_ASSERT_VALUES_EQUAL(newSize, GetNodeAttrs().GetSize());

        const auto data = ReadFullData();
        if (newSize < currentSize) {
            UNIT_ASSERT_VALUES_EQUAL(Data.substr(0, newSize), data);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(Data, data.substr(0, currentSize));
            if (const ui64 diff = newSize - currentSize; newSize != currentSize) {
                TString expected(diff, '\0');
                UNIT_ASSERT_VALUES_EQUAL(expected, data.substr(currentSize, diff));

                WriteData(currentSize, diff);
            }
        }

        Data = ReadFullData();
    }

    void PerformFlush()
    {
        Tablet->FlushBytes();
        Tablet->Flush();
    }

    void PerformCompaction()
    {
        static auto hasher = CreateRangeIdHasher(RangeIdHasherType);

        const TByteRange maxFileBlocks(0, GetDefaultMaxFileBlocks(), BlockSize);

        TVector<ui32> rangeIds;
        SplitRange(
            maxFileBlocks.FirstBlock(),
            maxFileBlocks.BlockCount(),
            BlockGroupSize,
            [&] (ui32 blockOffset, ui32 blocksCount) {
                rangeIds.push_back(GetMixedRangeIndex(
                    *hasher,
                    Id,
                    static_cast<ui32>(maxFileBlocks.FirstBlock() + blockOffset),
                    blocksCount));
            });

        for (const ui32 rangeId : rangeIds) {
            Tablet->Compaction(rangeId);
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_Data_Stress)
{
    Y_UNIT_TEST_F(ShouldTruncateAndAllocateFiles, TEnvironment)
    {
#define PERFORM_TEST(testSteps)                                                \
    {                                                                          \
        std::uniform_int_distribution<ui32> dist(0, EStepType::MAX - 1);       \
                                                                               \
        for (size_t i = 0; i < testSteps; ++i) {                               \
            const ui32 type = dist(Engine);                                    \
            switch (type) {                                                    \
                case EStepType::Truncate: {                                    \
                    PerformTruncate();                                         \
                    break;                                                     \
                }                                                              \
                case EStepType::AllocateDefault: {                             \
                    PerformAllocate(0);                                        \
                    break;                                                     \
                }                                                              \
                case EStepType::AllocateKeepSize: {                            \
                    PerformAllocate(                                           \
                        ProtoFlag(NProto::TAllocateDataRequest::F_KEEP_SIZE)); \
                    break;                                                     \
                }                                                              \
                case EStepType::AllocatePunch: {                               \
                    PerformAllocate(                                           \
                        ProtoFlag(NProto::TAllocateDataRequest::F_PUNCH_HOLE) |\
                        ProtoFlag(NProto::TAllocateDataRequest::F_KEEP_SIZE)); \
                    break;                                                     \
                }                                                              \
                case EStepType::AllocateZero: {                                \
                    PerformAllocate(                                           \
                        ProtoFlag(NProto::TAllocateDataRequest::F_ZERO_RANGE));\
                    break;                                                     \
                }                                                              \
                case EStepType::AllocateZeroKeepSize: {                        \
                    PerformAllocate(                                           \
                        ProtoFlag(NProto::TAllocateDataRequest::F_ZERO_RANGE) |\
                        ProtoFlag(NProto::TAllocateDataRequest::F_KEEP_SIZE)); \
                    break;                                                     \
                }                                                              \
                case EStepType::AllocateUnshare: {                             \
                    PerformAllocate(                                           \
                        ProtoFlag(                                             \
                            NProto::TAllocateDataRequest::F_UNSHARE_RANGE));   \
                    break;                                                     \
                }                                                              \
                default: {                                                     \
                    UNIT_ASSERT_C(false, "Unreachable code");                  \
                }                                                              \
            }                                                                  \
        }                                                                      \
                                                                               \
        Resize(0);                                                             \
        UNIT_ASSERT_VALUES_EQUAL(0, GetNodeAttrs().GetSize());                 \
                                                                               \
        Data.clear();                                                          \
        UNIT_ASSERT_VALUES_EQUAL(Data, ReadFullData());                        \
                                                                               \
        PerformFlush();                                                        \
        PerformCompaction();                                                   \
                                                                               \
        const auto state = GetStorageStats();                                  \
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetFreshBytesCount());               \
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetFreshBlocksCount());              \
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetMixedBlocksCount());              \
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetMixedBlobsCount());               \
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetGarbageBlocksCount());            \
    }                                                                          \
// PERFORM_TEST

        const auto sanitizerType = GetEnv("SANITIZER_TYPE");
        // temporary logging
        Cerr << "sanitizer: " << sanitizerType << Endl;
        const THashSet<TString> slowSanitizers({"thread", "undefined", "address"});
        const ui32 d = slowSanitizers.contains(sanitizerType) ? 20 : 1;

        PERFORM_TEST(5'000 / d);
        PERFORM_TEST(1'000 / d);

#undef PERFORM_TEST
    }

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

        TTestEnv env({}, std::move(storageConfig));

        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
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
}

}   // namespace NCloud::NFileStore::NStorage
