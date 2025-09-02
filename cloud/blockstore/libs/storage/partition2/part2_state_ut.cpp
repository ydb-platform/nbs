#include "part2_state.h"

#include <cloud/blockstore/libs/storage/testlib/test_executor.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/map.h>
#include <util/generic/size_literals.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/cast.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 TestTabletId = 1;
const ui32 DataChannelStart = 3;
const ui32 MaxBlobSize = 4_MB;
const ui32 MaxRangesPerBlob = 8;

TString GetBlockContent(char fill = 0, size_t size = DefaultBlockSize)
{
    return TString(size, fill);
}

auto Blocks(const TBlockRange32& blockRange, ui64 commitId)
{
    TVector<TBlock> blocks;
    blocks.reserve(blockRange.Size());
    for (ui32 i = blockRange.Start; i <= blockRange.End; ++i) {
        blocks.push_back(TBlock(i, commitId, InvalidCommitId, false));
    }
    return blocks;
}

NProto::TPartitionMeta DefaultConfig(
    size_t blocksCount = 1024,
    size_t blockSize = DefaultBlockSize,
    size_t channelCount = 1)
{
    NProto::TPartitionMeta meta;

    auto& config = *meta.MutableConfig();
    config.SetBlocksCount(blocksCount);
    config.SetBlockSize(blockSize);
    config.SetZoneBlockCount(32 * MaxBlocksCount);

    auto cps = config.MutableExplicitChannelProfiles();
    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::System));
    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Log));
    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Index));

    for (ui32 i = 0; i < channelCount; ++i) {
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Merged));
    }

    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Fresh));

    return meta;
}

TBackpressureFeaturesConfig DefaultBPConfig()
{
    return {
        {
            30,     // compaction score limit
            10,     // compaction score threshold
            10,     // compaction score feature max value
        },
        {
            1600_KB,// fresh byte count limit
            400_KB, // fresh byte count threshold
            10,     // fresh byte count feature max value
        },
        {
            8_MB,   // cleanup queue size limit
            4_MB,   // cleanup queue size threshold
            10,     // cleanup queue size feature max value
        },
    };
}

TFreeSpaceConfig DefaultFreeSpaceConfig()
{
    return {
        0.25,   // free space threshold
        0.15,   // min free space
    };
}

TIndexCachingConfig DefaultIndexCachingConfig()
{
    return {
        10,     // to-mixed conversion factor
        5,      // to-ranges conversion factor
        1000,   // blocklist cache size
    };
}

////////////////////////////////////////////////////////////////////////////////

struct TNoBackpressurePolicy
    : ICompactionPolicy
{
    TCompactionScore CalculateScore(const TRangeStat& stat) const override
    {
        return stat.BlobCount;
    }

    bool BackpressureEnabled() const override
    {
        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFreshBlockVisitor final
    : public IFreshBlockVisitor
{
    using TBlockMap = TMap<TBlock, TString, TBlockCompare>;

private:
    TBlockMap Blocks;

public:
    void Visit(const TBlock& block, TStringBuf blockContent) override
    {
        TBlockMap::iterator it;
        bool inserted;

        std::tie(it, inserted) = Blocks.emplace(block, blockContent);
        UNIT_ASSERT(inserted);
    }

    void DumpBlocks(IOutputStream& out) const
    {
        for (const auto& kv: Blocks) {
            out << "BlockIndex: " << kv.first.BlockIndex
                << ", MinCommitId: " << kv.first.MinCommitId
                << ", MaxCommitId: " << kv.first.MaxCommitId
                << Endl;
        }
    }

    TVector<TBlock> GetBlocks() const
    {
        TVector<TBlock> result(Reserve(Blocks.size()));
        for (const auto& kv: Blocks) {
            result.push_back(kv.first);
        }
        return result;
    }

    TString GetBlockContent(ui32 blockIndex, ui64 commitId) const
    {
        auto it = Blocks.find(TBlockKey{ blockIndex, commitId });
        if (it != Blocks.end()) {
            return it->second;
        }
        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TBlobRef
{
    TPartialBlobId BlobId;
    ui16 BlobOffset = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TMergedBlockVisitor final
    : public IMergedBlockVisitor
{
    using TBlockMap = TMap<TBlock, TBlobRef, TBlockCompare>;

private:
    TBlockMap Blocks;

public:
    void Visit(
        const TBlock& block,
        const TPartialBlobId& blobId,
        ui16 blobOffset) override
    {
        TBlockMap::iterator it;
        bool inserted;

        std::tie(it, inserted) = Blocks.emplace(
            block,
            TBlobRef{ blobId, blobOffset });
        UNIT_ASSERT(inserted);
    }

    void DumpBlocks(IOutputStream& out) const
    {
        for (const auto& kv: Blocks) {
            out << "BlockIndex: " << kv.first.BlockIndex
                << ", MinCommitId: " << kv.first.MinCommitId
                << ", MaxCommitId: " << kv.first.MaxCommitId
                << Endl;
        }
    }

    TVector<TBlock> GetBlocks() const
    {
        TVector<TBlock> result(Reserve(Blocks.size()));
        for (const auto& kv: Blocks) {
            result.push_back(kv.first);
        }
        return result;
    }

    TBlobRef GetBlockContent(ui32 blockIndex, ui64 commitId) const
    {
        auto it = Blocks.find(TBlockKey{ blockIndex, commitId });
        if (it != Blocks.end()) {
            return it->second;
        }
        return {};
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartition2StateTest)
{
    // TODO: zone-related tests

    Y_UNIT_TEST(ShouldStoreFreshBlocks)
    {
        TPartitionState state(
            DefaultConfig(),
            TestTabletId,
            0,
            5,  // channelCount
            MaxBlobSize,
            MaxRangesPerBlob,
            EOptimizationMode::OptimizeForLongRanges,
            BuildDefaultCompactionPolicy(5),
            DefaultBPConfig(),
            DefaultFreeSpaceConfig(),
            DefaultIndexCachingConfig()
        );

        ui64 commitId = state.GenerateCommitId();
        {
            auto block1 = TBlock(1, commitId, InvalidCommitId, false);
            auto block2 = TBlock(2, commitId, InvalidCommitId, false);
            auto one = GetBlockContent(1);
            auto two = GetBlockContent(2);
            state.WriteFreshBlock(block1, {one.data(), one.size()});
            state.WriteFreshBlock(block2, {two.data(), two.size()});
        };

        {
            TFreshBlockVisitor visitor;
            state.FindFreshBlocks(visitor);

            auto result = visitor.GetBlocks();
            ASSERT_PARTITION2_BLOCK_LISTS_EQUAL(result, TVector<TBlock>({
                TBlock(1, commitId, InvalidCommitId, false),
                TBlock(2, commitId, InvalidCommitId, false),
            }));

            UNIT_ASSERT_EQUAL(visitor.GetBlockContent(1, commitId), GetBlockContent(1));
            UNIT_ASSERT_EQUAL(visitor.GetBlockContent(2, commitId), GetBlockContent(2));
        };
    }

    Y_UNIT_TEST(ShouldMarkFreshBlocksDeleted)
    {
        TPartitionState state(
            DefaultConfig(),
            TestTabletId,
            0,
            5,  // channelCount
            MaxBlobSize,
            MaxRangesPerBlob,
            EOptimizationMode::OptimizeForLongRanges,
            BuildDefaultCompactionPolicy(5),
            DefaultBPConfig(),
            DefaultFreeSpaceConfig(),
            DefaultIndexCachingConfig()
        );

        TTestExecutor executor;
        executor.WriteTx([&] (TPartitionDatabase db) {
            db.InitSchema();
        });
        executor.WriteTx([&] (TPartitionDatabase db) {
            state.InitIndex(db, TBlockRange32::WithLength(0, 1024));
        });

        ui64 commitId1 = state.GenerateCommitId();
        {
            auto block1 = TBlock(1, commitId1, InvalidCommitId, false);
            auto block2 = TBlock(2, commitId1, InvalidCommitId, false);

            auto one = GetBlockContent(1);
            auto two = GetBlockContent(2);

            state.WriteFreshBlock(block1, {one.data(), one.size()});
            state.WriteFreshBlock(block2, {two.data(), two.size()});
        };

        ui64 commitId2 = state.GenerateCommitId();
        executor.WriteTx([&] (TPartitionDatabase db) {
            state.AddFreshBlockUpdate(
                db, {commitId2, TBlockRange32::MakeOneBlock(1)});
            state.AddFreshBlockUpdate(
                db, {commitId2, TBlockRange32::MakeOneBlock(2)});
        });

        {
            auto block1 = TBlock(1, commitId2, InvalidCommitId, false);
            auto block2 = TBlock(2, commitId2, InvalidCommitId, false);

            auto oneone = GetBlockContent(11);
            auto twotwo = GetBlockContent(22);

            state.WriteFreshBlock(block1, {oneone.data(), oneone.size()});
            state.WriteFreshBlock(block2, {twotwo.data(), twotwo.size()});
        }

        {
            TFreshBlockVisitor visitor;
            state.FindFreshBlocks(
                commitId1,
                TBlockRange32::MakeClosedInterval(1, 2),
                visitor);

            auto result = visitor.GetBlocks();
            ASSERT_PARTITION2_BLOCK_LISTS_EQUAL(result, TVector<TBlock>({
                TBlock(1, commitId1, InvalidCommitId, false),
                TBlock(2, commitId1, InvalidCommitId, false),
            }));

            UNIT_ASSERT_EQUAL(visitor.GetBlockContent(1, commitId1), GetBlockContent(1));
            UNIT_ASSERT_EQUAL(visitor.GetBlockContent(2, commitId1), GetBlockContent(2));
        };

        {
            TFreshBlockVisitor visitor;
            state.FindFreshBlocks(visitor);

            auto result = visitor.GetBlocks();
            ASSERT_PARTITION2_BLOCK_LISTS_EQUAL(result, TVector<TBlock>({
                TBlock(1, commitId2, InvalidCommitId, false),
                TBlock(1, commitId1, commitId2, false),
                TBlock(2, commitId2, InvalidCommitId, false),
                TBlock(2, commitId1, commitId2, false),
            }));

            UNIT_ASSERT_EQUAL(visitor.GetBlockContent(1, commitId2), GetBlockContent(11));
            UNIT_ASSERT_EQUAL(visitor.GetBlockContent(2, commitId2), GetBlockContent(22));
        };
    }

    Y_UNIT_TEST(ShouldDeleteFreshBlocks)
    {
        TPartitionState state(
            DefaultConfig(),
            TestTabletId,
            0,
            5,  // channelCount
            MaxBlobSize,
            MaxRangesPerBlob,
            EOptimizationMode::OptimizeForLongRanges,
            BuildDefaultCompactionPolicy(5),
            DefaultBPConfig(),
            DefaultFreeSpaceConfig(),
            DefaultIndexCachingConfig()
        );

        ui64 commitId1 = state.GenerateCommitId();
        {
            auto block1 = TBlock(1, commitId1, InvalidCommitId, false);
            auto block2 = TBlock(2, commitId1, InvalidCommitId, false);

            auto one = GetBlockContent(1);
            auto two = GetBlockContent(2);

            state.WriteFreshBlock(block1, {one.data(), one.size()});
            state.WriteFreshBlock(block2, {two.data(), two.size()});
        };

        ui64 commitId2 = state.GenerateCommitId();
        {
            state.DeleteFreshBlock(1, commitId1);
            state.DeleteFreshBlock(2, commitId1);

            auto block1 = TBlock(1, commitId2, InvalidCommitId, false);
            auto block2 = TBlock(2, commitId2, InvalidCommitId, false);

            auto one = GetBlockContent(11);
            auto two = GetBlockContent(22);

            state.WriteFreshBlock(block1, {one.data(), one.size()});
            state.WriteFreshBlock(block2, {two.data(), two.size()});
        }

        {
            TFreshBlockVisitor visitor;
            state.FindFreshBlocks(
                commitId1,
                TBlockRange32::MakeClosedInterval(1, 2),
                visitor);

            auto result = visitor.GetBlocks();
            UNIT_ASSERT(!result);
        };

        {
            TFreshBlockVisitor visitor;
            state.FindFreshBlocks(visitor);

            auto result = visitor.GetBlocks();
            ASSERT_PARTITION2_BLOCK_LISTS_EQUAL(result, TVector<TBlock>({
                TBlock(1, commitId2, InvalidCommitId, false),
                TBlock(2, commitId2, InvalidCommitId, false),
            }));

            UNIT_ASSERT_EQUAL(visitor.GetBlockContent(1, commitId2), GetBlockContent(11));
            UNIT_ASSERT_EQUAL(visitor.GetBlockContent(2, commitId2), GetBlockContent(22));
        };
    }

    Y_UNIT_TEST(ShouldStoreMergedBlocks)
    {
        TPartitionState state(
            DefaultConfig(),
            TestTabletId,
            0,
            5,  // channelCount
            MaxBlobSize,
            MaxRangesPerBlob,
            EOptimizationMode::OptimizeForLongRanges,
            BuildDefaultCompactionPolicy(5),
            DefaultBPConfig(),
            DefaultFreeSpaceConfig(),
            DefaultIndexCachingConfig()
        );

        TTestExecutor executor;
        executor.WriteTx([&] (TPartitionDatabase db) {
            db.InitSchema();
        });
        executor.WriteTx([&] (TPartitionDatabase db) {
            state.InitIndex(db, TBlockRange32::WithLength(0, 1024));
        });

        TBlockRange32 blockRange;
        TPartialBlobId blobId;

        ui64 commitId = state.GenerateCommitId();
        executor.WriteTx([&] (TPartitionDatabase db) {
            TVector<TBlock> blocks = {
                TBlock(1, commitId, InvalidCommitId, false),
                TBlock(2, commitId, InvalidCommitId, false),
            };

            blockRange = TBlockRange32::MakeClosedInterval(
                blocks.front().BlockIndex,
                blocks.back().BlockIndex);

            blobId = state.GenerateBlobId(
                EChannelDataKind::Merged,
                EChannelPermission::UserWritesAllowed,
                commitId,
                blockRange.Size() * DefaultBlockSize);

            state.WriteBlob(db, blobId, blocks);
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            TMergedBlockVisitor visitor;
            UNIT_ASSERT(state.FindMergedBlocks(db, blockRange, visitor));

            auto result = visitor.GetBlocks();
            ASSERT_PARTITION2_BLOCK_LISTS_EQUAL(result, TVector<TBlock>({
                TBlock(1, commitId, InvalidCommitId, false),
                TBlock(2, commitId, InvalidCommitId, false),
            }));
        });
    }

    Y_UNIT_TEST(ShouldStoreMergedBlocksInMixedIndex)
    {
        TPartitionState state(
            DefaultConfig(),
            TestTabletId,
            0,
            5,  // channelCount
            MaxBlobSize,
            MaxRangesPerBlob,
            EOptimizationMode::OptimizeForLongRanges,
            BuildDefaultCompactionPolicy(5),
            DefaultBPConfig(),
            DefaultFreeSpaceConfig(),
            {1024, 0, 0}
        );

        TTestExecutor executor;
        executor.WriteTx([&] (TPartitionDatabase db) {
            db.InitSchema();
        });
        executor.WriteTx([&] (TPartitionDatabase db) {
            state.InitIndex(db, TBlockRange32::WithLength(0, 1024));
        });

        TBlockRange32 blockRange;
        TPartialBlobId blobId;

        ui64 commitId = state.GenerateCommitId();
        executor.WriteTx([&] (TPartitionDatabase db) {
            state.UpdateIndexStructures(
                db,
                TInstant::Seconds(1),
                TBlockRange32::WithLength(0, 100)
            );

            TVector<TBlock> blocks = {
                TBlock(1, commitId, InvalidCommitId, false),
                TBlock(2, commitId, InvalidCommitId, false),
            };

            blockRange = TBlockRange32::MakeClosedInterval(
                blocks.front().BlockIndex,
                blocks.back().BlockIndex);

            blobId = state.GenerateBlobId(
                EChannelDataKind::Merged,
                EChannelPermission::UserWritesAllowed,
                commitId,
                blockRange.Size() * DefaultBlockSize);

            state.WriteBlob(db, blobId, blocks);
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            TMergedBlockVisitor visitor;
            UNIT_ASSERT(state.FindMergedBlocks(db, blockRange, visitor));

            auto result = visitor.GetBlocks();
            ASSERT_PARTITION2_BLOCK_LISTS_EQUAL(result, TVector<TBlock>({
                TBlock(1, commitId, InvalidCommitId, false),
                TBlock(2, commitId, InvalidCommitId, false),
            }));
        });
    }

    // TODO: test with mixed index
    Y_UNIT_TEST(ShouldMarkMergedBlocksDeleted)
    {
        TPartitionState state(
            DefaultConfig(),
            TestTabletId,
            0,
            5,  // channelCount
            MaxBlobSize,
            MaxRangesPerBlob,
            EOptimizationMode::OptimizeForLongRanges,
            BuildDefaultCompactionPolicy(5),
            DefaultBPConfig(),
            DefaultFreeSpaceConfig(),
            DefaultIndexCachingConfig()
        );

        TTestExecutor executor;
        executor.WriteTx([&] (TPartitionDatabase db) {
            db.InitSchema();
        });
        executor.WriteTx([&] (TPartitionDatabase db) {
            state.InitIndex(db, TBlockRange32::WithLength(0, 1024));
        });

        auto blockRange1 = TBlockRange32::MakeClosedInterval(1, 3);
        TPartialBlobId blobId1;

        ui64 commitId1 = state.GenerateCommitId();
        executor.WriteTx([&] (TPartitionDatabase db) {
            auto blocks = Blocks(blockRange1, commitId1);

            blobId1 = state.GenerateBlobId(
                EChannelDataKind::Merged,
                EChannelPermission::UserWritesAllowed,
                commitId1,
                blockRange1.Size() * DefaultBlockSize);

            state.WriteBlob(db, blobId1, blocks);
        });

        auto blockRange2 = TBlockRange32::MakeClosedInterval(1, 2);
        TPartialBlobId blobId2;

        ui64 commitId2 = state.GenerateCommitId();
        executor.WriteTx([&] (TPartitionDatabase db) {
            state.MarkMergedBlocksDeleted(db, blockRange2, commitId2);

            auto blocks = Blocks(blockRange2, commitId2);

            blobId2 = state.GenerateBlobId(
                EChannelDataKind::Merged,
                EChannelPermission::UserWritesAllowed,
                commitId2,
                blockRange2.Size() * DefaultBlockSize);

            state.WriteBlob(db, blobId2, blocks);
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            TMergedBlockVisitor visitor;
            UNIT_ASSERT(state.FindMergedBlocks(db, commitId1, blockRange1, visitor));

            auto result = visitor.GetBlocks();
            ASSERT_PARTITION2_BLOCK_LISTS_EQUAL(result, TVector<TBlock>({
                TBlock(1, commitId1, InvalidCommitId, false),
                TBlock(2, commitId1, InvalidCommitId, false),
                TBlock(3, commitId1, InvalidCommitId, false),
            }));
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            TMergedBlockVisitor visitor;
            UNIT_ASSERT(state.FindMergedBlocks(db, blockRange1, visitor));

            auto result = visitor.GetBlocks();
            ASSERT_PARTITION2_BLOCK_LISTS_EQUAL(result, TVector<TBlock>({
                TBlock(1, commitId2, InvalidCommitId, false),
                TBlock(1, commitId1, commitId2, false),
                TBlock(2, commitId2, InvalidCommitId, false),
                TBlock(2, commitId1, commitId2, false),
                TBlock(3, commitId1, InvalidCommitId, false),
            }));
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            TVector<TBlock> blocks1 = {
                TBlock(1, commitId1, InvalidCommitId, false),
                TBlock(2, commitId1, InvalidCommitId, false),
                TBlock(3, commitId1, InvalidCommitId, false),
            };
            state.UpdateBlob(db, blobId1, true, blocks1);
            TVector<TBlock> blocks2 = {
                TBlock(1, commitId2, InvalidCommitId, false),
                TBlock(2, commitId2, InvalidCommitId, false),
            };
            state.UpdateBlob(db, blobId2, true, blocks2);
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            TMergedBlockVisitor visitor;
            UNIT_ASSERT(state.FindMergedBlocks(db, commitId1, blockRange1, visitor));

            auto result = visitor.GetBlocks();
            // block 3 commit id was rebased to commitId2 => we should get an
            // empty block list here
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 0);
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            TMergedBlockVisitor visitor;
            UNIT_ASSERT(state.FindMergedBlocks(db, commitId2, blockRange1, visitor));

            auto result = visitor.GetBlocks();
            ASSERT_PARTITION2_BLOCK_LISTS_EQUAL(result, TVector<TBlock>({
                TBlock(1, commitId2, InvalidCommitId, false),
                TBlock(2, commitId2, InvalidCommitId, false),
                TBlock(3, commitId2, InvalidCommitId, false),
            }));
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            TMergedBlockVisitor visitor;
            UNIT_ASSERT(state.FindMergedBlocks(db, blockRange1, visitor));

            auto result = visitor.GetBlocks();
            ASSERT_PARTITION2_BLOCK_LISTS_EQUAL(result, TVector<TBlock>({
                TBlock(1, commitId2, InvalidCommitId, false),
                TBlock(1, 0, 0, false),
                TBlock(2, commitId2, InvalidCommitId, false),
                TBlock(2, 0, 0, false),
                TBlock(3, commitId2, InvalidCommitId, false),
            }));
        });
    }

    // TODO: test with mixed index
    Y_UNIT_TEST(ShouldDeleteMergedBlocks)
    {
        TPartitionState state(
            DefaultConfig(),
            TestTabletId,
            0,
            5,  // channelCount
            MaxBlobSize,
            MaxRangesPerBlob,
            EOptimizationMode::OptimizeForLongRanges,
            BuildDefaultCompactionPolicy(5),
            DefaultBPConfig(),
            DefaultFreeSpaceConfig(),
            DefaultIndexCachingConfig()
        );

        TTestExecutor executor;
        executor.WriteTx([&] (TPartitionDatabase db) {
            db.InitSchema();
        });
        executor.WriteTx([&] (TPartitionDatabase db) {
            state.InitIndex(db, TBlockRange32::WithLength(0, 1024));
        });

        TBlockRange32 blockRange1;
        TPartialBlobId blobId1;

        ui64 commitId1 = state.GenerateCommitId();
        executor.WriteTx([&] (TPartitionDatabase db) {
            TVector<TBlock> blocks = {
                TBlock(1, commitId1, InvalidCommitId, false),
                TBlock(2, commitId1, InvalidCommitId, false),
            };

            blockRange1 = TBlockRange32::MakeClosedInterval(
                blocks.front().BlockIndex,
                blocks.back().BlockIndex);

            blobId1 = state.GenerateBlobId(
                EChannelDataKind::Merged,
                EChannelPermission::UserWritesAllowed,
                commitId1,
                blockRange1.Size() * DefaultBlockSize);

            state.WriteBlob(db, blobId1, blocks);
        });

        TBlockRange32 blockRange2;
        TPartialBlobId blobId2;

        ui64 commitId2 = state.GenerateCommitId();
        executor.WriteTx([&] (TPartitionDatabase db) {
            state.DeleteBlob(db, blobId1);

            TVector<TBlock> blocks = {
                TBlock(1, commitId2, InvalidCommitId, false),
                TBlock(2, commitId2, InvalidCommitId, false),
            };

            blockRange2 = TBlockRange32::MakeClosedInterval(
                blocks.front().BlockIndex,
                blocks.back().BlockIndex);

            blobId2 = state.GenerateBlobId(
                EChannelDataKind::Merged,
                EChannelPermission::UserWritesAllowed,
                commitId2,
                blockRange2.Size() * DefaultBlockSize);

            state.WriteBlob(db, blobId2, blocks);
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            TMergedBlockVisitor visitor;
            UNIT_ASSERT(state.FindMergedBlocks(db, commitId1, blockRange1, visitor));

            auto result = visitor.GetBlocks();
            UNIT_ASSERT(!result);
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            TMergedBlockVisitor visitor;
            UNIT_ASSERT(state.FindMergedBlocks(db, blockRange2, visitor));

            auto result = visitor.GetBlocks();
            ASSERT_PARTITION2_BLOCK_LISTS_EQUAL(result, TVector<TBlock>({
                TBlock(1, commitId2, InvalidCommitId, false),
                TBlock(2, commitId2, InvalidCommitId, false),
            }));
        });
    }

    Y_UNIT_TEST(UpdatePermissions)
    {
        for (size_t channelCount = 1; channelCount <= 3; ++channelCount) {
            TPartitionState state(
                DefaultConfig(1024, DefaultBlockSize, channelCount),
                TestTabletId,
                0,
                4 + channelCount,
                MaxBlobSize,
                MaxRangesPerBlob,
                EOptimizationMode::OptimizeForLongRanges,
                BuildDefaultCompactionPolicy(5),
                DefaultBPConfig(),
                DefaultFreeSpaceConfig(),
                DefaultIndexCachingConfig()
            );

            UNIT_ASSERT(state.IsCompactionAllowed());

            ui32 channelId = 0;
            const double baseScore = 1;
            const double maxScore = 100;

#define CHECK_DISK_SPACE_SCORE(expected)                                \
            UNIT_ASSERT_DOUBLES_EQUAL(                                  \
                expected,                                               \
                state.CalculateCurrentBackpressure().DiskSpaceScore,    \
                1e-5                                                    \
            )                                                           \
// CHECK_DISK_SPACE_SCORE

            while (channelId < 3) {
                UNIT_ASSERT(!state.UpdatePermissions(
                    channelId,
                    EChannelPermission::SystemWritesAllowed
                    | EChannelPermission::UserWritesAllowed
                ));
                UNIT_ASSERT(state.IsCompactionAllowed());
                CHECK_DISK_SPACE_SCORE(baseScore);

                UNIT_ASSERT(
                    !state.UpdatePermissions(
                        channelId, EChannelPermission::SystemWritesAllowed
                    )
                );
                UNIT_ASSERT(state.IsCompactionAllowed());
                CHECK_DISK_SPACE_SCORE(baseScore);

                UNIT_ASSERT(state.UpdatePermissions(
                    channelId, EChannelPermission::UserWritesAllowed
                ));
                UNIT_ASSERT(!state.IsCompactionAllowed());
                CHECK_DISK_SPACE_SCORE(maxScore);

                UNIT_ASSERT(!state.UpdatePermissions(channelId, {}));
                UNIT_ASSERT(!state.IsCompactionAllowed());
                CHECK_DISK_SPACE_SCORE(maxScore);

                UNIT_ASSERT(state.UpdatePermissions(
                    channelId,
                    EChannelPermission::SystemWritesAllowed
                    | EChannelPermission::UserWritesAllowed
                ));
                CHECK_DISK_SPACE_SCORE(baseScore);

                ++channelId;
            }

            const TVector<TVector<double>> expectedScores{
                {baseScore, maxScore},
                {baseScore, 2, maxScore},
                {baseScore, 1.5, 3, maxScore},
            };
            while (channelId <= 2 + channelCount) {
                const auto currentScore =
                    expectedScores[channelCount - 1][channelId - 3];
                const auto nextScore =
                    expectedScores[channelCount - 1][channelId - 3 + 1];

                UNIT_ASSERT(!state.UpdatePermissions(
                    channelId,
                    EChannelPermission::SystemWritesAllowed
                    | EChannelPermission::UserWritesAllowed
                ));
                UNIT_ASSERT(state.IsCompactionAllowed());
                CHECK_DISK_SPACE_SCORE(currentScore);

                UNIT_ASSERT(state.UpdatePermissions(
                    channelId, EChannelPermission::SystemWritesAllowed
                ));
                UNIT_ASSERT(state.IsCompactionAllowed());
                CHECK_DISK_SPACE_SCORE(nextScore);

                UNIT_ASSERT(state.UpdatePermissions(
                    channelId, EChannelPermission::UserWritesAllowed
                ));
                UNIT_ASSERT_VALUES_EQUAL(
                    channelId < 2 + channelCount, state.IsCompactionAllowed()
                );
                CHECK_DISK_SPACE_SCORE(currentScore);

                UNIT_ASSERT(state.UpdatePermissions(channelId, {}));
                UNIT_ASSERT_VALUES_EQUAL(
                    channelId < 2 + channelCount, state.IsCompactionAllowed()
                );
                CHECK_DISK_SPACE_SCORE(nextScore);

                ++channelId;
            }

            // TODO: fresh channels
        }
    }

    Y_UNIT_TEST(TestReassignedChannelsCollection)
    {
        TPartitionState state(
            DefaultConfig(1024, DefaultBlockSize, 2),
            TestTabletId,
            0,
            6,  // channelCount
            MaxBlobSize,
            MaxRangesPerBlob,
            EOptimizationMode::OptimizeForLongRanges,
            BuildDefaultCompactionPolicy(5),
            DefaultBPConfig(),
            DefaultFreeSpaceConfig(),
            DefaultIndexCachingConfig()
        );

        UNIT_ASSERT(
            state.IsWriteAllowed(EChannelPermission::UserWritesAllowed)
        );
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetChannelsToReassign().size());

        state.UpdatePermissions(
            DataChannelStart,
            EChannelPermission::SystemWritesAllowed
        );

        UNIT_ASSERT(
            state.IsWriteAllowed(EChannelPermission::UserWritesAllowed)
        );
        auto channelsToReassign = state.GetChannelsToReassign();
        UNIT_ASSERT_VALUES_EQUAL(1, channelsToReassign.size());
        UNIT_ASSERT_VALUES_EQUAL(DataChannelStart, channelsToReassign[0]);

        state.UpdatePermissions(
            DataChannelStart + 1,
            EChannelPermission::SystemWritesAllowed
        );

        UNIT_ASSERT(
            !state.IsWriteAllowed(EChannelPermission::UserWritesAllowed)
        );
        channelsToReassign = state.GetChannelsToReassign();
        UNIT_ASSERT_VALUES_EQUAL(2, channelsToReassign.size());
        UNIT_ASSERT_VALUES_EQUAL(DataChannelStart, channelsToReassign[0]);
        UNIT_ASSERT_VALUES_EQUAL(DataChannelStart + 1, channelsToReassign[1]);

        state.RegisterReassignRequestFromBlobStorage(DataChannelStart + 2);
        channelsToReassign = state.GetChannelsToReassign();
        UNIT_ASSERT_VALUES_EQUAL(3, channelsToReassign.size());
        UNIT_ASSERT_VALUES_EQUAL(DataChannelStart, channelsToReassign[0]);
        UNIT_ASSERT_VALUES_EQUAL(DataChannelStart + 1, channelsToReassign[1]);
        UNIT_ASSERT_VALUES_EQUAL(DataChannelStart + 2, channelsToReassign[2]);
    }

    Y_UNIT_TEST(TestReassignedChannelsPercentageThreshold)
    {
        TPartitionState state(
            DefaultConfig(1024, DefaultBlockSize, 96),
            TestTabletId,
            0,
            100,  // channelCount
            MaxBlobSize,
            MaxRangesPerBlob,
            EOptimizationMode::OptimizeForLongRanges,
            BuildDefaultCompactionPolicy(5),
            DefaultBPConfig(),
            DefaultFreeSpaceConfig(),
            DefaultIndexCachingConfig(),
            Max(),  // maxIORequestsInFlight
            10      // reassignChannelsPercentageThreshold
        );

        UNIT_ASSERT(
            state.IsWriteAllowed(EChannelPermission::UserWritesAllowed)
        );
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetChannelsToReassign().size());

        for (ui32 i = 0; i < 9; ++i) {
            state.UpdatePermissions(
                DataChannelStart + i,
                EChannelPermission::SystemWritesAllowed
            );
        }

        UNIT_ASSERT(
            state.IsWriteAllowed(EChannelPermission::UserWritesAllowed)
        );
        auto channelsToReassign = state.GetChannelsToReassign();
        UNIT_ASSERT_VALUES_EQUAL(0, channelsToReassign.size());

        state.UpdatePermissions(
            DataChannelStart + 9,
            EChannelPermission::SystemWritesAllowed
        );

        UNIT_ASSERT(
            state.IsWriteAllowed(EChannelPermission::UserWritesAllowed)
        );
        channelsToReassign = state.GetChannelsToReassign();
        UNIT_ASSERT_VALUES_EQUAL(10, channelsToReassign.size());
        UNIT_ASSERT_VALUES_EQUAL(DataChannelStart, channelsToReassign[0]);
        UNIT_ASSERT_VALUES_EQUAL(DataChannelStart + 9, channelsToReassign[9]);

        for (ui32 i = 0; i < 10; ++i) {
            state.UpdatePermissions(
                DataChannelStart + i,
                EChannelPermission::UserWritesAllowed
                    | EChannelPermission::SystemWritesAllowed
            );
        }

        UNIT_ASSERT(
            state.IsWriteAllowed(EChannelPermission::UserWritesAllowed)
        );
        channelsToReassign = state.GetChannelsToReassign();
        UNIT_ASSERT_VALUES_EQUAL(0, channelsToReassign.size());

        state.UpdatePermissions(
            0,
            EChannelPermission::SystemWritesAllowed
        );
        UNIT_ASSERT(
            !state.IsWriteAllowed(EChannelPermission::UserWritesAllowed)
        );
        channelsToReassign = state.GetChannelsToReassign();
        UNIT_ASSERT_VALUES_EQUAL(1, channelsToReassign.size());
        UNIT_ASSERT_VALUES_EQUAL(0, channelsToReassign[0]);
    }

    Y_UNIT_TEST(UpdateChannelFreeSpaceShare)
    {
        const size_t maxChannelCount = 3;

        for (size_t channelCount = 1; channelCount <= maxChannelCount; ++channelCount) {
            TPartitionState state(
                DefaultConfig(1024, DefaultBlockSize, channelCount),
                TestTabletId,
                0,
                channelCount + 4,
                MaxBlobSize,
                MaxRangesPerBlob,
                EOptimizationMode::OptimizeForLongRanges,
                BuildDefaultCompactionPolicy(5),
                DefaultBPConfig(),
                DefaultFreeSpaceConfig(),
                DefaultIndexCachingConfig()
            );

            const double baseScore = 1;
            const double maxScore = 100;

            for (ui32 ch = 0; ch < state.GetChannelCount(); ++ch) {
                const auto kind = state.GetChannelDataKind(ch);
                switch (kind) {
                    case EChannelDataKind::System:
                    case EChannelDataKind::Log:
                    case EChannelDataKind::Index: {
                        UNIT_ASSERT(!state.UpdateChannelFreeSpaceShare(ch, 0));
                        UNIT_ASSERT(!state.UpdateChannelFreeSpaceShare(ch, 0.25));
                        CHECK_DISK_SPACE_SCORE(baseScore);

                        UNIT_ASSERT(state.UpdateChannelFreeSpaceShare(ch, 0.20));
                        CHECK_DISK_SPACE_SCORE(2);

                        UNIT_ASSERT(state.UpdateChannelFreeSpaceShare(ch, 0.15));
                        CHECK_DISK_SPACE_SCORE(maxScore);

                        UNIT_ASSERT(!state.UpdateChannelFreeSpaceShare(ch, 0));
                        UNIT_ASSERT(state.UpdateChannelFreeSpaceShare(ch, 1));

                        CHECK_DISK_SPACE_SCORE(baseScore);

                        break;
                    }
                    case EChannelDataKind::Mixed:
                    case EChannelDataKind::Merged: {
                        UNIT_ASSERT(state.UpdateChannelFreeSpaceShare(ch, 0.2));

                        constexpr ui32 FirstDataChannel = 3;
                        CHECK_DISK_SPACE_SCORE(
                            1 / (1 - 0.5 * (ch - FirstDataChannel + 1) / channelCount)
                        );
                        break;
                    }

                    case EChannelDataKind::Fresh: {
                        UNIT_ASSERT(state.UpdateChannelFreeSpaceShare(ch, 0.2));
                        CHECK_DISK_SPACE_SCORE(maxScore);
                        break;
                    }

                    default: {
                        Y_ABORT("unsupported kind: %u", static_cast<ui32>(kind));
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(ShouldPickProperNextChannel)
    {
        auto meta = DefaultConfig(1024, DefaultBlockSize, MaxMergedChannelCount);

        for (auto kind: {EChannelDataKind::Mixed, EChannelDataKind::Merged}) {
            TPartitionState state(
                meta,
                TestTabletId,
                0,
                MaxDataChannelCount,
                MaxBlobSize,
                MaxRangesPerBlob,
                EOptimizationMode::OptimizeForLongRanges,
                BuildDefaultCompactionPolicy(5),
                DefaultBPConfig(),
                DefaultFreeSpaceConfig(),
                DefaultIndexCachingConfig()
            );

            auto blobId = state.GenerateBlobId(
                kind,
                EChannelPermission::UserWritesAllowed,
                1,
                1024);
            UNIT_ASSERT(blobId.Channel() == TPartitionSchema::FirstDataChannel);
        }
    }

    Y_UNIT_TEST(PickProperNextChannelWithExplicitChannelProfiles)
    {
        NProto::TPartitionMeta meta;
        auto& config = *meta.MutableConfig();
        config.SetBlocksCount(1024);
        config.SetBlockSize(DefaultBlockSize);
        config.SetZoneBlockCount(32 * MaxBlocksCount);

        auto cps = config.MutableExplicitChannelProfiles();
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::System));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Log));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Index));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Merged));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Merged));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Mixed));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Merged));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Merged));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Merged));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Merged));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Mixed));

        TPartitionState state(
            meta,
            TestTabletId,
            0,
            config.ExplicitChannelProfilesSize(),
            MaxBlobSize,
            MaxRangesPerBlob,
            EOptimizationMode::OptimizeForLongRanges,
            BuildDefaultCompactionPolicy(5),
            DefaultBPConfig(),
            DefaultFreeSpaceConfig(),
            DefaultIndexCachingConfig()
        );

        const auto perm = EChannelPermission::UserWritesAllowed;
        auto kind = EChannelDataKind::Merged;

        auto blobId = state.GenerateBlobId(kind, perm, 1, 1024);
        UNIT_ASSERT(blobId.Channel() == 3);

        kind = EChannelDataKind::Mixed;

        blobId = state.GenerateBlobId(kind, perm, 1, 1024);
        UNIT_ASSERT(blobId.Channel() == 5);
        blobId = state.GenerateBlobId(kind, perm, 1, 1024);
        UNIT_ASSERT(blobId.Channel() == 10);
        blobId = state.GenerateBlobId(kind, perm, 1, 1024);
        UNIT_ASSERT(blobId.Channel() == 5);

        kind = EChannelDataKind::Merged;

        blobId = state.GenerateBlobId(kind, perm, 1, 1024);
        UNIT_ASSERT(blobId.Channel() == 4);
        blobId = state.GenerateBlobId(kind, perm, 1, 1024);
        UNIT_ASSERT(blobId.Channel() == 6);
        blobId = state.GenerateBlobId(kind, perm, 1, 1024);
        UNIT_ASSERT(blobId.Channel() == 7);
        blobId = state.GenerateBlobId(kind, perm, 1, 1024);
        UNIT_ASSERT(blobId.Channel() == 8);
        blobId = state.GenerateBlobId(kind, perm, 1, 1024);
        UNIT_ASSERT(blobId.Channel() == 9);
        blobId = state.GenerateBlobId(kind, perm, 1, 1024);
        UNIT_ASSERT(blobId.Channel() == 3);
    }

    Y_UNIT_TEST(PickNextChannelWithProperFreeSpaceShare)
    {
        auto meta = DefaultConfig(1024, DefaultBlockSize, 2);

        for (auto kind: {EChannelDataKind::Mixed, EChannelDataKind::Merged}) {
            TPartitionState state(
                meta,
                TestTabletId,
                0,
                6,  // channelCount
                MaxBlobSize,
                MaxRangesPerBlob,
                EOptimizationMode::OptimizeForLongRanges,
                BuildDefaultCompactionPolicy(5),
                DefaultBPConfig(),
                DefaultFreeSpaceConfig(),
                DefaultIndexCachingConfig()
            );

            {
                auto blobId = state.GenerateBlobId(
                    kind,
                    EChannelPermission::UserWritesAllowed,
                    1,
                    1024
                );
                UNIT_ASSERT_VALUES_EQUAL(
                    ui32(TPartitionSchema::FirstDataChannel),
                    blobId.Channel()
                );
                auto blobId2 = state.GenerateBlobId(
                    kind,
                    EChannelPermission::UserWritesAllowed,
                    1,
                    1024
                );
                UNIT_ASSERT_VALUES_EQUAL(
                    ui32(TPartitionSchema::FirstDataChannel + 1),
                    blobId2.Channel()
                );
            }

            UNIT_ASSERT_VALUES_EQUAL(0, state.GetAlmostFullChannelCount());
            state.UpdateChannelFreeSpaceShare(
                TPartitionSchema::FirstDataChannel,
                0.15
            );
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetAlmostFullChannelCount());

            for (ui32 i = 0; i < 10; ++i) {
                auto blobId = state.GenerateBlobId(
                    kind,
                    EChannelPermission::UserWritesAllowed,
                    1,
                    1024
                );
                UNIT_ASSERT_VALUES_EQUAL(
                    ui32(TPartitionSchema::FirstDataChannel + 1),
                    blobId.Channel()
                );
            }

            state.UpdateChannelFreeSpaceShare(
                TPartitionSchema::FirstDataChannel,
                0.16
            );
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetAlmostFullChannelCount());

            ui32 firstChannelSelected = 0;
            for (ui32 i = 0; i < 1000; ++i) {
                auto blobId = state.GenerateBlobId(
                    kind,
                    EChannelPermission::UserWritesAllowed,
                    1,
                    1024
                );
                if (blobId.Channel() == TPartitionSchema::FirstDataChannel) {
                    ++firstChannelSelected;
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(
                        ui32(TPartitionSchema::FirstDataChannel + 1),
                        blobId.Channel()
                    );
                }
            }

            UNIT_ASSERT(firstChannelSelected < 150 && firstChannelSelected > 50);

            state.UpdateChannelFreeSpaceShare(
                TPartitionSchema::FirstDataChannel,
                0.16
            );
            state.UpdateChannelFreeSpaceShare(
                TPartitionSchema::FirstDataChannel + 1,
                0.161
            );
            UNIT_ASSERT_VALUES_EQUAL(2, state.GetAlmostFullChannelCount());

            firstChannelSelected = 0;
            for (ui32 i = 0; i < 1000; ++i) {
                auto blobId = state.GenerateBlobId(
                    kind,
                    EChannelPermission::UserWritesAllowed,
                    1,
                    1024
                );
                if (blobId.Channel() == TPartitionSchema::FirstDataChannel) {
                    ++firstChannelSelected;
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(
                        ui32(TPartitionSchema::FirstDataChannel + 1),
                        blobId.Channel()
                    );
                }
            }

            UNIT_ASSERT(firstChannelSelected < 150 && firstChannelSelected > 50);
        }
    }

    Y_UNIT_TEST(PickMergedChannelIfAllMixedChannelsAreFull)
    {
        NProto::TPartitionMeta meta;
        auto& config = *meta.MutableConfig();
        config.SetBlocksCount(1024);
        config.SetBlockSize(DefaultBlockSize);
        config.SetZoneBlockCount(32 * MaxBlocksCount);

        config.AddExplicitChannelProfiles()->SetDataKind(
            static_cast<ui32>(EChannelDataKind::System));
        config.AddExplicitChannelProfiles()->SetDataKind(
            static_cast<ui32>(EChannelDataKind::Log));
        config.AddExplicitChannelProfiles()->SetDataKind(
            static_cast<ui32>(EChannelDataKind::Index));
        config.AddExplicitChannelProfiles()->SetDataKind(
            static_cast<ui32>(EChannelDataKind::Mixed));
        config.AddExplicitChannelProfiles()->SetDataKind(
            static_cast<ui32>(EChannelDataKind::Merged));

        TPartitionState state(
            meta,
            TestTabletId,
            0,
            config.ExplicitChannelProfilesSize(),
            MaxBlobSize,
            MaxRangesPerBlob,
            EOptimizationMode::OptimizeForLongRanges,
            BuildDefaultCompactionPolicy(5),
            DefaultBPConfig(),
            DefaultFreeSpaceConfig(),
            DefaultIndexCachingConfig()
        );

        {
            auto mixedBlobId = state.GenerateBlobId(
                EChannelDataKind::Mixed,
                EChannelPermission::UserWritesAllowed,
                1,
                1024
            );
            UNIT_ASSERT_VALUES_EQUAL(
                ui32(TPartitionSchema::FirstDataChannel),
                mixedBlobId.Channel()
            );
            auto mergedBlobId = state.GenerateBlobId(
                EChannelDataKind::Merged,
                EChannelPermission::UserWritesAllowed,
                1,
                1024
            );
            UNIT_ASSERT_VALUES_EQUAL(
                ui32(TPartitionSchema::FirstDataChannel + 1),
                mergedBlobId.Channel()
            );
        }

        state.UpdatePermissions(TPartitionSchema::FirstDataChannel, {});

        {
            auto mixedBlobId = state.GenerateBlobId(
                EChannelDataKind::Mixed,
                EChannelPermission::UserWritesAllowed,
                1,
                1024
            );
            UNIT_ASSERT_VALUES_EQUAL(
                ui32(TPartitionSchema::FirstDataChannel + 1),
                mixedBlobId.Channel()
            );
        }
    }

    Y_UNIT_TEST(CalculateCurrentBackpressure)
    {
        TPartitionState state(
            DefaultConfig(1000),
            TestTabletId,
            0,
            5,  // channelCount
            MaxBlobSize,
            MaxRangesPerBlob,
            EOptimizationMode::OptimizeForLongRanges,
            BuildDefaultCompactionPolicy(5),
            DefaultBPConfig(),
            DefaultFreeSpaceConfig(),
            DefaultIndexCachingConfig()
        );

        state.GetBlobs().InitializeZone(0);

        const auto initialBackpressure = state.CalculateCurrentBackpressure();
        UNIT_ASSERT_VALUES_EQUAL(1, initialBackpressure.FreshIndexScore);
        UNIT_ASSERT_VALUES_EQUAL(1, initialBackpressure.CompactionScore);
        UNIT_ASSERT_VALUES_EQUAL(1, initialBackpressure.DiskSpaceScore);
        UNIT_ASSERT_VALUES_EQUAL(1, initialBackpressure.CleanupScore);

        TVector<TOwningFreshBlock> freshBlocks;
        for (ui32 i = 0; i < 100; ++i) {
            freshBlocks.emplace_back(TBlock{i, 1, 1, false}, ToString(i));
        }
        state.InitFreshBlocks(freshBlocks);
        state.GetCompactionMap().Update(0, 10, 10, 10, false);
        state.AddBlobUpdateByFresh({TBlockRange32::WithLength(0, 1024), 1, 1});

        const auto marginalBackpressure = state.CalculateCurrentBackpressure();
        UNIT_ASSERT_DOUBLES_EQUAL(1, marginalBackpressure.FreshIndexScore, 1e-5);
        UNIT_ASSERT_DOUBLES_EQUAL(1, marginalBackpressure.CompactionScore, 1e-5);
        UNIT_ASSERT_DOUBLES_EQUAL(1, marginalBackpressure.CleanupScore, 1e-5);

        freshBlocks.clear();
        for (ui32 i = 100; i < 400; ++i) {
            freshBlocks.emplace_back(TBlock{i, 1, 1, false}, ToString(i));
        }
        state.InitFreshBlocks(freshBlocks);
        state.GetCompactionMap().Update(0, 30, 30, 30, false);
        state.AddBlobUpdateByFresh({TBlockRange32::WithLength(1024, 1024), 2, 2});

        const auto maxBackpressure = state.CalculateCurrentBackpressure();
        UNIT_ASSERT_DOUBLES_EQUAL(10, maxBackpressure.FreshIndexScore, 1e-5);
        UNIT_ASSERT_DOUBLES_EQUAL(10, maxBackpressure.CompactionScore, 1e-5);
        UNIT_ASSERT_DOUBLES_EQUAL(10, maxBackpressure.CleanupScore, 1e-5);

        state.GetCompactionMap().Update(0, 100, 100, 100, false);

        const auto maxBackpressure2 = state.CalculateCurrentBackpressure();
        UNIT_ASSERT_DOUBLES_EQUAL(10, maxBackpressure2.CompactionScore, 1e-5);
    }

    Y_UNIT_TEST(CompactionBackpressureShouldBeZeroIfNotRequiredByPolicy)
    {
        TPartitionState state(
            DefaultConfig(1000),
            TestTabletId,
            0,
            5,  // channelCount
            MaxBlobSize,
            MaxRangesPerBlob,
            EOptimizationMode::OptimizeForLongRanges,
            std::make_shared<TNoBackpressurePolicy>(),
            DefaultBPConfig(),
            DefaultFreeSpaceConfig(),
            DefaultIndexCachingConfig()
        );

        state.GetCompactionMap().Update(0, 30, 30, 30, false);

        const auto bp = state.CalculateCurrentBackpressure();
        UNIT_ASSERT_VALUES_EQUAL(0, bp.CompactionScore);
    }

    Y_UNIT_TEST(ShouldUpdateIndexStructures)
    {
        // TODO
    }

    Y_UNIT_TEST(ShouldReturnInvalidCommitIdWhenItOverflows)
    {
        auto meta = DefaultConfig();
        auto& config = *meta.MutableConfig();
        auto cps = config.MutableExplicitChannelProfiles();
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::System));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Log));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Index));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Merged));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Merged));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Mixed));

        TPartitionState state(
            meta,
            TestTabletId,
            0,
            config.ExplicitChannelProfilesSize(),
            MaxBlobSize,
            MaxRangesPerBlob,
            EOptimizationMode::OptimizeForLongRanges,
            BuildDefaultCompactionPolicy(5),
            DefaultBPConfig(),
            DefaultFreeSpaceConfig(),
            DefaultIndexCachingConfig(),
            Max<ui32>(),  // maxIORequestsInFlight
            0,            // reassignChannelsPercentageThreshold
            100,          // reassignFreshChannelsPercentageThreshold
            100,          // reassignMixedChannelsPercentageThreshold
            false,        // reassignSystemChannelsImmediately
            Max<ui32>()   // lastStep
        );

        UNIT_ASSERT(state.GenerateCommitId() == InvalidCommitId);
    }

    Y_UNIT_TEST(ShouldStoreFreshBlockUpdates)
    {
        TPartitionState state(
            DefaultConfig(),
            TestTabletId,
            0,
            5,  // channelCount
            MaxBlobSize,
            MaxRangesPerBlob,
            EOptimizationMode::OptimizeForLongRanges,
            BuildDefaultCompactionPolicy(5),
            DefaultBPConfig(),
            DefaultFreeSpaceConfig(),
            DefaultIndexCachingConfig()
        );

        TTestExecutor executor;
        executor.WriteTx([&] (TPartitionDatabase db) {
            db.InitSchema();
        });
        executor.WriteTx([&] (TPartitionDatabase db) {
            state.InitIndex(db, TBlockRange32::WithLength(0, 1024));
        });

        ui64 commitId1 = state.GenerateCommitId();
        ui64 commitId2 = state.GenerateCommitId();
        ui64 commitId3 = state.GenerateCommitId();

        TFreshBlockUpdates updates = {
            { commitId1, TBlockRange32::MakeClosedInterval(1, 4) },
            { commitId2, TBlockRange32::MakeClosedInterval(3, 5) },
            { commitId2, TBlockRange32::MakeClosedInterval(8, 9) },
            { commitId3, TBlockRange32::MakeClosedInterval(2, 6) },
            { commitId3, TBlockRange32::MakeClosedInterval(4, 8) },
        };

        executor.WriteTx([&] (TPartitionDatabase db) {
            for (const auto update: updates) {
                state.AddFreshBlockUpdate(db, update);
            }
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            TFreshBlockUpdates actual;
            db.ReadFreshBlockUpdates(actual);

            UNIT_ASSERT_VALUES_EQUAL(5, actual.size());
            UNIT_ASSERT_VALUES_EQUAL(actual, updates);
        });

        state.SetLastFlushCommitId(commitId2);

        executor.WriteTx([&] (TPartitionDatabase db) {
            state.TrimFreshBlockUpdates(db);
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            TFreshBlockUpdates actual;
            db.ReadFreshBlockUpdates(actual);

            UNIT_ASSERT_VALUES_EQUAL(2, actual.size());
            UNIT_ASSERT_VALUES_EQUAL(actual[0], updates[3]);
            UNIT_ASSERT_VALUES_EQUAL(actual[1], updates[4]);
        });
    }

    Y_UNIT_TEST(TestReassignedMixedChannelsPercentageThreshold)
    {
        const ui32 mixedChannelCount = 10;
        const ui32 mergedChannelCount = 10;
        const ui32 reassignMixedChannelsPercentageThreshold = 20;

        NProto::TPartitionMeta meta;

        auto& config = *meta.MutableConfig();
        config.SetBlockSize(DefaultBlockSize);
        config.SetBlocksCount(1024);
        config.SetZoneBlockCount(32 * MaxBlocksCount);

        auto* cps = config.MutableExplicitChannelProfiles();
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::System));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Log));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Index));
        for (ui32 i = 0; i < mergedChannelCount; ++i) {
            cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Merged));
        }
        for (ui32 i = 0; i < mixedChannelCount; ++i) {
            cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Mixed));
        }
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Fresh));

        TPartitionState state(
            meta,
            TestTabletId,
            0,
            mixedChannelCount + mergedChannelCount + DataChannelStart + 1, // channelCount
            MaxBlobSize,
            MaxRangesPerBlob,
            EOptimizationMode::OptimizeForLongRanges,
            BuildDefaultCompactionPolicy(5),
            DefaultBPConfig(),
            DefaultFreeSpaceConfig(),
            DefaultIndexCachingConfig(),
            Max(),  // maxIORequestsInFlight
            100,    // reassignChannelsPercentageThreshold
            100,    // reassignFreshChannelsPercentageThreshold
            reassignMixedChannelsPercentageThreshold
        );

        UNIT_ASSERT_VALUES_EQUAL(0, state.GetChannelsToReassign().size());

        {
            state.UpdatePermissions(
                DataChannelStart + mergedChannelCount,
                EChannelPermission::SystemWritesAllowed);

            UNIT_ASSERT_VALUES_EQUAL(0, state.GetChannelsToReassign().size());

            state.UpdatePermissions(
                DataChannelStart + mergedChannelCount + 5,
                EChannelPermission::SystemWritesAllowed);

            const auto channelsToReassign = state.GetChannelsToReassign();
            UNIT_ASSERT_VALUES_EQUAL(2, channelsToReassign.size());
            UNIT_ASSERT_VALUES_EQUAL(
                DataChannelStart + mergedChannelCount,
                channelsToReassign[0]);
            UNIT_ASSERT_VALUES_EQUAL(
                DataChannelStart + mergedChannelCount + 5,
                channelsToReassign[1]);
        }

        {
            state.UpdatePermissions(
                DataChannelStart + mergedChannelCount,
                EChannelPermission::UserWritesAllowed |
                    EChannelPermission::SystemWritesAllowed);
            state.UpdatePermissions(
                DataChannelStart + mergedChannelCount + 5,
                EChannelPermission::UserWritesAllowed |
                    EChannelPermission::SystemWritesAllowed);
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetChannelsToReassign().size());
        }
    }

    Y_UNIT_TEST(TestReassignSystemChannelsImmediately)
    {
        const ui32 mixedChannelCount = 10;
        const ui32 mergedChannelCount = 10;

        NProto::TPartitionMeta meta;

        auto& config = *meta.MutableConfig();
        config.SetBlockSize(DefaultBlockSize);
        config.SetBlocksCount(1024);
        config.SetZoneBlockCount(32 * MaxBlocksCount);

        auto* cps = config.MutableExplicitChannelProfiles();
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::System));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Log));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Index));
        for (ui32 i = 0; i < mergedChannelCount; ++i) {
            cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Merged));
        }
        for (ui32 i = 0; i < mixedChannelCount; ++i) {
            cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Mixed));
        }
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Fresh));

        TPartitionState state(
            meta,
            TestTabletId,
            0,
            mixedChannelCount + mergedChannelCount + DataChannelStart + 1, // channelCount
            MaxBlobSize,
            MaxRangesPerBlob,
            EOptimizationMode::OptimizeForLongRanges,
            BuildDefaultCompactionPolicy(5),
            DefaultBPConfig(),
            DefaultFreeSpaceConfig(),
            DefaultIndexCachingConfig(),
            Max(), // maxIORequestsInFlight
            100,   // reassignChannelsPercentageThreshold
            100,   // reassignFreshChannelsPercentageThreshold
            100,   // reassignMixedChannelsPercentageThreshold
            true   // reassignSystemChannelsImmediately
        );

        UNIT_ASSERT_VALUES_EQUAL(0, state.GetChannelsToReassign().size());

        {
            state.UpdatePermissions(
                1, // Log
                EChannelPermission::SystemWritesAllowed);

            const auto channelsToReassign = state.GetChannelsToReassign();
            UNIT_ASSERT_VALUES_EQUAL(1, channelsToReassign.size());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                channelsToReassign[0]);
        }

        {
            state.UpdatePermissions(
                1,
                EChannelPermission::UserWritesAllowed |
                    EChannelPermission::SystemWritesAllowed);
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetChannelsToReassign().size());
        }
    }

    Y_UNIT_TEST(TestReassignFreshChannelsAfterCertainThreshold)
    {
        const ui32 mixedChannelCount = 10;
        const ui32 mergedChannelCount = 10;
        const ui32 freshChannelCount = 10;
        const ui32 reassignFreshChannelsPercentageThreshold = 20;

        NProto::TPartitionMeta meta;

        auto& config = *meta.MutableConfig();
        config.SetBlockSize(DefaultBlockSize);
        config.SetBlocksCount(1024);
        config.SetZoneBlockCount(32 * MaxBlocksCount);

        auto* cps = config.MutableExplicitChannelProfiles();
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::System));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Log));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Index));
        for (ui32 i = 0; i < mergedChannelCount; ++i) {
            cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Merged));
        }
        for (ui32 i = 0; i < mixedChannelCount; ++i) {
            cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Mixed));
        }
        for (ui32 i = 0; i < freshChannelCount; ++i) {
            cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Fresh));
        }

        TPartitionState state(
            meta,
            TestTabletId,
            0,
            mixedChannelCount + mergedChannelCount + DataChannelStart + freshChannelCount, // channelCount
            MaxBlobSize,
            MaxRangesPerBlob,
            EOptimizationMode::OptimizeForLongRanges,
            BuildDefaultCompactionPolicy(5),
            DefaultBPConfig(),
            DefaultFreeSpaceConfig(),
            DefaultIndexCachingConfig(),
            Max(),  // maxIORequestsInFlight
            100,                                      // reassignChannelsPercentageThreshold
            reassignFreshChannelsPercentageThreshold  // reassignFreshChannelsPercentageThreshold
        );

        UNIT_ASSERT_VALUES_EQUAL(0, state.GetChannelsToReassign().size());

        {
            state.UpdatePermissions(
                DataChannelStart + mergedChannelCount + mixedChannelCount,
                EChannelPermission::SystemWritesAllowed);

            UNIT_ASSERT_VALUES_EQUAL(0, state.GetChannelsToReassign().size());

            state.UpdatePermissions(
                DataChannelStart + mergedChannelCount + mixedChannelCount + 5,
                EChannelPermission::SystemWritesAllowed);

            const auto channelsToReassign = state.GetChannelsToReassign();
            UNIT_ASSERT_VALUES_EQUAL(2, channelsToReassign.size());
            UNIT_ASSERT_VALUES_EQUAL(
                DataChannelStart + mergedChannelCount + mixedChannelCount,
                channelsToReassign[0]);
            UNIT_ASSERT_VALUES_EQUAL(
                DataChannelStart + mergedChannelCount + mixedChannelCount + 5,
                channelsToReassign[1]);
        }

        {
            state.UpdatePermissions(
                DataChannelStart + mergedChannelCount + mixedChannelCount,
                EChannelPermission::UserWritesAllowed |
                    EChannelPermission::SystemWritesAllowed);
            state.UpdatePermissions(
                DataChannelStart + mergedChannelCount + mixedChannelCount + 5,
                EChannelPermission::UserWritesAllowed |
                    EChannelPermission::SystemWritesAllowed);
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetChannelsToReassign().size());
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2

template <>
inline void Out<NCloud::NBlockStore::NStorage::NPartition2::TFreshBlockUpdate>(
    IOutputStream& out,
    const NCloud::NBlockStore::NStorage::NPartition2::TFreshBlockUpdate& update)
{
    out << "CommitId=" << update.CommitId
        << ", BlockRange=" << DescribeRange(update.BlockRange);
}
