#include "part2_database.h"
#include "part2_schema.h"

#include <cloud/blockstore/libs/storage/testlib/test_executor.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <initializer_list>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString GetBlockContent(char fill)
{
    return TString(DefaultBlockSize, fill);
}

TString CommitId2Str(ui64 commitId)
{
    return commitId == InvalidCommitId ? "x" : ToString(commitId);
}

template <typename TTable>
void AssertIndexEntry(
    TPartitionDatabase& db,
    const TPartialBlobId& expectedBlobId,
    const TBlockRange32& expectedBlockRange,
    const NProto::TBlobMeta& expectedBlobMeta)
{
    auto it = db.Table<TTable>().Range().Select();

    UNIT_ASSERT(it.IsReady());
    UNIT_ASSERT(it.IsValid());
    UNIT_ASSERT_VALUES_EQUAL(
        expectedBlockRange.Start,
        it.template GetValue<typename TTable::RangeStart>());
    UNIT_ASSERT_VALUES_EQUAL(
        expectedBlockRange.End,
        it.template GetValue<typename TTable::RangeEnd>());
    UNIT_ASSERT_VALUES_EQUAL(
        expectedBlobId.CommitId(),
        it.template GetValue<typename TTable::BlobCommitId>());
    UNIT_ASSERT_VALUES_EQUAL(
        expectedBlobId.UniqueId(),
        it.template GetValue<typename TTable::BlobId>());

    const auto blobMeta =
        it.template GetValue<typename TTable::BlobMeta>();
    UNIT_ASSERT_VALUES_EQUAL(
        expectedBlobMeta.SerializeAsString(),
        blobMeta.SerializeAsString());

    UNIT_ASSERT(it.Next());
    UNIT_ASSERT(!it.IsValid());
}

////////////////////////////////////////////////////////////////////////////////

struct TTestBlockVisitor final
    : public IBlocksIndexVisitor
    , public IBlobsVisitor
    , public IMixedBlocksIndexVisitor
{
    TStringBuilder Result;
    THashMap<TPartialBlobId, TBlockRange32, TPartialBlobIdHash> BlobToRange;

    bool Visit(
        TBlockRange32 blockRange,
        const TPartialBlobId& blobId,
        ui32 skippedBlocksCount) override
    {
        Y_UNUSED(skippedBlocksCount);

        return Visit(blockRange, blobId);
    }

    bool Visit(TBlockRange32 blockRange, const TPartialBlobId& blobId) override
    {
        BlobToRange[blobId] = blockRange;

        return true;
    }

    bool Visit(
        ui32 blockIndex,
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui16 blobOffset) override
    {
        Y_UNUSED(blobId);
        Y_UNUSED(blobOffset);

        if (Result) {
            Result << " ";
        }
        Result << "#" << blockIndex << ":" << CommitId2Str(commitId);
        return true;
    }

    bool VisitBlock(
        ui32 blockIndex,
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui16 blobOffset,
        ui8 compactionRangeCount) override
    {
        Y_UNUSED(blobId);
        Y_UNUSED(blobOffset);

        if (Result) {
            Result << " ";
        }
        Result << "#" << blockIndex << ":" << CommitId2Str(commitId) << ":"
               << ui32{compactionRangeCount};
        return true;
    }
};

struct TTestBlockVisitorWithBlobOffset final
    : public IExtendedBlocksIndexVisitor
{
    TStringBuilder Result;

    bool Visit(
        ui32 blockIndex,
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui16 blobOffset,
        ui32 checksum) override
    {
        Y_UNUSED(blobId);

        if (Result) {
            Result << " ";
        }
        Result << "#" << blockIndex
            << ":" << CommitId2Str(commitId)
            << ":" << blobOffset;
        if (checksum) {
            Result << "##" << checksum;
        }
        return true;
    }
};

struct TTestLevelIndexVisitor final
    : public IBlocksIndexVisitor
{
    TStringBuilder Result;

    bool Visit(
        ui32 blockIndex,
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui16 blobOffset) override
    {
        if (Result) {
            Result << " ";
        }
        Result << "#" << blockIndex << ":" << commitId << ":"
               << blobId.CommitId() << ":" << blobId.UniqueId() << ":"
               << blobOffset;
        return true;
    }
};

struct TTestBlobVisitor final
    : public IBlobsIndexVisitor
{
    ui64 ReadCount = 0;

    bool Visit(
        ui64 commitId,
        ui64 blobId,
        const NProto::TBlobMeta& blobMeta,
        const TStringBuf blockMask) override
    {
        Y_UNUSED(commitId);
        Y_UNUSED(blobId);
        Y_UNUSED(blobMeta);
        Y_UNUSED(blockMask);
        ++ReadCount;
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartition2DatabaseTest)
{
    Y_UNIT_TEST(ShouldWriteL0AndL1Blobs)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TPartitionDatabase db) { db.InitSchema(); });

        const TPartialBlobId l0BlobId(10, 1);
        const auto l0BlockRange =
            TBlockRange32::MakeClosedInterval(100, 200);
        NProto::TBlobMeta l0BlobMeta;
        l0BlobMeta.MutableMixedBlocks()->AddBlocks(100);

        const TPartialBlobId l1BlobId(20, 2);
        const auto l1BlockRange =
            TBlockRange32::MakeClosedInterval(300, 400);
        NProto::TBlobMeta l1BlobMeta;
        l1BlobMeta.MutableMixedBlocks()->AddBlocks(300);

        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                db.WriteL0Blob(l0BlobId, l0BlockRange, l0BlobMeta);
                db.WriteL1Blob(l1BlobId, l1BlockRange, l1BlobMeta);
            });

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
                AssertIndexEntry<TPartitionSchema::L0Index>(
                    db,
                    l0BlobId,
                    l0BlockRange,
                    l0BlobMeta);
                AssertIndexEntry<TPartitionSchema::L1Index>(
                    db,
                    l1BlobId,
                    l1BlockRange,
                    l1BlobMeta);
            });
    }

    Y_UNIT_TEST(ShouldFindBlocksInL0AndL1Indices)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TPartitionDatabase db) { db.InitSchema(); });

        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                const auto writeBlob = [&](TPartialBlobId blobId,
                                           TBlockRange32 blockRange,
                                           std::initializer_list<ui32> blocks,
                                           std::initializer_list<ui64> commitIds)
                {
                    NProto::TBlobMeta blobMeta;
                    auto* mixedBlocks = blobMeta.MutableMixedBlocks();
                    for (ui32 blockIndex: blocks) {
                        mixedBlocks->AddBlocks(blockIndex);
                    }
                    for (ui64 commitId: commitIds) {
                        mixedBlocks->AddCommitIds(commitId);
                    }

                    db.WriteL0Blob(blobId, blockRange, blobMeta);
                    db.WriteL1Blob(blobId, blockRange, blobMeta);
                };

                writeBlob(
                    TPartialBlobId(5, 1),
                    TBlockRange32::MakeClosedInterval(0, 5),
                    {0, 5},
                    {});
                writeBlob(
                    TPartialBlobId(10, 2),
                    TBlockRange32::MakeClosedInterval(10, 19),
                    {10, 15, 19},
                    {});
                writeBlob(
                    TPartialBlobId(30, 3),
                    TBlockRange32::MakeClosedInterval(15, 19),
                    {15, 19},
                    {5, 15});
                writeBlob(
                    TPartialBlobId(40, 4),
                    TBlockRange32::MakeClosedInterval(20, 25),
                    {20, 25},
                    {15, 25});
                writeBlob(
                    TPartialBlobId(10, 5),
                    TBlockRange32::MakeClosedInterval(30, 39),
                    {30, 39},
                    {});
            });

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& database)
            {
                TPartitionDatabase db(database, 10, 20);
                const auto blockRange =
                    TBlockRange32::MakeClosedInterval(12, 22);
                constexpr ui64 maxCommitId = 15;
                constexpr TStringBuf expected =
                    "#15:10:10:2:1 #19:10:10:2:2 "
                    "#15:5:30:3:0 #19:15:30:3:1 #20:15:40:4:0";

                TTestLevelIndexVisitor l0Visitor;
                UNIT_ASSERT(db.FindBlocksInL0Index(
                    l0Visitor,
                    blockRange,
                    maxCommitId));
                UNIT_ASSERT_VALUES_EQUAL(expected, l0Visitor.Result);

                TTestLevelIndexVisitor l1Visitor;
                UNIT_ASSERT(db.FindBlocksInL1Index(
                    l1Visitor,
                    blockRange,
                    maxCommitId));
                UNIT_ASSERT_VALUES_EQUAL(expected, l1Visitor.Result);
            });
    }

    Y_UNIT_TEST(ShouldStorePartitionMeta)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TPartitionDatabase db) {
            db.InitSchema();
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            NProto::TPartitionMeta meta;
            db.WriteMeta(meta);
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            TMaybe<NProto::TPartitionMeta> meta;
            UNIT_ASSERT(db.ReadMeta(meta));
            UNIT_ASSERT(meta.Defined());
        });
    }

    Y_UNIT_TEST(ShouldReadFreshBlocks)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TPartitionDatabase db) {
            db.InitSchema();
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            ui64 commitId = executor.CommitId();
            auto zero = GetBlockContent(0);
            auto one = GetBlockContent(1);
            auto two = GetBlockContent(2);
            db.WriteFreshBlock(0, commitId, {zero.data(), zero.size()});
            db.WriteFreshBlock(1, commitId, {one.data(), one.size()});
            db.WriteFreshBlock(2, commitId, {two.data(), two.size()});
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            ui64 commitId = executor.CommitId();
            auto one = GetBlockContent(1);
            db.WriteFreshBlock(1, commitId, {one.data(), one.size()});
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            TVector<TOwningFreshBlock> blocks;
            UNIT_ASSERT(db.ReadFreshBlocks(blocks));
            UNIT_ASSERT_VALUES_EQUAL(4, blocks.size());
            UNIT_ASSERT_VALUES_EQUAL(0, blocks[0].Meta.BlockIndex);
            UNIT_ASSERT_VALUES_EQUAL(2, blocks[0].Meta.CommitId);
            UNIT_ASSERT_VALUES_EQUAL(true, blocks[0].Meta.IsStoredInDb);
            UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(0), blocks[0].Content);
            UNIT_ASSERT_VALUES_EQUAL(1, blocks[1].Meta.BlockIndex);
            UNIT_ASSERT_VALUES_EQUAL(3, blocks[1].Meta.CommitId);
            UNIT_ASSERT_VALUES_EQUAL(true, blocks[1].Meta.IsStoredInDb);
            UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(1), blocks[1].Content);
            UNIT_ASSERT_VALUES_EQUAL(1, blocks[2].Meta.BlockIndex);
            UNIT_ASSERT_VALUES_EQUAL(2, blocks[2].Meta.CommitId);
            UNIT_ASSERT_VALUES_EQUAL(true, blocks[2].Meta.IsStoredInDb);
            UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(1), blocks[2].Content);
            UNIT_ASSERT_VALUES_EQUAL(2, blocks[3].Meta.BlockIndex);
            UNIT_ASSERT_VALUES_EQUAL(2, blocks[3].Meta.CommitId);
            UNIT_ASSERT_VALUES_EQUAL(true, blocks[3].Meta.IsStoredInDb);
            UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(2), blocks[3].Content);
        });
    }

    Y_UNIT_TEST(ShouldFindRangeOfMixedBlocks)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TPartitionDatabase db) {
            db.InitSchema();
        });

        executor.WriteTx(
            [&](TPartitionDatabase db)
            { db.WriteMixedBlocks(executor.MakeBlobId(), {0, 1, 2}, 12); });

        executor.WriteTx(
            [&](TPartitionDatabase db)
            { db.WriteMixedBlocks(executor.MakeBlobId(), {4, 5, 6}, 13); });

        ui64 maxCommitId = executor.WriteTx(
            [&](TPartitionDatabase db)
            { db.WriteMixedBlocks(executor.MakeBlobId(), {2, 3, 4}, 14); });

        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                db.WriteMixedBlocks(
                    executor.MakeBlobId(),
                    {0, 1, 2, 3, 4, 5, 6},
                    15);
            });

        executor.ReadTx([&] (TPartitionDatabase db) {
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMixedBlocks(
                    visitor,
                    TBlockRange32::WithLength(0, 2),
                    true,   // precharge
                    maxCommitId
                ));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#0:2:12 #1:2:12");
            }
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMixedBlocks(
                    visitor,
                    TBlockRange32::MakeClosedInterval(5, 6),
                    true,   // precharge
                    maxCommitId
                ));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#5:3:13 #6:3:13");
            }
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMixedBlocks(
                    visitor,
                    TBlockRange32::MakeClosedInterval(2, 3),
                    true,   // precharge
                    maxCommitId
                ));
                UNIT_ASSERT_VALUES_EQUAL(
                    visitor.Result,
                    "#2:4:14 #2:2:12 #3:4:14");
            }
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMixedBlocks(
                    visitor,
                    TBlockRange32::MakeClosedInterval(3, 4),
                    true,   // precharge
                    maxCommitId
                ));
                UNIT_ASSERT_VALUES_EQUAL(
                    visitor.Result,
                    "#3:4:14 #4:4:14 #4:3:13");
            }
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMixedBlocks(
                    visitor,
                    TBlockRange32::MakeClosedInterval(1, 5),
                    true,   // precharge
                    maxCommitId
                ));
                UNIT_ASSERT_VALUES_EQUAL(
                    visitor.Result,
                    "#1:2:12 #2:4:14 #2:2:12 #3:4:14 #4:4:14 #4:3:13 #5:3:13");
            }
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMixedBlocks(
                    visitor,
                    TBlockRange32::MakeClosedInterval(1, 5),
                    true    // precharge
                ));
                UNIT_ASSERT_VALUES_EQUAL(
                    visitor.Result,
                    "#1:5:15 #1:2:12 #2:5:15 #2:4:14 #2:2:12 #3:5:15 #3:4:14 #4:5:15 "
                    "#4:4:14 #4:3:13 #5:5:15 #5:3:13");
            }
        });
    }

    Y_UNIT_TEST(ShouldFindMixedBlocks)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TPartitionDatabase db) {
            db.InitSchema();
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            db.WriteMixedBlocks(
                executor.MakeBlobId(),
                {0, 1, 2}, 12);
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            db.WriteMixedBlocks(
                executor.MakeBlobId(),
                {4, 5, 6}, 13);
        });

        ui64 maxCommitId = executor.WriteTx([&] (TPartitionDatabase db) {
            db.WriteMixedBlocks(
                executor.MakeBlobId(),
                {2, 3, 4}, 14);
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            db.WriteMixedBlocks(
                executor.MakeBlobId(),
                {0, 1, 2, 3, 4, 5, 6}, 15);
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMixedBlocks(visitor, TVector<ui32>{0, 1}, maxCommitId));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#0:2:12 #1:2:12");
            }
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMixedBlocks(visitor, TVector<ui32>{5, 6}, maxCommitId));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#5:3:13 #6:3:13");
            }
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMixedBlocks(visitor, TVector<ui32>{2, 3}, maxCommitId));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#2:4:14 #2:2:12 #3:4:14");
            }
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMixedBlocks(visitor, TVector<ui32>{3, 4}, maxCommitId));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#3:4:14 #4:4:14 #4:3:13");
            }
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMixedBlocks(visitor, TVector<ui32>{1, 3, 5}, maxCommitId));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#1:2:12 #3:4:14 #5:3:13");
            }
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMixedBlocks(visitor, TVector<ui32>{1, 3, 5}));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#1:5:15 #1:2:12 #3:5:15 #3:4:14 #5:5:15 #5:3:13");
            }
        });
    }

    Y_UNIT_TEST(ShouldFindRangeOfMergedBlocks)
    {
        // TODO: test holeMask and skipMask

        TTestExecutor executor;
        executor.WriteTx([&] (TPartitionDatabase db) {
            db.InitSchema();
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            db.WriteMergedBlocks(
                executor.MakeBlobId(),
                TBlockRange32::WithLength(0, 3),
                TBlockMask()
            );
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            db.WriteMergedBlocks(
                executor.MakeBlobId(),
                TBlockRange32::MakeClosedInterval(4, 6),
                TBlockMask()
            );
        });

        ui64 maxCommitId = executor.WriteTx([&] (TPartitionDatabase db) {
            db.WriteMergedBlocks(
                executor.MakeBlobId(),
                TBlockRange32::MakeClosedInterval(2, 4),
                TBlockMask()
            );
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            db.WriteMergedBlocks(
                executor.MakeBlobId(),
                TBlockRange32::WithLength(0, 7),
                TBlockMask()
            );
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMergedBlocks(
                    visitor,
                    TBlockRange32::WithLength(0, 2),
                    true,   // precharge
                    MaxBlocksCount,
                    maxCommitId
                ));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#0:2 #1:2");
            }
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMergedBlocks(
                    visitor,
                    TBlockRange32::MakeClosedInterval(5, 6),
                    true,   // precharge
                    MaxBlocksCount,
                    maxCommitId
                ));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#5:3 #6:3");
            }
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMergedBlocks(
                    visitor,
                    TBlockRange32::MakeClosedInterval(2, 3),
                    true,   // precharge
                    MaxBlocksCount,
                    maxCommitId
                ));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#2:2 #2:4 #3:4");
            }
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMergedBlocks(
                    visitor,
                    TBlockRange32::MakeClosedInterval(3, 4),
                    true,   // precharge
                    MaxBlocksCount,
                    maxCommitId
                ));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#3:4 #4:4 #4:3");
            }
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMergedBlocks(
                    visitor,
                    TBlockRange32::MakeClosedInterval(1, 5),
                    true,   // precharge
                    MaxBlocksCount,
                    maxCommitId
                ));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#1:2 #2:2 #2:4 #3:4 #4:4 #4:3 #5:3");
            }
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMergedBlocks(
                    visitor,
                    TBlockRange32::MakeClosedInterval(1, 5),
                    true,   // precharge
                    MaxBlocksCount
                ));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#1:2 #2:2 #2:4 #3:4 #4:4 #1:5 #2:5 #3:5 #4:5 #5:5 #4:3 #5:3");
            }
        });
    }

    Y_UNIT_TEST(ShouldFindMergedBlocks)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TPartitionDatabase db) {
            db.InitSchema();
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            db.WriteMergedBlocks(
                executor.MakeBlobId(),
                TBlockRange32::WithLength(0, 3),
                TBlockMask()
            );
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            db.WriteMergedBlocks(
                executor.MakeBlobId(),
                TBlockRange32::MakeClosedInterval(4, 6),
                TBlockMask()
            );
        });

        ui64 maxCommitId = executor.WriteTx([&] (TPartitionDatabase db) {
            db.WriteMergedBlocks(
                executor.MakeBlobId(),
                TBlockRange32::MakeClosedInterval(2, 4),
                TBlockMask()
            );
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            db.WriteMergedBlocks(
                executor.MakeBlobId(),
                TBlockRange32::WithLength(0, 7),
                TBlockMask()
            );
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMergedBlocks(
                    visitor,
                    TVector<ui32>{0, 1},
                    MaxBlocksCount,
                    maxCommitId
                ));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#0:2 #1:2");
            }
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMergedBlocks(
                    visitor,
                    TVector<ui32>{5, 6},
                    MaxBlocksCount,
                    maxCommitId
                ));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#5:3 #6:3");
            }
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMergedBlocks(
                    visitor,
                    TVector<ui32>{2, 3},
                    MaxBlocksCount,
                    maxCommitId
                ));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#2:2 #2:4 #3:4");
            }
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMergedBlocks(
                    visitor,
                    TVector<ui32>{3, 4},
                    MaxBlocksCount,
                    maxCommitId
                ));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#3:4 #4:4 #4:3");
            }
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMergedBlocks(
                    visitor,
                    TVector<ui32>{1, 3, 5},
                    MaxBlocksCount,
                    maxCommitId
                ));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#1:2 #3:4 #5:3");
            }
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMergedBlocks(
                    visitor,
                    TVector<ui32>{1, 3, 5},
                    MaxBlocksCount
                ));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#1:2 #3:4 #1:5 #3:5 #5:5 #5:3");
            }
        });
    }

    Y_UNIT_TEST(ShouldFindBlobsInBlobsIndex)
    {
        TTestExecutor executor;

        executor.WriteTx([&] (TPartitionDatabase db) {
            db.InitSchema();
        });

        auto minCommitId = executor.WriteTx([&] (TPartitionDatabase db) {
            NProto::TBlobMeta blobMeta;
            auto& mergedBlocks = *blobMeta.MutableMergedBlocks();
            mergedBlocks.SetStart(0);
            mergedBlocks.SetEnd(1023);
            db.WriteBlobMeta(
                executor.MakeBlobId(),
                blobMeta);
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            NProto::TBlobMeta blobMeta;
            auto& mergedBlocks = *blobMeta.MutableMergedBlocks();
            mergedBlocks.SetStart(1024);
            mergedBlocks.SetEnd(2047);
            db.WriteBlobMeta(
                executor.MakeBlobId(),
                blobMeta);
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            NProto::TBlobMeta blobMeta;
            auto& mergedBlocks = *blobMeta.MutableMergedBlocks();
            mergedBlocks.SetStart(2048);
            mergedBlocks.SetEnd(3071);
            db.WriteBlobMeta(
                executor.MakeBlobId(),
                blobMeta);
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            TVector<TPartialBlobId> newBlobs;
            UNIT_ASSERT(db.ReadNewBlobs(newBlobs, minCommitId));
            UNIT_ASSERT_EQUAL(newBlobs.size(), 3);
        });
    }

    Y_UNIT_TEST(ShouldStoreUsedBlocks)
    {
        constexpr size_t blocksCount = 1000;

        TTestExecutor executor;
        executor.WriteTx([&] (TPartitionDatabase db) {
            db.InitSchema();
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            TCompressedBitmap bitmap(blocksCount);
            bool read = false;
            db.ReadLogicalUsedBlocks(bitmap, read);
            UNIT_ASSERT(!read);
        });

        TCompressedBitmap bitmap(blocksCount);

        bitmap.Set(0, 1);
        bitmap.Set(10, 50);
        bitmap.Set(100, 900);
        bitmap.Unset(150, 160);
        bitmap.Unset(500, 501);

        executor.WriteTx([&] (TPartitionDatabase db) {
            auto serializer = bitmap.RangeSerializer(0, bitmap.Capacity());
            TCompressedBitmap::TSerializedChunk sc;
            while (serializer.Next(&sc)) {
                db.WriteUsedBlocks(sc);
                db.WriteLogicalUsedBlocks(sc);
            }
        });

        TCompressedBitmap loadedUsedBlocks(blocksCount);
        TCompressedBitmap loadedLogicalUsedBlocks(blocksCount);

        executor.ReadTx([&] (TPartitionDatabase db) {
            TCompressedBitmap usedBlockMap(blocksCount);
            TCompressedBitmap logicalUsedBlockMap(blocksCount);

            db.ReadUsedBlocks(usedBlockMap);
            bool read = false;
            db.ReadLogicalUsedBlocks(logicalUsedBlockMap, read);

            loadedUsedBlocks = std::move(usedBlockMap);
            loadedLogicalUsedBlocks = std::move(logicalUsedBlockMap);
        });

        UNIT_ASSERT_EQUAL(bitmap.Count(), loadedUsedBlocks.Count());
        UNIT_ASSERT_EQUAL(bitmap.Count(), loadedLogicalUsedBlocks.Count());
    }

    Y_UNIT_TEST(ShouldCorrectlyDeleteCheckpoint)
    {
        TTestExecutor executor;

        executor.WriteTx([&] (TPartitionDatabase db) {
            db.InitSchema();
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            TCheckpoint checkpoint(
                "checkpoint",
                42,
                "",
                Now(),
                {}
            );

            db.WriteCheckpoint(checkpoint, false);
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            TVector<TCheckpoint> checkpoints;
            THashMap<TString, ui64> checkpointId2CommitId;

            db.ReadCheckpoints(checkpoints, checkpointId2CommitId);

            UNIT_ASSERT_VALUES_EQUAL(1, checkpoints.size());
            UNIT_ASSERT_VALUES_EQUAL("checkpoint", checkpoints[0].CheckpointId);
            UNIT_ASSERT_VALUES_EQUAL(1, checkpointId2CommitId.size());
            UNIT_ASSERT_VALUES_EQUAL(42, checkpointId2CommitId["checkpoint"]);

        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            db.DeleteCheckpoint("checkpoint", false);
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            TVector<TCheckpoint> checkpoints;
            THashMap<TString, ui64> checkpointId2CommitId;

            db.ReadCheckpoints(checkpoints, checkpointId2CommitId);

            UNIT_ASSERT_VALUES_EQUAL(0, checkpoints.size());
            UNIT_ASSERT_VALUES_EQUAL(0, checkpointId2CommitId.size());
        });
    }

    Y_UNIT_TEST(ShouldCorrectlyWriteDeleteCheckpointData)
    {
        TTestExecutor executor;

        executor.WriteTx([&] (TPartitionDatabase db) {
            db.InitSchema();
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            TCheckpoint checkpoint(
                "checkpoint",
                42,
                "",
                Now(),
                {}
            );

            db.WriteCheckpoint(checkpoint, false);
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            db.DeleteCheckpoint("checkpoint", true);
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            TVector<TCheckpoint> checkpoints;
            THashMap<TString, ui64> checkpointId2CommitId;

            db.ReadCheckpoints(checkpoints, checkpointId2CommitId);

            UNIT_ASSERT_VALUES_EQUAL(0, checkpoints.size());
            UNIT_ASSERT_VALUES_EQUAL(1, checkpointId2CommitId.size());
            UNIT_ASSERT_VALUES_EQUAL(42, checkpointId2CommitId["checkpoint"]);
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            db.DeleteCheckpoint("checkpoint", false);
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            TVector<TCheckpoint> checkpoints;
            THashMap<TString, ui64> checkpointId2CommitId;

            db.ReadCheckpoints(checkpoints, checkpointId2CommitId);

            UNIT_ASSERT_VALUES_EQUAL(0, checkpoints.size());
            UNIT_ASSERT_VALUES_EQUAL(0, checkpointId2CommitId.size());
        });
    }

    Y_UNIT_TEST(ShouldCorrectlyWriteCheckpointWithoutData)
    {
        TTestExecutor executor;

        executor.WriteTx([&] (TPartitionDatabase db) {
            db.InitSchema();
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            TCheckpoint checkpoint(
                "checkpoint",
                42,
                "",
                Now(),
                {}
            );

            db.WriteCheckpoint(checkpoint, true);
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            TVector<TCheckpoint> checkpoints;
            THashMap<TString, ui64> checkpointId2CommitId;

            db.ReadCheckpoints(checkpoints, checkpointId2CommitId);

            UNIT_ASSERT_VALUES_EQUAL(0, checkpoints.size());
            UNIT_ASSERT_VALUES_EQUAL(1, checkpointId2CommitId.size());
            UNIT_ASSERT_VALUES_EQUAL(42, checkpointId2CommitId["checkpoint"]);
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            db.DeleteCheckpoint("checkpoint", false);
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            TVector<TCheckpoint> checkpoints;
            THashMap<TString, ui64> checkpointId2CommitId;

            db.ReadCheckpoints(checkpoints, checkpointId2CommitId);

            UNIT_ASSERT_VALUES_EQUAL(0, checkpoints.size());
            UNIT_ASSERT_VALUES_EQUAL(0, checkpointId2CommitId.size());
        });
    }

    Y_UNIT_TEST(ShouldFindBlobsInBlobIndex)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TPartitionDatabase db) {
            db.InitSchema();
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            db.WriteBlobMeta(
                executor.MakeBlobId(),
                {});
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            db.WriteBlobMeta(
                executor.MakeBlobId(),
                {});
        });

        auto blobId = executor.MakeBlobId();

        executor.WriteTx([&] (TPartitionDatabase db) {
            db.WriteBlobMeta(
                blobId,
                {});
        });

        auto lastBlob = executor.MakeBlobId();
        executor.WriteTx([&] (TPartitionDatabase db) {
            db.WriteBlobMeta(
                lastBlob,
                {});
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            {
                TTestBlobVisitor visitor;
                UNIT_ASSERT_VALUES_EQUAL(
                    static_cast<ui32>(TPartitionDatabase::EBlobIndexScanProgress::Completed),
                    static_cast<ui32>(db.FindBlocksInBlobsIndex(
                        visitor,
                        MakePartialBlobId(0, 0),
                        blobId,
                        100)
                ));
                UNIT_ASSERT_VALUES_EQUAL(visitor.ReadCount, 3);
            }
            {
                TTestBlobVisitor visitor;
                UNIT_ASSERT_VALUES_EQUAL(
                    static_cast<ui32>(TPartitionDatabase::EBlobIndexScanProgress::Completed),
                    static_cast<ui32>(db.FindBlocksInBlobsIndex(
                        visitor,
                        blobId,
                        lastBlob,
                        100)
                ));
                UNIT_ASSERT_VALUES_EQUAL(visitor.ReadCount, 2);
            }
        });
    }

    Y_UNIT_TEST(ShouldUseSkipMaskInFindBlocksInBlobsIndex)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TPartitionDatabase db) {
            db.InitSchema();
        });

        TPartialBlobId blob1;
        auto range1 = TBlockRange32::MakeClosedInterval(100, 120);
        TBlockMask skipMask1;
        skipMask1.Set(8, 14);
        skipMask1.Set(21, skipMask1.Size());

        TPartialBlobId blob2;
        auto range2 = TBlockRange32::MakeClosedInterval(110, 140);
        TBlockMask skipMask2;
        skipMask2.Set(10, 20);
        skipMask2.Set(31, skipMask2.Size());

        executor.WriteTx([&] (TPartitionDatabase db) {
            NProto::TBlobMeta meta;

            auto* mb = meta.MutableMergedBlocks();
            mb->SetStart(range1.Start);
            mb->SetEnd(range1.End);
            mb->SetSkipped(5);
            blob1 = executor.MakeBlobId();
            db.WriteBlobMeta(blob1, meta);
            db.WriteMergedBlocks(blob1, range1, skipMask1);
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            NProto::TBlobMeta meta;

            auto* mb = meta.MutableMergedBlocks();
            mb->SetStart(range2.Start);
            mb->SetEnd(range2.End);
            mb->SetSkipped(10);
            meta.AddBlockChecksums(111);
            meta.AddBlockChecksums(222);
            meta.AddBlockChecksums(333);
            blob2 = executor.MakeBlobId();
            db.WriteBlobMeta(blob2, meta);
            db.WriteMergedBlocks(blob2, range2, skipMask2);
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            TTestBlockVisitorWithBlobOffset visitor;
            db.FindBlocksInBlobsIndex(
                visitor,
                MaxBlocksCount,
                blob1);

            UNIT_ASSERT_VALUES_EQUAL(
                "#100:2:0 #101:2:1 #102:2:2 #103:2:3 #104:2:4 #105:2:5 #106:2:6"
                " #107:2:7 #108:x:65535 #109:x:65535 #110:x:65535 #111:x:65535"
                " #112:x:65535 #113:x:65535 #114:2:8 #115:2:9 #116:2:10"
                " #117:2:11 #118:2:12 #119:2:13 #120:2:14",
                visitor.Result);
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            TTestBlockVisitorWithBlobOffset visitor;
            db.FindBlocksInBlobsIndex(
                visitor,
                MaxBlocksCount,
                blob2);

            UNIT_ASSERT_VALUES_EQUAL(
                "#110:3:0##111 #111:3:1##222 #112:3:2##333 #113:3:3 #114:3:4"
                " #115:3:5 #116:3:6 #117:3:7 #118:3:8 #119:3:9 #120:x:65535"
                " #121:x:65535 #122:x:65535 #123:x:65535 #124:x:65535"
                " #125:x:65535 #126:x:65535 #127:x:65535 #128:x:65535"
                " #129:x:65535 #130:3:10 #131:3:11 #132:3:12 #133:3:13 #134:3:14"
                " #135:3:15 #136:3:16 #137:3:17 #138:3:18 #139:3:19 #140:3:20",
                visitor.Result);
        });
    }

    Y_UNIT_TEST(ShouldCorrectlyWriteAndReadCompactionMap)
    {
        constexpr size_t RangeSize = 1024;
        constexpr size_t BlockSize = 4096;
        constexpr ui64 BlockCount = 1_TB / BlockSize;
        constexpr ui64 RangeCount = BlockCount / RangeSize;

        TTestExecutor executor;
        executor.WriteTx([&](TPartitionDatabase db) { db.InitSchema(); });

        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                NProto::TBlobMeta meta;

                for (size_t i = 0; i < RangeCount; ++i) {
                    db.WriteCompactionMap(
                        i * RangeSize,
                        i % 100 + 1,
                        i % 1023 + 1);
                }
            });

        // loading compaction map per one call
        TVector<TCompactionCounter> compactionMap1;
        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
                db.ReadCompactionMap(compactionMap1);
                UNIT_ASSERT_VALUES_EQUAL(RangeCount, compactionMap1.size());
            });

        for (size_t i = 0; i < RangeCount; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                i * RangeSize,
                compactionMap1[i].BlockIndex);
            UNIT_ASSERT_VALUES_EQUAL(
                i % 100 + 1,
                compactionMap1[i].Stat.BlobCount);
            UNIT_ASSERT_VALUES_EQUAL(
                i % 1023 + 1,
                compactionMap1[i].Stat.BlockCount);
        }

        // loading compaction map lazily
        TVector<TCompactionCounter> compactionMap2;
        constexpr size_t RangeCountPerRun = 500;
        for (ui32 i = 0; i < RangeCount; i += RangeCountPerRun) {
            const ui32 mapSize = compactionMap2.size();
            executor.ReadTx(
                [&](TPartitionDatabase db)
                {
                    db.ReadCompactionMap(
                        TBlockRange32::WithLength(
                            i * RangeSize,
                            RangeCountPerRun * RangeSize),
                        compactionMap2);
                });
            UNIT_ASSERT_VALUES_EQUAL(
                Min(RangeCountPerRun, RangeCount - i),
                compactionMap2.size() - mapSize);
        }

        UNIT_ASSERT_VALUES_EQUAL(compactionMap1.size(), compactionMap2.size());
        for (ui32 i = 0; i < RangeCount; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                compactionMap1[i].BlockIndex,
                compactionMap2[i].BlockIndex);
            UNIT_ASSERT_VALUES_EQUAL(
                compactionMap1[i].Stat.BlockCount,
                compactionMap2[i].Stat.BlockCount);
            UNIT_ASSERT_VALUES_EQUAL(
                compactionMap1[i].Stat.BlobCount,
                compactionMap2[i].Stat.BlobCount);
        }
    }

    Y_UNIT_TEST(ShouldFindRangesForMergedBlobs)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TPartitionDatabase db) { db.InitSchema(); });

        TVector<TPartialBlobId> blobIds;

        TVector<TBlockRange32> ranges = {
            TBlockRange32::WithLength(0, 3),
            TBlockRange32::MakeClosedInterval(4, 6),
            TBlockRange32::MakeClosedInterval(2, 4),
            TBlockRange32::WithLength(0, 7)};

        for (size_t i = 0; i < ranges.size(); ++i) {
            executor.WriteTx(
                [&](TPartitionDatabase db)
                {
                    blobIds.push_back(executor.MakeBlobId());
                    db.WriteMergedBlocks(
                        blobIds.back(),
                        ranges[i],
                        TBlockMask());
                });
        }

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
                {
                    TTestBlockVisitor visitor;
                    UNIT_ASSERT(db.FindMergedBlocks(
                        visitor,
                        visitor,
                        TBlockRange32::WithLength(0, 2),
                        true,   // precharge
                        MaxBlocksCount));

                    THashMap<TPartialBlobId, TBlockRange32, TPartialBlobIdHash>
                        expectedContent{
                            {blobIds[0], ranges[0]},
                            {blobIds[3], ranges[3]},
                        };

                    ASSERT_MAP_EQUAL(expectedContent, visitor.BlobToRange);
                }

                {
                    TTestBlockVisitor visitor;
                    UNIT_ASSERT(db.FindMergedBlocks(
                        visitor,
                        visitor,
                        TBlockRange32::WithLength(0, 2),
                        true,   // precharge
                        MaxBlocksCount,
                        blobIds[2].CommitId()));

                    THashMap<TPartialBlobId, TBlockRange32, TPartialBlobIdHash>
                        expectedContent{
                            {blobIds[0], ranges[0]},
                        };

                    ASSERT_MAP_EQUAL(expectedContent, visitor.BlobToRange);
                }

                {
                    TTestBlockVisitor visitor;
                    UNIT_ASSERT(db.FindMergedBlocks(
                        visitor,
                        visitor,
                        TBlockRange32::MakeClosedInterval(5, 6),
                        true,   // precharge
                        MaxBlocksCount));

                    THashMap<TPartialBlobId, TBlockRange32, TPartialBlobIdHash>
                        expectedContent{
                            {blobIds[1], ranges[1]},
                            {blobIds[3], ranges[3]},
                        };

                    ASSERT_MAP_EQUAL(expectedContent, visitor.BlobToRange);
                }
            });
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
