#include "part_database.h"

#include <cloud/blockstore/libs/storage/testlib/test_executor.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString GetBlockContent(char fill)
{
    return TString(DefaultBlockSize, fill);
}

////////////////////////////////////////////////////////////////////////////////

struct TTestBlockVisitor final
    : public IBlocksIndexVisitor
{
    TStringBuilder Result;

    void MarkBlock(ui32 blockIndex, ui64 commitId)
    {
        if (Result) {
            Result << " ";
        }
        Result << "#" << blockIndex << ":" << commitId;
    }

    bool Visit(
        ui32 blockIndex,
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui16 blobOffset) override
    {
        Y_UNUSED(blobId);
        Y_UNUSED(blobOffset);

        MarkBlock(blockIndex, commitId);
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

Y_UNIT_TEST_SUITE(TPartitionDatabaseTest)
{
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
            db.WriteFreshBlock(0, commitId, {zero.Data(), zero.Size()});
            db.WriteFreshBlock(1, commitId, {one.Data(), one.Size()});
            db.WriteFreshBlock(2, commitId, {two.Data(), two.Size()});
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            ui64 commitId = executor.CommitId();
            auto one = GetBlockContent(1);
            db.WriteFreshBlock(1, commitId, {one.Data(), one.Size()});
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

        executor.WriteTx([&] (TPartitionDatabase db) {
            db.WriteMixedBlocks(
                executor.MakeBlobId(),
                {0, 1, 2});
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            db.WriteMixedBlocks(
                executor.MakeBlobId(),
                {4, 5, 6});
        });

        ui64 maxCommitId = executor.WriteTx([&] (TPartitionDatabase db) {
            db.WriteMixedBlocks(
                executor.MakeBlobId(),
                {2, 3, 4});
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            db.WriteMixedBlocks(
                executor.MakeBlobId(),
                {0, 1, 2, 3, 4, 5, 6});
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMixedBlocks(
                    visitor,
                    TBlockRange32(0, 1),
                    true,   // precharge
                    maxCommitId
                ));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#0:2 #1:2");
            }
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMixedBlocks(
                    visitor,
                    TBlockRange32(5, 6),
                    true,   // precharge
                    maxCommitId
                ));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#5:3 #6:3");
            }
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMixedBlocks(
                    visitor,
                    TBlockRange32(2, 3),
                    true,   // precharge
                    maxCommitId
                ));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#2:4 #2:2 #3:4");
            }
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMixedBlocks(
                    visitor,
                    TBlockRange32(3, 4),
                    true,   // precharge
                    maxCommitId
                ));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#3:4 #4:4 #4:3");
            }
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMixedBlocks(
                    visitor,
                    TBlockRange32(1, 5),
                    true,   // precharge
                    maxCommitId
                ));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#1:2 #2:4 #2:2 #3:4 #4:4 #4:3 #5:3");
            }
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMixedBlocks(
                    visitor,
                    TBlockRange32(1, 5),
                    true    // precharge
                ));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#1:5 #1:2 #2:5 #2:4 #2:2 #3:5 #3:4 #4:5 #4:4 #4:3 #5:5 #5:3");
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
                {0, 1, 2});
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            db.WriteMixedBlocks(
                executor.MakeBlobId(),
                {4, 5, 6});
        });

        ui64 maxCommitId = executor.WriteTx([&] (TPartitionDatabase db) {
            db.WriteMixedBlocks(
                executor.MakeBlobId(),
                {2, 3, 4});
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            db.WriteMixedBlocks(
                executor.MakeBlobId(),
                {0, 1, 2, 3, 4, 5, 6});
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMixedBlocks(visitor, TVector<ui32>{0, 1}, maxCommitId));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#0:2 #1:2");
            }
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMixedBlocks(visitor, TVector<ui32>{5, 6}, maxCommitId));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#5:3 #6:3");
            }
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMixedBlocks(visitor, TVector<ui32>{2, 3}, maxCommitId));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#2:4 #2:2 #3:4");
            }
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMixedBlocks(visitor, TVector<ui32>{3, 4}, maxCommitId));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#3:4 #4:4 #4:3");
            }
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMixedBlocks(visitor, TVector<ui32>{1, 3, 5}, maxCommitId));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#1:2 #3:4 #5:3");
            }
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMixedBlocks(visitor, TVector<ui32>{1, 3, 5}));
                UNIT_ASSERT_VALUES_EQUAL(visitor.Result, "#1:5 #1:2 #3:5 #3:4 #5:5 #5:3");
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
                TBlockRange32(0, 2),
                TBlockMask()
            );
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            db.WriteMergedBlocks(
                executor.MakeBlobId(),
                TBlockRange32(4, 6),
                TBlockMask()
            );
        });

        ui64 maxCommitId = executor.WriteTx([&] (TPartitionDatabase db) {
            db.WriteMergedBlocks(
                executor.MakeBlobId(),
                TBlockRange32(2, 4),
                TBlockMask()
            );
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            db.WriteMergedBlocks(
                executor.MakeBlobId(),
                TBlockRange32(0, 6),
                TBlockMask()
            );
        });

        executor.ReadTx([&] (TPartitionDatabase db) {
            {
                TTestBlockVisitor visitor;
                UNIT_ASSERT(db.FindMergedBlocks(
                    visitor,
                    TBlockRange32(0, 1),
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
                    TBlockRange32(5, 6),
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
                    TBlockRange32(2, 3),
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
                    TBlockRange32(3, 4),
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
                    TBlockRange32(1, 5),
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
                    TBlockRange32(1, 5),
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
                TBlockRange32(0, 2),
                TBlockMask()
            );
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            db.WriteMergedBlocks(
                executor.MakeBlobId(),
                TBlockRange32(4, 6),
                TBlockMask()
            );
        });

        ui64 maxCommitId = executor.WriteTx([&] (TPartitionDatabase db) {
            db.WriteMergedBlocks(
                executor.MakeBlobId(),
                TBlockRange32(2, 4),
                TBlockMask()
            );
        });

        executor.WriteTx([&] (TPartitionDatabase db) {
            db.WriteMergedBlocks(
                executor.MakeBlobId(),
                TBlockRange32(0, 6),
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

            db.WriteCheckpoint(checkpoint);
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

            db.WriteCheckpoint(checkpoint);
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
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
