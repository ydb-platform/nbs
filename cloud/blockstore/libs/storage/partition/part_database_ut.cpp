#include "part_database.h"

#include "model/merged_blob_compression.h"
#include "part_schema.h"

#include <cloud/blockstore/libs/storage/testlib/test_executor.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

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
        const TBlockMask& skipMask) override
    {
        Y_UNUSED(skipMask);
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

    Y_UNIT_TEST(ShouldKeepMalformedUnconfirmedMetadataDistinctFromLegacyRaw)
    {
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });
        const auto range = TBlockRange32::WithLength(0, 8);
        TPartialBlobId legacyId;
        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                legacyId = executor.MakeBlobId(range.Size());
                db.WriteUnconfirmedBlob(
                    legacyId, TBlobToConfirm(legacyId.UniqueId(), range, {}));
            });
        for (const TString& damaged:
             {TString("\x12\x80", 2), TString("\xff", 1), TString("\0", 1)})
        {
            NProto::TBlobMeta metadata;
            UNIT_ASSERT(!metadata.ParseFromString(damaged));
            TPartialBlobId id;
            executor.WriteTx(
                [&](TPartitionDatabase db)
                {
                    id = executor.MakeBlobId(range.Size());
                    db.WriteUnconfirmedBlob(
                        id, TBlobToConfirm(id.UniqueId(), range, {}));
                    using TTable = TPartitionSchema::UnconfirmedBlobs;
                    db.Table<TTable>()
                        .Key(id.CommitId(), id.UniqueId())
                        .Update(NKikimr::NIceDb::TUpdate<TTable::Metadata>(
                            damaged));
                });
            // A fresh database wrapper/transaction must reconstruct presence
            // of the invalid descriptor from bytes, not retain an in-memory
            // one.
            executor.ReadTx(
                [&](TPartitionDatabase db)
                {
                    TCommitIdToBlobsToConfirm blobs;
                    UNIT_ASSERT(db.ReadUnconfirmedBlobs(blobs));
                    UNIT_ASSERT(
                        !blobs.at(legacyId.CommitId()).at(0).Compression);
                    const auto& restored = blobs.at(id.CommitId()).at(0);
                    UNIT_ASSERT_VALUES_EQUAL(restored.UniqueId, id.UniqueId());
                    UNIT_ASSERT_VALUES_EQUAL(restored.BlockRange, range);
                    UNIT_ASSERT(restored.Compression);
                    UNIT_ASSERT(restored.Checksums.empty());
                    UNIT_ASSERT(HasError(ValidateMergedBlobCompression(
                        *restored.Compression,
                        id.BlobSize(), DefaultBlockSize)));
                });
        }
    }

    Y_UNIT_TEST(ShouldPersistCompressedMetadataAndRejectCopyMismatch)
    {
        struct TVisitor: IBlocksIndexVisitor {
            TMergedBlobFormat Format;
            TVector<ui16> Offsets;
            bool Visit(ui32, ui64, const TPartialBlobId&, ui16) override { return true; }
            bool VisitMerged(ui32, ui64, const TPartialBlobId&, ui16 offset,
                const TMergedBlobFormat& format) override
            {
                Format = format;
                Offsets.push_back(offset);
                return true;
            }
        };
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });
        NProto::TBlobMeta meta;
        meta.MutableMergedBlocks()->SetStart(0);
        meta.MutableMergedBlocks()->SetEnd(4);
        meta.MutableMergedBlocks()->SetSkipped(2);
        TBlockMask skipped;
        skipped.Set(1);
        skipped.Set(3);
        TCompressedMergedBlob compressed;
        UNIT_ASSERT(!HasError(CompressMergedBlob(
            TString(3 * DefaultBlockSize, 'x'), DefaultBlockSize, 10, meta, compressed)));
        UNIT_ASSERT(!compressed.Payload.empty());
        *meta.MutableCompression() = compressed.Compression;
        TPartialBlobId id;
        executor.WriteTx([&](TPartitionDatabase db) {
            id = TPartialBlobId(0, executor.Step, 3, compressed.Payload.size(), 0, 0);
            db.WriteMergedBlocks(id, TBlockRange32::WithLength(0, 5), skipped,
                &compressed.Compression);
            db.WriteBlobMeta(id, meta);
            db.WriteUnconfirmedBlob(id, TBlobToConfirm(id.UniqueId(),
                TBlockRange32::WithLength(0, 3), {11, 22, 33},
                std::make_shared<NProto::TBlobCompression>(compressed.Compression)));
            db.WriteCleanupQueue(id, id.CommitId() + 1, 3);
        });
        executor.ReadTx([&](TPartitionDatabase db) {
            TVisitor visitor;
            UNIT_ASSERT(db.FindMergedBlocks(visitor, TVector<ui32>{0, 4}, MaxBlocksCount));
            UNIT_ASSERT_VALUES_EQUAL(visitor.Offsets.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(visitor.Offsets[0], 0);
            UNIT_ASSERT_VALUES_EQUAL(visitor.Offsets[1], 2);
            UNIT_ASSERT(!visitor.Format.Invalid);
            UNIT_ASSERT(visitor.Format.Compression);
            UNIT_ASSERT_VALUES_EQUAL(visitor.Format.LogicalBlocks, 3);
            UNIT_ASSERT_VALUES_EQUAL(visitor.Format.Compression->SerializeAsString(),
                compressed.Compression.SerializeAsString());
            TCommitIdToBlobsToConfirm unconfirmed;
            UNIT_ASSERT(db.ReadUnconfirmedBlobs(unconfirmed));
            const auto& restored = unconfirmed.at(id.CommitId()).at(0);
            UNIT_ASSERT_VALUES_EQUAL(restored.UniqueId, id.UniqueId());
            UNIT_ASSERT_VALUES_EQUAL(restored.Checksums.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(restored.Checksums[2], 33);
            UNIT_ASSERT(restored.Compression);
            UNIT_ASSERT_VALUES_EQUAL(restored.Compression->SerializeAsString(),
                compressed.Compression.SerializeAsString());
            TVector<TCleanupQueueItem> items;
            UNIT_ASSERT(db.ReadCleanupQueue(items));
            UNIT_ASSERT_VALUES_EQUAL(items.size(), 1);
            TCleanupQueue queue(DefaultBlockSize);
            UNIT_ASSERT(queue.Add(items));
            UNIT_ASSERT_VALUES_EQUAL(queue.GetQueueBlocks(), 3);
            UNIT_ASSERT_VALUES_EQUAL(queue.GetQueueBytes(), compressed.Payload.size());
            UNIT_ASSERT(queue.Remove({id, id.CommitId() + 1, {}, 0}));
            UNIT_ASSERT_VALUES_EQUAL(queue.GetQueueBlocks(), 0);
        });
        executor.WriteTx([&](TPartitionDatabase db) {
            meta.MutableCompression()->SetCodec(99);
            db.WriteBlobMeta(id, meta);
        });
        executor.ReadTx([&](TPartitionDatabase db) {
            TVisitor visitor;
            UNIT_ASSERT(db.FindMergedBlocks(visitor, TVector<ui32>{0, 4}, MaxBlocksCount));
            UNIT_ASSERT(visitor.Format.Invalid);
        });
        executor.WriteTx([&](TPartitionDatabase db) {
            NProto::TBlobCompression empty;
            *meta.MutableCompression() = empty;
            db.WriteBlobMeta(id, meta);
            db.WriteMergedBlocks(id, TBlockRange32::WithLength(0, 5), skipped, &empty);
        });
        executor.ReadTx([&](TPartitionDatabase db) {
            TVisitor visitor;
            UNIT_ASSERT(db.FindMergedBlocks(visitor, TVector<ui32>{0}, MaxBlocksCount));
            UNIT_ASSERT(visitor.Format.Compression);
            UNIT_ASSERT(HasError(ValidateMergedBlobCompression(
                *visitor.Format.Compression, id.BlobSize(), DefaultBlockSize)));
        });
    }

    Y_UNIT_TEST(ShouldRejectMalformedDurableBlobMetadata)
    {
        struct TVisitor final: IBlocksIndexVisitor
        {
            ui32 Count = 0;

            bool Visit(ui32, ui64, const TPartialBlobId&, ui16) override
            {
                UNIT_FAIL("Invalid metadata must retain merged format");
                return false;
            }

            bool VisitMerged(
                ui32,
                ui64,
                const TPartialBlobId&,
                ui16, const TMergedBlobFormat& format) override
            {
                UNIT_ASSERT(format.Invalid);
                UNIT_ASSERT(format.Compression);
                ++Count;
                return true;
            }
        };

        struct TBlobVisitor final: IBlobsIndexVisitor
        {
            ui32 Count = 0;

            bool Visit(
                ui64, ui64, const NProto::TBlobMeta& meta, TStringBuf) override
            {
                UNIT_ASSERT(meta.HasCompression());
                UNIT_ASSERT(!meta.HasMergedBlocks());
                UNIT_ASSERT(!meta.HasMixedBlocks());
                ++Count;
                return true;
            }
        };

        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) { db.InitSchema(); });
        const auto range = TBlockRange32::WithLength(0, 8);
        NProto::TBlobMeta meta;
        meta.MutableMergedBlocks()->SetStart(range.Start);
        meta.MutableMergedBlocks()->SetEnd(range.End);
        TCompressedMergedBlob compressed;
        UNIT_ASSERT(!HasError(CompressMergedBlob(
            TString(range.Size() * DefaultBlockSize, 'x'),
            DefaultBlockSize, 10, meta, compressed)));
        UNIT_ASSERT(!compressed.Payload.empty());
        *meta.MutableCompression() = compressed.Compression;
        TString truncated = meta.SerializeAsString();
        truncated.pop_back();
        NProto::TBlobMeta parsed;
        UNIT_ASSERT(!parsed.ParseFromString(truncated));
        for (const TString& damaged:
             {truncated, TString("\xff", 1), TString("\0", 1), TString{}})
        {
            TPartialBlobId id;
            executor.WriteTx(
                [&](TPartitionDatabase db)
                {
                    id = TPartialBlobId(
                        0, executor.Step, 3, compressed.Payload.size(), 0, 0);
                    db.WriteMergedBlocks(
                        id,
                        range,
                        {}, &compressed.Compression);
                    db.WriteBlobMeta(id, meta);
                    using TTable = TPartitionSchema::BlobsIndex;
                    db.Table<TTable>()
                        .Key(id.CommitId(), id.UniqueId())
                        .Update(NKikimr::NIceDb::TUpdate<TTable::BlobMeta>(
                            damaged));
                });
            executor.ReadTx(
                [&](TPartitionDatabase db)
                {
                    TMaybe<NProto::TBlobMeta> restored;
                    UNIT_ASSERT(db.ReadBlobMeta(id, restored));
                    UNIT_ASSERT(restored);
                    UNIT_ASSERT(restored->HasCompression());
                    UNIT_ASSERT(!restored->HasMergedBlocks());
                    UNIT_ASSERT(!restored->HasMixedBlocks());
                    UNIT_ASSERT_VALUES_EQUAL(restored->BlockChecksumsSize(), 0);
                    UNIT_ASSERT(HasError(ValidateMergedBlobCompression(
                        restored->GetCompression(),
                        id.BlobSize(), DefaultBlockSize)));
                    TMaybe<TBlockMask> mask;
                    restored.Clear();
                    UNIT_ASSERT(db.ReadBlobInfo(id, mask, restored));
                    UNIT_ASSERT(restored && restored->HasCompression());
                    TVisitor visitor;
                    UNIT_ASSERT(db.FindMergedBlocks(
                        visitor,
                        TVector<ui32>{0, 7}, MaxBlocksCount));
                    UNIT_ASSERT(visitor.Count >= 2);
                    TTestBlockVisitorWithBlobOffset blocks;
                    UNIT_ASSERT(
                        db.FindBlocksInBlobsIndex(blocks, MaxBlocksCount, id));
                    UNIT_ASSERT(blocks.Result.empty());
                    UNIT_ASSERT(db.FindBlocksInBlobsIndex(
                        blocks, MaxBlocksCount, range));
                    UNIT_ASSERT(blocks.Result.empty());
                    TBlobVisitor blobs;
                    db.FindBlocksInBlobsIndex(blobs, id, id, 1);
                    UNIT_ASSERT_VALUES_EQUAL(blobs.Count, 1);
                });
        }
    }

    Y_UNIT_TEST(ShouldCheckBothMergedCompressionCopies)
    {
        for (bool indexCompression: {false, true}) {
            for (bool metaCompression: {false, true}) {
                struct TVisitor final: IBlocksIndexVisitor
                {
                    TMergedBlobFormat Format;
                    TVector<ui16> Offsets;

                    bool Visit(ui32, ui64, const TPartialBlobId&, ui16) override
                    {
                        return true;
                    }

                    bool VisitMerged(ui32, ui64, const TPartialBlobId&,
                                     ui16 offset,
                                     const TMergedBlobFormat& format) override
                    {
                        Format = format;
                        Offsets.push_back(offset);
                        return true;
                    }
                };

                TTestExecutor executor;
                executor.WriteTx([](TPartitionDatabase db)
                                 { db.InitSchema(); });
                const auto range = TBlockRange32::WithLength(0, 5);
                TBlockMask skipped;
                skipped.Set(1);
                skipped.Set(3);
                NProto::TBlobMeta meta;
                meta.MutableMergedBlocks()->SetStart(range.Start);
                meta.MutableMergedBlocks()->SetEnd(range.End);
                meta.MutableMergedBlocks()->SetSkipped(skipped.Count());
                TCompressedMergedBlob compressed;
                UNIT_ASSERT(!HasError(CompressMergedBlob(
                    TString(3 * DefaultBlockSize, 'x'), DefaultBlockSize, 10,
                    meta, compressed)));
                UNIT_ASSERT(!compressed.Payload.empty());
                if (metaCompression) {
                    *meta.MutableCompression() = compressed.Compression;
                }

                executor.WriteTx(
                    [&](TPartitionDatabase db)
                    {
                        const TPartialBlobId id(
                            0,
                            executor.Step,
                            3,
                            indexCompression && metaCompression
                                ? compressed.Payload.size()
                                : 3 * DefaultBlockSize, 0, 0);
                        db.WriteMergedBlocks(
                            id,
                            range,
                            skipped,
                            indexCompression ? &compressed.Compression
                                             : nullptr);
                        db.WriteBlobMeta(id, meta);
                    });

                executor.ReadTx(
                    [&](TPartitionDatabase db)
                    {
                        TVisitor visitor;
                        UNIT_ASSERT(db.FindMergedBlocks(
                            visitor,
                            range,
                            true,   // precharge
                            MaxBlocksCount, Max<ui64>(),
                            true));   // verifyRawBlobMeta
                        UNIT_ASSERT_VALUES_EQUAL(visitor.Offsets.size(), 3);
                        for (ui16 i = 0; i < visitor.Offsets.size(); ++i) {
                            UNIT_ASSERT_VALUES_EQUAL(visitor.Offsets[i], i);
                        }
                        UNIT_ASSERT_VALUES_EQUAL(visitor.Format.LogicalBlocks,
                                                 3);
                        UNIT_ASSERT_VALUES_EQUAL(
                            bool(visitor.Format.Compression), indexCompression);
                        UNIT_ASSERT_VALUES_EQUAL(
                            visitor.Format.Invalid,
                            indexCompression != metaCompression);
                    });
            }
        }
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
                using TTable = TPartitionSchema::CompactionMap;

                // Simulate a row written before AdditionalData was added.
                db.Table<TTable>()
                    .Key(0)
                    .Update(NKikimr::NIceDb::TUpdate<TTable::BlobCount>(1))
                    .Update(NKikimr::NIceDb::TUpdate<TTable::BlockCount>(1));

                for (size_t i = 1; i < RangeCount; ++i) {
                    db.WriteCompactionMap(
                        i * RangeSize,
                        i % 100 + 1,
                        i % 1023 + 1,
                        i % 511 + 1);
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
            UNIT_ASSERT_VALUES_EQUAL(
                i ? i % 511 + 1 : 0,
                compactionMap1[i].Stat.MixedBlockCount);
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
            UNIT_ASSERT_VALUES_EQUAL(
                compactionMap1[i].Stat.MixedBlockCount,
                compactionMap2[i].Stat.MixedBlockCount);
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

}   // namespace NCloud::NBlockStore::NStorage::NPartition
