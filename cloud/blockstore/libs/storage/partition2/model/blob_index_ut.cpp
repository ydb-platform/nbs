#include "blob_index.h"

#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>   // XXX

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

bool operator==(const TBlockAndLocation& l, const TBlockAndLocation& r)
{
    return l.Block == r.Block && l.Location.BlobId == r.Location.BlobId &&
           l.Location.BlobOffset == r.Location.BlobOffset;
}

bool operator<(const TBlockAndLocation& l, const TBlockAndLocation& r)
{
    return l.Location.BlobId == r.Location.BlobId
               ? l.Location.BlobOffset < r.Location.BlobOffset
               : l.Location.BlobId < r.Location.BlobId;
}

bool operator<(const TBlobUpdate& l, const TBlobUpdate& r)
{
    return l.CommitId == r.CommitId ? l.BlockRange.Start < r.BlockRange.Start
                                    : l.CommitId < r.CommitId;
}

namespace {

////////////////////////////////////////////////////////////////////////////////

void AddBlob(
    TBlobIndex& index,
    const TPartialBlobId& blobId,
    const TBlockRange32& blockRange,
    ui16 checkpointBlockCount = 0)
{
    // TODO: check return value
    index
        .AddBlob(blobId, {blockRange}, blockRange.Size(), checkpointBlockCount);
}

void AddBlockList(
    TBlobIndex& index,
    const TPartialBlobId& blobId,
    const TBlockRange32& blockRange)
{
    TBlockListBuilder b;
    for (ui32 i = blockRange.Start; i <= blockRange.End; ++i) {
        b.AddBlock(i - blockRange.Start, i, blobId.CommitId(), false);
    }

    index.AddBlockList(0, blobId, b.Finish(), blockRange.Size());
}

auto MixedBlocks(
    ui64 commitId,
    ui64 maxCommitId,
    const TPartialBlobId& blobId,
    const TBlockRange32& blockRange,
    ui16 offset = 0)
{
    TVector<TBlockAndLocation> v;

    for (ui32 i = blockRange.Start; i <= blockRange.End; ++i) {
        v.push_back(
            {{i, commitId, maxCommitId, false},
             {blobId, static_cast<ui16>(i - blockRange.Start + offset)}});
    }

    return v;
}

template <typename T>
auto JoinVecs(const TVector<TVector<T>>& vecs)
{
    TVector<T> v;
    for (const auto& vv: vecs) {
        for (const auto& x: vv) {
            v.push_back(x);
        }
    }

    return v;
}

void AddMixedBlocks(
    TBlobIndex& index,
    const TPartialBlobId& blobId,
    const TBlockRange32& blockRange)
{
    const auto blocks =
        MixedBlocks(blobId.CommitId(), InvalidCommitId, blobId, blockRange);
    for (const auto& b: blocks) {
        index.WriteOrUpdateMixedBlock(b);
    }
}

TVector<TPartialBlobId> FindBlobs(
    const TBlobIndex& index,
    const TBlockRange32& blockRange)
{
    TVector<TPartialBlobId> result;
    for (const auto* blob: index.FindBlobs(blockRange)) {
        result.push_back(blob->BlobId);
    }

    Sort(result);
    return result;
}

TVector<TDeletedBlock> DeletedBlocks(
    const TVector<TBlockRange32>& ranges,
    const TVector<ui64>& commits)
{
    TVector<TDeletedBlock> result;
    for (ui32 i = 0; i < ranges.size(); ++i) {
        const auto& range = ranges[i];

        for (ui32 b = range.Start; b <= range.End; ++b) {
            result.emplace_back(b, commits[i]);
        }
    }

    Sort(result);

    return result;
}

////////////////////////////////////////////////////////////////////////////////

const ui32 ZoneCount = 6;
const ui32 ZoneBlockCount = 50;
const ui32 BlockListCacheSize = 1000;
const ui32 MaxBlocksInBlob = 1024;
const EOptimizationMode DeletedRangesMapMode =
    EOptimizationMode::OptimizeForShortRanges;

void InitAllZones(TBlobIndex& index)
{
    for (ui32 z = 0; z < ZoneCount; ++z) {
        index.InitializeZone(z);
    }
    index.FinishGlobalDataInitialization();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlobIndexTest)
{
    static const TPartialBlobId blobId1 = {1, 0};
    static const TPartialBlobId blobId2 = {2, 0};
    static const TPartialBlobId blobId3 = {3, 0};
    static const TPartialBlobId blobId4 = {4, 0};
    static const TPartialBlobId blobId5 = {5, 0};

    Y_UNIT_TEST(ShouldStoreBlobs)
    {
        TBlobIndex index(
            ZoneCount,
            ZoneBlockCount,
            BlockListCacheSize,
            MaxBlocksInBlob,
            DeletedRangesMapMode);
        InitAllZones(index);

        AddBlob(index, blobId1, TBlockRange32::WithLength(0, 100), 1);
        AddBlob(index, blobId2, TBlockRange32::WithLength(0, 50), 2);
        AddBlob(index, blobId3, TBlockRange32::WithLength(40, 20), 3);
        AddBlob(index, blobId4, TBlockRange32::WithLength(50, 50), 4);

        index.AddBlob(
            blobId5,
            {
                TBlockRange32::WithLength(10, 20),
                TBlockRange32::WithLength(45, 10),
            },
            30,
            0);

        {
            auto blob1 = index.FindBlob(0, blobId1);
            UNIT_ASSERT(blob1.Blob);
            UNIT_ASSERT_VALUES_EQUAL(1, blob1.Blob->CheckpointBlockCount);
            UNIT_ASSERT_VALUES_EQUAL(GlobalZoneId, blob1.ZoneId);

            auto blob2 = index.FindBlob(0, blobId2);
            UNIT_ASSERT(blob2.Blob);
            UNIT_ASSERT_VALUES_EQUAL(2, blob2.Blob->CheckpointBlockCount);
            UNIT_ASSERT_VALUES_EQUAL(0, blob2.ZoneId);

            auto blob3 = index.FindBlob(0, blobId3);
            UNIT_ASSERT(blob3.Blob);
            UNIT_ASSERT_VALUES_EQUAL(3, blob3.Blob->CheckpointBlockCount);
            UNIT_ASSERT_VALUES_EQUAL(GlobalZoneId, blob3.ZoneId);

            auto blob4 = index.FindBlob(0, blobId4);
            UNIT_ASSERT(blob4.Blob);
            UNIT_ASSERT_VALUES_EQUAL(4, blob4.Blob->CheckpointBlockCount);
            UNIT_ASSERT_VALUES_EQUAL(1, blob4.ZoneId);

            UNIT_ASSERT(!index.FindBlob(0, {111, 0}).Blob);
        }

        {
            auto result = FindBlobs(index, TBlockRange32::WithLength(0, 10));
            ASSERT_VECTORS_EQUAL(
                result,
                TVector<TPartialBlobId>({
                    blobId1,
                    blobId2,
                }));
        }

        {
            auto result =
                FindBlobs(index, TBlockRange32::MakeClosedInterval(10, 20));
            ASSERT_VECTORS_EQUAL(
                result,
                TVector<TPartialBlobId>({
                    blobId1,
                    blobId2,
                    blobId5,
                }));
        }

        {
            auto result =
                FindBlobs(index, TBlockRange32::MakeClosedInterval(45, 55));
            ASSERT_VECTORS_EQUAL(
                result,
                TVector<TPartialBlobId>({
                    blobId1,
                    blobId2,
                    blobId3,
                    blobId4,
                    blobId5,
                }));
        }

        {
            auto result = FindBlobs(index, TBlockRange32::WithLength(90, 10));
            ASSERT_VECTORS_EQUAL(
                result,
                TVector<TPartialBlobId>({
                    blobId1,
                    blobId4,
                }));
        }
    }

    Y_UNIT_TEST(ShouldStoreGarbage)
    {
        TBlobIndex index(
            ZoneCount,
            ZoneBlockCount,
            BlockListCacheSize,
            MaxBlocksInBlob,
            DeletedRangesMapMode);
        InitAllZones(index);

        AddBlob(index, blobId1, TBlockRange32::WithLength(0, 100));
        AddBlob(index, blobId2, TBlockRange32::WithLength(0, 50));
        AddBlob(index, blobId3, TBlockRange32::WithLength(40, 20));
        AddBlob(index, blobId4, TBlockRange32::WithLength(10, 40));

        index.AddGarbage(0, blobId1, 10);
        index.AddGarbage(0, blobId2, 40);
        index.AddGarbage(0, blobId3, 30);
        index.AddGarbage(0, blobId4, 20);

        UNIT_ASSERT_VALUES_EQUAL(index.FindGarbage(0, blobId1), 10);
        UNIT_ASSERT_VALUES_EQUAL(index.FindGarbage(0, blobId2), 40);
        UNIT_ASSERT_VALUES_EQUAL(index.FindGarbage(0, blobId3), 30);
        UNIT_ASSERT_VALUES_EQUAL(index.FindGarbage(0, blobId4), 20);

        {
            auto garbage = index.GetTopGarbage(Max<size_t>(), Max<size_t>());
            auto result = SelectFirst(garbage.BlobCounters);
            ASSERT_VECTORS_EQUAL(
                result,
                TVector<TPartialBlobId>({
                    blobId2,
                    blobId4,
                }));
            UNIT_ASSERT_VALUES_EQUAL(0, garbage.ZoneId);
        }

        index.RemoveBlob(0, blobId4);
        UNIT_ASSERT_VALUES_EQUAL(index.FindGarbage(0, blobId4), 0);

        {
            auto garbage = index.GetTopGarbage(Max<size_t>(), Max<size_t>());
            auto result = SelectFirst(garbage.BlobCounters);
            ASSERT_VECTORS_EQUAL(
                result,
                TVector<TPartialBlobId>({
                    blobId3,
                    blobId1,
                }));
            UNIT_ASSERT_VALUES_EQUAL(GlobalZoneId, garbage.ZoneId);
        }

        index.AddGarbage(0, blobId1, 100);

        UNIT_ASSERT_VALUES_EQUAL(index.FindGarbage(0, blobId1), 100);

        {
            auto garbage = index.GetTopGarbage(Max<size_t>(), Max<size_t>());
            auto result = SelectFirst(garbage.BlobCounters);
            ASSERT_VECTORS_EQUAL(
                result,
                TVector<TPartialBlobId>({
                    blobId1,
                    blobId3,
                }));
            UNIT_ASSERT_VALUES_EQUAL(GlobalZoneId, garbage.ZoneId);
        }
    }

    Y_UNIT_TEST(ShouldMarkAndFindDeletedBlocks)
    {
        TBlobIndex index(
            ZoneCount,
            ZoneBlockCount,
            BlockListCacheSize,
            MaxBlocksInBlob,
            DeletedRangesMapMode);
        InitAllZones(index);

        AddBlob(index, blobId1, TBlockRange32::WithLength(0, 100));
        AddBlob(index, blobId2, TBlockRange32::WithLength(100, 100));

        index.MarkBlocksDeleted(
            {TBlockRange32::MakeClosedInterval(50, 250), 3, 1});
        index.OnCheckpoint(1);
        index.MarkBlocksDeleted(
            {TBlockRange32::MakeClosedInterval(25, 70), 4, 2});

        UNIT_ASSERT_VALUES_EQUAL(247, index.GetPendingUpdates());

        ASSERT_VECTORS_EQUAL(
            index.FindDeletedBlocks(TBlockRange32::WithLength(0, 200), 2),
            DeletedBlocks({}, {}));

        ASSERT_VECTORS_EQUAL(
            index.FindDeletedBlocks(TBlockRange32::WithLength(0, 200), 3),
            DeletedBlocks({TBlockRange32::WithLength(50, 150)}, {3}));

        ASSERT_VECTORS_EQUAL(
            index.FindDeletedBlocks(TBlockRange32::WithLength(0, 200), 4),
            DeletedBlocks(
                {TBlockRange32::MakeClosedInterval(25, 70),
                 TBlockRange32::WithLength(50, 150)},
                {4, 3}));

        ASSERT_VECTORS_EQUAL(
            index.FindDeletedBlocks(TBlockRange32::WithLength(0, 200)),
            DeletedBlocks(
                {TBlockRange32::MakeClosedInterval(25, 70),
                 TBlockRange32::WithLength(50, 150)},
                {4, 3}));
    }

    Y_UNIT_TEST(ShouldFindDirtyBlobs)
    {
        TBlobIndex index(
            ZoneCount,
            ZoneBlockCount,
            BlockListCacheSize,
            MaxBlocksInBlob,
            DeletedRangesMapMode);
        InitAllZones(index);

        AddBlob(index, blobId1, TBlockRange32::WithLength(0, 100));
        AddBlob(index, blobId2, TBlockRange32::WithLength(100, 100));
        AddBlob(index, blobId3, TBlockRange32::WithLength(50, 100));

        index.MarkBlocksDeleted({TBlockRange32::WithLength(20, 30), 4, 1});
        index.MarkBlocksDeleted({TBlockRange32::WithLength(60, 30), 5, 2});

        index.OnCheckpoint(6);
        index.MarkBlocksDeleted({TBlockRange32::WithLength(110, 40), 7, 3});
        index.MarkBlocksDeleted({TBlockRange32::WithLength(60, 20), 8, 4});
        index.MarkBlocksDeleted({TBlockRange32::WithLength(40, 45), 9, 5});

        //
        //  0   |   1   |   2   |   max
        //  30
        //          30
        //  -------- checkpoint --------
        //                  40
        //          20
        //                          45

        UNIT_ASSERT_VALUES_EQUAL(50, index.GetPendingUpdates());
        UNIT_ASSERT_VALUES_EQUAL(1, index.SelectZoneToCleanup());
        index.SeparateChunkForCleanup(10);
        auto extracted = index.ExtractDirtyBlobs();
        ASSERT_VECTOR_CONTENTS_EQUAL(
            TVector<TPartialBlobId>({
                blobId1,
                blobId3,
            }),
            extracted);

        auto deletionsInfo = index.CleanupDirtyRanges();
        ASSERT_VECTOR_CONTENTS_EQUAL(
            TVector<TBlobUpdate>({
                {TBlockRange32::WithLength(60, 30), 5, 2},
            }),
            deletionsInfo.BlobUpdates);

        UNIT_ASSERT_VALUES_EQUAL(1, deletionsInfo.ZoneId);

        //
        //  0   |   1   |   2   |   max
        //  30
        //  -------- checkpoint --------
        //                  40
        //          20
        //                          45

        UNIT_ASSERT_VALUES_EQUAL(45, index.GetPendingUpdates());
        UNIT_ASSERT_VALUES_EQUAL(GlobalZoneId, index.SelectZoneToCleanup());
        index.SeparateChunkForCleanup(11);
        extracted = index.ExtractDirtyBlobs();
        ASSERT_VECTOR_CONTENTS_EQUAL(
            TVector<TPartialBlobId>({
                blobId1,
                blobId3,
            }),
            extracted);

        deletionsInfo = index.CleanupDirtyRanges();
        ASSERT_VECTOR_CONTENTS_EQUAL(
            TVector<TBlobUpdate>({
                {TBlockRange32::WithLength(40, 45), 9, 5},
            }),
            deletionsInfo.BlobUpdates);

        UNIT_ASSERT_VALUES_EQUAL(GlobalZoneId, deletionsInfo.ZoneId);

        //
        //  0   |   1   |   2   |   max
        //  30
        //  -------- checkpoint --------
        //                  40
        //          20

        UNIT_ASSERT_VALUES_EQUAL(40, index.GetPendingUpdates());
        UNIT_ASSERT_VALUES_EQUAL(2, index.SelectZoneToCleanup());
        index.SeparateChunkForCleanup(12);
        extracted = index.ExtractDirtyBlobs();
        ASSERT_VECTOR_CONTENTS_EQUAL(
            TVector<TPartialBlobId>({
                blobId2,
                blobId3,
            }),
            extracted);

        deletionsInfo = index.CleanupDirtyRanges();
        ASSERT_VECTOR_CONTENTS_EQUAL(
            TVector<TBlobUpdate>({
                {TBlockRange32::WithLength(110, 40), 7, 3},
            }),
            deletionsInfo.BlobUpdates);

        UNIT_ASSERT_VALUES_EQUAL(2, deletionsInfo.ZoneId);

        //
        //  0   |   1   |   2   |   max
        //  30
        //  -------- checkpoint --------
        //          20

        UNIT_ASSERT_VALUES_EQUAL(30, index.GetPendingUpdates());
        UNIT_ASSERT_VALUES_EQUAL(0, index.SelectZoneToCleanup());
        index.SeparateChunkForCleanup(13);
        extracted = index.ExtractDirtyBlobs();
        ASSERT_VECTOR_CONTENTS_EQUAL(
            TVector<TPartialBlobId>({
                blobId1,
            }),
            extracted);

        deletionsInfo = index.CleanupDirtyRanges();
        ASSERT_VECTOR_CONTENTS_EQUAL(
            TVector<TBlobUpdate>({
                {TBlockRange32::WithLength(20, 30), 4, 1},
            }),
            deletionsInfo.BlobUpdates);

        UNIT_ASSERT_VALUES_EQUAL(0, deletionsInfo.ZoneId);

        //
        //  0   |   1   |   2   |   max
        //  -------- checkpoint --------
        //          20

        UNIT_ASSERT_VALUES_EQUAL(20, index.GetPendingUpdates());

        UNIT_ASSERT_VALUES_EQUAL(1, index.SelectZoneToCleanup());
        index.SeparateChunkForCleanup(14);
        extracted = index.ExtractDirtyBlobs();
        ASSERT_VECTOR_CONTENTS_EQUAL(
            TVector<TPartialBlobId>({
                blobId1,
                blobId3,
            }),
            extracted);

        deletionsInfo = index.CleanupDirtyRanges();
        ASSERT_VECTOR_CONTENTS_EQUAL(
            TVector<TBlobUpdate>({
                {TBlockRange32::WithLength(60, 20), 8, 4},
            }),
            deletionsInfo.BlobUpdates);

        UNIT_ASSERT_VALUES_EQUAL(1, deletionsInfo.ZoneId);

        //
        //  0   |   1   |   2   |   max
        //  -------- checkpoint --------

        UNIT_ASSERT_VALUES_EQUAL(0, index.GetPendingUpdates());
        UNIT_ASSERT_VALUES_EQUAL(GlobalZoneId, index.SelectZoneToCleanup());
        index.SeparateChunkForCleanup(15);
        extracted = index.ExtractDirtyBlobs();
        ASSERT_VECTOR_CONTENTS_EQUAL(TVector<TPartialBlobId>({}), extracted);
    }

    Y_UNIT_TEST(ShouldFindDirtyBlobsInMixedZones)
    {
        TBlobIndex index(
            ZoneCount,
            ZoneBlockCount,
            BlockListCacheSize,
            MaxBlocksInBlob,
            DeletedRangesMapMode);
        InitAllZones(index);

        AddBlob(index, blobId1, TBlockRange32::MakeClosedInterval(0, 40));
        AddBlockList(index, blobId1, TBlockRange32::MakeClosedInterval(0, 40));
        index.MarkBlocksDeleted(
            {TBlockRange32::MakeClosedInterval(10, 20), 1, 1});

        index.ConvertToMixedIndex(0, {});
        index.ConvertToMixedIndex(1, {});
        index.ConvertToMixedIndex(2, {});

        AddBlob(index, blobId2, TBlockRange32::MakeClosedInterval(60, 80));
        AddMixedBlocks(
            index,
            blobId2,
            TBlockRange32::MakeClosedInterval(60, 80));
        index.MarkBlocksDeleted(
            {TBlockRange32::MakeClosedInterval(60, 70), 2, 2});

        AddBlob(index, blobId3, TBlockRange32::MakeClosedInterval(110, 130));
        AddMixedBlocks(
            index,
            blobId3,
            TBlockRange32::MakeClosedInterval(110, 130));

        UNIT_ASSERT_VALUES_EQUAL(11, index.GetPendingUpdates());

        UNIT_ASSERT_VALUES_EQUAL(1, index.SelectZoneToCleanup());
        index.SeparateChunkForCleanup(blobId3.CommitId());
        auto extracted = index.ExtractDirtyBlobs();
        ASSERT_VECTOR_CONTENTS_EQUAL(
            TVector<TPartialBlobId>({
                blobId2,
            }),
            extracted);

        auto deletionsInfo = index.CleanupDirtyRanges();
        ASSERT_VECTOR_CONTENTS_EQUAL(
            TVector<TBlobUpdate>({
                {TBlockRange32::MakeClosedInterval(60, 70), 2, 2},
            }),
            deletionsInfo.BlobUpdates);

        UNIT_ASSERT_VALUES_EQUAL(1, deletionsInfo.ZoneId);

        UNIT_ASSERT_VALUES_EQUAL(11, index.GetPendingUpdates());
        UNIT_ASSERT_VALUES_EQUAL(0, index.SelectZoneToCleanup());
        index.SeparateChunkForCleanup(blobId3.CommitId());
        extracted = index.ExtractDirtyBlobs();
        ASSERT_VECTOR_CONTENTS_EQUAL(
            TVector<TPartialBlobId>({
                blobId1,
            }),
            extracted);

        deletionsInfo = index.CleanupDirtyRanges();
        ASSERT_VECTOR_CONTENTS_EQUAL(
            TVector<TBlobUpdate>({
                {TBlockRange32::MakeClosedInterval(10, 20), 1, 1},
            }),
            deletionsInfo.BlobUpdates);

        UNIT_ASSERT_VALUES_EQUAL(0, deletionsInfo.ZoneId);

        UNIT_ASSERT_VALUES_EQUAL(0, index.GetPendingUpdates());
        UNIT_ASSERT_VALUES_EQUAL(GlobalZoneId, index.SelectZoneToCleanup());
        index.SeparateChunkForCleanup(blobId3.CommitId());
        extracted = index.ExtractDirtyBlobs();
        ASSERT_VECTOR_CONTENTS_EQUAL(TVector<TPartialBlobId>({}), extracted);
    }

    Y_UNIT_TEST(ShouldFindMixedBlocks)
    {
        TBlobIndex index(
            ZoneCount,
            ZoneBlockCount,
            BlockListCacheSize,
            MaxBlocksInBlob,
            DeletedRangesMapMode);
        InitAllZones(index);

        AddBlob(index, blobId1, TBlockRange32::MakeClosedInterval(0, 40));
        AddBlockList(index, blobId1, TBlockRange32::MakeClosedInterval(0, 40));
        index.OnCheckpoint(1);
        index.MarkBlocksDeleted(
            {TBlockRange32::MakeClosedInterval(10, 20), 2, 1});

        index.ConvertToMixedIndex(0, {1});
        index.ConvertToMixedIndex(1, {1});
        index.ConvertToMixedIndex(2, {1});

        AddBlob(index, blobId2, TBlockRange32::MakeClosedInterval(60, 80));
        AddMixedBlocks(
            index,
            blobId2,
            TBlockRange32::MakeClosedInterval(60, 80));
        index.OnCheckpoint(2);
        index.MarkBlocksDeleted(
            {TBlockRange32::MakeClosedInterval(60, 70), 3, 2});

        AddBlob(index, blobId3, TBlockRange32::MakeClosedInterval(110, 130));
        AddMixedBlocks(
            index,
            blobId3,
            TBlockRange32::MakeClosedInterval(110, 130));

        ASSERT_VECTORS_EQUAL(
            index.FindMixedBlocks(TBlockRange32::MakeClosedInterval(0, 200), 1),
            MixedBlocks(
                1,
                InvalidCommitId,
                blobId1,
                TBlockRange32::MakeClosedInterval(0, 40)));

        ASSERT_VECTORS_EQUAL(
            index.FindMixedBlocks(TBlockRange32::MakeClosedInterval(0, 200), 2),
            JoinVecs<TBlockAndLocation>({
                MixedBlocks(
                    1,
                    InvalidCommitId,
                    blobId1,
                    TBlockRange32::WithLength(0, 10)),
                MixedBlocks(
                    1,
                    InvalidCommitId,
                    blobId1,
                    TBlockRange32::WithLength(21, 20),
                    21),
                MixedBlocks(
                    2,
                    InvalidCommitId,
                    blobId2,
                    TBlockRange32::MakeClosedInterval(60, 80)),
            }));

        ASSERT_VECTORS_EQUAL(
            index.FindMixedBlocks(TBlockRange32::MakeClosedInterval(0, 200), 3),
            JoinVecs<TBlockAndLocation>({
                MixedBlocks(
                    1,
                    InvalidCommitId,
                    blobId1,
                    TBlockRange32::WithLength(0, 10)),
                MixedBlocks(
                    1,
                    InvalidCommitId,
                    blobId1,
                    TBlockRange32::WithLength(21, 20),
                    21),
                MixedBlocks(
                    2,
                    InvalidCommitId,
                    blobId2,
                    TBlockRange32::WithLength(71, 10),
                    11),
                MixedBlocks(
                    InvalidCommitId,
                    InvalidCommitId,
                    blobId3,
                    TBlockRange32::MakeClosedInterval(110, 130)),
            }));

        for (const auto& b: index.FindAllMixedBlocks(
                 TBlockRange32::MakeClosedInterval(0, 200)))
        {
            Cdbg << "BLOCK\t" << b << Endl;
        }

        ASSERT_VECTOR_CONTENTS_EQUAL(
            index.FindAllMixedBlocks(TBlockRange32::MakeClosedInterval(0, 200)),
            JoinVecs<TBlockAndLocation>({
                MixedBlocks(
                    1,
                    InvalidCommitId,
                    blobId1,
                    TBlockRange32::WithLength(0, 10)),
                MixedBlocks(
                    1,
                    2,
                    blobId1,
                    TBlockRange32::MakeClosedInterval(10, 20),
                    10),
                MixedBlocks(
                    1,
                    InvalidCommitId,
                    blobId1,
                    TBlockRange32::WithLength(21, 20),
                    21),
                MixedBlocks(
                    2,
                    3,
                    blobId2,
                    TBlockRange32::MakeClosedInterval(60, 70)),
                MixedBlocks(
                    2,
                    InvalidCommitId,
                    blobId2,
                    TBlockRange32::WithLength(71, 10),
                    11),
                MixedBlocks(
                    InvalidCommitId,
                    InvalidCommitId,
                    blobId3,
                    TBlockRange32::MakeClosedInterval(110, 130)),
            }));
    }

    Y_UNIT_TEST(ShouldApplyUpdates)
    {
        TBlobIndex index(
            ZoneCount,
            ZoneBlockCount,
            BlockListCacheSize,
            MaxBlocksInBlob,
            DeletedRangesMapMode);
        InitAllZones(index);

        TVector<TBlock> blocks{
            {0, 1, InvalidCommitId, false},
            {1, 10, InvalidCommitId, false},
            {1, 1, 1, false},
            {1, 2, InvalidCommitId, false},
            {1, 1, 2, false},
            {3, 5, InvalidCommitId, false},
            {3, 0, 0, false},
            {5, 10, InvalidCommitId, false},
            {5, 4, InvalidCommitId, false},
        };

        index.AddBlob(blobId1, {TBlockRange32::WithLength(0, 6)}, 9, 0);
        index.AddBlockList(0, blobId1, BuildBlockList(blocks), blocks.size());

        index.MarkBlocksDeleted({TBlockRange32::MakeOneBlock(5), 1, 1});
        index.OnCheckpoint(4);
        // delayed deletion which appeared after barrier should be added
        // to the chunk it belongs to (the one before checkpoint)
        index.MarkBlocksDeleted({TBlockRange32::MakeOneBlock(1), 3, 2});

        index.MarkBlocksDeleted({TBlockRange32::MakeOneBlock(1), 4, 3});
        index.MarkBlocksDeleted({TBlockRange32::MakeOneBlock(2), 5, 4});
        index.OnCheckpoint(6);
        index.MarkBlocksDeleted({TBlockRange32::MakeOneBlock(1), 7, 5});
        index.MarkBlocksDeleted({TBlockRange32::MakeOneBlock(5), 10, 6});

        const ui32 updateCount =
            index.ApplyUpdates(TBlockRange32::WithLength(0, 6), blocks);

        UNIT_ASSERT_VALUES_EQUAL(2, updateCount);

        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[0].MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[1].MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(1, blocks[2].MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(3, blocks[3].MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(2, blocks[4].MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[5].MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(0, blocks[6].MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[7].MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(10, blocks[8].MaxCommitId);
    }

    Y_UNIT_TEST(ShouldPruneBlockListCache)
    {
        TBlobIndex index(
            ZoneCount,
            ZoneBlockCount,
            30,
            MaxBlocksInBlob,
            DeletedRangesMapMode);
        InitAllZones(index);

        AddBlob(index, blobId1, TBlockRange32::WithLength(1, 10));
        AddBlockList(index, blobId1, TBlockRange32::WithLength(1, 10));
        AddBlob(index, blobId2, TBlockRange32::WithLength(11, 10));
        AddBlockList(index, blobId2, TBlockRange32::WithLength(11, 10));
        AddBlob(index, blobId3, TBlockRange32::WithLength(21, 10));
        AddBlockList(index, blobId3, TBlockRange32::WithLength(21, 10));

        UNIT_ASSERT(index.FindBlockList(0, blobId1));
        UNIT_ASSERT(index.FindBlockList(0, blobId2));
        UNIT_ASSERT(index.FindBlockList(0, blobId3));

        UNIT_ASSERT_VALUES_EQUAL(3, index.GetBlockListCount());

        // using blocklists 1 and 3, blocklist 2 now has the lowest use count
        index.FindBlockList(0, blobId1);
        index.FindBlockList(0, blobId3);

        AddBlob(index, blobId4, TBlockRange32::WithLength(31, 10));
        AddBlockList(index, blobId4, TBlockRange32::WithLength(31, 10));

        // blocklist 2 should be removed
        UNIT_ASSERT(!index.FindBlockList(0, blobId2));
        UNIT_ASSERT(index.FindBlockList(0, blobId4));

        // using blocklists 1 and 4, blocklist 3 now has the lowest use count
        index.FindBlockList(0, blobId1);
        index.FindBlockList(0, blobId4);
        index.FindBlockList(0, blobId4);

        AddBlockList(index, blobId2, TBlockRange32::WithLength(11, 10));

        // blocklist 3 should be removed
        UNIT_ASSERT(!index.FindBlockList(0, blobId3));
        UNIT_ASSERT(index.FindBlockList(0, blobId2));

        // disable cache pruning
        index.SetPruneBlockListCache(false);

        AddBlockList(index, blobId3, TBlockRange32::WithLength(21, 10));

        // now all 4 blocklists should be present in cache
        UNIT_ASSERT_VALUES_EQUAL(4, index.GetBlockListCount());

        UNIT_ASSERT(index.FindBlockList(0, blobId1));
        UNIT_ASSERT(index.FindBlockList(0, blobId2));
        UNIT_ASSERT(index.FindBlockList(0, blobId3));
        UNIT_ASSERT(index.FindBlockList(0, blobId4));

        // enabling pruning again
        index.SetPruneBlockListCache(true);
        // one of the blocklists should be removed after this step
        index.PruneStep();

        UNIT_ASSERT_VALUES_EQUAL(3, index.GetBlockListCount());

        // blocklist 3 should be removed
        UNIT_ASSERT(index.FindBlockList(0, blobId1));
        UNIT_ASSERT(index.FindBlockList(0, blobId2));
        UNIT_ASSERT(!index.FindBlockList(0, blobId3));
        UNIT_ASSERT(index.FindBlockList(0, blobId4));
    }

    Y_UNIT_TEST(ShouldBuildRanges)
    {
        ASSERT_VECTORS_EQUAL(
            BuildRanges({{1, 0, 0, false}}, 1),
            TBlockRanges({
                TBlockRange32::MakeOneBlock(1),
            }));

        ASSERT_VECTORS_EQUAL(
            BuildRanges({{1, 0, 0, false}}, 100),
            TBlockRanges({
                TBlockRange32::MakeOneBlock(1),
            }));

        ASSERT_VECTORS_EQUAL(
            BuildRanges(
                {
                    {1, 0, 0, false},
                    {2, 0, 0, false},
                    {3, 0, 0, false},
                },
                1),
            TBlockRanges({
                TBlockRange32::MakeClosedInterval(1, 3),
            }));

        ASSERT_VECTORS_EQUAL(
            BuildRanges(
                {
                    {1, 0, 0, false},
                    {2, 0, 0, false},
                    {3, 0, 0, false},
                },
                100),
            TBlockRanges({
                TBlockRange32::MakeClosedInterval(1, 3),
            }));

        TVector<TBlock> sparseBlocks = {
            {1, 0, 0, false},
            {2, 0, 0, false},
            {3, 0, 0, false},
            {5, 0, 0, false},
            {6, 0, 0, false},
            {7, 0, 0, false},
            {10, 0, 0, false},
            {11, 0, 0, false},
            {20, 0, 0, false},
            {100, 0, 0, false},
            {101, 0, 0, false},
            {106, 0, 0, false},
            {107, 0, 0, false},
            {108, 0, 0, false},
        };

        // holes:
        // 4-4
        // 8-9
        // 12-19
        // 21-99
        // 102-105

        ASSERT_VECTORS_EQUAL(
            BuildRanges(sparseBlocks, 1),
            TBlockRanges({
                TBlockRange32::MakeClosedInterval(1, 108),
            }));

        ASSERT_VECTORS_EQUAL(
            BuildRanges(sparseBlocks, 2),
            TBlockRanges({
                TBlockRange32::MakeClosedInterval(1, 20),
                TBlockRange32::MakeClosedInterval(100, 108),
            }));

        ASSERT_VECTORS_EQUAL(
            BuildRanges(sparseBlocks, 3),
            TBlockRanges({
                TBlockRange32::MakeClosedInterval(1, 11),
                TBlockRange32::MakeClosedInterval(20, 20),
                TBlockRange32::MakeClosedInterval(100, 108),
            }));

        ASSERT_VECTORS_EQUAL(
            BuildRanges(sparseBlocks, 4),
            TBlockRanges({
                TBlockRange32::MakeClosedInterval(1, 11),
                TBlockRange32::MakeClosedInterval(20, 20),
                TBlockRange32::MakeClosedInterval(100, 101),
                TBlockRange32::MakeClosedInterval(106, 108),
            }));

        ASSERT_VECTORS_EQUAL(
            BuildRanges(sparseBlocks, 5),
            TBlockRanges({
                TBlockRange32::MakeClosedInterval(1, 7),
                TBlockRange32::MakeClosedInterval(10, 11),
                TBlockRange32::MakeClosedInterval(20, 20),
                TBlockRange32::MakeClosedInterval(100, 101),
                TBlockRange32::MakeClosedInterval(106, 108),
            }));

        ASSERT_VECTORS_EQUAL(
            BuildRanges(sparseBlocks, 6),
            TBlockRanges({
                TBlockRange32::MakeClosedInterval(1, 3),
                TBlockRange32::MakeClosedInterval(5, 7),
                TBlockRange32::MakeClosedInterval(10, 11),
                TBlockRange32::MakeClosedInterval(20, 20),
                TBlockRange32::MakeClosedInterval(100, 101),
                TBlockRange32::MakeClosedInterval(106, 108),
            }));

        ASSERT_VECTORS_EQUAL(
            BuildRanges(sparseBlocks, 7),
            TBlockRanges({
                TBlockRange32::MakeClosedInterval(1, 3),
                TBlockRange32::MakeClosedInterval(5, 7),
                TBlockRange32::MakeClosedInterval(10, 11),
                TBlockRange32::MakeClosedInterval(20, 20),
                TBlockRange32::MakeClosedInterval(100, 101),
                TBlockRange32::MakeClosedInterval(106, 108),
            }));
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2

template <>
inline void Out<NCloud::NBlockStore::TBlockRange32>(
    IOutputStream& out,
    const NCloud::NBlockStore::TBlockRange32& range)
{
    out << "[" << range.Start << ", " << range.End << "]";
}

template <>
inline void Out<NCloud::NBlockStore::NStorage::NPartition2::TDeletedBlock>(
    IOutputStream& out,
    const NCloud::NBlockStore::NStorage::NPartition2::TDeletedBlock& db)
{
    out << db.BlockIndex << "@" << db.CommitId;
}

template <>
inline void Out<NCloud::NBlockStore::NStorage::NPartition2::TBlockAndLocation>(
    IOutputStream& out,
    const NCloud::NBlockStore::NStorage::NPartition2::TBlockAndLocation& b)
{
    out << b.Block.BlockIndex << "@" << b.Block.MinCommitId << "-"
        << b.Block.MaxCommitId << "/" << b.Location.BlobId << "+"
        << b.Location.BlobOffset;
}

template <>
inline void Out<NCloud::NBlockStore::NStorage::NPartition2::TBlobUpdate>(
    IOutputStream& out,
    const NCloud::NBlockStore::NStorage::NPartition2::TBlobUpdate& b)
{
    out << b.BlockRange.Start << ".." << b.BlockRange.End << "@" << b.CommitId
        << "+" << b.DeletionId;
}
