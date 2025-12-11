#include "mixed_index.h"

#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/output.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TBlockComp
{
    bool operator()(
        const TBlockAndLocation& l,
        const TBlockAndLocation& r) const
    {
        if (l.Block.BlockIndex != r.Block.BlockIndex) {
            return l.Block.BlockIndex < r.Block.BlockIndex;
        }

        return l.Block.MinCommitId < r.Block.MinCommitId;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMixedIndexTest)
{
    static const TPartialBlobId blobId0 = {1, 0};
    static const TPartialBlobId blobId1 = {2, 0};
    static const TPartialBlobId blobId2 = {3, 0};
    static const TPartialBlobId blobId3 = {4, 0};
    static const TPartialBlobId blobId4 = {5, 0};
    static const TPartialBlobId blobId5 = {6, 0};
    static const TPartialBlobId blobId6 = {7, 0};

    Y_UNIT_TEST(ShouldStoreActualData)
    {
        TMixedIndex index;
        index.SetOrUpdateBlock(0, {blobId1, 1});
        index.SetOrUpdateBlock(5, {blobId1, 4});
        index.SetOrUpdateBlock(11, {blobId2, 2});
        index.SetOrUpdateBlock(11, {blobId4, 7});
        index.SetOrUpdateBlock(16, {blobId3, 1});
        index.ClearBlock(16);

        TBlockAndLocation b;
        UNIT_ASSERT(!index.FindBlock(1, &b));

        UNIT_ASSERT(index.FindBlock(0, &b));
        UNIT_ASSERT_VALUES_EQUAL(blobId1, b.Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(1, b.Location.BlobOffset);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);

        UNIT_ASSERT(index.FindBlock(5, &b));
        UNIT_ASSERT_VALUES_EQUAL(blobId1, b.Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(4, b.Location.BlobOffset);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);

        UNIT_ASSERT(index.FindBlock(11, &b));
        UNIT_ASSERT_VALUES_EQUAL(blobId4, b.Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(7, b.Location.BlobOffset);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);

        UNIT_ASSERT(!index.FindBlock(16, &b));
        UNIT_ASSERT_VALUES_EQUAL(InvalidBlobOffset, b.Location.BlobOffset);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);
    }

    Y_UNIT_TEST(ShouldStoreCheckpointData)
    {
        TMixedIndex index;
        for (ui32 i = 0; i < 10; ++i) {
            index.SetOrUpdateBlock(i, {blobId1, static_cast<ui16>(i + 10)});
        }

        index.OnCheckpoint(1);
        index.SetOrUpdateBlock(
            {{100, 1, InvalidCommitId, false}, {blobId1, 200}});

        for (ui32 i = 1; i < 11; ++i) {
            index.SetOrUpdateBlock(i, {blobId2, static_cast<ui16>(i + 20)});
        }

        index.OnCheckpoint(3);

        for (ui32 i = 2; i < 12; ++i) {
            index.SetOrUpdateBlock(i, {blobId3, static_cast<ui16>(i + 30)});
        }

        for (ui32 i = 0; i < 12; ++i) {
            TBlockAndLocation b;

            if (i < 10) {
                UNIT_ASSERT(index.FindBlock(1, i, &b));
                UNIT_ASSERT_VALUES_EQUAL(blobId1, b.Location.BlobId);
                UNIT_ASSERT_VALUES_EQUAL(i + 10, b.Location.BlobOffset);
                UNIT_ASSERT_VALUES_EQUAL(1, b.Block.MinCommitId);
                UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);
            } else {
                UNIT_ASSERT(!index.FindBlock(1, i, &b));
            }

            if (i < 11) {
                UNIT_ASSERT(index.FindBlock(3, i, &b));
                if (i > 0) {
                    UNIT_ASSERT_VALUES_EQUAL(blobId2, b.Location.BlobId);
                    UNIT_ASSERT_VALUES_EQUAL(i + 20, b.Location.BlobOffset);
                    UNIT_ASSERT_VALUES_EQUAL(3, b.Block.MinCommitId);
                    UNIT_ASSERT_VALUES_EQUAL(
                        InvalidCommitId,
                        b.Block.MaxCommitId);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(blobId1, b.Location.BlobId);
                    UNIT_ASSERT_VALUES_EQUAL(i + 10, b.Location.BlobOffset);
                    UNIT_ASSERT_VALUES_EQUAL(1, b.Block.MinCommitId);
                    UNIT_ASSERT_VALUES_EQUAL(
                        InvalidCommitId,
                        b.Block.MaxCommitId);
                }
            } else {
                UNIT_ASSERT(!index.FindBlock(3, i, &b));
            }

            UNIT_ASSERT(index.FindBlock(4, i, &b));
            if (i == 0) {
                UNIT_ASSERT_VALUES_EQUAL(blobId1, b.Location.BlobId);
                UNIT_ASSERT_VALUES_EQUAL(i + 10, b.Location.BlobOffset);
                UNIT_ASSERT_VALUES_EQUAL(1, b.Block.MinCommitId);
                UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);
            } else if (i == 1) {
                UNIT_ASSERT_VALUES_EQUAL(blobId2, b.Location.BlobId);
                UNIT_ASSERT_VALUES_EQUAL(i + 20, b.Location.BlobOffset);
                UNIT_ASSERT_VALUES_EQUAL(3, b.Block.MinCommitId);
                UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(blobId3, b.Location.BlobId);
                UNIT_ASSERT_VALUES_EQUAL(i + 30, b.Location.BlobOffset);
                UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MinCommitId);
                UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);
            }
        }

        {
            TBlockAndLocation b;
            UNIT_ASSERT(index.FindBlock(1, 100, &b));
            UNIT_ASSERT_VALUES_EQUAL(blobId1, b.Location.BlobId);
            UNIT_ASSERT_VALUES_EQUAL(200, b.Location.BlobOffset);
            UNIT_ASSERT_VALUES_EQUAL(1, b.Block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);

            UNIT_ASSERT(index.FindBlock(100, &b));
            UNIT_ASSERT_VALUES_EQUAL(blobId1, b.Location.BlobId);
            UNIT_ASSERT_VALUES_EQUAL(200, b.Location.BlobOffset);
            UNIT_ASSERT_VALUES_EQUAL(1, b.Block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);
        }

        index.OnCheckpointDeletion(1);

        {
            TBlockAndLocation b;
            UNIT_ASSERT(index.FindBlock(1, 100, &b));
            UNIT_ASSERT_VALUES_EQUAL(blobId1, b.Location.BlobId);
            UNIT_ASSERT_VALUES_EQUAL(200, b.Location.BlobOffset);
            UNIT_ASSERT_VALUES_EQUAL(3, b.Block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);

            UNIT_ASSERT(index.FindBlock(100, &b));
            UNIT_ASSERT_VALUES_EQUAL(blobId1, b.Location.BlobId);
            UNIT_ASSERT_VALUES_EQUAL(200, b.Location.BlobOffset);
            UNIT_ASSERT_VALUES_EQUAL(3, b.Block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);
        }

        for (ui32 i = 0; i < 12; ++i) {
            TBlockAndLocation b;

            if (i < 11) {
                UNIT_ASSERT(index.FindBlock(1, i, &b));
                if (i > 0) {
                    UNIT_ASSERT_VALUES_EQUAL(blobId2, b.Location.BlobId);
                    UNIT_ASSERT_VALUES_EQUAL(i + 20, b.Location.BlobOffset);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(blobId1, b.Location.BlobId);
                    UNIT_ASSERT_VALUES_EQUAL(i + 10, b.Location.BlobOffset);
                }
                UNIT_ASSERT_VALUES_EQUAL(3, b.Block.MinCommitId);
                UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);
            } else {
                UNIT_ASSERT(!index.FindBlock(1, i, &b));
            }
        }

        for (ui32 i = 2; i < 12; i += 2) {
            index.SetOrUpdateBlock(i, {blobId4, static_cast<ui16>(i + 40)});
        }

        for (ui32 i = 2; i < 12; ++i) {
            TBlockAndLocation b;

            UNIT_ASSERT(index.FindBlock(i, &b));

            if (i % 2) {
                UNIT_ASSERT_VALUES_EQUAL(blobId3, b.Location.BlobId);
                UNIT_ASSERT_VALUES_EQUAL(i + 30, b.Location.BlobOffset);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(blobId4, b.Location.BlobId);
                UNIT_ASSERT_VALUES_EQUAL(i + 40, b.Location.BlobOffset);
            }

            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);
        }

        index.OnCheckpoint(5);
        index.SetOrUpdateBlock(0, {blobId1, 111});
        index.SetOrUpdateBlock(1, {blobId1, 222});
        index.SetOrUpdateBlock(2, {blobId1, 333});

        {
            TBlockAndLocation b;
            UNIT_ASSERT(index.FindBlock(0, &b));
            UNIT_ASSERT_VALUES_EQUAL(blobId1, b.Location.BlobId);
            UNIT_ASSERT_VALUES_EQUAL(111, b.Location.BlobOffset);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);

            UNIT_ASSERT(index.FindBlock(5, 0, &b));
            UNIT_ASSERT_VALUES_EQUAL(blobId1, b.Location.BlobId);
            UNIT_ASSERT_VALUES_EQUAL(10, b.Location.BlobOffset);
            UNIT_ASSERT_VALUES_EQUAL(3, b.Block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);

            UNIT_ASSERT(index.FindBlock(1, &b));
            UNIT_ASSERT_VALUES_EQUAL(blobId1, b.Location.BlobId);
            UNIT_ASSERT_VALUES_EQUAL(222, b.Location.BlobOffset);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);

            UNIT_ASSERT(index.FindBlock(5, 1, &b));
            UNIT_ASSERT_VALUES_EQUAL(blobId2, b.Location.BlobId);
            UNIT_ASSERT_VALUES_EQUAL(21, b.Location.BlobOffset);
            UNIT_ASSERT_VALUES_EQUAL(3, b.Block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);

            UNIT_ASSERT(index.FindBlock(2, &b));
            UNIT_ASSERT_VALUES_EQUAL(blobId1, b.Location.BlobId);
            UNIT_ASSERT_VALUES_EQUAL(333, b.Location.BlobOffset);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);

            UNIT_ASSERT(index.FindBlock(5, 2, &b));
            UNIT_ASSERT_VALUES_EQUAL(blobId4, b.Location.BlobId);
            UNIT_ASSERT_VALUES_EQUAL(42, b.Location.BlobOffset);
            UNIT_ASSERT_VALUES_EQUAL(5, b.Block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);
        }
    }

    Y_UNIT_TEST(ShouldProperlyProcessCheckpointDeletion)
    {
        TMixedIndex index;

        // cp 1
        index.SetOrUpdateBlock({{0, 1, InvalidCommitId, false}, {blobId1, 0}});
        index.SetOrUpdateBlock({{1, 1, InvalidCommitId, false}, {blobId1, 1}});
        index.SetOrUpdateBlock({{6, 1, InvalidCommitId, false}, {blobId1, 2}});

        index.OnCheckpoint(1);

        // lagging block
        index.SetOrUpdateBlock({{2, 1, InvalidCommitId, false}, {blobId2, 0}});

        // cp 1
        // 0 -> blobId1, 0
        // 1 -> blobId1, 1
        // 2 -> blobId2, 0
        // 6 -> blobId1, 2

        // cp 3
        index.SetOrUpdateBlock({{0, 2, InvalidCommitId, false}, {blobId2, 1}});

        index.SetOrUpdateBlock({{4, 3, InvalidCommitId, false}, {blobId2, 2}});

        index.OnCheckpoint(3);

        // lagging block
        index.SetOrUpdateBlock({{2, 3, InvalidCommitId, false}, {blobId3, 0}});

        // cp 3
        // 0 -> blobId2, 1
        // 1 -> blobId1, 1
        // 2 -> blobId3, 0
        // 4 -> blobId2, 2
        // 6 -> blobId1, 2

        // actual state
        index.SetOrUpdateBlock({{1, 4, InvalidCommitId, false}, {blobId3, 1}});
        index.SetOrUpdateBlock({{2, 7, InvalidCommitId, false}, {blobId3, 2}});
        index.SetOrUpdateBlock({{5, 10, InvalidCommitId, false}, {blobId3, 3}});

        // actual state
        // 0 -> blobId2, 1
        // 1 -> blobId3, 1
        // 2 -> blobId3, 2
        // 4 -> blobId2, 2
        // 5 -> blobId3, 3
        // 6 -> blobId1, 2

        index.OnCheckpointDeletion(1);

        {
            TBlockAndLocation b;
            UNIT_ASSERT(index.FindBlock(1, 0, &b));
            UNIT_ASSERT_VALUES_EQUAL(blobId2, b.Location.BlobId);
            UNIT_ASSERT_VALUES_EQUAL(1, b.Location.BlobOffset);
            UNIT_ASSERT_VALUES_EQUAL(3, b.Block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);

            UNIT_ASSERT(index.FindBlock(1, 1, &b));
            UNIT_ASSERT_VALUES_EQUAL(blobId1, b.Location.BlobId);
            UNIT_ASSERT_VALUES_EQUAL(1, b.Location.BlobOffset);
            UNIT_ASSERT_VALUES_EQUAL(3, b.Block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);

            UNIT_ASSERT(index.FindBlock(1, 2, &b));
            UNIT_ASSERT_VALUES_EQUAL(blobId3, b.Location.BlobId);
            UNIT_ASSERT_VALUES_EQUAL(0, b.Location.BlobOffset);
            UNIT_ASSERT_VALUES_EQUAL(3, b.Block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);

            UNIT_ASSERT(index.FindBlock(1, 4, &b));
            UNIT_ASSERT_VALUES_EQUAL(blobId2, b.Location.BlobId);
            UNIT_ASSERT_VALUES_EQUAL(2, b.Location.BlobOffset);
            UNIT_ASSERT_VALUES_EQUAL(3, b.Block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);

            UNIT_ASSERT(index.FindBlock(1, 6, &b));
            UNIT_ASSERT_VALUES_EQUAL(blobId1, b.Location.BlobId);
            UNIT_ASSERT_VALUES_EQUAL(2, b.Location.BlobOffset);
            UNIT_ASSERT_VALUES_EQUAL(3, b.Block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);
        }

        index.OnCheckpointDeletion(3);

        {
            // actual state
            // 0 -> blobId2, 1
            // 1 -> blobId3, 1
            // 2 -> blobId3, 2
            // 4 -> blobId2, 2
            // 5 -> blobId3, 3
            // 6 -> blobId1, 2
            TBlockAndLocation b;
            UNIT_ASSERT(index.FindBlock(3, 0, &b));
            UNIT_ASSERT_VALUES_EQUAL(blobId2, b.Location.BlobId);
            UNIT_ASSERT_VALUES_EQUAL(1, b.Location.BlobOffset);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);

            UNIT_ASSERT(index.FindBlock(3, 1, &b));
            UNIT_ASSERT_VALUES_EQUAL(blobId3, b.Location.BlobId);
            UNIT_ASSERT_VALUES_EQUAL(1, b.Location.BlobOffset);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);

            UNIT_ASSERT(index.FindBlock(3, 2, &b));
            UNIT_ASSERT_VALUES_EQUAL(blobId3, b.Location.BlobId);
            UNIT_ASSERT_VALUES_EQUAL(2, b.Location.BlobOffset);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);

            UNIT_ASSERT(index.FindBlock(3, 4, &b));
            UNIT_ASSERT_VALUES_EQUAL(blobId2, b.Location.BlobId);
            UNIT_ASSERT_VALUES_EQUAL(2, b.Location.BlobOffset);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);

            UNIT_ASSERT(index.FindBlock(3, 5, &b));
            UNIT_ASSERT_VALUES_EQUAL(blobId3, b.Location.BlobId);
            UNIT_ASSERT_VALUES_EQUAL(3, b.Location.BlobOffset);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);

            UNIT_ASSERT(index.FindBlock(3, 6, &b));
            UNIT_ASSERT_VALUES_EQUAL(blobId1, b.Location.BlobId);
            UNIT_ASSERT_VALUES_EQUAL(2, b.Location.BlobOffset);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, b.Block.MaxCommitId);
        }
    }

    Y_UNIT_TEST(ShouldFindAllBlockVersions)
    {
        TMixedIndex index;

        index.SetOrUpdateBlock(0, {blobId1, 0});
        index.SetOrUpdateBlock(1, {blobId1, 1});
        index.SetOrUpdateBlock(2, {blobId1, 2});
        index.SetOrUpdateBlock(3, {blobId1, 3});

        index.OnCheckpoint(1);
        index.SetOrUpdateBlock(0, {blobId2, 0});
        index.SetOrUpdateBlock(1, {blobId2, 1});
        index.SetOrUpdateBlock(2, {blobId2, 2});

        index.OnCheckpoint(3);
        index.SetOrUpdateBlock(0, {blobId3, 0});
        index.SetOrUpdateBlock(1, {blobId3, 1});

        index.OnCheckpoint(5);
        index.SetOrUpdateBlock(0, {blobId4, 0});
        index.SetOrUpdateBlock(4, {blobId4, 1});

        auto blocks = index.FindAllBlocks(TBlockRange32::WithLength(0, 5));
        Sort(blocks.begin(), blocks.end(), TBlockComp());

        UNIT_ASSERT_VALUES_EQUAL(11, blocks.size());

        // block 0
        UNIT_ASSERT_VALUES_EQUAL(0, blocks[0].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(1, blocks[0].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(3, blocks[0].Block.MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(0, blocks[1].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(3, blocks[1].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(5, blocks[1].Block.MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(0, blocks[2].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(5, blocks[2].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(6, blocks[2].Block.MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(0, blocks[3].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[3].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[3].Block.MaxCommitId);

        // block 1
        UNIT_ASSERT_VALUES_EQUAL(1, blocks[4].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(1, blocks[4].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(3, blocks[4].Block.MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(1, blocks[5].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(3, blocks[5].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(5, blocks[5].Block.MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(1, blocks[6].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(5, blocks[6].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[6].Block.MaxCommitId);

        // block 2
        UNIT_ASSERT_VALUES_EQUAL(2, blocks[7].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(1, blocks[7].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(3, blocks[7].Block.MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(2, blocks[8].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(3, blocks[8].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[8].Block.MaxCommitId);

        // block 3
        UNIT_ASSERT_VALUES_EQUAL(3, blocks[9].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(1, blocks[9].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[9].Block.MaxCommitId);

        // block 4
        UNIT_ASSERT_VALUES_EQUAL(4, blocks[10].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[10].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[10].Block.MaxCommitId);

        blocks = index.FindAllBlocks(TBlockRange32::WithLength(1, 2));
        Sort(blocks.begin(), blocks.end(), TBlockComp());

        UNIT_ASSERT_VALUES_EQUAL(5, blocks.size());

        // block 1
        UNIT_ASSERT_VALUES_EQUAL(1, blocks[0].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(1, blocks[0].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(3, blocks[0].Block.MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(1, blocks[1].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(3, blocks[1].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(5, blocks[1].Block.MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(1, blocks[2].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(5, blocks[2].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[2].Block.MaxCommitId);

        // block 2
        UNIT_ASSERT_VALUES_EQUAL(2, blocks[3].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(1, blocks[3].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(3, blocks[3].Block.MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(2, blocks[4].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(3, blocks[4].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[4].Block.MaxCommitId);
    }

    Y_UNIT_TEST(ShouldUpdateLocation)
    {
        TMixedIndex index;

        index.SetOrUpdateBlock(5, {blobId1, 1});
        index.OnCheckpoint(1);
        index.SetOrUpdateBlock(5, {blobId2, 1});

        index.SetOrUpdateBlock(5, {blobId3, 2});

        TBlockAndLocation b;
        UNIT_ASSERT(index.FindBlock(5, &b));
        UNIT_ASSERT_VALUES_EQUAL(blobId3, b.Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(2, b.Location.BlobOffset);

        UNIT_ASSERT(index.FindBlock(1, 5, &b));
        UNIT_ASSERT_VALUES_EQUAL(blobId1, b.Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(1, b.Location.BlobOffset);

        index.SetOrUpdateBlock({{5, 1, InvalidCommitId, false}, {blobId4, 3}});
        UNIT_ASSERT(index.FindBlock(1, 5, &b));
        UNIT_ASSERT_VALUES_EQUAL(blobId4, b.Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(3, b.Location.BlobOffset);

        UNIT_ASSERT(index.FindBlock(5, &b));
        UNIT_ASSERT_VALUES_EQUAL(blobId3, b.Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(2, b.Location.BlobOffset);
    }

    Y_UNIT_TEST(ShouldStoreLaggingCheckpointData)
    {
        TMixedIndex index;

        index.SetOrUpdateBlock(5, {blobId1, 1});
        index.OnCheckpoint(1);
        index.SetOrUpdateBlock({{6, 1, InvalidCommitId, false}, {blobId2, 3}});
        // lagging block, deleted after checkpoint
        index.SetOrUpdateBlock({{7, 1, 2, false}, {blobId2, 4}});

        TBlockAndLocation b;

        UNIT_ASSERT(index.FindBlock(1, 6, &b));
        UNIT_ASSERT_VALUES_EQUAL(blobId2, b.Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(3, b.Location.BlobOffset);

        UNIT_ASSERT(index.FindBlock(1, 7, &b));
        UNIT_ASSERT_VALUES_EQUAL(blobId2, b.Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(4, b.Location.BlobOffset);

        UNIT_ASSERT(!index.FindBlock(7, &b));

        index.SetOrUpdateBlock({{6, 1, InvalidCommitId, false}, {blobId3, 4}});

        UNIT_ASSERT(index.FindBlock(1, 6, &b));
        UNIT_ASSERT_VALUES_EQUAL(blobId3, b.Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(4, b.Location.BlobOffset);

        auto blocks = index.FindAllBlocks(TBlockRange32::WithLength(0, 100));
        UNIT_ASSERT_VALUES_EQUAL(3, blocks.size());

        UNIT_ASSERT_VALUES_EQUAL(5, blocks[0].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(1, blocks[0].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[0].Block.MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(blobId1, blocks[0].Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(1, blocks[0].Location.BlobOffset);

        UNIT_ASSERT_VALUES_EQUAL(6, blocks[1].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(1, blocks[1].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[1].Block.MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(blobId3, blocks[1].Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(4, blocks[1].Location.BlobOffset);

        UNIT_ASSERT_VALUES_EQUAL(7, blocks[2].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(1, blocks[2].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(2, blocks[2].Block.MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(blobId2, blocks[2].Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(4, blocks[2].Location.BlobOffset);

        index.OnCheckpoint(5);
        // lagging block for checkpoint 1, deleted before checkpoint 5
        index.SetOrUpdateBlock({{8, 1, 3, false}, {blobId4, 1}});

        UNIT_ASSERT(index.FindBlock(1, 8, &b));
        UNIT_ASSERT_VALUES_EQUAL(blobId4, b.Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(1, b.Location.BlobOffset);

        UNIT_ASSERT(!index.FindBlock(2, 8, &b));
        UNIT_ASSERT(!index.FindBlock(8, &b));

        index.SetOrUpdateBlock({{5, 6, InvalidCommitId, false}, {blobId4, 2}});

        UNIT_ASSERT(index.FindBlock(1, 5, &b));
        UNIT_ASSERT_VALUES_EQUAL(blobId1, b.Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(1, b.Location.BlobOffset);

        UNIT_ASSERT(index.FindBlock(5, 5, &b));
        UNIT_ASSERT_VALUES_EQUAL(blobId1, b.Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(1, b.Location.BlobOffset);

        UNIT_ASSERT(index.FindBlock(6, 5, &b));
        UNIT_ASSERT_VALUES_EQUAL(blobId4, b.Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(2, b.Location.BlobOffset);

        index.SetOrUpdateBlock({{5, 1, 6, false}, {blobId1, 1}});

        // old version of this block should remain in place
        UNIT_ASSERT(index.FindBlock(1, 5, &b));
        UNIT_ASSERT_VALUES_EQUAL(blobId1, b.Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(1, b.Location.BlobOffset);

        UNIT_ASSERT(index.FindBlock(5, 5, &b));
        UNIT_ASSERT_VALUES_EQUAL(blobId1, b.Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(1, b.Location.BlobOffset);

        // new version of this block should not be overwritten
        UNIT_ASSERT(index.FindBlock(6, 5, &b));
        UNIT_ASSERT_VALUES_EQUAL(blobId4, b.Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(2, b.Location.BlobOffset);

        blocks = index.FindAllBlocks(TBlockRange32::WithLength(0, 100));
        UNIT_ASSERT_VALUES_EQUAL(5, blocks.size());

        UNIT_ASSERT_VALUES_EQUAL(5, blocks[0].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[0].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[0].Block.MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(blobId4, blocks[0].Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(2, blocks[0].Location.BlobOffset);

        UNIT_ASSERT_VALUES_EQUAL(5, blocks[1].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(1, blocks[1].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(6, blocks[1].Block.MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(blobId1, blocks[1].Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(1, blocks[1].Location.BlobOffset);

        UNIT_ASSERT_VALUES_EQUAL(6, blocks[2].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(1, blocks[2].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[2].Block.MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(blobId3, blocks[2].Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(4, blocks[2].Location.BlobOffset);

        UNIT_ASSERT_VALUES_EQUAL(7, blocks[3].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(1, blocks[3].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(5, blocks[3].Block.MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(blobId2, blocks[3].Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(4, blocks[3].Location.BlobOffset);

        UNIT_ASSERT_VALUES_EQUAL(8, blocks[4].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(1, blocks[4].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(5, blocks[4].Block.MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(blobId4, blocks[4].Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(1, blocks[4].Location.BlobOffset);
    }

    Y_UNIT_TEST(ShouldNotReturnClearedBlocks)
    {
        TMixedIndex index;
        index.SetOrUpdateBlock(1, {blobId1, 3});
        index.SetOrUpdateBlock(2, {blobId1, 4});
        index.OnCheckpoint(1);
        index.ClearBlock(1);
        index.OnCheckpoint(2);

        auto blocks = index.FindAllBlocks(TBlockRange32::WithLength(0, 100));
        UNIT_ASSERT_VALUES_EQUAL(2, blocks.size());

        UNIT_ASSERT_VALUES_EQUAL(1, blocks[0].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(1, blocks[0].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(2, blocks[0].Block.MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(blobId1, blocks[0].Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(3, blocks[0].Location.BlobOffset);

        UNIT_ASSERT_VALUES_EQUAL(2, blocks[1].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(1, blocks[1].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[1].Block.MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(blobId1, blocks[1].Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(4, blocks[1].Location.BlobOffset);
    }

    Y_UNIT_TEST(ShouldBuildIndex)
    {
        const auto range = TBlockRange32::WithLength(100, 300);
        TMixedIndexBuilder builder(range);

        builder.AddBlock({{110, 2, 3, false}, {blobId0, 5}});
        builder.AddBlock({{110, 1, 3, false}, {blobId1, 5}});
        builder.AddBlock({{110, 3, 7, false}, {blobId2, 6}});
        builder.AddBlock({{110, 7, InvalidCommitId, false}, {blobId3, 7}});

        builder.AddBlock({{111, 1, 7, false}, {blobId1, 6}});
        builder.AddBlock({{111, 7, InvalidCommitId, false}, {blobId2, 8}});

        builder.AddBlock({{112, 9, InvalidCommitId, false}, {blobId4, 1}});

        builder.AddBlock({{113, 9, 12, false}, {blobId4, 2}});

        // rebased block, written before checkpoint
        builder.AddBlock({{114, 6, 12, false}, {blobId4, 3}});
        // non-rebased block, written after checkpoint, overlaps with the
        // previous version by commit id range
        builder.AddBlock({{114, 8, InvalidCommitId, false}, {blobId4, 4}});

        builder.AddOverwrittenBlob(blobId0);
        builder.AddOverwrittenBlob(blobId1);
        builder.AddOverwrittenBlob(blobId2);
        builder.AddOverwrittenBlob(blobId3);
        builder.AddOverwrittenBlob(blobId4);

        auto index = builder.Build({6});

        TBlockAndLocation b;
        UNIT_ASSERT(index->FindBlock(110, &b));
        UNIT_ASSERT_VALUES_EQUAL(blobId3, b.Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(7, b.Location.BlobOffset);

        UNIT_ASSERT(index->FindBlock(1, 110, &b));
        UNIT_ASSERT_VALUES_EQUAL(blobId2, b.Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(6, b.Location.BlobOffset);

        UNIT_ASSERT(index->FindBlock(6, 110, &b));
        UNIT_ASSERT_VALUES_EQUAL(blobId2, b.Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(6, b.Location.BlobOffset);

        UNIT_ASSERT(index->FindBlock(111, &b));
        UNIT_ASSERT_VALUES_EQUAL(blobId2, b.Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(8, b.Location.BlobOffset);

        UNIT_ASSERT(index->FindBlock(6, 111, &b));
        UNIT_ASSERT_VALUES_EQUAL(blobId1, b.Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(6, b.Location.BlobOffset);

        UNIT_ASSERT(index->FindBlock(112, &b));
        UNIT_ASSERT_VALUES_EQUAL(blobId4, b.Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(1, b.Location.BlobOffset);

        UNIT_ASSERT(!index->FindBlock(113, &b));

        UNIT_ASSERT(index->FindBlock(114, &b));
        UNIT_ASSERT_VALUES_EQUAL(blobId4, b.Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(4, b.Location.BlobOffset);

        UNIT_ASSERT(index->FindBlock(6, 114, &b));
        UNIT_ASSERT_VALUES_EQUAL(blobId4, b.Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(3, b.Location.BlobOffset);

        auto blocks = index->FindAllBlocks(range);
        Sort(blocks.begin(), blocks.end(), TBlockComp());

        for (const auto& b: blocks) {
            Cdbg << b.Block.BlockIndex << "@" << b.Block.MinCommitId << "-"
                 << b.Block.MaxCommitId << Endl;
            Cdbg << b.Location.BlobId << "+" << b.Location.BlobOffset << Endl;
        }

        UNIT_ASSERT_VALUES_EQUAL(7, blocks.size());

        UNIT_ASSERT_VALUES_EQUAL(110, blocks[0].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(6, blocks[0].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(7, blocks[0].Block.MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(blobId2, blocks[0].Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(6, blocks[0].Location.BlobOffset);

        UNIT_ASSERT_VALUES_EQUAL(110, blocks[1].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[1].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[1].Block.MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(blobId3, blocks[1].Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(7, blocks[1].Location.BlobOffset);

        UNIT_ASSERT_VALUES_EQUAL(111, blocks[2].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(6, blocks[2].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(7, blocks[2].Block.MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(blobId1, blocks[2].Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(6, blocks[2].Location.BlobOffset);

        UNIT_ASSERT_VALUES_EQUAL(111, blocks[3].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[3].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[3].Block.MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(blobId2, blocks[3].Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(8, blocks[3].Location.BlobOffset);

        UNIT_ASSERT_VALUES_EQUAL(112, blocks[4].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[4].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[4].Block.MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(blobId4, blocks[4].Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(1, blocks[4].Location.BlobOffset);

        UNIT_ASSERT_VALUES_EQUAL(114, blocks[5].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(6, blocks[5].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(7, blocks[5].Block.MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(blobId4, blocks[5].Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(3, blocks[5].Location.BlobOffset);

        UNIT_ASSERT_VALUES_EQUAL(114, blocks[6].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[6].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[6].Block.MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(blobId4, blocks[6].Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(4, blocks[6].Location.BlobOffset);

        auto blobIds = index->ExtractOverwrittenBlobIds();
        ASSERT_VECTOR_CONTENTS_EQUAL(
            TVector<TPartialBlobId>(blobIds.begin(), blobIds.end()),
            TVector<TPartialBlobId>(
                {blobId0, blobId1, blobId2, blobId3, blobId4}));
    }

    Y_UNIT_TEST(ShouldBuildIndexWithMultipleCheckpoints)
    {
        const auto bi = 1;

        const auto range = TBlockRange32::MakeOneBlock(bi);
        TMixedIndexBuilder builder(range);

        // added before first checkpoint, rebased
        builder.AddBlock({{bi, 2, 5, false}, {blobId1, 1}});
        // added after first checkpoint, before second checkpoint, not rebased
        builder.AddBlock({{bi, 4, 6, false}, {blobId2, 2}});
        // added after second checkpoint
        builder.AddBlock({{bi, 6, InvalidCommitId, false}, {blobId3, 3}});

        auto index = builder.Build({3, 5});

        auto blocks = index->FindAllBlocks(range);
        Sort(blocks.begin(), blocks.end(), TBlockComp());

        for (const auto& b: blocks) {
            Cdbg << b.Block.BlockIndex << "@" << b.Block.MinCommitId << "-"
                 << b.Block.MaxCommitId << Endl;
            Cdbg << b.Location.BlobId << "+" << b.Location.BlobOffset << Endl;
        }

        UNIT_ASSERT_VALUES_EQUAL(3, blocks.size());

        UNIT_ASSERT_VALUES_EQUAL(bi, blocks[0].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(3, blocks[0].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(5, blocks[0].Block.MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(blobId1, blocks[0].Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(1, blocks[0].Location.BlobOffset);

        UNIT_ASSERT_VALUES_EQUAL(bi, blocks[1].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(5, blocks[1].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(6, blocks[1].Block.MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(blobId2, blocks[1].Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(2, blocks[1].Location.BlobOffset);

        UNIT_ASSERT_VALUES_EQUAL(bi, blocks[2].Block.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[2].Block.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[2].Block.MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(blobId3, blocks[2].Location.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(3, blocks[2].Location.BlobOffset);
    }

    Y_UNIT_TEST(ShouldTrackOverwrittenBlobs)
    {
        TMixedIndex index;

        //      0   1   2   3   4
        // 1:   b0              b1
        // 2:       b2  b3  b4
        // now: b6  b5  b5  x

        index.SetOrUpdateBlock(0, {blobId0, 1});
        index.SetOrUpdateBlock(4, {blobId1, 1});
        index.OnCheckpoint(1);

        index.SetOrUpdateBlock(1, {blobId2, 1});
        index.SetOrUpdateBlock(3, {blobId4, 1});
        index.OnCheckpoint(2);

        index.SetOrUpdateBlock({{2, 2, InvalidCommitId, false}, {blobId3, 1}});

        index.SetOrUpdateBlock(0, {blobId5, 1});
        index.SetOrUpdateBlock(1, {blobId5, 2});
        index.SetOrUpdateBlock(2, {blobId5, 3});
        index.SetOrUpdateBlock(0, {blobId6, 1});
        index.ClearBlock(3);

        auto blobIds = index.ExtractOverwrittenBlobIds();
        ASSERT_VECTOR_CONTENTS_EQUAL(
            TVector<TPartialBlobId>(blobIds.begin(), blobIds.end()),
            TVector<TPartialBlobId>(
                {blobId0, blobId2, blobId3, blobId4, blobId5}));
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
