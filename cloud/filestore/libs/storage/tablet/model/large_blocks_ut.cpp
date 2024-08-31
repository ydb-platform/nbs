#include "large_blocks.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLargeBlocksTest)
{
    Y_UNIT_TEST(ShouldTrackDeletionMarkers)
    {
        const ui64 nodeId1 = 111;

        const ui64 l = 1_GB / 4_KB;
        const ui64 rem = 128_MB / 4_KB;
        const ui32 blobBlocks = 4_MB / 4_KB;

        TLargeBlocks lb(TDefaultAllocator::Instance());

        ui64 commitId = 1;
        TVector<TVector<TBlock>> blobs;
        ui64 blockIndex = 0;
        while (blockIndex < 2 * l + rem) {
            ++commitId;
            auto& blob = blobs.emplace_back();
            for (ui32 i = 0; i < blobBlocks; ++i) {
                blob.emplace_back(
                    nodeId1,
                    blockIndex++,
                    commitId,
                    InvalidCommitId);
            }
        }

        lb.AddDeletionMarker({nodeId1, ++commitId, 0, l});
        lb.AddDeletionMarker({nodeId1, commitId, l, l});
        lb.AddDeletionMarker({nodeId1, commitId, 2 * l, rem});

        lb.ApplyDeletionMarkers(blobs[0]);
        for (auto& block: blobs[0]) {
            UNIT_ASSERT_VALUES_EQUAL(commitId, block.MaxCommitId);
            block.MaxCommitId = InvalidCommitId;
        }

        auto processed = lb.ExtractProcessedDeletionMarkers();
        UNIT_ASSERT_VALUES_EQUAL(0, processed.size());

        for (ui32 i = 0; i < l / blobBlocks; ++i) {
            lb.ApplyDeletionMarkers(blobs[i]);
            for (auto& block: blobs[i]) {
                UNIT_ASSERT_VALUES_EQUAL(commitId, block.MaxCommitId);
                block.MaxCommitId = InvalidCommitId;
            }
        }

        processed = lb.ExtractProcessedDeletionMarkers();
        UNIT_ASSERT_VALUES_EQUAL(0, processed.size());

        lb.ApplyAndUpdateDeletionMarkers(blobs[0]);
        for (const auto& block: blobs[0]) {
            UNIT_ASSERT_VALUES_EQUAL(commitId, block.MaxCommitId);
        }

        for (ui32 i = 0; i < l / blobBlocks; ++i) {
            processed = lb.ExtractProcessedDeletionMarkers();
            UNIT_ASSERT_VALUES_EQUAL(0, processed.size());
            lb.ApplyAndUpdateDeletionMarkers(blobs[i]);
            for (const auto& block: blobs[i]) {
                UNIT_ASSERT_VALUES_EQUAL(commitId, block.MaxCommitId);
            }
        }

        processed = lb.ExtractProcessedDeletionMarkers();
        UNIT_ASSERT_VALUES_EQUAL(1, processed.size());
        UNIT_ASSERT_VALUES_EQUAL(nodeId1, processed[0].NodeId);
        UNIT_ASSERT_VALUES_EQUAL(0, processed[0].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(l, processed[0].BlockCount);
        UNIT_ASSERT_VALUES_EQUAL(commitId, processed[0].CommitId);

        for (ui32 i = l / blobBlocks; i < (2 * l + rem) / blobBlocks; ++i) {
            lb.ApplyAndUpdateDeletionMarkers(blobs[i]);
            for (const auto& block: blobs[i]) {
                UNIT_ASSERT_VALUES_EQUAL(commitId, block.MaxCommitId);
            }
        }

        processed = lb.ExtractProcessedDeletionMarkers();
        UNIT_ASSERT_VALUES_EQUAL(2, processed.size());
        UNIT_ASSERT_VALUES_EQUAL(nodeId1, processed[0].NodeId);
        UNIT_ASSERT_VALUES_EQUAL(l, processed[0].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(l, processed[0].BlockCount);
        UNIT_ASSERT_VALUES_EQUAL(commitId, processed[0].CommitId);
        UNIT_ASSERT_VALUES_EQUAL(nodeId1, processed[1].NodeId);
        UNIT_ASSERT_VALUES_EQUAL(2 * l, processed[1].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(rem, processed[1].BlockCount);
        UNIT_ASSERT_VALUES_EQUAL(commitId, processed[1].CommitId);
    }
}

}   // namespace NCloud::NFileStore::NStorage
