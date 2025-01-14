#include "large_blocks.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TVisitor: ILargeBlockVisitor
{
    TVector<TBlockDeletion> Deletions;

    void Accept(const TBlockDeletion& deletion) override
    {
        Deletions.push_back(deletion);
    }
};

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

        // simulating large truncate ops
        lb.AddDeletionMarker({nodeId1, ++commitId, 0, l});
        lb.AddDeletionMarker({nodeId1, commitId, l, l});
        lb.AddDeletionMarker({nodeId1, commitId, 2 * l, rem});

        auto one = lb.GetOne();
        UNIT_ASSERT(one.IsValid());
        UNIT_ASSERT_VALUES_EQUAL(nodeId1, one.NodeId);
        UNIT_ASSERT_VALUES_EQUAL(0, one.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(l, one.BlockCount);
        UNIT_ASSERT_VALUES_EQUAL(commitId, one.CommitId);

        // simulating writes
        UNIT_ASSERT(lb.ApplyDeletionMarkers(blobs[0]));
        for (auto& block: blobs[0]) {
            UNIT_ASSERT_VALUES_EQUAL(commitId, block.MaxCommitId);
        }

        auto processed = lb.ExtractProcessedDeletionMarkers();
        UNIT_ASSERT_VALUES_EQUAL(0, processed.size());

        for (ui32 i = 0; i < l / blobBlocks; ++i) {
            const bool affected = lb.ApplyDeletionMarkers(blobs[i]);
            if (i) {
                UNIT_ASSERT(affected);
            } else {
                UNIT_ASSERT(!affected);
            }
            for (auto& block: blobs[i]) {
                UNIT_ASSERT_VALUES_EQUAL(commitId, block.MaxCommitId);
                block.MaxCommitId = InvalidCommitId;
            }
        }

        // simulating reads
        TVisitor visitor;
        ui32 visitIndex = l;
        lb.FindBlocks(visitor, nodeId1, commitId + 1, visitIndex, blobBlocks);
        UNIT_ASSERT_VALUES_EQUAL(blobBlocks, visitor.Deletions.size());
        for (ui32 i = 0; i < blobBlocks; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                nodeId1,
                visitor.Deletions[i].NodeId);
            UNIT_ASSERT_VALUES_EQUAL(
                visitIndex + i,
                visitor.Deletions[i].BlockIndex);
            UNIT_ASSERT_VALUES_EQUAL(
                commitId,
                visitor.Deletions[i].CommitId);
        }

        visitor.Deletions.clear();
        visitIndex = 2 * l + rem - blobBlocks / 2;
        lb.FindBlocks(visitor, nodeId1, commitId + 1, visitIndex, blobBlocks);
        UNIT_ASSERT_VALUES_EQUAL(blobBlocks / 2, visitor.Deletions.size());
        for (ui32 i = 0; i < blobBlocks / 2; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                nodeId1,
                visitor.Deletions[i].NodeId);
            UNIT_ASSERT_VALUES_EQUAL(
                visitIndex + i,
                visitor.Deletions[i].BlockIndex);
            UNIT_ASSERT_VALUES_EQUAL(
                commitId,
                visitor.Deletions[i].CommitId);
        }

        processed = lb.ExtractProcessedDeletionMarkers();
        UNIT_ASSERT_VALUES_EQUAL(0, processed.size());

        // simulating Cleanup ops
        UNIT_ASSERT(lb.ApplyDeletionMarkers(blobs[0]));
        for (const auto& block: blobs[0]) {
            UNIT_ASSERT_VALUES_EQUAL(commitId, block.MaxCommitId);
        }
        lb.MarkProcessed(nodeId1, commitId, blobs[0][0].BlockIndex, blobBlocks);

        for (ui32 i = 0; i < l / blobBlocks; ++i) {
            processed = lb.ExtractProcessedDeletionMarkers();
            UNIT_ASSERT_VALUES_EQUAL(0, processed.size());
            const bool affected = lb.ApplyDeletionMarkers(blobs[i]);
            if (i) {
                UNIT_ASSERT(affected);
            } else {
                UNIT_ASSERT(!affected);
            }
            for (const auto& block: blobs[i]) {
                UNIT_ASSERT_VALUES_EQUAL(commitId, block.MaxCommitId);
            }
            lb.MarkProcessed(
                nodeId1,
                commitId,
                blobs[i][0].BlockIndex,
                blobBlocks);
        }

        processed = lb.ExtractProcessedDeletionMarkers();
        UNIT_ASSERT_VALUES_EQUAL(1, processed.size());
        UNIT_ASSERT_VALUES_EQUAL(nodeId1, processed[0].NodeId);
        UNIT_ASSERT_VALUES_EQUAL(0, processed[0].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(l, processed[0].BlockCount);
        UNIT_ASSERT_VALUES_EQUAL(commitId, processed[0].CommitId);

        one = lb.GetOne();
        UNIT_ASSERT(one.IsValid());
        UNIT_ASSERT_VALUES_EQUAL(nodeId1, one.NodeId);
        UNIT_ASSERT_VALUES_EQUAL(l, one.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(l, one.BlockCount);
        UNIT_ASSERT_VALUES_EQUAL(commitId, one.CommitId);

        for (ui32 i = l / blobBlocks; i < (2 * l + rem) / blobBlocks; ++i) {
            UNIT_ASSERT(lb.ApplyDeletionMarkers(blobs[i]));
            for (const auto& block: blobs[i]) {
                UNIT_ASSERT_VALUES_EQUAL(commitId, block.MaxCommitId);
            }
            lb.MarkProcessed(
                nodeId1,
                commitId,
                blobs[i][0].BlockIndex,
                blobBlocks);
        }

        visitor.Deletions.clear();
        lb.FindBlocks(visitor, nodeId1, commitId + 1, visitIndex, blobBlocks);
        UNIT_ASSERT_VALUES_EQUAL(0, visitor.Deletions.size());

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

        one = lb.GetOne();
        UNIT_ASSERT(!one.IsValid());
    }

    Y_UNIT_TEST(ShouldWorkWithCommitIdsAndNodeIdsProperly)
    {
        const ui64 nodeId1 = 111;
        const ui64 nodeId2 = 222;

        const ui64 l = 1_GB / 4_KB;

        TLargeBlocks lb(TDefaultAllocator::Instance());

        TVector<TBlock> blocks{
            {nodeId1, 1024, 10001, InvalidCommitId},
            {nodeId1, 1025, 10005, InvalidCommitId},
            {nodeId1, 1026, 10007, InvalidCommitId},
            {nodeId1, 1027, 10006, InvalidCommitId},
            {nodeId1, 1060, 10008, InvalidCommitId},
            {nodeId2, 10240, 10002, InvalidCommitId},
            {nodeId2, 10241, 10002, InvalidCommitId},
            {nodeId2, 10242, 10012, InvalidCommitId},
        };

        // simulating large truncate ops
        lb.AddDeletionMarker({nodeId1, 10002, 100, l});
        lb.AddDeletionMarker({nodeId1, 10007, 1026, l});

        auto one = lb.GetOne();
        UNIT_ASSERT(one.IsValid());
        UNIT_ASSERT_VALUES_EQUAL(nodeId1, one.NodeId);
        UNIT_ASSERT_VALUES_EQUAL(100, one.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(l, one.BlockCount);
        UNIT_ASSERT_VALUES_EQUAL(10002, one.CommitId);

        // simulating writes
        UNIT_ASSERT(lb.ApplyDeletionMarkers(blocks));
        // affected by marker1
        UNIT_ASSERT_VALUES_EQUAL(10002, blocks[0].MaxCommitId);
        // not affected by marker1 due to marker commitId not being newer than
        // block commitId
        // not affected by marker2 due to non-overlapping ranges
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[1].MaxCommitId);
        // not affected by marker1 and marker2 due to marker commitId not being
        // newer than block commitId
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[2].MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(10007, blocks[3].MaxCommitId);
        // not affected by marker1 and marker2 due to marker commitId not being
        // newer than block commitId
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[4].MaxCommitId);
        // the following 3 are not affected because there are no markers for
        // nodeId2
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[5].MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[6].MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[7].MaxCommitId);

        UNIT_ASSERT(!lb.ApplyDeletionMarkers(blocks));

        auto processed = lb.ExtractProcessedDeletionMarkers();
        UNIT_ASSERT_VALUES_EQUAL(0, processed.size());

        // simulating reads
        TVisitor visitor;
        ui32 visitIndex = 10000;
        lb.AddDeletionMarker({nodeId2, 10003, visitIndex, l});
        lb.FindBlocks(visitor, nodeId2, 10002, visitIndex, 241);
        UNIT_ASSERT_VALUES_EQUAL(0, visitor.Deletions.size());
        lb.FindBlocks(visitor, nodeId2, 10003, visitIndex, 241);
        UNIT_ASSERT_VALUES_EQUAL(241, visitor.Deletions.size());
        for (ui32 i = 0; i < visitor.Deletions.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                visitIndex + i,
                visitor.Deletions[i].BlockIndex);
            UNIT_ASSERT_VALUES_EQUAL(10003, visitor.Deletions[i].CommitId);
            UNIT_ASSERT_VALUES_EQUAL(nodeId2, visitor.Deletions[i].NodeId);
        }

        for (auto& block: blocks) {
            block.MaxCommitId = InvalidCommitId;
        }

        // simulating Cleanup ops
        lb.MarkProcessed(nodeId1, 10009, 100, 1024);

        processed = lb.ExtractProcessedDeletionMarkers();
        UNIT_ASSERT_VALUES_EQUAL(0, processed.size());

        lb.MarkProcessed(nodeId1, 10009, 1124, 100 + l - 1124);

        processed = lb.ExtractProcessedDeletionMarkers();
        UNIT_ASSERT_VALUES_EQUAL(1, processed.size());
        UNIT_ASSERT_VALUES_EQUAL(nodeId1, processed[0].NodeId);
        UNIT_ASSERT_VALUES_EQUAL(100, processed[0].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(l, processed[0].BlockCount);
        UNIT_ASSERT_VALUES_EQUAL(10002, processed[0].CommitId);

        one = lb.GetOne();
        UNIT_ASSERT(one.IsValid());
        // TLargeBlocks::TImpl contains a THashMap for NodeIds inside it so
        // actually it's not guaranteed that nodeId2 deletion marker will be
        // returned (and not nodeId1 deletion marker)
        UNIT_ASSERT_VALUES_EQUAL(nodeId2, one.NodeId);
        UNIT_ASSERT_VALUES_EQUAL(visitIndex, one.BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(l, one.BlockCount);
        UNIT_ASSERT_VALUES_EQUAL(10003, one.CommitId);
    }

    Y_UNIT_TEST(ShouldExtractDeletionMarkersForAllCommits)
    {
        const ui64 nodeId1 = 111;

        const ui64 l = 1_GB / 4_KB;

        TLargeBlocks lb(TDefaultAllocator::Instance());

        lb.AddDeletionMarker(TDeletionMarker(nodeId1, 1001, 0, l));
        lb.AddDeletionMarker(TDeletionMarker(nodeId1, 1002, 0, l));
        lb.AddDeletionMarker(TDeletionMarker(nodeId1, 1003, 0, l));

        while (true) {
            const auto one = lb.GetOne();
            if (!one.IsValid()) {
                break;
            }

            lb.MarkProcessed(nodeId1, 1004, one.BlockIndex, one.BlockCount);
        }

        const auto processed = lb.ExtractProcessedDeletionMarkers();
        UNIT_ASSERT_VALUES_EQUAL(3, processed.size());
        UNIT_ASSERT_VALUES_EQUAL(nodeId1, processed[0].NodeId);
        UNIT_ASSERT_VALUES_EQUAL(1001, processed[0].CommitId);
        UNIT_ASSERT_VALUES_EQUAL(0, processed[0].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(l, processed[0].BlockCount);
        UNIT_ASSERT_VALUES_EQUAL(nodeId1, processed[1].NodeId);
        UNIT_ASSERT_VALUES_EQUAL(1002, processed[1].CommitId);
        UNIT_ASSERT_VALUES_EQUAL(0, processed[1].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(l, processed[1].BlockCount);
        UNIT_ASSERT_VALUES_EQUAL(nodeId1, processed[2].NodeId);
        UNIT_ASSERT_VALUES_EQUAL(1003, processed[2].CommitId);
        UNIT_ASSERT_VALUES_EQUAL(0, processed[2].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(l, processed[2].BlockCount);
    }
}

}   // namespace NCloud::NFileStore::NStorage
