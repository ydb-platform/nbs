#include "flush_blocks_visitor.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <cloud/storage/core/libs/common/block_data_ref.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 BlockSize = 4;
constexpr ui32 CompactionRangeSize = 8;
constexpr ui32 CompactionThreshold = 8;
constexpr ui32 MaxBlobRangeSize = 32 * CompactionRangeSize;
constexpr ui32 MaxBlocksInBlob = MaxBlobRangeSize;
constexpr ui64 TabletId = 1;

TString MakeBlockContent(size_t visitOrderIndex)
{
    const char fill = static_cast<char>('a' + (visitOrderIndex % 26));
    return TString(BlockSize, fill);
}

ui32 ExpectedChecksum(size_t visitOrderIndex)
{
    const TString content = MakeBlockContent(visitOrderIndex);
    return ComputeDefaultDigest(TBlockDataRef(content.data(), content.size()));
}

void VisitDataBlock(
    TFlushBlocksVisitor& visitor,
    ui32 blockIndex,
    const TString& content)
{
    const TFreshBlock block{
        TBlock(blockIndex, 1, false),
        content,
        {}
    };

    visitor.Visit(block);
}

TVector<TFlushBlocksVisitor::TBlob> BuildBlobs(
    const TVector<ui32>& blockIndices,
    bool readBlockMaskOnCompactionOptimizationEnabled,
    ui64 splitByCompactionRangeMaxBlobCount,
    ui64 diskPrefixLengthWithBlockChecksumsInBlobs = 0,
    ui64 writeBlobSizeThreshold = 1)
{
    TCompactionMap compactionMap(
        CompactionRangeSize,
        BuildDefaultCompactionPolicy(CompactionThreshold, 0));

    TVector<TFlushBlocksVisitor::TBlob> blobs;
    TFlushBlocksVisitor visitor(
        BlockSize,
        /*flushBlobSizeThreshold*/ 1,
        MaxBlobRangeSize,
        MaxBlocksInBlob,
        diskPrefixLengthWithBlockChecksumsInBlobs,
        compactionMap,
        readBlockMaskOnCompactionOptimizationEnabled,
        splitByCompactionRangeMaxBlobCount,
        TabletId,
        writeBlobSizeThreshold,
        blobs);

    for (size_t i = 0; i < blockIndices.size(); ++i) {
        const TString content = MakeBlockContent(i);
        VisitDataBlock(visitor, blockIndices[i], content);
    }

    visitor.Finish();
    return blobs;
}

TFlushBlocksVisitor::TBlob BuildBlob(const TVector<ui32>& blockIndices,
                                     bool withContent = true)
{
    TBlockBuffer blobContent;
    TVector<TBlock> blocks;
    blocks.reserve(blockIndices.size());

    for (const ui32 blockIndex: blockIndices) {
        if (withContent) {
            blobContent.AddBlock(BlockSize, 'x');
        }
        blocks.emplace_back(blockIndex, 1, false);
    }

    return {
        std::move(blobContent),
        std::move(blocks),
        {},
        0,
    };
}

EChannelDataKind ChooseChannelDataKind(
    TFlushBlocksVisitor::TBlob& blob, ui32 writeBlobThreshold,
    ui32 localRangeBlockCount, double localRangeFillThreshold,
    double localRangesFilledThreshold)
{
    NProto::TStorageServiceConfig storageServiceConfig;
    storageServiceConfig.SetWriteBlobThreshold(writeBlobThreshold);
    storageServiceConfig.SetLocalRangeSizeForChannelDataKindCalculation(
        localRangeBlockCount * BlockSize);
    storageServiceConfig
        .SetLocalRangeFillThresholdForChannelDataKindCalculation(
            localRangeFillThreshold);
    storageServiceConfig.SetLocalRangesFilledForBlobChannelDataKindCalculation(
        localRangesFilledThreshold);

    const TStorageConfig config(std::move(storageServiceConfig), nullptr);

    NProto::TPartitionConfig partitionConfig;
    partitionConfig.SetBlockSize(BlockSize);
    partitionConfig.SetStorageMediaKind(
        NProto::EStorageMediaKind::STORAGE_MEDIA_HDD);

    return ChooseChannelDataKindForFlushBlob(config, partitionConfig, blob);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFlushBlocksVisitorTest)
{
    Y_UNIT_TEST(ShouldCalculateCompactionRangeCount)
    {
        {
            const auto blobs = BuildBlobs(
                {1, 2, 10, 11, 20},
                /*readBlockMaskOnCompactionOptimizationEnabled*/ true,
                /*splitByCompactionRangeMaxBlobCount*/ 0);

            UNIT_ASSERT_VALUES_EQUAL(1, blobs.size());
            UNIT_ASSERT_VALUES_EQUAL(5, blobs[0].Blocks.size());
            UNIT_ASSERT_VALUES_EQUAL(3, blobs[0].CompactionRangeCount);
        }

        {
            const auto blobs = BuildBlobs(
                {1,
                 2,
                 MaxBlobRangeSize + 1,
                 MaxBlobRangeSize + 2,
                 2 * MaxBlobRangeSize,
                 2 * MaxBlobRangeSize + CompactionRangeSize,
                 2 * MaxBlobRangeSize + 2 * CompactionRangeSize},
                /*readBlockMaskOnCompactionOptimizationEnabled*/ true,
                /*splitByCompactionRangeMaxBlobCount*/ 0);

            UNIT_ASSERT_VALUES_EQUAL(3, blobs.size());

            UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].Blocks.size());
            UNIT_ASSERT_VALUES_EQUAL(1, blobs[0].CompactionRangeCount);

            UNIT_ASSERT_VALUES_EQUAL(2, blobs[1].Blocks.size());
            UNIT_ASSERT_VALUES_EQUAL(1, blobs[1].CompactionRangeCount);

            UNIT_ASSERT_VALUES_EQUAL(3, blobs[2].Blocks.size());
            UNIT_ASSERT_VALUES_EQUAL(3, blobs[2].CompactionRangeCount);
        }

        {
            const auto blobs = BuildBlobs(
                {1,
                 2,
                 MaxBlobRangeSize + 1,
                 MaxBlobRangeSize + 2,
                 2 * MaxBlobRangeSize,
                 2 * MaxBlobRangeSize + CompactionRangeSize,
                 2 * MaxBlobRangeSize + 2 * CompactionRangeSize},
                /*readBlockMaskOnCompactionOptimizationEnabled*/ false,
                /*splitByCompactionRangeMaxBlobCount*/ 0);

            UNIT_ASSERT_VALUES_EQUAL(3, blobs.size());

            UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].Blocks.size());
            UNIT_ASSERT_VALUES_EQUAL(0, blobs[0].CompactionRangeCount);

            UNIT_ASSERT_VALUES_EQUAL(2, blobs[1].Blocks.size());
            UNIT_ASSERT_VALUES_EQUAL(0, blobs[1].CompactionRangeCount);

            UNIT_ASSERT_VALUES_EQUAL(3, blobs[2].Blocks.size());
            UNIT_ASSERT_VALUES_EQUAL(0, blobs[2].CompactionRangeCount);
        }
    }

    Y_UNIT_TEST(ShouldSplitBlobByCompactionRangeBorders)
    {
        const auto blobs = BuildBlobs(
            {1, 2, 10, 11, 20},
            /*splitOptimizationEnabled*/ true,
            /*splitByCompactionRangeMaxBlobCount*/ 3);

        UNIT_ASSERT_VALUES_EQUAL(3, blobs.size());

        UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(1, blobs[0].Blocks[0].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].Blocks[1].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(1, blobs[0].CompactionRangeCount);

        UNIT_ASSERT_VALUES_EQUAL(2, blobs[1].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(10, blobs[1].Blocks[0].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(11, blobs[1].Blocks[1].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(1, blobs[1].CompactionRangeCount);

        UNIT_ASSERT_VALUES_EQUAL(1, blobs[2].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(20, blobs[2].Blocks[0].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(1, blobs[2].CompactionRangeCount);
    }

    Y_UNIT_TEST(ShouldNotSplitBlobIfRangeCountIsGreaterThanLimit)
    {
        const auto blobs = BuildBlobs(
            {1, 2, 10, 11, 20},
            /*splitOptimizationEnabled*/ true,
            /*splitByCompactionRangeMaxBlobCount*/ 2);

        UNIT_ASSERT_VALUES_EQUAL(1, blobs.size());
        UNIT_ASSERT_VALUES_EQUAL(5, blobs[0].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(3, blobs[0].CompactionRangeCount);
    }

    Y_UNIT_TEST(ShouldNotSplitBlobIfOptimizationIsDisabled)
    {
        const auto blobs = BuildBlobs(
            {1, 2, 10, 11, 20},
            /*splitOptimizationEnabled*/ false,
            /*splitByCompactionRangeMaxBlobCount*/ 3);

        UNIT_ASSERT_VALUES_EQUAL(1, blobs.size());
        UNIT_ASSERT_VALUES_EQUAL(5, blobs[0].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(0, blobs[0].CompactionRangeCount);
    }

    Y_UNIT_TEST(ShouldSplitChecksumsByCompactionRangeBorders)
    {
        // All 5 blocks are below the checksum boundary => Checksums.size() ==
        // Blocks.size(). After the split into 3 compaction-range pieces the
        // checksums of each block must remain attached to that block.
        const auto blobs = BuildBlobs(
            {1, 2, 10, 11, 20},
            /*splitOptimizationEnabled*/ true,
            /*splitByCompactionRangeMaxBlobCount*/ 3,
            /*diskPrefixLengthWithBlockChecksumsInBlobs*/ 100 * BlockSize);

        UNIT_ASSERT_VALUES_EQUAL(3, blobs.size());

        UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].Checksums.size());
        UNIT_ASSERT_VALUES_EQUAL(ExpectedChecksum(0), blobs[0].Checksums[0]);
        UNIT_ASSERT_VALUES_EQUAL(ExpectedChecksum(1), blobs[0].Checksums[1]);

        UNIT_ASSERT_VALUES_EQUAL(2, blobs[1].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(2, blobs[1].Checksums.size());
        UNIT_ASSERT_VALUES_EQUAL(ExpectedChecksum(2), blobs[1].Checksums[0]);
        UNIT_ASSERT_VALUES_EQUAL(ExpectedChecksum(3), blobs[1].Checksums[1]);

        UNIT_ASSERT_VALUES_EQUAL(1, blobs[2].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(1, blobs[2].Checksums.size());
        UNIT_ASSERT_VALUES_EQUAL(ExpectedChecksum(4), blobs[2].Checksums[0]);
    }

    Y_UNIT_TEST(ShouldSplitBlobWithPartialChecksumsAcrossRangeBoundary)
    {
        // Checksum boundary = 15 blocks => blocks {1, 2, 10, 11} are
        // checksummed and Checksums.size() == 4, but block 20 is above the
        // boundary so the trailing piece must end up with an empty Checksums
        // vector while still preserving the checksums for the earlier pieces.
        const auto blobs = BuildBlobs(
            {1, 2, 10, 11, 20},
            /*splitOptimizationEnabled*/ true,
            /*splitByCompactionRangeMaxBlobCount*/ 3,
            /*diskPrefixLengthWithBlockChecksumsInBlobs*/ 15 * BlockSize);

        UNIT_ASSERT_VALUES_EQUAL(3, blobs.size());

        UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].Checksums.size());
        UNIT_ASSERT_VALUES_EQUAL(ExpectedChecksum(0), blobs[0].Checksums[0]);
        UNIT_ASSERT_VALUES_EQUAL(ExpectedChecksum(1), blobs[0].Checksums[1]);

        UNIT_ASSERT_VALUES_EQUAL(2, blobs[1].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(2, blobs[1].Checksums.size());
        UNIT_ASSERT_VALUES_EQUAL(ExpectedChecksum(2), blobs[1].Checksums[0]);
        UNIT_ASSERT_VALUES_EQUAL(ExpectedChecksum(3), blobs[1].Checksums[1]);

        UNIT_ASSERT_VALUES_EQUAL(1, blobs[2].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(0, blobs[2].Checksums.size());
    }

    Y_UNIT_TEST(ShouldSplitBlobWithChecksumBoundaryInsidePiece)
    {
        // Checksum boundary = 11 blocks => blocks {1, 2, 10} are checksummed
        // (Checksums.size() == 3), block 11 lies inside the second
        // compaction-range piece but is above the boundary, and block 20 is
        // also above the boundary. The middle piece must therefore receive
        // exactly one checksum (for block 10) and the last piece must end up
        // with no checksums.
        const auto blobs = BuildBlobs(
            {1, 2, 10, 11, 20},
            /*splitOptimizationEnabled*/ true,
            /*splitByCompactionRangeMaxBlobCount*/ 3,
            /*diskPrefixLengthWithBlockChecksumsInBlobs*/ 11 * BlockSize);

        UNIT_ASSERT_VALUES_EQUAL(3, blobs.size());

        UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].Checksums.size());
        UNIT_ASSERT_VALUES_EQUAL(ExpectedChecksum(0), blobs[0].Checksums[0]);
        UNIT_ASSERT_VALUES_EQUAL(ExpectedChecksum(1), blobs[0].Checksums[1]);

        UNIT_ASSERT_VALUES_EQUAL(2, blobs[1].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(1, blobs[1].Checksums.size());
        UNIT_ASSERT_VALUES_EQUAL(ExpectedChecksum(2), blobs[1].Checksums[0]);

        UNIT_ASSERT_VALUES_EQUAL(1, blobs[2].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(0, blobs[2].Checksums.size());
    }

    Y_UNIT_TEST(ShouldSplitWhenOriginalBlobIsSmallerThanWriteBlobThreshold)
    {
        // BlockSize = 4, blocks {1, 2, 10, 11, 20} => original blob is
        // 5 * 4 = 20 bytes which is below the WriteBlobSizeThreshold (100).
        // The split pieces are 8, 8 and 4 bytes — also all below the
        // threshold. Hugeness of the original blob matches hugeness of every
        // piece (none of them is huge), so the blob must be split.
        const auto blobs = BuildBlobs(
            {1, 2, 10, 11, 20},
            /*splitOptimizationEnabled*/ true,
            /*splitByCompactionRangeMaxBlobCount*/ 3,
            /*diskPrefixLengthWithBlockChecksumsInBlobs*/ 0,
            /*writeBlobSizeThreshold*/ 100);

        UNIT_ASSERT_VALUES_EQUAL(3, blobs.size());

        UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(1, blobs[0].Blocks[0].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].Blocks[1].BlockIndex);

        UNIT_ASSERT_VALUES_EQUAL(2, blobs[1].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(10, blobs[1].Blocks[0].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(11, blobs[1].Blocks[1].BlockIndex);

        UNIT_ASSERT_VALUES_EQUAL(1, blobs[2].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(20, blobs[2].Blocks[0].BlockIndex);
    }

    Y_UNIT_TEST(ShouldSplitWhenOriginalAndAllSplitBlobsAreHuge)
    {
        // BlockSize = 4, blocks {1, 2, 10, 11, 20} => original blob is 20
        // bytes (huge w.r.t. threshold 4). Pieces are 8, 8 and 4 bytes — all
        // >= 4, i.e. all huge. Hugeness matches between the original and
        // every piece, so the blob must be split.
        const auto blobs = BuildBlobs(
            {1, 2, 10, 11, 20},
            /*splitOptimizationEnabled*/ true,
            /*splitByCompactionRangeMaxBlobCount*/ 3,
            /*diskPrefixLengthWithBlockChecksumsInBlobs*/ 0,
            /*writeBlobSizeThreshold*/ 4);

        UNIT_ASSERT_VALUES_EQUAL(3, blobs.size());

        UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(1, blobs[0].Blocks[0].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].Blocks[1].BlockIndex);

        UNIT_ASSERT_VALUES_EQUAL(2, blobs[1].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(10, blobs[1].Blocks[0].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(11, blobs[1].Blocks[1].BlockIndex);

        UNIT_ASSERT_VALUES_EQUAL(1, blobs[2].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(20, blobs[2].Blocks[0].BlockIndex);
    }

    Y_UNIT_TEST(ShouldNotSplitWhenOriginalIsHugeButSomeSplitBlobsAreNotHuge)
    {
        // BlockSize = 4, blocks {1, 2, 10, 11, 20} => original blob is 20
        // bytes (huge w.r.t. threshold 8). Pieces are 8, 8 and 4 bytes —
        // the last piece (4 bytes) is below the threshold, so it is NOT
        // huge while the original blob IS huge. Splitting would turn a huge
        // blob into a mix of huge and small pieces, so it must be skipped.
        const auto blobs = BuildBlobs(
            {1, 2, 10, 11, 20},
            /*splitOptimizationEnabled*/ true,
            /*splitByCompactionRangeMaxBlobCount*/ 3,
            /*diskPrefixLengthWithBlockChecksumsInBlobs*/ 0,
            /*writeBlobSizeThreshold*/ 8);

        UNIT_ASSERT_VALUES_EQUAL(1, blobs.size());
        UNIT_ASSERT_VALUES_EQUAL(5, blobs[0].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(3, blobs[0].CompactionRangeCount);
        UNIT_ASSERT_VALUES_EQUAL(1, blobs[0].Blocks[0].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(2, blobs[0].Blocks[1].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(10, blobs[0].Blocks[2].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(11, blobs[0].Blocks[3].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(20, blobs[0].Blocks[4].BlockIndex);
    }

    Y_UNIT_TEST(ShouldKeepChecksumsTogetherWhenBlobIsNotSplit)
    {
        // splitByCompactionRangeMaxBlobCount = 2 is below the 3 compaction
        // ranges spanned by the visited blocks, so the blob must NOT be split.
        // All checksums should remain in a single blob in visit order.
        const auto blobs = BuildBlobs(
            {1, 2, 10, 11, 20},
            /*splitOptimizationEnabled*/ true,
            /*splitByCompactionRangeMaxBlobCount*/ 2,
            /*diskPrefixLengthWithBlockChecksumsInBlobs*/ 100 * BlockSize);

        UNIT_ASSERT_VALUES_EQUAL(1, blobs.size());
        UNIT_ASSERT_VALUES_EQUAL(5, blobs[0].Blocks.size());
        UNIT_ASSERT_VALUES_EQUAL(5, blobs[0].Checksums.size());
        for (size_t i = 0; i < 5; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(ExpectedChecksum(i), blobs[0].Checksums[i]);
        }
    }

    Y_UNIT_TEST(ShouldChooseMergedChannelForHugeDenseBlob)
    {
        // Both local ranges have exactly two out of four blocks filled. The
        // blob size and local-range density are exactly at their thresholds.
        auto blob = BuildBlob({0, 1, 4, 5});

        UNIT_ASSERT(
            ChooseChannelDataKind(blob, /*writeBlobThreshold*/ 4 * BlockSize,
                                  /*localRangeBlockCount*/ 4,
                                  /*localRangeFillThreshold*/ 0.5,
                                  /*localRangesFilledThreshold*/ 1.0) ==
            EChannelDataKind::Merged);
    }

    Y_UNIT_TEST(ShouldChooseMergedChannelAtBlobDensityThreshold)
    {
        // The first local range is dense and the second is sparse, so exactly
        // half of the ranges touched by the blob are dense.
        auto blob = BuildBlob({0, 1, 4});

        UNIT_ASSERT(
            ChooseChannelDataKind(blob, /*writeBlobThreshold*/ 3 * BlockSize,
                                  /*localRangeBlockCount*/ 4,
                                  /*localRangeFillThreshold*/ 0.5,
                                  /*localRangesFilledThreshold*/ 0.5) ==
            EChannelDataKind::Merged);
    }

    Y_UNIT_TEST(ShouldChooseMixedChannelForSmallBlob)
    {
        auto blob = BuildBlob({0, 1, 4, 5});

        UNIT_ASSERT(
            ChooseChannelDataKind(
                blob, /*writeBlobThreshold*/ 4 * BlockSize + 1,
                /*localRangeBlockCount*/ 4, /*localRangeFillThreshold*/ 0.5,
                /*localRangesFilledThreshold*/ 1.0) == EChannelDataKind::Mixed);
    }

    Y_UNIT_TEST(ShouldChooseMixedChannelForSparseBlob)
    {
        // The adjacent indices straddle a local-range boundary, so each range
        // has only one out of four blocks filled.
        auto blob = BuildBlob({3, 4});

        UNIT_ASSERT(
            ChooseChannelDataKind(
                blob, /*writeBlobThreshold*/ 2 * BlockSize,
                /*localRangeBlockCount*/ 4, /*localRangeFillThreshold*/ 0.5,
                /*localRangesFilledThreshold*/ 0.5) == EChannelDataKind::Mixed);
    }

    Y_UNIT_TEST(ShouldCountOnlyUniqueBlocksForLocalRangeDensity)
    {
        // Multiple versions of the same block do not fill more positions in
        // the local range.
        auto blob = BuildBlob({0, 0, 0, 0, 4, 4, 4, 4});

        UNIT_ASSERT(
            ChooseChannelDataKind(
                blob, /*writeBlobThreshold*/ BlockSize,
                /*localRangeBlockCount*/ 4, /*localRangeFillThreshold*/ 0.5,
                /*localRangesFilledThreshold*/ 0.5) == EChannelDataKind::Mixed);
    }

    Y_UNIT_TEST(ShouldChooseMixedChannelForZeroBlob)
    {
        auto blob = BuildBlob({0, 1, 4, 5}, /*withContent*/ false);

        UNIT_ASSERT(
            ChooseChannelDataKind(
                blob, /*writeBlobThreshold*/ 0, /*localRangeBlockCount*/ 4,
                /*localRangeFillThreshold*/ 0.5,
                /*localRangesFilledThreshold*/ 1.0) == EChannelDataKind::Mixed);
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
