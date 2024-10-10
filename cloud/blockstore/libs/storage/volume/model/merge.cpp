#include "merge.h"

#include <cloud/blockstore/libs/storage/volume/model/stripe.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void MergeStripedBitMask(
    const TBlockRange64& originalRange,
    const TBlockRange64& stripeRange,
    const ui32 blocksPerStripe,
    const ui32 partitionsCount,
    const ui32 partitionId,
    const TString& srcMask,
    TString& dstMask)
{
    const auto actualDstSize = (originalRange.Size() + 7) / 8;

    if (dstMask.size() < actualDstSize) {
        dstMask.resize(actualDstSize, 0);
    }

    for (ui32 i = 0; i < static_cast<ui32>(srcMask.size()); ++i) {
        for (ui32 bitIdx = 0; bitIdx < 8; ++bitIdx) {
            auto bit = srcMask[i] & (1 << bitIdx);
            if (!bit) {
                continue;
            }

            const auto index = RelativeToGlobalIndex(
                blocksPerStripe,
                stripeRange.Start + i * 8 + bitIdx,
                partitionsCount,
                partitionId
            );

            int blockIdx = index - originalRange.Start;
            int maskChunk = blockIdx / 8;
            int maskPos = blockIdx % 8;

            dstMask[maskChunk] = dstMask[maskChunk] | (1 << maskPos);
        }
    }
}

void MergeDescribeBlocksResponse(
    NProto::TDescribeBlocksResponse& src,
    NProto::TDescribeBlocksResponse& dst,
    const ui32 blocksPerStripe,
    const ui32 blockSize,
    const ui32 partitionsCount,
    const ui32 partitionId)
{
    for (const auto& freshBlockRange: src.GetFreshBlockRanges()) {
        SplitFreshBlockRangeFromRelativeToGlobalIndices(
            freshBlockRange,
            blocksPerStripe,
            blockSize,
            partitionsCount,
            partitionId,
            &dst);
    }

    const auto& srcBlobPieces = src.GetBlobPieces();

    for (const auto& blobPiece: srcBlobPieces) {
        NProto::TBlobPiece dstBlobPiece;
        dstBlobPiece.MutableBlobId()->CopyFrom(blobPiece.GetBlobId());
        dstBlobPiece.SetBSGroupId(blobPiece.GetBSGroupId());

        for (const auto& srcRange: blobPiece.GetRanges()) {
            SplitBlobPieceRangeFromRelativeToGlobalIndices(
                srcRange,
                blocksPerStripe,
                partitionsCount,
                partitionId,
                &dstBlobPiece);
        }
        dst.MutableBlobPieces()->Add(std::move(dstBlobPiece));
    }
}

void SplitFreshBlockRangeFromRelativeToGlobalIndices(
    const NProto::TFreshBlockRange& srcRange,
    const ui32 blocksPerStripe,
    const ui32 blockSize,
    const ui32 partitionsCount,
    const ui32 partitionId,
    NProto::TDescribeBlocksResponse* dst)
{
    const ui32 startIndex = srcRange.GetStartIndex();
    ui32 blocksCount = 0;

    const char* srcRangePtr = srcRange.GetBlocksContent().data();
    while (blocksCount < srcRange.GetBlocksCount()) {
        const auto index = RelativeToGlobalIndex(
            blocksPerStripe,
            startIndex + blocksCount,
            partitionsCount,
            partitionId);

        const auto stripeRange = CalculateStripeRange(blocksPerStripe, index);

        const auto rangeBlocksCount = std::min(
            static_cast<ui64>(stripeRange.End) - index + 1,
            static_cast<ui64>(srcRange.GetBlocksCount()) - blocksCount);

        NProto::TFreshBlockRange dstRange;
        dstRange.SetStartIndex(index);
        dstRange.SetBlocksCount(rangeBlocksCount);

        const ui64 bytesCount = rangeBlocksCount * blockSize;
        dstRange.MutableBlocksContent()->resize(bytesCount);
        char* dstRangePtr = dstRange.MutableBlocksContent()->begin();
        std::memcpy(
            dstRangePtr,
            srcRangePtr,
            bytesCount);

        srcRangePtr += bytesCount;
        blocksCount += rangeBlocksCount;

        dst->MutableFreshBlockRanges()->Add(std::move(dstRange));
    }
}

void SplitBlobPieceRangeFromRelativeToGlobalIndices(
    const NProto::TRangeInBlob& srcRange,
    const ui32 blocksPerStripe,
    const ui32 partitionsCount,
    const ui32 partitionId,
    NProto::TBlobPiece* dstBlobPiece)
{
    const ui32 blobOffset = srcRange.GetBlobOffset();
    const ui32 blockIndex = srcRange.GetBlockIndex();
    ui32 blocksCount = 0;

    while (blocksCount < srcRange.GetBlocksCount()) {
        const auto index = RelativeToGlobalIndex(
            blocksPerStripe,
            blockIndex + blocksCount,
            partitionsCount,
            partitionId);

        const auto stripeRange = CalculateStripeRange(blocksPerStripe, index);

        const auto rangeBlocksCount = std::min(
            static_cast<ui64>(stripeRange.End) - index + 1,
            static_cast<ui64>(srcRange.GetBlocksCount()) - blocksCount);

        NProto::TRangeInBlob dstRange;
        dstRange.SetBlobOffset(blobOffset + blocksCount);
        dstRange.SetBlockIndex(index);
        dstRange.SetBlocksCount(rangeBlocksCount);

        blocksCount += rangeBlocksCount;

        dstBlobPiece->MutableRanges()->Add(std::move(dstRange));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
