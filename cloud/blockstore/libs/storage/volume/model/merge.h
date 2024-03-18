#pragma once

#include <cloud/blockstore/libs/storage/api/volume.h>

#include <cloud/blockstore/libs/common/block_range.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void MergeStripedBitMask(
    const TBlockRange64& originalRange,
    const TBlockRange64& stripeRange,
    const ui32 blocksPerStripe,
    const ui32 partitionsCount,
    const ui32 partitionId,
    const TString& srcMask,
    TString& dstMask);

void MergeDescribeBlocksResponse(
    NProto::TDescribeBlocksResponse& src,
    NProto::TDescribeBlocksResponse& dst,
    const ui32 blocksPerStripe,
    const ui32 blockSize,
    const ui32 partitionsCount,
    const ui32 partitionId);

void SplitFreshBlockRangeFromRelativeToGlobalIndices(
    const NProto::TFreshBlockRange& srcRange,
    NProto::TDescribeBlocksResponse* dst,
    const ui32 blocksPerStripe,
    const ui32 blockSize,
    const ui32 partitionsCount,
    const ui32 partitionId);

void SplitBlobPieceRangeFromRelativeToGlobalIndices(
    const NProto::TRangeInBlob& srcRange,
    NProto::TBlobPiece* dstBlobPiece,
    const ui32 blocksPerStripe,
    const ui32 partitionsCount,
    const ui32 partitionId);

}   // namespace NCloud::NBlockStore::NStorage
