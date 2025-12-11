#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/protos_ydb/volume.pb.h>

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
    const ui32 blocksPerStripe,
    const ui32 blockSize,
    const ui32 partitionsCount,
    const ui32 partitionId,
    NProto::TDescribeBlocksResponse* dst);

void SplitBlobPieceRangeFromRelativeToGlobalIndices(
    const NProto::TRangeInBlob& srcRange,
    const ui32 blocksPerStripe,
    const ui32 partitionsCount,
    const ui32 partitionId,
    NProto::TBlobPiece* dstBlobPiece);

}   // namespace NCloud::NBlockStore::NStorage
