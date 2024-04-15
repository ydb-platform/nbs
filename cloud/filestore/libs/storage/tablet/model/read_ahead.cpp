#include "read_ahead.h"

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

bool IsCloseToSequential(
    const TRingBuffer<TReadAheadCache::TRange>& byteRanges,
    ui32 maxGapPercentage)
{
    if (byteRanges.Size() == 0) {
        return false;
    }

    TVector<TReadAheadCache::TRange> tmp(Reserve(byteRanges.Size()));
    ui64 minOffset = Max<ui64>();
    ui64 maxEnd = 0;
    for (ui32 i = 0; i < byteRanges.Size(); ++i) {
        tmp.push_back(byteRanges.Front(i));
        minOffset = Min(minOffset, tmp.back().Offset);
        maxEnd = Max(maxEnd, tmp.back().End());
    }
    SortBy(tmp, [] (const TReadAheadCache::TRange& range) {
        return std::make_pair(range.Offset, range.Length);
    });

    const ui64 totalLength = maxEnd - minOffset;
    if (totalLength < 1_MB) {
        return false;
    }

    double totalGap = 0;
    for (ui32 i = 1; i < tmp.size(); ++i) {
        const auto& prev = tmp[i - 1];
        const auto& cur = tmp[i];
        if (prev.End() < cur.Offset) {
            totalGap += cur.Offset - prev.End();
        }
    }

    return totalGap * 100 / totalLength <= maxGapPercentage;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TReadAheadCache::TReadAheadCache(IAllocator* allocator)
    : Allocator(allocator)
{
    // TODO: use Allocator
    Y_UNUSED(Allocator);
}

TReadAheadCache::~TReadAheadCache() = default;

void TReadAheadCache::Reset(
    ui32 maxNodes,
    ui32 maxResultsPerNode,
    ui32 rangeSize,
    ui32 maxGapPercentage)
{
    MaxNodes = maxNodes;
    MaxResultsPerNode = maxResultsPerNode;
    RangeSize = rangeSize;
    MaxGapPercentage = maxGapPercentage;
    NodeStates.clear();
}

bool TReadAheadCache::TryFillResult(
    ui64 nodeId,
    const TByteRange& range,
    NProtoPrivate::TDescribeDataResponse* response)
{
    auto* nodeState = NodeStates.FindPtr(nodeId);
    if (!nodeState) {
        return false;
    }

    for (ui32 i = 0; i < nodeState->DescribeResults.Size(); ++i) {
        const auto& result = nodeState->DescribeResults.Back(i);
        if (result.Range.Contains(range)) {
            FilterResult(range, result.Response, response);
            return true;
        }
    }

    return false;
}

TMaybe<TByteRange> TReadAheadCache::RegisterDescribe(
    ui64 nodeId,
    const TByteRange inputRange)
{
    if (!RangeSize) {
        return {};
    }

    auto& nodeState = Access(nodeId);
    nodeState.LastRanges.PushBack(TRange(inputRange));
    if (IsCloseToSequential(nodeState.LastRanges, MaxGapPercentage)
            && inputRange.Length < RangeSize)
    {
        return TByteRange(inputRange.Offset, RangeSize, inputRange.BlockSize);
    }

    return {};
}

void TReadAheadCache::InvalidateCache(ui64 nodeId)
{
    NodeStates.clear(nodeId);
}

void TReadAheadCache::RegisterResult(
    ui64 nodeId,
    const TByteRange& range,
    const NProtoPrivate::TDescribeDataResponse& result)
{
    Access(nodeId).DescribeResults.PushBack({TRange(range), result});
}

TReadAheadCache::TNodeState& TReadAheadCache::Access(ui64 nodeId)
{
    // TODO: LRU eviction
    if (NodeStates.size() >= MaxNodes && !NodeStates.contains(nodeId)) {
        NodeStates.clear();
    }

    auto& nodeState = NodeStates[nodeId];
    if (!nodeState.DescribeResults.Capacity()) {
        nodeState.DescribeResults.Reset(MaxResultsPerNode);
    }

    return nodeState;
}

////////////////////////////////////////////////////////////////////////////////

void FilterResult(
    const TByteRange& range,
    const NProtoPrivate::TDescribeDataResponse& src,
    NProtoPrivate::TDescribeDataResponse* dst)
{
    dst->SetFileSize(src.GetFileSize());
    for (const auto& freshRange: src.GetFreshDataRanges()) {
        const TByteRange freshByteRange(
            freshRange.GetOffset(),
            freshRange.GetContent().size(),
            range.BlockSize);

        auto intersection = range.Intersect(freshByteRange);
        if (!intersection.Length) {
            continue;
        }

        auto* dstRange = dst->AddFreshDataRanges();
        dstRange->SetOffset(intersection.Offset);
        const ui64 offsetDiff = intersection.Offset - freshByteRange.Offset;
        *dstRange->MutableContent() =
            freshRange.GetContent().substr(offsetDiff, intersection.Length);
    }

    for (const auto& blobPiece: src.GetBlobPieces()) {
        NProtoPrivate::TBlobPiece dstPiece;
        for (const auto& blobRange: blobPiece.GetRanges()) {
            const TByteRange blobByteRange(
                blobRange.GetOffset(),
                blobRange.GetLength(),
                range.BlockSize);

            auto intersection = range.Intersect(blobByteRange);
            if (!intersection.Length) {
                continue;
            }

            auto* dstRange = dstPiece.AddRanges();
            dstRange->SetOffset(intersection.Offset);
            dstRange->SetLength(intersection.Length);
            const ui64 offsetDiff = intersection.Offset - blobByteRange.Offset;
            dstRange->SetBlobOffset(blobRange.GetBlobOffset() + offsetDiff);
        }

        if (dstPiece.RangesSize()) {
            *dstPiece.MutableBlobId() = blobPiece.GetBlobId();
            dstPiece.SetBSGroupId(blobPiece.GetBSGroupId());
            *dst->AddBlobPieces() = std::move(dstPiece);
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
