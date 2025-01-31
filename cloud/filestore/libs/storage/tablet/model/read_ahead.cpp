#include "read_ahead.h"

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 NoHandle = Max<ui64>();

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
    ui32 maxGapPercentage,
    ui32 maxHandlesPerNode)
{
    MaxNodes = maxNodes;
    MaxResultsPerNode = maxResultsPerNode;
    RangeSize = rangeSize;
    MaxGapPercentage = maxGapPercentage;
    MaxHandlesPerNode = maxHandlesPerNode;
    NodeStates.clear();
}

////////////////////////////////////////////////////////////////////////////////

bool TReadAheadCache::TryFillResultImpl(
    ui64 nodeId,
    ui64 handle,
    const TByteRange& range,
    NProtoPrivate::TDescribeDataResponse* response)
{
    auto* nodeState = NodeStates.FindPtr(nodeId);
    if (!nodeState) {
        return false;
    }

    auto* handleState = nodeState->HandleStates.FindPtr(handle);
    if (!handleState) {
        return false;
    }

    for (ui32 i = 0; i < handleState->DescribeResults.Size(); ++i) {
        const auto& result = handleState->DescribeResults.Back(i);
        if (result.Range.Contains(range)) {
            FilterResult(range, result.Response, response);
            return true;
        }
    }

    return false;
}

bool TReadAheadCache::TryFillResult(
    ui64 nodeId,
    ui64 handle,
    const TByteRange& range,
    NProtoPrivate::TDescribeDataResponse* response)
{
    if (TryFillResultImpl(nodeId, handle, range, response)) {
        return true;
    }

    return TryFillResultImpl(nodeId, NoHandle, range, response);
}

////////////////////////////////////////////////////////////////////////////////

TMaybe<TByteRange> TReadAheadCache::RegisterDescribeImpl(
    ui64 nodeId,
    ui64 handle,
    const TByteRange inputRange)
{
    auto& handleState = Access(nodeId, handle);
    handleState.LastRanges.PushBack(TRange(inputRange));
    if (IsCloseToSequential(handleState.LastRanges, MaxGapPercentage)
            && inputRange.Length < RangeSize)
    {
        return TByteRange(inputRange.Offset, RangeSize, inputRange.BlockSize);
    }

    return {};
}

TMaybe<TByteRange> TReadAheadCache::RegisterDescribe(
    ui64 nodeId,
    ui64 handle,
    const TByteRange inputRange)
{
    if (!RangeSize) {
        return {};
    }

    auto result = RegisterDescribeImpl(nodeId, handle, inputRange);
    if (result) {
        return result;
    }

    return RegisterDescribeImpl(nodeId, NoHandle, inputRange);
}

////////////////////////////////////////////////////////////////////////////////

void TReadAheadCache::InvalidateCache(ui64 nodeId)
{
    NodeStates.erase(nodeId);
}

void TReadAheadCache::OnDestroyHandle(ui64 nodeId, ui64 handle)
{
    if (auto* nodeState = NodeStates.FindPtr(nodeId)) {
        nodeState->HandleStates.erase(handle);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TReadAheadCache::RegisterResult(
    ui64 nodeId,
    ui64 handle,
    const TByteRange& range,
    const NProtoPrivate::TDescribeDataResponse& result)
{
    Access(nodeId, handle).DescribeResults.PushBack({TRange(range), result});
    Access(nodeId, NoHandle).DescribeResults.PushBack({TRange(range), result});
}

TReadAheadCache::THandleState& TReadAheadCache::Access(ui64 nodeId, ui64 handle)
{
    // TODO: LRU eviction
    if (NodeStates.size() >= MaxNodes && !NodeStates.contains(nodeId)) {
        NodeStates.clear();
    }

    auto& nodeState = NodeStates[nodeId];
    // TODO: LRU eviction
    // +1 needed for the NoHandle entry
    if (nodeState.HandleStates.size() >= MaxHandlesPerNode + 1
            && !nodeState.HandleStates.contains(handle))
    {
        nodeState.HandleStates.clear();
    }

    auto& handleState = nodeState.HandleStates[handle];
    if (!handleState.DescribeResults.Capacity()) {
        handleState.DescribeResults.Reset(MaxResultsPerNode);
    }

    return handleState;
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
