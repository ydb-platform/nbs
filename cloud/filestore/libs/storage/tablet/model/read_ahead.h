#pragma once

#include "public.h"

#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/common/byte_range.h>
#include <cloud/storage/core/libs/common/ring_buffer.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/memory/alloc.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TReadAheadCacheStats
{
    ui64 NodeCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TReadAheadCache
{
public:
    struct TRange
    {
        ui64 Offset = 0;
        ui32 Length = 0;

        TRange() = default;

        explicit TRange(const TByteRange& byteRange)
            : Offset(byteRange.Offset)
            , Length(byteRange.Length)
        {}

        [[nodiscard]] ui64 End() const
        {
            return Offset + Length;
        }

        [[nodiscard]] bool Contains(const TByteRange& byteRange) const
        {
            return TByteRange(Offset, Length, byteRange.BlockSize)
                .Contains(byteRange);
        }
    };

private:
    struct TDescribeResult
    {
        TRange Range;
        NProtoPrivate::TDescribeDataResponse Response;
    };

    using TDescribeResults = TRingBuffer<TDescribeResult>;
    using TByteRanges = TRingBuffer<TRange>;

    struct THandleState
    {
        static const ui32 RANGE_COUNT = 32;
        TByteRanges LastRanges;
        TDescribeResults DescribeResults;

        THandleState()
            : LastRanges(RANGE_COUNT)
            , DescribeResults(0)
        {}
    };

    struct TNodeState
    {
        THashMap<ui64, THandleState> HandleStates;
    };

    using TNodeStates = THashMap<ui64, TNodeState>;

private:
    IAllocator* Allocator;
    TNodeStates NodeStates;

    ui32 MaxNodes = 0;
    ui32 MaxResultsPerNode = 0;
    ui32 RangeSize = 0;
    ui32 MaxGapPercentage = 0;
    ui32 MaxHandlesPerNode = 0;

public:
    explicit TReadAheadCache(IAllocator* allocator);
    ~TReadAheadCache();

    void Reset(
        ui32 maxNodes,
        ui32 maxResultsPerNode,
        ui32 rangeSize,
        ui32 maxGapPercentage,
        ui32 maxHandlesPerNode);

    bool TryFillResult(
        ui64 nodeId,
        ui64 handle,
        const TByteRange& range,
        NProtoPrivate::TDescribeDataResponse* response);

    // returns the suggested range to describe
    TMaybe<TByteRange>
    RegisterDescribe(ui64 nodeId, ui64 handle, const TByteRange inputRange);
    void InvalidateCache(ui64 nodeId);
    void OnDestroyHandle(ui64 nodeId, ui64 handle);
    void RegisterResult(
        ui64 nodeId,
        ui64 handle,
        const TByteRange& range,
        const NProtoPrivate::TDescribeDataResponse& result);

    [[nodiscard]] TReadAheadCacheStats GetStats() const
    {
        return {.NodeCount = NodeStates.size()};
    }

private:
    THandleState& Access(ui64 nodeId, ui64 handle);

    bool TryFillResultImpl(
        ui64 nodeId,
        ui64 handle,
        const TByteRange& range,
        NProtoPrivate::TDescribeDataResponse* response);

    TMaybe<TByteRange>
    RegisterDescribeImpl(ui64 nodeId, ui64 handle, const TByteRange inputRange);

    void RegisterResultImpl(
        ui64 nodeId,
        ui64 handle,
        const TByteRange& range,
        const NProtoPrivate::TDescribeDataResponse& result);
};

////////////////////////////////////////////////////////////////////////////////

void FilterResult(
    const TByteRange& range,
    const NProtoPrivate::TDescribeDataResponse& src,
    NProtoPrivate::TDescribeDataResponse* dst);

}   // namespace NCloud::NFileStore::NStorage
