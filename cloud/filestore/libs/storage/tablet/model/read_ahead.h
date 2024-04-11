#pragma once

#include "public.h"

#include <cloud/filestore/libs/storage/model/range.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/common/ring_buffer.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/memory/alloc.h>

namespace NCloud::NFileStore::NStorage {

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
        {
        }

        [[nodiscard]] ui64 End() const
        {
            return Offset + Length;
        }

        [[nodiscard]] bool Overlaps(const TByteRange& byteRange) const
        {
            return TByteRange(
                Offset,
                Length,
                byteRange.BlockSize).Overlaps(byteRange);
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

    struct TNodeState
    {
        static const ui32 RANGE_COUNT = 32;
        TByteRanges LastRanges;
        TDescribeResults DescribeResults;

        TNodeState()
            : LastRanges(RANGE_COUNT)
            , DescribeResults(0)
        {
        }
    };

    using TNodeStates = THashMap<ui64, TNodeState>;

private:
    IAllocator* Allocator;
    TNodeStates NodeStates;

    ui32 MaxNodes = 0;
    ui32 MaxResultsPerNode = 0;
    ui32 RangeSize = 0;
    ui32 MaxGapPercentage = 0;

public:
    TReadAheadCache(IAllocator* allocator);
    ~TReadAheadCache();

    void Reset(
        ui32 maxNodes,
        ui32 maxResultsPerNode,
        ui32 rangeSize,
        ui32 maxGapPercentage);

    bool TryFillResult(
        ui64 nodeId,
        const TByteRange& range,
        NProtoPrivate::TDescribeDataResponse* response);

    // returns the suggested range to describe
    TMaybe<TByteRange> RegisterDescribe(
        ui64 nodeId,
        const TByteRange inputRange);
    void InvalidateCache(ui64 nodeId);
    void RegisterResult(
        ui64 nodeId,
        const TByteRange& range,
        const NProtoPrivate::TDescribeDataResponse& result);

    ui32 CacheSize() const
    {
        return NodeStates.size();
    }

private:
    TNodeState& Access(ui64 nodeId);
};

}   // namespace NCloud::NFileStore::NStorage
