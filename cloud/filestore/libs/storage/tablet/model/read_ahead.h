#pragma once

#include "public.h"

#include <cloud/filestore/libs/storage/model/range.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/memory/alloc.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TReadAheadCache
{
    using TDescribeResults = TDeque<NProtoPrivate::TDescribeDataResponse>;
    using TNodeId2DescribeResults = THashMap<ui64, TDescribeResults>;

private:
    IAllocator* Allocator;
    TNodeId2DescribeResults NodeId2DescribeResults;

public:
    TReadAheadCache(IAllocator* allocator);
    ~TReadAheadCache();

    void Reset(
        ui32 maxNodes,
        ui32 maxResultsPerNode,
        ui32 rangeSize);

    bool TryFillResult(
        const NProtoPrivate::TDescribeDataRequest& request,
        NProtoPrivate::TDescribeDataResponse* response);

    // returns the suggested range to describe
    TMaybe<TByteRange> RegisterDescribe(
        ui64 nodeId,
        const TByteRange inputRange);
    void InvalidateCache(ui64 nodeId);
    void RegisterResult(
        ui64 nodeId,
        const NProtoPrivate::TDescribeDataResponse& result);
};

}   // namespace NCloud::NFileStore::NStorage
