#include "read_ahead.h"

namespace NCloud::NFileStore::NStorage {

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
    ui32 rangeSize)
{
    Y_UNUSED(maxNodes);
    Y_UNUSED(maxResultsPerNode);
    Y_UNUSED(rangeSize);
}

bool TReadAheadCache::TryFillResult(
    const NProtoPrivate::TDescribeDataRequest& request,
    NProtoPrivate::TDescribeDataResponse* response)
{
    Y_UNUSED(request);
    Y_UNUSED(response);

    return false;
}

TMaybe<TByteRange> TReadAheadCache::RegisterDescribe(
    ui64 nodeId,
    const TByteRange inputRange)
{
    Y_UNUSED(nodeId);
    Y_UNUSED(inputRange);

    return {};
}

void TReadAheadCache::InvalidateCache(ui64 nodeId)
{
    NodeId2DescribeResults.clear(nodeId);
}

void TReadAheadCache::RegisterResult(
    ui64 nodeId,
    const NProtoPrivate::TDescribeDataResponse& result)
{
    Y_UNUSED(nodeId);
    Y_UNUSED(result);
}

}   // namespace NCloud::NFileStore::NStorage
