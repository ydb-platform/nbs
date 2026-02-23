#pragma once

#include "node_cache.h"
#include "read_write_range_lock.h"

#include <cloud/filestore/public/api/protos/data.pb.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/deque.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct TNodeFlushState
{
    TVector<std::shared_ptr<NProto::TWriteDataRequest>> WriteRequests;
    TVector<std::shared_ptr<NProto::TWriteDataRequest>> FailedWriteRequests;
    size_t AffectedWriteDataEntriesCount = 0;
    size_t InFlightWriteRequestsCount = 0;
    bool Executing = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TFlushRequest
{
    const ui64 SequenceId = 0;

    // The promise is fulfilled when there are no pending or unflushed requests
    // with SequenceId <= TFlushRequest::SequenceId (for a node or global)
    NThreading::TPromise<void> Promise = NThreading::NewPromise();

    explicit TFlushRequest(ui64 sequenceId)
        : SequenceId(sequenceId)
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TNodeState
{
    const ui64 NodeId;

    // Holds pending, unflushed and flushed requests
    // Tracks cached data parts
    TNodeCache Cache;

    // Prevent from concurrent read and write requests with overlapping ranges
    TReadWriteRangeLock RangeLock;

    TNodeFlushState FlushState;

    // Flush requests are fulfilled when there are no pending or unflushed
    // requests with SequenceId less or equal than |TFlushRequest::SequenceId|.
    // Flush requests are stored in chronological order: SequenceId values are
    // strictly increasing so newer flush requests have larger SequenceId.
    TDeque<TFlushRequest> FlushRequests;

    // All entries with RequestId <= |AutomaticFlushRequestId| are to be flushed
    ui64 AutomaticFlushRequestId = 0;

    // Cached data extends the node size but until the data is flushed,
    // the changes are not visible to the tablet. FileSystem requests that
    // return node attributes or rely on it (GetAttr, Lookup, Read, ReadDir)
    // should have the node size adjusted to this value.
    ui64 CachedNodeSize = 0;

    explicit TNodeState(ui64 nodeId)
        : NodeId(nodeId)
    {}

    bool CanBeDeleted() const
    {
        if (Cache.Empty() && RangeLock.Empty()) {
            Y_ABORT_UNLESS(!FlushState.Executing);
            return true;
        }
        return false;
    }

    bool ShouldFlush(ui64 maxFlushAllRequestId) const
    {
        if (!Cache.HasUnflushedRequests()) {
            return false;
        }

        if (!FlushRequests.empty()) {
            return true;
        }

        const ui64 minRequestId = Cache.GetMinUnflushedSequenceId();
        return minRequestId <= maxFlushAllRequestId ||
               minRequestId <= AutomaticFlushRequestId;
    }
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
