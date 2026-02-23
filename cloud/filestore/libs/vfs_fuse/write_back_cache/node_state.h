#pragma once

#include "node_cache.h"

#include <cloud/filestore/public/api/protos/data.pb.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/deque.h>
#include <util/generic/set.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

enum class ENodeFlushStatus
{
    // Node has no unflushed requests
    NothingToFlush,

    // Node has unflushed requests but flush has not been requested yet
    ReadyToFlush,

    // Flush has been requested for a node
    FlushRequested
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
    // Holds pending, unflushed and flushed requests
    // Tracks cached data parts
    TNodeCache Cache;

    // Prevents flushed requests from being evicted from Cache
    TMultiSet<ui64> CachedDataPins;

    ENodeFlushStatus FlushStatus = ENodeFlushStatus::NothingToFlush;

    // Flush requests are fulfilled when there are no pending or unflushed
    // requests with SequenceId less or equal than |TFlushRequest::SequenceId|.
    // Flush requests are stored in chronological order: SequenceId values are
    // strictly increasing so newer flush requests have larger SequenceId.
    TDeque<TFlushRequest> FlushRequests;

    // Cached data extends the node size but until the data is flushed,
    // the changes are not visible to the tablet. FileSystem requests that
    // return node attributes or rely on it (GetAttr, Lookup, Read, ReadDir)
    // should have the node size adjusted to this value.
    ui64 CachedNodeSize = 0;

    bool CanBeDeleted() const
    {
        if (Cache.Empty() && CachedDataPins.empty()) {
            Y_ABORT_UNLESS(FlushRequests.empty());
            Y_ABORT_UNLESS(FlushStatus == ENodeFlushStatus::NothingToFlush);
            return true;
        }
        return false;
    }

    ENodeFlushStatus GetExpectedFlushStatus(ui64 flushAllSequenceId) const
    {
        if (FlushStatus == ENodeFlushStatus::FlushRequested) {
            // Once Flush has been scheduled, the status can be changed only in
            // FlushSucceeded and FlushFailed calls
            return ENodeFlushStatus::FlushRequested;
        }
        if (!Cache.HasUnflushedRequests()) {
            return ENodeFlushStatus::NothingToFlush;
        }
        if (!FlushRequests.empty() ||
            Cache.GetMinUnflushedSequenceId() <= flushAllSequenceId)
        {
            return ENodeFlushStatus::FlushRequested;
        }
        return ENodeFlushStatus::ReadyToFlush;
    }
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
