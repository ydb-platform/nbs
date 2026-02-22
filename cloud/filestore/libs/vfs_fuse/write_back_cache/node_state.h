#pragma once

#include "node_cache.h"

#include <cloud/filestore/public/api/protos/data.pb.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/set.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct THandleState
{
    // Number of pending and unflushed requests
    ui64 RequestCount = 0;

    // The promise is fulfilled when |RequestCount| hits 0
    NThreading::TPromise<NCloud::NProto::TError> ReadyToReleasePromise;
};

////////////////////////////////////////////////////////////////////////////////

enum class ENodeFlushStatus
{
    // Node has no unflushed requests
    NothingToFlush,

    // Node has unflushed requests but flush conditions are not met
    ReadyToFlush,

    // Flush is scheduled for the node
    FlushScheduled
};

////////////////////////////////////////////////////////////////////////////////

struct TFlushRequest
{
    const ui64 SequenceId = 0;

    // The promise is fulfilled when there are no pending or unflushed requests
    // with SequenceId <= TFlushRequest::SequenceId (for a node or global)
    NThreading::TPromise<NCloud::NProto::TError> Promise =
        NThreading::NewPromise<NCloud::NProto::TError>();

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

    // Flush requests are fulfilled when there are no pending or unflushed
    // requests with SequenceId less or equal than |TFlushRequest::SequenceId|.
    // Flush requests are stored in chronological order: SequenceId values are
    // strictly increasing so newer flush requests have larger SequenceId.
    TDeque<TFlushRequest> FlushRequests;
    ENodeFlushStatus FlushStatus = ENodeFlushStatus::NothingToFlush;

    // Holds active request handles and tracks handle release
    THashMap<ui64, THandleState> Handles;

    // Number of handles that have been requested for release.
    // When HandleReleaseCount == Handles.size(), the next flush failure
    // will cause all pending requests to be failed and cached data to be
    // dropped
    size_t HandleReleaseCount = 0;

    // Cached data extends the node size but until the data is flushed,
    // the changes are not visible to the tablet. FileSystem requests that
    // return node attributes or rely on it (GetAttr, Lookup, Read, ReadDir)
    // should have the node size adjusted to this value.
    ui64 CachedNodeSize = 0;

    bool CanBeDeleted() const
    {
        return Cache.Empty() && FlushRequests.empty() && Handles.empty() &&
               CachedDataPins.empty() && HandleReleaseCount == 0;
    }
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
