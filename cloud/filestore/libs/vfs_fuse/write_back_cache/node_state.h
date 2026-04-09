#pragma once

#include "node_cache.h"

#include <cloud/filestore/public/api/protos/data.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/set.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct THandleState
{
    // Number of pending and unflushed requests associated with the handle
    ui64 ActiveWriteDataRequestCount = 0;

    // The promise is fulfilled when |RequestCount| hits 0
    // Not initialized by default unless ReleaseHandle is requested
    NThreading::TPromise<NCloud::NProto::TError> ReadyToReleasePromise;
};

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
    NThreading::TPromise<NCloud::NProto::TError> Promise =
        NThreading::NewPromise<NCloud::NProto::TError>();

    explicit TFlushRequest(ui64 sequenceId)
        : SequenceId(sequenceId)
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TBarrier
{
    // The promise is fulfilled when all requests associated with a node with
     // SequenceId less than or equal to the barrier id (the key in
     // TNodeState::Barriers) are evicted
    NThreading::TPromise<TResultOrError<ui64>> Promise;

    bool IsAcquired = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TNodeState
{
    // Holds pending, unflushed and flushed requests
    // Tracks cached data parts
    TNodeCache Cache;

    // Flushed requests with SequenceId >= PinId are prevented
    // from being evicted from cache.
    // Used by ReadData request handler in order to keep the cached data
    // available until the data is no longer needed (avoid copying data to
    // the temporary buffer under lock)
    TMultiSet<ui64> CachedDataPins;

    ENodeFlushStatus FlushStatus = ENodeFlushStatus::NothingToFlush;

    // Flush requests are fulfilled when there are no pending or unflushed
    // requests with SequenceId less or equal than |TFlushRequest::SequenceId|.
    // Flush requests are stored in chronological order: SequenceId values are
    // strictly increasing so newer flush requests have larger SequenceId.
    TDeque<TFlushRequest> FlushRequests;

    // Holds active request handles and tracks handle release
    // Key: handle
    THashMap<ui64, THandleState> Handles;

    // Number of handles that have been requested for release.
    // When HandleToReleaseCount == Handles.size(), the next flush failure
    // will cause all pending requests to be failed and cached data to be
    // dropped
    size_t HandleToReleaseCount = 0;

    // Requests with SequenceId > BarrierId are prevented from being flushed
    TMap<ui64, TBarrier> Barriers;

    bool CanBeDeleted() const
    {
        if (Cache.Empty() && CachedDataPins.empty() && Barriers.empty()) {
            Y_ABORT_UNLESS(FlushRequests.empty());
            Y_ABORT_UNLESS(FlushStatus == ENodeFlushStatus::NothingToFlush);
            Y_ABORT_UNLESS(Handles.empty());
            Y_ABORT_UNLESS(HandleToReleaseCount == 0);
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

        const ui64 minUnflushedSequenceId = Cache.GetMinUnflushedSequenceId();

        if (!Barriers.empty()) {
            // Having a barrier means that there is an operation that wants
            // the data prior to barrier acquisition to be flushed and evicted.
            // Therefore, flush should be scheduled if there is such data.
            // Also, barrier prevents newer data from being flushed.
            return minUnflushedSequenceId < Barriers.cbegin()->first
                       ? ENodeFlushStatus::FlushRequested
                       : ENodeFlushStatus::NothingToFlush;
        }

        if (HandleToReleaseCount > 0) {
            return ENodeFlushStatus::FlushRequested;
        }

        if (!FlushRequests.empty() ||
            minUnflushedSequenceId <= flushAllSequenceId)
        {
            return ENodeFlushStatus::FlushRequested;
        }
        return ENodeFlushStatus::ReadyToFlush;
    }
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
