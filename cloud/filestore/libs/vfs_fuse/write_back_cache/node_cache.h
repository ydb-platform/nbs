#pragma once

#include "flush_batch_limits.h"
#include "flush_batch_write_request_counter.h"
#include "write_data_request.h"

#include <cloud/storage/core/libs/common/disjoint_interval_map.h>

#include <util/generic/deque.h>
#include <util/generic/function_ref.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct TCachedDataPart
{
    // Offset in ReadData response buffer - this is relative to the requested
    // |offset| in TNodeCache::GetCachedData
    ui64 RelativeOffset = 0;
    TStringBuf Data;
};

////////////////////////////////////////////////////////////////////////////////

struct TCachedData
{
    TVector<TCachedDataPart> Parts;
    // The expected size of ReadData response. If the ReadData response has less
    // length than this value, it should be extended up to this value.
    // This is needed to avoid truncation when there are unflushed data parts
    // beyond the requested range.
    ui64 ReadDataByteCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

// The class is not thread-safe
class TNodeCache
{
public:
    using TCachedWriteDataRequestVisitor =
        TFunctionRef<void(const TCachedWriteDataRequest* request)>;

private:
    TDeque<std::unique_ptr<TPendingWriteDataRequest>> PendingRequests;
    TDeque<std::unique_ptr<TCachedWriteDataRequest>> UnflushedRequests;
    TDeque<std::unique_ptr<TCachedWriteDataRequest>> FlushedRequests;
    TDisjointIntervalMap<ui64, TCachedWriteDataRequest*> CachedData;

    // Requests from UnflushedRequests grouped by flush batches
    // Invariant: sum of values == UnflushedRequests.size()
    TDeque<ui64> FlushBatchRequestCountQueue;
    TFlushBatchWriteRequestCounter IncompleteFlushBatchWriteRequestCounter;

    // Cached data extends the node size but until the data is flushed,
    // the changes are not visible to the tablet. FileSystem requests that
    // return node attributes or rely on it (GetAttr, Lookup, Read, ReadDir)
    // should have the node size adjusted to these values.
    ui64 MaxWrittenOffset = 0;

public:
    void EnqueuePendingRequest(
        std::unique_ptr<TPendingWriteDataRequest> request);

    std::unique_ptr<TPendingWriteDataRequest> DequeuePendingRequest();

    // Flush batches are built when adding requests to unflushed queue
    // Note: flushBatchLimits are passed to the function to avoid storing them
    // in TNodeCache
    void EnqueueUnflushedRequest(
        const TFlushBatchLimits& flushBatchLimits,
        std::unique_ptr<TCachedWriteDataRequest> request);

    TCachedWriteDataRequest* MoveFrontUnflushedRequestToFlushed();

    std::unique_ptr<TCachedWriteDataRequest> DequeueFlushedRequest();

    bool Empty() const;

    bool HasPendingRequests() const;

    bool HasUnflushedRequests() const;
    ui64 GetMinUnflushedSequenceId() const;
    ui64 GetMaxUnflushedSequenceId() const;

    bool HasPendingOrUnflushedRequests() const;
    ui64 GetMinPendingOrUnflushedSequenceId() const;
    ui64 GetMaxPendingOrUnflushedSequenceId() const;

    bool HasFlushedRequests() const;
    ui64 GetMinFlushedSequenceId() const;
    ui64 GetMaxFlushedSequenceId() const;

    size_t GetExpectedFlushBatchCount() const;

    void VisitUnflushedRequestsFromFrontFlushBatch(
        const TCachedWriteDataRequestVisitor& visitor);

    TCachedData GetCachedData(
        ui64 offset,
        ui64 byteCount,
        ui64 maxEvictableSequenceId) const;

    ui64 GetMaxWrittenOffset() const;
    void ResetMaxWrittenOffset();

private:
    void AddUnflushedRequestToFlushBatch(
        const TFlushBatchLimits& flushBatchLimits,
        ui64 begin,
        ui64 end);
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
