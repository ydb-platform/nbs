#include "node_cache.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <util/string/printf.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

void TNodeCache::EnqueuePendingRequest(
    std::unique_ptr<TPendingWriteDataRequest> request)
{
    // Validate SequenceId ordering
    if (!PendingRequests.empty()) {
        Y_ABORT_UNLESS(
            PendingRequests.back()->GetSequenceId() < request->GetSequenceId());
    } else if (!UnflushedRequests.empty()) {
        Y_ABORT_UNLESS(
            UnflushedRequests.back()->GetSequenceId() <
            request->GetSequenceId());
    } else if (!FlushedRequests.empty()) {
        Y_ABORT_UNLESS(
            FlushedRequests.back()->GetSequenceId() < request->GetSequenceId());
    }

    PendingRequests.push_back(std::move(request));
}

std::unique_ptr<TPendingWriteDataRequest> TNodeCache::DequeuePendingRequest()
{
    Y_ABORT_UNLESS(!PendingRequests.empty());

    auto res = std::move(PendingRequests.front());
    PendingRequests.pop_front();

    return res;
}

void TNodeCache::EnqueueUnflushedRequest(
    const TFlushBatchLimits& flushBatchLimits,
    std::unique_ptr<TCachedWriteDataRequest> request)
{
    Y_ABORT_UNLESS(request->GetAllocationPtr() != nullptr);
    Y_ABORT_UNLESS(request->GetByteCount() > 0);

    // Validate SequenceId ordering
    if (!PendingRequests.empty()) {
        Y_ABORT_UNLESS(
            request->GetSequenceId() <
            PendingRequests.front()->GetSequenceId());
    }
    if (!UnflushedRequests.empty()) {
        Y_ABORT_UNLESS(
            UnflushedRequests.back()->GetSequenceId() <
            request->GetSequenceId());
    } else if (!FlushedRequests.empty()) {
        Y_ABORT_UNLESS(
            FlushedRequests.back()->GetSequenceId() < request->GetSequenceId());
    }

    const ui64 begin = request->GetOffset();
    const ui64 end = begin + request->GetByteCount();

    CachedData.VisitOverlapping(
        begin,
        end,
        [this, begin, end](auto it)
        {
            const auto prev = it->second;
            CachedData.Remove(it);

            if (prev.Begin < begin) {
                CachedData.Add(prev.Begin, begin, prev.Value);
            }

            if (end < prev.End) {
                CachedData.Add(end, prev.End, prev.Value);
            }
        });

    CachedData.Add(begin, end, request.get());
    UnflushedRequests.push_back(std::move(request));
    MaxWrittenOffset = Max(MaxWrittenOffset, end);

    AddUnflushedRequestToFlushBatch(flushBatchLimits, begin, end);
}

TCachedWriteDataRequest* TNodeCache::MoveFrontUnflushedRequestToFlushed()
{
    Y_ABORT_UNLESS(!UnflushedRequests.empty());
    Y_ABORT_UNLESS(!FlushBatchRequestCountQueue.empty());

    auto* cachedRequest = UnflushedRequests.front().get();
    FlushedRequests.push_back(std::move(UnflushedRequests.front()));
    UnflushedRequests.pop_front();

    if (FlushBatchRequestCountQueue.size() == 1 &&
        !IncompleteFlushBatchWriteRequestCounter.IsEmpty())
    {
        // We are trying to pop a request from an incomplete flush batch
        // This may happen when the cached data is dropped without flushing
        IncompleteFlushBatchWriteRequestCounter.Reset();
    }

    FlushBatchRequestCountQueue.front()--;
    if (FlushBatchRequestCountQueue.front() == 0) {
        FlushBatchRequestCountQueue.pop_front();
    }

    return cachedRequest;
}

std::unique_ptr<TCachedWriteDataRequest> TNodeCache::DequeueFlushedRequest()
{
    Y_ABORT_UNLESS(!FlushedRequests.empty());

    auto cachedRequest = std::move(FlushedRequests.front());
    FlushedRequests.pop_front();

    const ui64 begin = cachedRequest->GetOffset();
    const ui64 end = begin + cachedRequest->GetByteCount();

    CachedData.VisitOverlapping(
        begin,
        end,
        [this, request = cachedRequest.get()](auto it)
        {
            if (it->second.Value == request) {
                CachedData.Remove(it);
            }
        });

    return cachedRequest;
}

bool TNodeCache::Empty() const
{
    return PendingRequests.empty() && UnflushedRequests.empty() &&
           FlushedRequests.empty();
}

bool TNodeCache::HasPendingRequests() const
{
    return !PendingRequests.empty();
}

bool TNodeCache::HasUnflushedRequests() const
{
    return !UnflushedRequests.empty();
}

ui64 TNodeCache::GetMinUnflushedSequenceId() const
{
    Y_ABORT_UNLESS(!UnflushedRequests.empty());
    return UnflushedRequests.front()->GetSequenceId();
}

ui64 TNodeCache::GetMaxUnflushedSequenceId() const
{
    Y_ABORT_UNLESS(!UnflushedRequests.empty());
    return UnflushedRequests.back()->GetSequenceId();
}

bool TNodeCache::HasPendingOrUnflushedRequests() const
{
    return !PendingRequests.empty() || !UnflushedRequests.empty();
}

ui64 TNodeCache::GetMinPendingOrUnflushedSequenceId() const
{
    Y_ABORT_UNLESS(!PendingRequests.empty() || !UnflushedRequests.empty());
    return !UnflushedRequests.empty()
               ? UnflushedRequests.front()->GetSequenceId()
               : PendingRequests.front()->GetSequenceId();
}

ui64 TNodeCache::GetMaxPendingOrUnflushedSequenceId() const
{
    Y_ABORT_UNLESS(!PendingRequests.empty() || !UnflushedRequests.empty());
    return !PendingRequests.empty() ? PendingRequests.back()->GetSequenceId()
                                    : UnflushedRequests.back()->GetSequenceId();
}

bool TNodeCache::HasFlushedRequests() const
{
    return !FlushedRequests.empty();
}

ui64 TNodeCache::GetMinFlushedSequenceId() const
{
    Y_ABORT_UNLESS(!FlushedRequests.empty());
    return FlushedRequests.front()->GetSequenceId();
}

ui64 TNodeCache::GetMaxFlushedSequenceId() const
{
    Y_ABORT_UNLESS(!FlushedRequests.empty());
    return FlushedRequests.back()->GetSequenceId();
}

size_t TNodeCache::GetExpectedFlushBatchCount() const
{
    return FlushBatchRequestCountQueue.size();
}

void TNodeCache::VisitUnflushedRequestsFromFrontFlushBatch(
    const TCachedWriteDataRequestVisitor& visitor)
{
    if (FlushBatchRequestCountQueue.empty()) {
        if (UnflushedRequests.empty()) {
            return;
        }

        // We may go here if the following invariant is violated:
        // sum(FlushBatchRequestCountQueue) == UnflushedRequests.size()
        ReportWriteBackCacheImpossibleState(Sprintf(
            "TNodeCache::VisitUnflushedRequestsFromFrontFlushBatch(): "
            "FlushBatchRequestCountQueue is empty while UnflushedRequests is "
            "not empty (UnflushedRequests.size()=%zu)",
            UnflushedRequests.size()));

        // Instead of crash, allow slow processing of unflushed requests
        FlushBatchRequestCountQueue.push_back(1);
    }

    if (FlushBatchRequestCountQueue.size() == 1 &&
        !IncompleteFlushBatchWriteRequestCounter.IsEmpty())
    {
        // We are trying to process an incomplete flush batch
        // Reset the counter to complete it and start building another batch
        IncompleteFlushBatchWriteRequestCounter.Reset();
    }

    auto remainingRequests = FlushBatchRequestCountQueue.front();

    for (const auto& it: UnflushedRequests) {
        if (remainingRequests == 0) {
            break;
        }
        visitor(it.get());
        remainingRequests--;
    }

    if (remainingRequests > 0) {
        // Invariant violation
        ReportWriteBackCacheImpossibleState(Sprintf(
            "TNodeCache::VisitUnflushedRequestsFromFrontFlushBatch(): "
            "UnflushedRequests contains less elements than needed to build a "
            "flush batch (remainingRequests=%lu)",
            remainingRequests));

        // Fix the invariant
        IncompleteFlushBatchWriteRequestCounter.Reset();

        // Put each remaining request into a separate flush batch since we have
        // no more guarantee about meeting the flush batch limits
        FlushBatchRequestCountQueue = TDeque<ui64>(UnflushedRequests.size(), 1);
    }
}

TCachedData TNodeCache::GetCachedData(
    ui64 offset,
    ui64 byteCount,
    ui64 maxEvictableSequenceId) const
{
    if (CachedData.empty()) {
        return {};
    }

    const ui64 end = CachedData.rbegin()->second.End;

    TCachedData result;

    // ReadDataByteCount is intentionally computed from the maximal cached write
    // end offset, not only from the parts visible under this read pin.
    // This value is only a response-size hint: writes can only extend the node
    // size, so the maximal observed written offset is safe to expose.
    result.ReadDataByteCount = end > offset ? Min(byteCount, end - offset) : 0;

    CachedData.VisitOverlapping(
        offset,
        offset + byteCount,
        [&result, offset, end = offset + byteCount, maxEvictableSequenceId](
            auto it)
        {
            const ui64 partOffset = Max(offset, it->second.Begin);
            const ui64 partByteCount = Min(end, it->second.End) - partOffset;

            TCachedWriteDataRequest* request = it->second.Value;

            // Requests with SequenceId > maxEvictableSequenceId are pinned and
            // are safe to be used.
            // Requests with SequenceId <= maxEvictableSequenceId may be evicted
            // at any moment and data pointers may become dangling - these parts
            // should be taken from ReadData response from the backend storage
            // instead of cache.
            if (request->GetSequenceId() > maxEvictableSequenceId) {
                TStringBuf data = request->GetBuffer(partOffset, partByteCount);
                result.Parts.push_back({partOffset - offset, data});
            }
        });

    return result;
}

ui64 TNodeCache::GetMaxWrittenOffset() const
{
    return MaxWrittenOffset;
}

void TNodeCache::ResetMaxWrittenOffset()
{
    Y_ABORT_UNLESS(FlushedRequests.empty());
    MaxWrittenOffset = CachedData.empty() ? 0 : CachedData.rbegin()->second.End;
}

void TNodeCache::AddUnflushedRequestToFlushBatch(
    const TFlushBatchLimits& flushBatchLimits,
    ui64 begin,
    ui64 end)
{
    const bool startNewFlushBatch =
        IncompleteFlushBatchWriteRequestCounter.IsEmpty();

    IncompleteFlushBatchWriteRequestCounter.AddRequestInterval(
        flushBatchLimits,
        begin,
        end);

    if (startNewFlushBatch) {
        FlushBatchRequestCountQueue.push_back(1);
        return;
    }

    const bool requestCountWithinLimits =
        flushBatchLimits.MaxWriteRequestsCount == 0 ||
        IncompleteFlushBatchWriteRequestCounter.GetWriteRequestCount() <=
            flushBatchLimits.MaxWriteRequestsCount;

    const bool sumRequestsSizeWithinLimits =
        flushBatchLimits.MaxSumWriteRequestsSize == 0 ||
        IncompleteFlushBatchWriteRequestCounter.GetSumWriteRequestsSize() <=
            flushBatchLimits.MaxSumWriteRequestsSize;

    if (requestCountWithinLimits && sumRequestsSizeWithinLimits) {
        if (FlushBatchRequestCountQueue.empty()) {
            ReportWriteBackCacheImpossibleState(
                "FlushBatchRequestCountQueue is empty while "
                "IncompleteFlushBatchRequestCounter is not empty");
        } else {
            FlushBatchRequestCountQueue.back()++;
            return;
        }
    }

    FlushBatchRequestCountQueue.push_back(1);

    IncompleteFlushBatchWriteRequestCounter.Reset();
    IncompleteFlushBatchWriteRequestCounter.AddRequestInterval(
        flushBatchLimits,
        begin,
        end);
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
