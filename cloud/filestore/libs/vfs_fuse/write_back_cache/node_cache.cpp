#include "node_cache.h"

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
}

TCachedWriteDataRequest* TNodeCache::MoveFrontUnflushedRequestToFlushed()
{
    Y_ABORT_UNLESS(!UnflushedRequests.empty());

    auto* cachedRequest = UnflushedRequests.front().get();
    FlushedRequests.push_back(std::move(UnflushedRequests.front()));
    UnflushedRequests.pop_front();

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

ui64 TNodeCache::GetMinPendingOrUnflushedSequenceId(ui64 defValue) const
{
    if (!UnflushedRequests.empty()) {
        return UnflushedRequests.front()->GetSequenceId();
    }
    if (!PendingRequests.empty()) {
        return PendingRequests.front()->GetSequenceId();
    }
    return defValue;
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

void TNodeCache::VisitUnflushedRequests(
    TCachedWriteDataRequestVisitor visitor) const
{
    for (const auto& e: UnflushedRequests) {
        if (!visitor(e.get())) {
            return;
        }
    }
}

TCachedData TNodeCache::GetCachedData(ui64 offset, ui64 byteCount) const
{
    if (CachedData.empty()) {
        return {};
    }

    const ui64 end = CachedData.rbegin()->second.End;

    TCachedData result;
    result.ReadDataByteCount = end > offset ? Min(byteCount, end - offset) : 0;

    CachedData.VisitOverlapping(
        offset,
        offset + byteCount,
        [&result, offset, end = offset + byteCount](auto it)
        {
            const ui64 partOffset = Max(offset, it->second.Begin);
            const ui64 partByteCount = Min(end, it->second.End) - partOffset;
            TCachedWriteDataRequest* request = it->second.Value;
            TStringBuf data = request->GetBuffer(partOffset, partByteCount);
            result.Parts.push_back({partOffset - offset, data});
        });

    return result;
}

ui64 TNodeCache::GetCachedDataEndOffset() const
{
    return CachedData.empty() ? 0 : CachedData.rbegin()->second.End;
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
