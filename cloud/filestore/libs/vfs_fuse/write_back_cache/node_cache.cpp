#include "node_cache.h"

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

void TNodeCache::PushUnflushed(std::unique_ptr<TCachedWriteDataRequest> entry)
{
    Y_ABORT_UNLESS(entry->GetAllocationPtr() != nullptr);
    Y_ABORT_UNLESS(entry->GetByteCount() > 0);

    if (!UnflushedRequests.empty()) {
        Y_ABORT_UNLESS(
            UnflushedRequests.back()->GetSequenceId() < entry->GetSequenceId());
    } else if (!FlushedRequests.empty()) {
        Y_ABORT_UNLESS(
            FlushedRequests.back()->GetSequenceId() < entry->GetSequenceId());
    }

    const ui64 begin = entry->GetOffset();
    const ui64 end = begin + entry->GetByteCount();

    IntervalMap.VisitOverlapping(
        begin,
        end,
        [this, begin, end](auto it)
        {
            const auto prev = it->second;
            IntervalMap.Remove(it);

            if (prev.Begin < begin) {
                IntervalMap.Add(prev.Begin, begin, prev.Value);
            }

            if (end < prev.End) {
                IntervalMap.Add(end, prev.End, prev.Value);
            }
        });

    IntervalMap.Add(begin, end, entry.get());
    UnflushedRequests.push_back(std::move(entry));
}

TCachedWriteDataRequest* TNodeCache::SetFrontFlushed()
{
    Y_ABORT_UNLESS(!UnflushedRequests.empty());

    auto* entry = UnflushedRequests.front().get();
    FlushedRequests.push_back(std::move(UnflushedRequests.front()));
    UnflushedRequests.pop_front();

    return entry;
}

auto TNodeCache::PopFlushed() -> std::unique_ptr<TCachedWriteDataRequest>
{
    Y_ABORT_UNLESS(!FlushedRequests.empty());

    auto entry = std::move(FlushedRequests.front());
    FlushedRequests.pop_front();

    const ui64 begin = entry->GetOffset();
    const ui64 end = begin + entry->GetByteCount();

    IntervalMap.VisitOverlapping(
        begin,
        end,
        [this, entry = entry.get()](auto it)
        {
            if (it->second.Value == entry) {
                IntervalMap.Remove(it);
            }
        });

    return entry;
}

bool TNodeCache::Empty() const
{
    return UnflushedRequests.empty() && FlushedRequests.empty();
}

bool TNodeCache::HasUnflushedRequests() const
{
    return !UnflushedRequests.empty();
}

ui64 TNodeCache::GetMinUnflushedSequenceId() const
{
    return UnflushedRequests.empty()
               ? 0
               : UnflushedRequests.front()->GetSequenceId();
}

ui64 TNodeCache::GetMaxUnflushedSequenceId() const
{
    return UnflushedRequests.empty()
               ? 0
               : UnflushedRequests.back()->GetSequenceId();
}

bool TNodeCache::HasFlushedRequests() const
{
    return !FlushedRequests.empty();
}

ui64 TNodeCache::GetMinFlushedSequenceId() const
{
    return FlushedRequests.empty() ? 0
                                   : FlushedRequests.front()->GetSequenceId();
}

void TNodeCache::VisitUnflushedRequests(const TEntryVisitor& visitor) const
{
    for (const auto& e: UnflushedRequests) {
        if (!visitor(e.get())) {
            return;
        }
    }
}

TCachedData TNodeCache::GetCachedData(ui64 offset, ui64 byteCount) const
{
    if (IntervalMap.empty()) {
        return {};
    }

    const auto end = IntervalMap.rbegin()->second.End;

    TCachedData result;
    result.ReadDataByteCount = end > offset ? Min(byteCount, end - offset) : 0;

    IntervalMap.VisitOverlapping(
        offset,
        offset + byteCount,
        [&result, offset, end = offset + byteCount](auto it)
        {
            const ui64 partOffset = Max(offset, it->second.Begin);
            const ui64 partByteCount = Min(end, it->second.End) - partOffset;
            TCachedWriteDataRequest* entry = it->second.Value;
            TStringBuf data = entry->GetBuffer(partOffset, partByteCount);
            result.Parts.push_back({partOffset - offset, data});
        });

    return result;
}

ui64 TNodeCache::GetCachedDataEndOffset() const
{
    return IntervalMap.empty() ? 0 : IntervalMap.rbegin()->second.End;
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
