#include "write_back_cache_impl.h"

#include <contrib/ydb/core/util/interval_set.h>

#include <util/generic/vector.h>

#include <algorithm>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

struct TWriteBackCache::TUtil::TPoint
{
    const TWriteDataEntry* Entry = nullptr;
    ui64 Offset = 0;
    bool IsEnd = false;
    size_t Order = 0;
};

////////////////////////////////////////////////////////////////////////////////

// static
auto TWriteBackCache::TUtil::CalculateDataPartsToRead(
    const TDeque<TWriteDataEntry*>& entries,
    ui64 startingFromOffset,
    ui64 length) -> TVector<TWriteDataEntryPart>
{
    Y_ABORT_UNLESS(length > 0);

    TVector<TPoint> points(Reserve(2*entries.size()));

    for (size_t i = 0; i < entries.size(); i++) {
        const auto* entry = entries[i];

        auto pointBegin = entry->Offset();
        auto pointEnd = entry->End();

        // intersect with [startingFromOffset, length)
        pointBegin = Max(pointBegin, startingFromOffset);
        pointEnd = Min(pointEnd, startingFromOffset + length);

        if (pointBegin < pointEnd) {
            points.emplace_back(entry, pointBegin, false, i);
            points.emplace_back(entry, pointEnd, true, i);
        }
    }

    return CalculateDataParts(std::move(points));
}

// static
auto TWriteBackCache::TUtil::CalculateDataParts(
    TVector<TPoint> points) -> TVector<TWriteDataEntryPart>
{
    if (points.empty()) {
        return {};
    }

    StableSort(points, [] (const auto& l, const auto& r) {
        return l.Offset < r.Offset;
    });

    TVector<TWriteDataEntryPart> res(Reserve(points.size()));

    const auto& heapComparator = [] (const auto& l, const auto& r) {
        return l.Order < r.Order;
    };
    TVector<TPoint> heap;

    const auto cutTop = [&res, &heap] (auto lastOffset, auto currOffset)
    {
        if (currOffset <= lastOffset) {
            // ignore
            return lastOffset;
        }

        const auto partLength = currOffset - lastOffset;
        const auto& top = heap.front();

        Y_DEBUG_ABORT_UNLESS(lastOffset >= top.Entry->Offset());
        const auto offsetInSource = lastOffset - top.Entry->Offset();

        if (!res.empty() &&
            res.back().Source == top.Entry &&
            res.back().End() == lastOffset)
        {
            // extend last entry
            res.back().Length += partLength;
        } else {
            res.emplace_back(
                top.Entry, // source
                offsetInSource,
                lastOffset,
                partLength);
        }

        // cut part of the top
        lastOffset += partLength;
        return lastOffset;
    };

    ui64 cutEnd = 0;
    for (const auto& point: points) {
        if (point.IsEnd) {
            // cut at every interval's end
            cutEnd = cutTop(cutEnd, point.Offset);
        } else {
            if (heap.empty()) {
                // no intervals before, start from scratch
                cutEnd = point.Offset;
            } else {
                if (point.Order > heap.front().Order) {
                    // new interval is now on top
                    cutEnd = cutTop(cutEnd, point.Offset);
                }
            }

            // add to heap
            heap.push_back(point);
            std::push_heap(heap.begin(), heap.end(), heapComparator);
        }

        // remove affected (cut) entries
        while (!heap.empty()) {
            const auto& top = heap.front();
            if (cutEnd < top.Entry->End()) {
                break;
            }

            // remove from heap
            std::pop_heap(heap.begin(), heap.end(), heapComparator);
            heap.pop_back();
        }
    }

    return res;
}

// static
auto TWriteBackCache::TUtil::InvertDataParts(
    const TVector<TWriteDataEntryPart>& sortedParts,
    ui64 startingFromOffset,
    ui64 length) -> TVector<TWriteDataEntryPart>
{
    Y_DEBUG_ABORT_UNLESS(IsSorted(sortedParts));

    if (sortedParts.empty()) {
        return {{
            .Offset = startingFromOffset,
            .Length = length
        }};
    }

    const ui64 end = startingFromOffset + length;

    TVector<TWriteDataEntryPart> res(Reserve(sortedParts.size() + 1));

    if (sortedParts.front().Offset > startingFromOffset) {
        auto partEnd = Min(sortedParts.front().Offset, end);
        res.push_back({
            .Offset = startingFromOffset,
            .Length = partEnd - startingFromOffset
        });
    }

    for (size_t i = 1; i < sortedParts.size(); i++) {
        // Calculate intersection of the gap between (i-1)th and ith parts with
        // the interval [startingFromOffset, maxOffset)
        auto partOffset = Max(sortedParts[i - 1].End(), startingFromOffset);
        auto partEnd = Min(sortedParts[i].Offset, end);

        if (partOffset >= end) {
            // The gap is located outside the interval to the right
            // Since the parts list is ordered, all remaining gaps
            // will be outside the interval
            break;
        }

        if (partEnd <= partOffset) {
            // The gap is located outside the interval to the left
            // or the gap has zero length
            continue;
        }

        res.push_back({
            .Offset = partOffset,
            .Length = partEnd - partOffset
        });
    }

    if (sortedParts.back().End() < end) {
        auto partOffset = Max(sortedParts.back().End(), startingFromOffset);
        res.push_back({
            .Offset = partOffset,
            .Length = end - partOffset
        });
    }

    return res;
}

// static
size_t TWriteBackCache::TUtil::CalculateEntriesCountToFlush(
    const TDeque<TWriteDataEntry*>& entries,
    ui32 maxWriteRequestSize,
    ui32 maxWriteRequestsCount,
    ui32 maxSumWriteRequestsSize)
{
    Y_ABORT_UNLESS(maxWriteRequestSize > 0);
    Y_ABORT_UNLESS(maxWriteRequestsCount > 0);
    Y_ABORT_UNLESS(maxSumWriteRequestsSize > 0);

    NKikimr::TIntervalSet<ui64, 16> intervalSet;
    ui64 requestsCount = 0;
    ui64 bytesCount = 0;

    for (size_t i = 0; i < entries.size(); i++) {
        const auto* entry = entries[i];

        ui64 size = entry->GetBuffer().Size();
        Y_ABORT_UNLESS(size > 0);

        intervalSet.Add(entry->Offset(), entry->End());

        // Value bytesCount has to be calculated using O(N) algorithm
        // because TIntervalSet does not track the sum of lengths of intervals.
        // Same for requestsCount.
        //
        // An euristic is used here to reduce complexity:
        // - Pessimistically increase requestsCount and sumLength
        // - Calculate the actual values only when the limits are exceeded
        //
        // Notes:
        // - maxWriteRequestsCount is expected to be small (default: 32)
        // - maxSumWriteRequestsSize (default: 16MiB) is expected to be times
        //   larger than maxWriteRequestSize (default: 1MiB) and the size of
        //   TWriteDataEntry buffer (maximal: 1MiB)
        requestsCount += (size - 1) / maxWriteRequestSize + 1;
        bytesCount += size;

        if (requestsCount > maxWriteRequestsCount ||
            bytesCount > maxSumWriteRequestsSize)
        {
            // Calculate the actual values
            requestsCount = 0;
            bytesCount = 0;

            for (const auto& interval: intervalSet) {
                auto size = interval.second - interval.first;
                requestsCount += (size - 1) / maxWriteRequestSize + 1;
                bytesCount += size;
            }

            // Still exceed the limits - don't accept more entries
            if (requestsCount > maxWriteRequestsCount ||
                bytesCount > maxSumWriteRequestsSize)
            {
                return i;
            }
        }
    }

    return entries.size();
}

// static
auto TWriteBackCache::TUtil::CalculateDataPartsToFlush(
    const TDeque<TWriteDataEntry*>& entries,
    size_t count) -> TVector<TWriteDataEntryPart>
{
    Y_ABORT_UNLESS(count > 0);
    Y_ABORT_UNLESS(count <= entries.size());

    TVector<TPoint> points(Reserve(2 * count));

    for (size_t i = 0; i < count; i++) {
        const auto* entry = entries[i];

        auto pointBegin = entry->Offset();
        auto pointEnd = entry->End();

        if (pointBegin < pointEnd) {
            points.emplace_back(entry, pointBegin, false, i);
            points.emplace_back(entry, pointEnd, true, i);
        }
    }

    return CalculateDataParts(std::move(points));
}

// static
bool TWriteBackCache::TUtil::IsSorted(const TVector<TWriteDataEntryPart>& parts)
{
    for (size_t i = 1; i < parts.size(); i++) {
        if (parts[i - 1].End() > parts[i].Offset) {
            return false;
        }
    }
    return true;
}

}   // namespace NCloud::NFileStore::NFuse
