#include "write_back_cache_impl.h"

#include <util/generic/vector.h>

#include <algorithm>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

// static
auto TWriteBackCache::TImpl::CalculateDataPartsToRead(
    const TVector<TWriteDataEntry*>& entries,
    ui64 startingFromOffset,
    ui64 length) -> TVector<TWriteDataEntryPart>
{
    struct TPoint
    {
        const TWriteDataEntry* Entry = nullptr;
        ui64 Offset = 0;
        bool IsEnd = false;
        size_t Order = 0;
    };

    TVector<TPoint> points(Reserve(2*entries.size()));

    for (size_t i = 0; i < entries.size(); i++) {
        const auto* entry = entries[i];

        auto pointBegin = entry->Offset;
        auto pointEnd = entry->End();
        if (length != 0) {
            // intersect with [startingFromOffset, length)
            pointBegin = Max(pointBegin, startingFromOffset);
            pointEnd = Min(pointEnd, startingFromOffset + length);
        }

        if (pointBegin < pointEnd) {
            points.emplace_back(entry, pointBegin, false, i);
            points.emplace_back(entry, pointEnd, true, i);
        }
    }

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

        Y_DEBUG_ABORT_UNLESS(lastOffset >= top.Entry->Offset);
        const auto offsetInSource = lastOffset - top.Entry->Offset;

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
auto TWriteBackCache::TImpl::InvertDataParts(
    const TVector<TWriteDataEntryPart>& parts,
    ui64 startingFromOffset,
    ui64 length) -> TVector<TWriteDataEntryPart>
{
    if (parts.empty()) {
        return { {
            .Offset = startingFromOffset,
            .Length = length
        } };
    }

    const ui64 maxOffset = startingFromOffset + length;

    TVector<TWriteDataEntryPart> res(Reserve(parts.size() + 1));

    if (parts.front().Offset > startingFromOffset) {
        res.push_back({
            .Offset = startingFromOffset,
            .Length = Min(parts.front().Offset, maxOffset) - startingFromOffset,
        });
    }

    for (size_t i = 1; i < parts.size(); i++) {
        if (parts[i - 1].End() >= maxOffset) {
            break;
        }
        if (parts[i - 1].End() == parts[i].Offset) {
            continue;
        }
        res.push_back({
            .Offset = parts[i - 1].End(),
            .Length = Min(parts[i].Offset, maxOffset) - parts[i - 1].End()
        });
    }

    if (parts.back().End() < maxOffset) {
        res.push_back({
            .Offset = parts.back().End(),
            .Length = maxOffset - parts.back().End()
        });
    }

    return res;
}

}   // namespace NCloud::NFileStore::NFuse
