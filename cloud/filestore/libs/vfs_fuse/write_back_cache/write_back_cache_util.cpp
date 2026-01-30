#include "write_back_cache_impl.h"

#include <contrib/ydb/core/util/interval_set.h>

#include <util/generic/vector.h>

#include <algorithm>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

// static
auto TWriteBackCache::TUtil::CalculateDataPartsToRead(
    const TWriteDataEntryIntervalMap& map,
    ui64 startingFromOffset,
    ui64 length) -> TVector<TWriteDataEntryPart>
{
    Y_ABORT_UNLESS(length > 0);

    TVector<TWriteDataEntryPart> res;

    map.VisitOverlapping(
        startingFromOffset,
        startingFromOffset + length,
        [&res, offset = startingFromOffset, end = startingFromOffset + length](
            auto it)
        {
            auto partOffset = Max(offset, it->second.Begin);
            auto partEnd = Min(end, it->second.End);
            auto* entry = it->second.Value;
            res.emplace_back(
                it->second.Value,
                partOffset - entry->GetOffset(),
                partOffset,
                partEnd - partOffset);
        });

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
        auto partOffset = Max(sortedParts[i - 1].GetEnd(), startingFromOffset);
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

    if (sortedParts.back().GetEnd() < end) {
        auto partOffset = Max(sortedParts.back().GetEnd(), startingFromOffset);
        res.push_back({
            .Offset = partOffset,
            .Length = end - partOffset
        });
    }

    return res;
}

// static
bool TWriteBackCache::TUtil::IsSorted(const TVector<TWriteDataEntryPart>& parts)
{
    for (size_t i = 1; i < parts.size(); i++) {
        if (parts[i - 1].GetEnd() > parts[i].Offset) {
            return false;
        }
    }
    return true;
}

}   // namespace NCloud::NFileStore::NFuse
