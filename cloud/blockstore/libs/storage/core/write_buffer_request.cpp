#include "write_buffer_request.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

using TRequest = TRequestInBuffer<TWriteBufferRequestData>;

namespace {

////////////////////////////////////////////////////////////////////////////////

ui32 CalculateRequestCount(
    const TRequestGrouping& g,
    const TVector<TRequest>& requests)
{
    ui32 groupedRequestCount = 0;
    for (const auto& group: g.Groups) {
        groupedRequestCount += group.Requests.size();
    }

    ui32 ungroupedRequestCount = requests.end() - g.FirstUngrouped;
    return groupedRequestCount + ungroupedRequestCount;
}

}   // namespace

TRequestGrouping GroupRequests(
    TVector<TRequest>& requests,
    ui32 totalWeight,
    ui32 minWeight,
    ui32 maxWeight,
    ui32 maxRange)
{
    Sort(
        requests.begin(),
        requests.end(),
        [](const TRequest& a, const TRequest& b)
        {
            const auto& ar = a.Data.Range;
            const auto& br = b.Data.Range;
            if (ar.Start == br.Start) {
                return ar.End > br.End;
            }

            return ar.Start < br.Start;
        });

    TRequestGrouping g;

    auto it = requests.begin();
    g.FirstUngrouped = it;

    // fast path
    if (totalWeight < minWeight) {
        bool haveDups = false;
        for (ui32 i = 1; i < requests.size(); ++i) {
            if (requests[i - 1].Data.Range.Overlaps(requests[i].Data.Range)) {
                haveDups = true;
                break;
            }
        }

        if (!haveDups) {
            return g;
        }
    }

    auto last = it;
    ui32 weight = 0;
    ui32 lastDiff = 0;

    TVector<std::pair<TRequest*, ui32>> dedup;

    auto flush = [&](TRequest* end, TRequest* last)
    {
        g.Groups.emplace_back();
        g.Groups.back().Weight = weight;

        auto d = dedup.begin();

        while (g.FirstUngrouped != end) {
            // applying the decrease in weight to all ranges in this group
            // except the last one
            if (d != dedup.end() && d->first == g.FirstUngrouped) {
                if (g.FirstUngrouped != last) {
                    g.FirstUngrouped->Weight -= d->second;
                    if (g.FirstUngrouped->Weight) {
                        g.FirstUngrouped->Data.Range.End -= d->second;
                    }
                }

                ++d;
            }

            g.Groups.back().Requests.push_back(g.FirstUngrouped);
            ++g.FirstUngrouped;
        }
    };

    while (it != requests.end()) {
        auto cur = it;

        // enforcing blob width limit (as in NBS-299)
        // this thing may actually produce blobs that are smaller than
        // minWeight, but flush op does the same thing
        // actually it might be a good idea to unify this code with the
        // logic from flush op and fix this thing
        const auto rangeSize =
            cur->Data.Range.End - g.FirstUngrouped->Data.Range.Start;

        if (weight) {
            if (weight + cur->Weight - lastDiff > maxWeight ||
                rangeSize > maxRange)
            {
                flush(cur, last);

                weight = 0;
                lastDiff = 0;
                dedup.clear();
            }
        }

        ++it;

        while (it != requests.end() &&
               it->Data.Range.End <= cur->Data.Range.End)
        {
            ++it;
        }

        ui32 diff = 0;
        if (it != requests.end() && it->Data.Range.Start <= cur->Data.Range.End)
        {
            const auto newEnd = it->Data.Range.Start - 1;
            Y_ABORT_UNLESS(cur->Data.Range.Start <= newEnd);
            diff = cur->Data.Range.End - newEnd;

            // saving the decrease in weight for this range
            dedup.push_back(std::make_pair(cur, diff));
        }

        for (auto it2 = cur + 1; it2 != it; ++it2) {
            // marking this range as empty
            dedup.push_back(std::make_pair(it2, it2->Data.Range.Size()));
        }

        Y_ABORT_UNLESS(cur->Weight > lastDiff);
        weight += cur->Weight - lastDiff;

        // deliberately ignoring all ranges with zero weight here
        last = cur;
        lastDiff = diff;
    }

    if (weight >= minWeight) {
        flush(requests.end(), requests.end());
    } else {
        // deduplicating the ungrouped requests
        for (const auto& d: dedup) {
            d.first->Weight -= d.second;
            if (d.first->Weight) {
                d.first->Data.Range.End -= d.second;
            }
        }
    }

    Y_DEBUG_ABORT_UNLESS(requests.size() == CalculateRequestCount(g, requests));

    return g;
}

}   // namespace NCloud::NBlockStore::NStorage
