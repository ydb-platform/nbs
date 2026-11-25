#include "log_index.h"

#include <cloud/storage/core/libs/common/verify.h>

#include <util/generic/utility.h>

namespace NCloud::NJournalled {

namespace {

////////////////////////////////////////////////////////////////////////////////

// How many entries EraseBelow removes before it lets go of the lock.
constexpr size_t MaxErasedEntriesPerLock = 32;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TLogPageIndex::InitLastIndexedLsn(ui64 lastIndexedLsn)
{
    std::lock_guard lock(Lock);

    LastIndexedLsn = lastIndexedLsn;
}

bool TLogPageIndex::TryApplyNext(const TLogRecord& record)
{
    {
        std::lock_guard lock(Lock);

        if (record.PrevLsn != LastIndexedLsn) {
            return false;
        }

        for (const auto& [pageNo, location]: record.PageMappings) {
            const auto hint = ClearRange(pageNo, pageNo + location.PageCount);

            const size_t sizeBefore = Entries.size();
            Entries.emplace_hint(
                hint,
                pageNo,
                TEntry{.Lsn = record.Lsn, .Location = location});

            STORAGE_VERIFY(Entries.size() == sizeBefore + 1, "PageNo", pageNo);
        }

        LastIndexedLsn = record.Lsn;
    }
    return true;
}

void TLogPageIndex::EraseBelow(
    ui64 lsn,
    const TVector<TPageRangeRef>& ranges)
{
    for (const auto& range: ranges) {
        const ui64 to = range.FirstPageNo + range.PageCount;

        ui64 pageNo = range.FirstPageNo;
        while (pageNo < to) {
            pageNo = EraseSlice(lsn, pageNo, to);
        }
    }
}

ui64 TLogPageIndex::EraseSlice(ui64 lsn, ui64 from, ui64 to)
{
    std::lock_guard lock(Lock);

    //
    // Start one entry earlier: it can begin before the range and still
    // cover its first pages
    //

    auto it = Entries.lower_bound(from);
    if (it != Entries.begin()) {
        --it;
    }

    size_t erasedCount = 0;

    while (it != Entries.end() && it->first < to) {
        const ui64 entryTo = it->first + it->second.Location.PageCount;

        if (entryTo <= from || it->second.Lsn >= lsn) {
            ++it;
            continue;
        }

        //
        // Stop here and let the caller take the lock again: the threads
        // that wait for the lock get their turn in between
        //

        if (erasedCount == MaxErasedEntriesPerLock) {
            return Max(from, it->first);
        }

        it = Entries.erase(it);
        ++erasedCount;
    }

    return to;
}

ui64 TLogPageIndex::GetLastIndexedLsn() const
{
    std::lock_guard lock(Lock);

    return LastIndexedLsn;
}

TLogPageIndex::TLookupResult TLogPageIndex::Lookup(
    const TVector<TPageRangeRef>& ranges,
    ui64 afterLsn) const
{
    TLookupResult result;

    {
        std::lock_guard lock(Lock);

        result.LastIndexedLsn = LastIndexedLsn;

        if (afterLsn >= LastIndexedLsn) {
            result.MissingRanges = ranges;
            return result;
        }

        for (const auto& range: ranges) {
            const ui64 from = range.FirstPageNo;
            const ui64 to = from + range.PageCount;

            // The first page of the range that is not looked at yet
            ui64 pageNo = from;

            auto addMissing = [&](ui64 end)
            {
                if (pageNo < end) {
                    result.MissingRanges.push_back(
                        {.FirstPageNo = pageNo, .PageCount = end - pageNo});
                }
            };

            auto it = Entries.lower_bound(from);
            if (it != Entries.begin()) {
                --it;
            }

            for (; it != Entries.end() && it->first < to; ++it) {
                const auto& [lsn, location] = it->second;
                const ui64 entryFrom = it->first;
                const ui64 entryTo = entryFrom + location.PageCount;

                if (entryTo <= from) {
                    continue;
                }

                if (lsn <= afterLsn) {
                    continue;
                }

                const ui64 clipFrom = Max(entryFrom, from);
                const ui64 clipTo = Min(entryTo, to);

                addMissing(clipFrom);
                pageNo = clipTo;

                result.Mappings.push_back(
                    TPageMapping{
                        .PageNo = clipFrom,
                        .Location = TPageRangeRef{
                            .FirstPageNo =
                                location.FirstPageNo + (clipFrom - entryFrom),
                            .PageCount = clipTo - clipFrom}});
            }

            addMissing(to);
        }
    }

    return result;
}

TLogPageIndex::TEntries::iterator TLogPageIndex::ClearRange(ui64 from, ui64 to)
{
    auto it = Entries.lower_bound(from);
    if (it != Entries.begin()) {
        --it;
    }

    while (it != Entries.end() && it->first < to) {
        const ui64 entryFrom = it->first;
        const ui64 lsn = it->second.Lsn;
        const TPageRangeRef location = it->second.Location;
        const ui64 entryTo = entryFrom + location.PageCount;

        if (entryTo <= from) {
            ++it;
            continue;
        }

        it = Entries.erase(it);

        if (entryFrom < from) {
            Entries.emplace_hint(
                it,
                entryFrom,
                TEntry{
                    .Lsn = lsn,
                    .Location = TPageRangeRef{
                        .FirstPageNo = location.FirstPageNo,
                        .PageCount = from - entryFrom}});
        }

        if (entryTo > to) {
            it = Entries.emplace_hint(
                it,
                to,
                TEntry{
                    .Lsn = lsn,
                    .Location = TPageRangeRef{
                        .FirstPageNo = location.FirstPageNo + (to - entryFrom),
                        .PageCount = entryTo - to}});
        }
    }

    return it;
}

}   // namespace NCloud::NJournalled
