#include "log_index.h"

#include <cloud/storage/core/libs/common/verify.h>

#include <util/generic/utility.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

void TLogPageIndex::InitLastIndexedLsn(ui64 lastIndexedLsn)
{
    with_lock (Lock) {
        LastIndexedLsn = lastIndexedLsn;
    }
}

bool TLogPageIndex::TryApplyNext(const TLogRecord& record)
{
    with_lock (Lock) {
        if (record.PrevLsn != LastIndexedLsn) {
            return false;
        }

        for (const auto& [pageNo, location]: record.PageMappings) {
            if (location.PageCount == 0) {
                continue;
            }

            const auto hint =
                ClearRange(pageNo, pageNo + location.PageCount);

            const size_t sizeBefore = Entries.size();
            Entries.emplace_hint(
                hint,
                pageNo,
                std::make_pair(record.Lsn, location));

            STORAGE_VERIFY(Entries.size() == sizeBefore + 1, "PageNo", pageNo);
        }

        LastIndexedLsn = record.Lsn;
    }
    return true;
}

void TLogPageIndex::EraseUpTo(ui64 lsn)
{
    with_lock (Lock) {
        for (auto it = Entries.begin(); it != Entries.end();) {
            if (it->second.first <= lsn) {
                it = Entries.erase(it);
            } else {
                ++it;
            }
        }
    }
}

ui64 TLogPageIndex::GetLastIndexedLsn() const
{
    with_lock (Lock) {
        return LastIndexedLsn;
    }
}

auto TLogPageIndex::Lookup(
    const TVector<TPageRange>& ranges,
    ui64 afterLsn) const -> TLookupResult
{
    TLookupResult result;

    with_lock (Lock) {
        result.LastIndexedLsn = LastIndexedLsn;

        if (afterLsn >= LastIndexedLsn) {
            return result;
        }

        for (const auto& range: ranges) {
            const ui64 from = range.FirstPageNo;
            const ui64 to = from + range.PageCount;

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

                result.Mappings.push_back(TPageMapping{
                    .PageNo = clipFrom,
                    .Location = TPageRange{
                        .FirstPageNo =
                            location.FirstPageNo + (clipFrom - entryFrom),
                        .PageCount = clipTo - clipFrom}});
            }
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
        const ui64 lsn = it->second.first;
        const TPageRange location = it->second.second;
        const ui64 entryTo = entryFrom + location.PageCount;

        if (!location.PageCount) {
            it = Entries.erase(it);
            continue;
        }

        if (entryTo <= from) {
            ++it;
            continue;
        }

        it = Entries.erase(it);

        if (entryFrom < from) {
            Entries.emplace_hint(
                it,
                entryFrom,
                std::make_pair(
                    lsn,
                    TPageRange{
                        .FirstPageNo = location.FirstPageNo,
                        .PageCount = from - entryFrom}));
        }

        if (entryTo > to) {
            it = Entries.emplace_hint(
                it,
                to,
                std::make_pair(
                    lsn,
                    TPageRange{
                        .FirstPageNo =
                            location.FirstPageNo + (to - entryFrom),
                        .PageCount = entryTo - to}));
        }
    }

    return it;
}

}   // namespace NCloud::NJournalled
