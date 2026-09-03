#include "log_index.h"

#include <cloud/storage/core/libs/common/verify.h>

#include <util/generic/utility.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

void TLogPageMap::InitLastIndexedLsn(ui64 lastIndexedLsn)
{
    with_lock (Lock) {
        LastIndexedLsn = lastIndexedLsn;
    }
}

bool TLogPageMap::AddNext(const TLogRecord& record)
{
    with_lock (Lock) {
        if (record.PrevLsn != LastIndexedLsn) {
            return false;
        }

        for (const auto& [pageNo, pageGroupRef]: record.PageGroupIndex) {
            if (pageGroupRef.PageCount == 0) {
                continue;
            }

            const auto hint =
                TrimOverlaps(Index, pageNo, pageNo + pageGroupRef.PageCount);

            const size_t sizeBefore = Index.size();
            Index.emplace_hint(
                hint,
                pageNo,
                std::make_pair(record.Lsn, pageGroupRef));

            STORAGE_VERIFY(Index.size() == sizeBefore + 1, "PageNo", pageNo);
        }

        LastIndexedLsn = record.Lsn;
    }
    return true;
}

void TLogPageMap::EraseTo(ui64 lsn)
{
    with_lock (Lock) {
        for (auto it = Index.begin(); it != Index.end();) {
            if (it->second.first <= lsn) {
                it = Index.erase(it);
            } else {
                ++it;
            }
        }
    }
}

ui64 TLogPageMap::GetLastIndexedLsn() const
{
    with_lock (Lock) {
        return LastIndexedLsn;
    }
}

auto TLogPageMap::GetIndex(
    const TVector<TPageGroupRef>& pages,
    ui64 afterLsn) const
    -> std::pair<ui64, TVector<std::pair<ui64, TPageGroupRef>>>
{
    std::pair<ui64, TVector<std::pair<ui64, TPageGroupRef>>> result;

    with_lock (Lock) {
        result.first = LastIndexedLsn;

        if (afterLsn >= LastIndexedLsn) {
            return result;
        }

        for (const auto& pageGroup: pages) {
            const ui64 from = pageGroup.FirstPageNo;
            const ui64 to = from + pageGroup.PageCount;

            auto it = Index.lower_bound(from);
            if (it != Index.begin()) {
                --it;
            }

            for (; it != Index.end() && it->first < to; ++it) {
                const auto& [lsn, ref] = it->second;
                const ui64 rangeFrom = it->first;
                const ui64 rangeTo = rangeFrom + ref.PageCount;

                if (rangeTo <= from) {
                    continue;
                }

                if (lsn <= afterLsn) {
                    continue;
                }

                const ui64 clipFrom = Max(rangeFrom, from);
                const ui64 clipTo = Min(rangeTo, to);

                result.second.emplace_back(
                    clipFrom,
                    TPageGroupRef{
                        .FirstPageNo = ref.FirstPageNo + (clipFrom - rangeFrom),
                        .PageCount = clipTo - clipFrom});
            }
        }
    }

    return result;
}

TLogPageMap::TPageIndex::iterator TLogPageMap::TrimOverlaps(
    TPageIndex& index,
    ui64 from,
    ui64 to)
{
    auto it = index.lower_bound(from);
    if (it != index.begin()) {
        --it;
    }

    while (it != index.end() && it->first < to) {
        const ui64 rangeFrom = it->first;
        const ui64 lsn = it->second.first;
        const TPageGroupRef ref = it->second.second;
        const ui64 rangeTo = rangeFrom + ref.PageCount;

        if (!ref.PageCount) {
            it = index.erase(it);
            continue;
        }

        if (rangeTo <= from) {
            ++it;
            continue;
        }

        it = index.erase(it);

        if (rangeFrom < from) {
            index.emplace_hint(
                it,
                rangeFrom,
                std::make_pair(
                    lsn,
                    TPageGroupRef{
                        .FirstPageNo = ref.FirstPageNo,
                        .PageCount = from - rangeFrom}));
        }

        if (rangeTo > to) {
            it = index.emplace_hint(
                it,
                to,
                std::make_pair(
                    lsn,
                    TPageGroupRef{
                        .FirstPageNo = ref.FirstPageNo + (to - rangeFrom),
                        .PageCount = rangeTo - to}));
        }
    }

    return it;
}

}   // namespace NCloud::NJournalled
