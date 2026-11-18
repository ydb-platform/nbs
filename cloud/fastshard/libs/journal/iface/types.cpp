#include "types.h"

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

TVector<TPageRangeView> MakePageRangeViews(const TVector<TPageRange>& ranges)
{
    TVector<TPageRangeView> views;
    views.reserve(ranges.size());

    for (const auto& range: ranges) {
        auto& view = views.emplace_back();
        view.FirstPageNo = range.FirstPageNo;
        view.Pages.reserve(range.Pages.size());

        for (const auto& page: range.Pages) {
            view.Pages.emplace_back(page.Data(), page.Size());
        }
    }

    return views;
}

TJournalRecordView MakeJournalRecordView(const TJournalRecord& record)
{
    return {
        .Lsn = record.Lsn,
        .PrevLsn = record.PrevLsn,
        .Ranges = MakePageRangeViews(record.Ranges)};
}

}   // namespace NCloud::NJournalled
