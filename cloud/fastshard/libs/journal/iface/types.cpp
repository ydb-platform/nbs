#include "types.h"

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

TVector<TPageRangeView> MakePageRangeViews(
    const TVector<TPageRangeRef>& rangeRefs,
    ui32 pageSize,
    TVector<TBuffer>* pages)
{
    ui64 pageCount = 0;
    for (const auto& ref: rangeRefs) {
        pageCount += ref.PageCount;
    }

    pages->resize(pageCount);

    TVector<TPageRangeView> rangeViews;
    rangeViews.reserve(rangeRefs.size());

    ui64 pageIndex = 0;
    for (const auto& ref: rangeRefs) {
        auto& view = rangeViews.emplace_back();
        view.FirstPageNo = ref.FirstPageNo;
        view.Pages.reserve(ref.PageCount);

        for (ui64 i = 0; i != ref.PageCount; ++i) {
            TBuffer& page = (*pages)[pageIndex++];
            page.Resize(pageSize);
            view.Pages.emplace_back(page.Data(), page.Size());
        }
    }

    return rangeViews;
}

TVector<TPageRangeRef> MakePageRangeRefs(
    const TVector<TPageRangeView>& rangeViews)
{
    TVector<TPageRangeRef> rangeRefs;
    rangeRefs.reserve(rangeViews.size());

    for (const auto& view: rangeViews) {
        rangeRefs.push_back(
            {.FirstPageNo = view.FirstPageNo, .PageCount = view.Pages.size()});
    }

    return rangeRefs;
}

}   // namespace NCloud::NJournalled
