#pragma once

#include <cloud/fastshard/libs/journal/iface/device.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/buffer.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////
// Helpers for the unit tests.

// The pages that were read for one range ref.
struct TPageGroup
{
    ui64 FirstPageNo = 0;
    TVector<TString> Pages;
};

// Groups the pages by the range refs they were read for. Fails if the number
// of pages is not what the refs ask for.
inline TResultOrError<TVector<TPageGroup>> GroupPages(
    const TVector<TPageRangeRef>& rangeRefs,
    const TVector<TBuffer>& pages)
{
    ui64 pageCount = 0;
    for (const auto& ref: rangeRefs) {
        pageCount += ref.PageCount;
    }

    if (pages.size() != pageCount) {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder()
                << "got " << pages.size() << " pages, expected " << pageCount);
    }

    TVector<TPageGroup> groups;
    groups.reserve(rangeRefs.size());

    size_t pageIndex = 0;
    for (const auto& ref: rangeRefs) {
        auto& group = groups.emplace_back();
        group.FirstPageNo = ref.FirstPageNo;

        for (ui64 i = 0; i != ref.PageCount; ++i) {
            const auto& page = pages[pageIndex++];
            group.Pages.emplace_back(page.Data(), page.Size());
        }
    }

    return groups;
}

// Copies the pages that the views point to. A test device uses it to keep the
// pages of a write after the write is done.
inline TVector<TPageRange> CopyPageRanges(const TVector<TPageRangeView>& views)
{
    TVector<TPageRange> ranges;
    ranges.reserve(views.size());

    for (const auto& view: views) {
        auto& range = ranges.emplace_back();
        range.FirstPageNo = view.FirstPageNo;

        for (const auto& page: view.Pages) {
            range.Pages.emplace_back(page.data(), page.size());
        }
    }

    return ranges;
}

// The views point to |pages|, so |pages| must live as long as the views.
inline TVector<TStringBuf> MakePageViews(const TVector<TBuffer>& pages)
{
    TVector<TStringBuf> views;
    views.reserve(pages.size());

    for (const auto& page: pages) {
        views.emplace_back(page.Data(), page.Size());
    }

    return views;
}

}   // namespace NCloud::NJournalled
