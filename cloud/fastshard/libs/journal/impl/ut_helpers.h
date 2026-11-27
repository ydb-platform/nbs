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

// Reads the pages of |rangeRefs| into new buffers of |pageSize| bytes, one
// buffer per page, in the order of the refs. Waits for the read.
inline TResultOrError<TVector<TBuffer>> ReadPages(
    IDevice& device,
    const TVector<TPageRangeRef>& rangeRefs,
    ui32 pageSize)
{
    TVector<TBuffer> pages;

    auto future =
        device.ReadPages(MakePageRangeViews(rangeRefs, pageSize, &pages));

    auto error = future.GetValueSync();
    if (HasError(error)) {
        return error;
    }

    return pages;
}

// Drops the zeroes at the end of the page: a page that is written shorter
// than the page size is read back padded with them.
inline TString TrimPage(TStringBuf page)
{
    while (page.EndsWith('\0')) {
        page.Chop(1);
    }

    return TString(page);
}

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

}   // namespace NCloud::NJournalled
