#include "device_helpers.h"

#include <util/string/builder.h>

namespace NCloud::NJournalled {

namespace {

////////////////////////////////////////////////////////////////////////////////

TVector<TPageRange> MakePageRangesImpl(
    const google::protobuf::RepeatedPtrField<NCloud::NProto::TDevicePageGroup>&
        groups)
{
    TVector<TPageRange> ranges;
    ranges.reserve(groups.size());

    for (const auto& group: groups) {
        auto& range = ranges.emplace_back();
        range.FirstPageNo = group.GetFirstPageNo();
        range.Pages.reserve(group.ContentSize());

        for (const auto& content: group.GetContent()) {
            range.Pages.emplace_back(content.data(), content.size());
        }
    }

    return ranges;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TVector<TPageRangeRef> MakePageRangeRefs(
    const NCloud::NProto::TReadPagesRequest& request)
{
    TVector<TPageRangeRef> rangeRefs;
    rangeRefs.reserve(request.PageGroupRefsSize());

    for (const auto& ref: request.GetPageGroupRefs()) {
        rangeRefs.push_back(
            {.FirstPageNo = ref.GetFirstPageNo(),
             .PageCount = ref.GetPageCount()});
    }

    return rangeRefs;
}

TVector<TPageRange> MakePageRanges(
    const NCloud::NProto::TWriteLogRecordRequest& request)
{
    return MakePageRangesImpl(request.GetPageGroups());
}

TVector<TPageRange> MakePageRanges(const NCloud::NProto::TJournalRecord& record)
{
    return MakePageRangesImpl(record.GetPageGroups());
}

NCloud::NProto::TReadPagesResponse MakeReadPagesResponse(
    const TVector<TPageRangeRef>& rangeRefs,
    const TVector<TBuffer>& pages)
{
    ui64 pageCount = 0;
    for (const auto& ref: rangeRefs) {
        pageCount += ref.PageCount;
    }

    if (pages.size() != pageCount) {
        return TErrorResponse(
            E_INVALID_STATE,
            TStringBuilder() << "the device returned " << pages.size()
                             << " pages, expected " << pageCount);
    }

    NCloud::NProto::TReadPagesResponse response;

    auto& groups = *response.MutablePageGroups();
    groups.Reserve(rangeRefs.size());

    ui64 pageIndex = 0;
    for (const auto& ref: rangeRefs) {
        auto& group = *groups.Add();
        group.SetFirstPageNo(ref.FirstPageNo);

        for (ui64 i = 0; i != ref.PageCount; ++i) {
            const auto& page = pages[pageIndex++];
            group.AddContent()->assign(page.Data(), page.Size());
        }
    }

    return response;
}

}   // namespace NCloud::NJournalled
