#include "proto_helpers.h"

#include <util/string/builder.h>

namespace NCloud::NJournalled {

namespace {

////////////////////////////////////////////////////////////////////////////////

TVector<TPageRange> MakePageRangesImpl(
    const google::protobuf::RepeatedPtrField<NProto::TDevicePageGroup>& groups)
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

void FillPageGroups(
    const TVector<TPageRange>& ranges,
    google::protobuf::RepeatedPtrField<NProto::TDevicePageGroup>* groups)
{
    groups->Reserve(ranges.size());

    for (const auto& range: ranges) {
        auto& group = *groups->Add();
        group.SetFirstPageNo(range.FirstPageNo);
        group.MutableContent()->Reserve(range.Pages.size());

        for (const auto& page: range.Pages) {
            group.AddContent()->assign(page.Data(), page.Size());
        }
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TVector<TPageRangeRef> MakePageRangeRefs(
    const NProto::TReadPagesRequest& request)
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

TVector<TPageRange> MakePageRanges(const NProto::TWritePagesRequest& request)
{
    return MakePageRangesImpl(request.GetPageGroups());
}

TVector<TPageRange> MakePageRanges(
    const NProto::TWriteLogRecordRequest& request)
{
    return MakePageRangesImpl(request.GetPageGroups());
}

NProto::TReadJournalTailResponse MakeReadJournalTailResponse(
    const TJournalTail& tail)
{
    NProto::TReadJournalTailResponse response;
    response.SetLsnLowWatermark(tail.LsnLowWatermark);
    response.MutableRecords()->Reserve(tail.Records.size());

    for (const auto& record: tail.Records) {
        auto& journalRecord = *response.AddRecords();
        journalRecord.SetLogSequenceNumber(record.Lsn);
        journalRecord.SetPrevLogSequenceNumber(record.PrevLsn);
        FillPageGroups(record.Ranges, journalRecord.MutablePageGroups());
    }

    return response;
}

NProto::TReadPagesResponse MakeReadPagesResponse(
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

    NProto::TReadPagesResponse response;

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

NProto::TReadPagesResponse MakeReadPagesResponse(
    const TVector<TPageRangeRef>& rangeRefs,
    const TReadPagesResult& result)
{
    auto response = MakeReadPagesResponse(rangeRefs, result.Pages);
    if (!HasError(response)) {
        response.SetLastAckedLogSequenceNumber(result.LastAckedLsn);
    }

    return response;
}

}   // namespace NCloud::NJournalled
