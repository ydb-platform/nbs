#include "proto_helpers.h"

#include <util/generic/ylimits.h>

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

TVector<TPageRangeView> MakePageRangeViews(
    const NProto::TReadPagesRequest& request,
    ui32 pageSize,
    NProto::TReadPagesResponse* response)
{
    TVector<TPageRangeView> rangeViews;
    rangeViews.reserve(request.PageGroupRefsSize());

    auto& groups = *response->MutablePageGroups();
    groups.Reserve(request.PageGroupRefsSize());

    for (const auto& ref: request.GetPageGroupRefs()) {
        auto& group = *groups.Add();
        group.SetFirstPageNo(ref.GetFirstPageNo());
        group.MutableContent()->Reserve(ref.GetPageCount());

        auto& view = rangeViews.emplace_back();
        view.FirstPageNo = ref.GetFirstPageNo();
        view.Pages.reserve(ref.GetPageCount());

        for (ui64 i = 0; i != ref.GetPageCount(); ++i) {
            auto& content = *group.AddContent();
            content.ReserveAndResize(pageSize);
            view.Pages.emplace_back(content.begin(), content.size());
        }
    }

    return rangeViews;
}

ui64 GetPageCount(const NProto::TReadPagesRequest& request)
{
    ui64 pageCount = 0;
    for (const auto& ref: request.GetPageGroupRefs()) {
        if (ref.GetPageCount() > Max<ui64>() - pageCount) {
            return Max<ui64>();
        }

        pageCount += ref.GetPageCount();
    }

    return pageCount;
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

}   // namespace NCloud::NJournalled
