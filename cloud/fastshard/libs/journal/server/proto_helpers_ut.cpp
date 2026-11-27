#include "proto_helpers.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/ylimits.h>

namespace NCloud::NJournalled {

namespace {

////////////////////////////////////////////////////////////////////////////////

using TPageGroups =
    google::protobuf::RepeatedPtrField<NProto::TDevicePageGroup>;

void AddPageGroup(
    TPageGroups* groups,
    ui64 firstPageNo,
    const TVector<TString>& content)
{
    auto& group = *groups->Add();
    group.SetFirstPageNo(firstPageNo);
    for (const auto& page: content) {
        group.AddContent(page);
    }
}

TPageRange MakePageRange(ui64 firstPageNo, const TVector<TString>& content)
{
    TPageRange range{.FirstPageNo = firstPageNo};
    for (const auto& page: content) {
        range.Pages.emplace_back(page.data(), page.size());
    }
    return range;
}

TVector<TString> GetContent(const TPageRange& range)
{
    TVector<TString> content;
    for (const auto& page: range.Pages) {
        content.emplace_back(page.Data(), page.Size());
    }
    return content;
}

TVector<TString> GetContent(const NProto::TDevicePageGroup& group)
{
    return {group.GetContent().begin(), group.GetContent().end()};
}

template <typename TRequest>
void CheckPageRangesOfRequest()
{
    TRequest request;
    AddPageGroup(request.MutablePageGroups(), 10, {"a", "bc"});
    AddPageGroup(request.MutablePageGroups(), 20, {});
    AddPageGroup(request.MutablePageGroups(), 30, {TString("d\0e", 3)});

    const auto ranges = MakePageRanges(request);

    UNIT_ASSERT_VALUES_EQUAL(3, ranges.size());
    UNIT_ASSERT_VALUES_EQUAL(10, ranges[0].FirstPageNo);
    UNIT_ASSERT_VALUES_EQUAL(
        (TVector<TString>{"a", "bc"}),
        GetContent(ranges[0]));
    UNIT_ASSERT_VALUES_EQUAL(20, ranges[1].FirstPageNo);
    UNIT_ASSERT_VALUES_EQUAL(0, ranges[1].Pages.size());
    UNIT_ASSERT_VALUES_EQUAL(30, ranges[2].FirstPageNo);
    UNIT_ASSERT_VALUES_EQUAL(
        (TVector<TString>{TString("d\0e", 3)}),
        GetContent(ranges[2]));

    UNIT_ASSERT_VALUES_EQUAL(0, MakePageRanges(TRequest()).size());
}

constexpr ui32 PageSize = 8;

NProto::TReadPagesRequest MakeRequest(
    const TVector<std::pair<ui64 /*firstPageNo*/, ui64 /*pageCount*/>>& refs)
{
    NProto::TReadPagesRequest request;

    for (const auto& [firstPageNo, pageCount]: refs) {
        auto& ref = *request.AddPageGroupRefs();
        ref.SetFirstPageNo(firstPageNo);
        ref.SetPageCount(pageCount);
    }

    return request;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TProtoHelpersTest)
{
    Y_UNIT_TEST(ShouldMakeViewsOfResponsePages)
    {
        const auto request = MakeRequest({{10, 2}, {20, 0}, {30, 1}});

        NProto::TReadPagesResponse response;
        const auto views = MakePageRangeViews(request, PageSize, &response);

        // a page group and a view per ref, a page of the page size per page

        UNIT_ASSERT_VALUES_EQUAL(3, response.PageGroupsSize());
        UNIT_ASSERT_VALUES_EQUAL(3, views.size());

        for (size_t i = 0; i != views.size(); ++i) {
            const auto& ref = request.GetPageGroupRefs(i);
            const auto& group = response.GetPageGroups(i);

            UNIT_ASSERT_VALUES_EQUAL(ref.GetFirstPageNo(), views[i].FirstPageNo);
            UNIT_ASSERT_VALUES_EQUAL(
                ref.GetFirstPageNo(),
                group.GetFirstPageNo());
            UNIT_ASSERT_VALUES_EQUAL(ref.GetPageCount(), views[i].Pages.size());
            UNIT_ASSERT_VALUES_EQUAL(ref.GetPageCount(), group.ContentSize());

            for (const auto& page: views[i].Pages) {
                UNIT_ASSERT_VALUES_EQUAL(PageSize, page.size());
            }
        }

        // what is written through the views lands in the response

        char fill = 'a';
        for (const auto& view: views) {
            for (const auto& page: view.Pages) {
                std::fill(page.begin(), page.end(), fill++);
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(
            TString(PageSize, 'a'),
            response.GetPageGroups(0).GetContent(0));
        UNIT_ASSERT_VALUES_EQUAL(
            TString(PageSize, 'b'),
            response.GetPageGroups(0).GetContent(1));
        UNIT_ASSERT_VALUES_EQUAL(
            TString(PageSize, 'c'),
            response.GetPageGroups(2).GetContent(0));
    }

    Y_UNIT_TEST(ShouldCountRequestedPages)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, GetPageCount(MakeRequest({})));
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            GetPageCount(MakeRequest({{10, 2}, {20, 0}, {30, 1}})));

        // the count does not wrap around

        UNIT_ASSERT_VALUES_EQUAL(
            Max<ui64>(),
            GetPageCount(MakeRequest({{0, Max<ui64>()}, {0, 2}})));
    }

    Y_UNIT_TEST(ShouldMakePageRangesFromWritePagesRequest)
    {
        CheckPageRangesOfRequest<NProto::TWritePagesRequest>();
    }

    Y_UNIT_TEST(ShouldMakePageRangesFromWriteLogRecordRequest)
    {
        CheckPageRangesOfRequest<NProto::TWriteLogRecordRequest>();
    }

    Y_UNIT_TEST(ShouldMakeReadJournalTailResponse)
    {
        TJournalTail tail{.LsnLowWatermark = 5};
        auto& first = tail.Records.emplace_back();
        first.Lsn = 7;
        first.PrevLsn = 6;
        first.Ranges.push_back(MakePageRange(10, {"a", "bc"}));
        first.Ranges.push_back(MakePageRange(30, {"d"}));
        auto& second = tail.Records.emplace_back();
        second.Lsn = 8;
        second.PrevLsn = 7;

        const auto response = MakeReadJournalTailResponse(tail);

        UNIT_ASSERT_C(!HasError(response), FormatError(response.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(5, response.GetLsnLowWatermark());
        UNIT_ASSERT_VALUES_EQUAL(2, response.RecordsSize());

        const auto& firstRecord = response.GetRecords(0);
        UNIT_ASSERT_VALUES_EQUAL(7, firstRecord.GetLogSequenceNumber());
        UNIT_ASSERT_VALUES_EQUAL(6, firstRecord.GetPrevLogSequenceNumber());
        UNIT_ASSERT_VALUES_EQUAL(2, firstRecord.PageGroupsSize());
        UNIT_ASSERT_VALUES_EQUAL(
            10,
            firstRecord.GetPageGroups(0).GetFirstPageNo());
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<TString>{"a", "bc"}),
            GetContent(firstRecord.GetPageGroups(0)));
        UNIT_ASSERT_VALUES_EQUAL(
            30,
            firstRecord.GetPageGroups(1).GetFirstPageNo());
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<TString>{"d"}),
            GetContent(firstRecord.GetPageGroups(1)));

        const auto& secondRecord = response.GetRecords(1);
        UNIT_ASSERT_VALUES_EQUAL(8, secondRecord.GetLogSequenceNumber());
        UNIT_ASSERT_VALUES_EQUAL(7, secondRecord.GetPrevLogSequenceNumber());
        UNIT_ASSERT_VALUES_EQUAL(0, secondRecord.PageGroupsSize());
    }

    Y_UNIT_TEST(ShouldMakeReadJournalTailResponseOfEmptyTail)
    {
        const auto response = MakeReadJournalTailResponse({});

        UNIT_ASSERT_C(!HasError(response), FormatError(response.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(0, response.GetLsnLowWatermark());
        UNIT_ASSERT_VALUES_EQUAL(0, response.RecordsSize());
    }

    Y_UNIT_TEST(ShouldKeepPageGroupsOfWriteLogRecordRequestInJournalTail)
    {
        NProto::TWriteLogRecordRequest request;
        AddPageGroup(request.MutablePageGroups(), 10, {"a", "bc"});
        AddPageGroup(request.MutablePageGroups(), 20, {});
        AddPageGroup(request.MutablePageGroups(), 30, {TString("d\0e", 3)});

        TJournalTail tail;
        tail.Records.push_back({.Ranges = MakePageRanges(request)});

        const auto response = MakeReadJournalTailResponse(tail);

        UNIT_ASSERT_VALUES_EQUAL(1, response.RecordsSize());
        const auto& groups = response.GetRecords(0).GetPageGroups();
        UNIT_ASSERT_VALUES_EQUAL(request.PageGroupsSize(), groups.size());
        for (int i = 0; i != groups.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                request.GetPageGroups(i).GetFirstPageNo(),
                groups[i].GetFirstPageNo(),
                i);
            UNIT_ASSERT_VALUES_EQUAL_C(
                GetContent(request.GetPageGroups(i)),
                GetContent(groups[i]),
                i);
        }
    }
}

}   // namespace NCloud::NJournalled
