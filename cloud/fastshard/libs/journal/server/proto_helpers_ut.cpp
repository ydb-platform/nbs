#include "proto_helpers.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TProtoHelpersTest)
{
    Y_UNIT_TEST(ShouldNotMakeResponseOfUnexpectedPageCount)
    {
        TVector<TBuffer> pages;
        pages.emplace_back("a", 1);

        const auto response =
            MakeReadPagesResponse({{.FirstPageNo = 10, .PageCount = 2}}, pages);

        UNIT_ASSERT_VALUES_EQUAL_C(
            E_INVALID_STATE,
            response.GetError().GetCode(),
            FormatError(response.GetError()));
    }

    Y_UNIT_TEST(ShouldSetLastAckedLsnInReadPagesResponse)
    {
        TReadPagesResult result{.LastAckedLsn = 42};
        result.Pages.emplace_back("a", 1);
        result.Pages.emplace_back("b", 1);

        const auto response = MakeReadPagesResponse(
            {{.FirstPageNo = 10, .PageCount = 2}},
            result);

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response.GetError().GetCode(),
            FormatError(response.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(42, response.GetLastAckedLogSequenceNumber());
        UNIT_ASSERT_VALUES_EQUAL(1, response.PageGroupsSize());
        UNIT_ASSERT_VALUES_EQUAL(
            10,
            response.GetPageGroups(0).GetFirstPageNo());
        UNIT_ASSERT_VALUES_EQUAL("a", response.GetPageGroups(0).GetContent(0));
        UNIT_ASSERT_VALUES_EQUAL("b", response.GetPageGroups(0).GetContent(1));
    }

    Y_UNIT_TEST(ShouldSplitPagesBetweenGroupsOfReadPagesResponse)
    {
        TVector<TBuffer> pages;
        for (const TStringBuf page: {"a", "b", "c", "d"}) {
            pages.emplace_back(page.data(), page.size());
        }

        const auto response = MakeReadPagesResponse(
            {{.FirstPageNo = 10, .PageCount = 1},
             {.FirstPageNo = 20, .PageCount = 0},
             {.FirstPageNo = 30, .PageCount = 3}},
            pages);

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response.GetError().GetCode(),
            FormatError(response.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(3, response.PageGroupsSize());
        UNIT_ASSERT_VALUES_EQUAL(
            10,
            response.GetPageGroups(0).GetFirstPageNo());
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<TString>{"a"}),
            GetContent(response.GetPageGroups(0)));
        UNIT_ASSERT_VALUES_EQUAL(
            20,
            response.GetPageGroups(1).GetFirstPageNo());
        UNIT_ASSERT_VALUES_EQUAL(0, response.GetPageGroups(1).ContentSize());
        UNIT_ASSERT_VALUES_EQUAL(
            30,
            response.GetPageGroups(2).GetFirstPageNo());
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<TString>{"b", "c", "d"}),
            GetContent(response.GetPageGroups(2)));
    }

    Y_UNIT_TEST(ShouldNotSetLastAckedLsnInFailedReadPagesResponse)
    {
        TReadPagesResult result{.LastAckedLsn = 42};
        result.Pages.emplace_back("a", 1);

        const auto response = MakeReadPagesResponse(
            {{.FirstPageNo = 10, .PageCount = 2}},
            result);

        UNIT_ASSERT_VALUES_EQUAL_C(
            E_INVALID_STATE,
            response.GetError().GetCode(),
            FormatError(response.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(0, response.GetLastAckedLogSequenceNumber());
        UNIT_ASSERT_VALUES_EQUAL(0, response.PageGroupsSize());
    }

    Y_UNIT_TEST(ShouldMakePageRangeRefsFromReadPagesRequest)
    {
        NProto::TReadPagesRequest request;
        UNIT_ASSERT_VALUES_EQUAL(0, MakePageRangeRefs(request).size());

        auto& first = *request.AddPageGroupRefs();
        first.SetFirstPageNo(10);
        first.SetPageCount(2);
        auto& second = *request.AddPageGroupRefs();
        second.SetFirstPageNo(1ull << 40);
        second.SetPageCount(1ull << 33);

        const auto refs = MakePageRangeRefs(request);

        UNIT_ASSERT_VALUES_EQUAL(2, refs.size());
        UNIT_ASSERT_VALUES_EQUAL(10, refs[0].FirstPageNo);
        UNIT_ASSERT_VALUES_EQUAL(2, refs[0].PageCount);
        UNIT_ASSERT_VALUES_EQUAL(1ull << 40, refs[1].FirstPageNo);
        UNIT_ASSERT_VALUES_EQUAL(1ull << 33, refs[1].PageCount);
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
