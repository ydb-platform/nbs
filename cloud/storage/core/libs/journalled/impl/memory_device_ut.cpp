#include "memory_device.h"

#include "device_helpers.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/journalled/iface/device.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>
#include <util/string/join.h>

namespace NCloud::NJournalled {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DefaultPageSize = 4096;

TVector<TPageRange> MakeRanges(
    const TVector<std::pair<ui64 /*firstPageNo*/, TVector<TString>>>& groups)
{
    TVector<TPageRange> ranges;

    for (const auto& [firstPageNo, content]: groups) {
        auto& range = ranges.emplace_back();
        range.FirstPageNo = firstPageNo;

        for (const auto& page: content) {
            range.Pages.emplace_back(page.data(), page.size());
        }
    }

    return ranges;
}

TVector<TPageRangeRef> MakeRangeRefs(
    const TVector<std::pair<ui64 /*firstPageNo*/, ui64 /*pageCount*/>>& refs)
{
    TVector<TPageRangeRef> rangeRefs;

    for (const auto& [firstPageNo, pageCount]: refs) {
        rangeRefs.push_back(
            {.FirstPageNo = firstPageNo, .PageCount = pageCount});
    }

    return rangeRefs;
}

TString ZeroedPage(ui32 pageSize = DefaultPageSize)
{
    return TString(pageSize, '\0');
}

// "10:[a,b] 20:[c]"
TString DescribeGroups(const NCloud::NProto::TReadPagesResponse& response)
{
    TVector<TString> groups;

    for (const auto& group: response.GetPageGroups()) {
        groups.push_back(TStringBuilder()
            << group.GetFirstPageNo()
            << ":[" << JoinSeq(",", group.GetContent()) << "]");
    }

    return JoinSeq(" ", groups);
}

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    IDevicePtr Device;

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        Device = CreateInMemoryDevice(DefaultPageSize);
    }

    void WritePages(
        const TVector<std::pair<ui64, TVector<TString>>>& groups)
    {
        const auto error =
            Device->WritePages(MakeRanges(groups)).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));
    }

    // the pages read are grouped after the refs they have been read for
    NCloud::NProto::TReadPagesResponse ReadPagesResponse(
        const TVector<std::pair<ui64, ui64>>& refs)
    {
        const auto rangeRefs = MakeRangeRefs(refs);

        const auto result = Device->ReadPages(rangeRefs).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result.GetError().GetCode(),
            FormatError(result.GetError()));

        auto response = MakeReadPagesResponse(rangeRefs, result.GetResult());

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response.GetError().GetCode(),
            FormatError(response.GetError()));

        return response;
    }

    TString ReadPages(const TVector<std::pair<ui64, ui64>>& refs)
    {
        return DescribeGroups(ReadPagesResponse(refs));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TInMemoryDeviceTest)
{
    Y_UNIT_TEST_F(ShouldReadWrittenPages, TFixture)
    {
        WritePages({{10, {"a", "b", "c"}}});

        UNIT_ASSERT_VALUES_EQUAL("10:[a,b,c]", ReadPages({{10, 3}}));

        // any subrange is readable as well

        UNIT_ASSERT_VALUES_EQUAL("11:[b]", ReadPages({{11, 1}}));
    }

    Y_UNIT_TEST_F(ShouldWriteEveryPageRangeOfTheRequest, TFixture)
    {
        WritePages({{10, {"a", "b"}}, {20, {"c"}}});

        UNIT_ASSERT_VALUES_EQUAL(
            "10:[a,b] 20:[c]",
            ReadPages({{10, 2}, {20, 1}}));
    }

    Y_UNIT_TEST_F(ShouldOverwritePages, TFixture)
    {
        WritePages({{10, {"a", "b", "c"}}});

        // the new content overrides the previous one, page by page

        WritePages({{11, {"B"}}});

        UNIT_ASSERT_VALUES_EQUAL("10:[a,B,c]", ReadPages({{10, 3}}));

        // an overlapping write overrides the pages it covers only

        WritePages({{9, {"Z", "A", "B"}}});

        UNIT_ASSERT_VALUES_EQUAL("9:[Z,A,B,c]", ReadPages({{9, 4}}));
    }

    Y_UNIT_TEST_F(ShouldReadZeroedPagesNeverWritten, TFixture)
    {
        {
            const auto response = ReadPagesResponse({{10, 3}});

            UNIT_ASSERT_VALUES_EQUAL(1, response.PageGroupsSize());

            const auto& group = response.GetPageGroups(0);
            UNIT_ASSERT_VALUES_EQUAL(10, group.GetFirstPageNo());
            UNIT_ASSERT_VALUES_EQUAL(3, group.ContentSize());

            for (const auto& content: group.GetContent()) {
                UNIT_ASSERT_VALUES_EQUAL(DefaultPageSize, content.size());
                UNIT_ASSERT_VALUES_EQUAL(ZeroedPage(), content);
            }
        }

        // the pages around the written ones are zeroed as well

        WritePages({{11, {"b"}}});

        {
            const auto response = ReadPagesResponse({{10, 3}});

            UNIT_ASSERT_VALUES_EQUAL(1, response.PageGroupsSize());

            const auto& group = response.GetPageGroups(0);
            UNIT_ASSERT_VALUES_EQUAL(3, group.ContentSize());
            UNIT_ASSERT_VALUES_EQUAL(ZeroedPage(), group.GetContent(0));
            UNIT_ASSERT_VALUES_EQUAL("b", group.GetContent(1));
            UNIT_ASSERT_VALUES_EQUAL(ZeroedPage(), group.GetContent(2));
        }
    }

    Y_UNIT_TEST_F(ShouldZeroPagesAccordingToTheDevicePageSize, TFixture)
    {
        Device = CreateInMemoryDevice(512);

        const auto response = ReadPagesResponse({{10, 1}});

        UNIT_ASSERT_VALUES_EQUAL(1, response.PageGroupsSize());

        const auto& group = response.GetPageGroups(0);
        UNIT_ASSERT_VALUES_EQUAL(1, group.ContentSize());
        UNIT_ASSERT_VALUES_EQUAL(512, group.GetContent(0).size());
        UNIT_ASSERT_VALUES_EQUAL(ZeroedPage(512), group.GetContent(0));
    }

    Y_UNIT_TEST_F(ShouldReturnAPagePerRequestedPage, TFixture)
    {
        WritePages({{10, {"a"}}});

        // a ref without pages yields no pages

        UNIT_ASSERT_VALUES_EQUAL(
            "10:[a] 20:[]",
            ReadPages({{10, 1}, {20, 0}}));

        UNIT_ASSERT_VALUES_EQUAL("", ReadPages({}));
    }

    Y_UNIT_TEST(ShouldNotMakeAResponseOfAnUnexpectedPageCount)
    {
        TVector<TBuffer> pages;
        pages.emplace_back("a", 1);

        const auto response = MakeReadPagesResponse(
            {{.FirstPageNo = 10, .PageCount = 2}},
            pages);

        UNIT_ASSERT_VALUES_EQUAL_C(
            E_INVALID_STATE,
            response.GetError().GetCode(),
            FormatError(response.GetError()));
        UNIT_ASSERT_STRING_CONTAINS(
            response.GetError().GetMessage(),
            "the device returned 1 pages, expected 2");
    }
}

}   // namespace NCloud::NJournalled
