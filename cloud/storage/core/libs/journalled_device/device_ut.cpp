#include "device.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>
#include <util/string/join.h>

namespace NCloud::NJournalled {

namespace {

////////////////////////////////////////////////////////////////////////////////

NCloud::NProto::TWriteLogRecordRequest MakeWriteRequest(
    const TVector<std::pair<ui64 /*firstPageNo*/, TVector<TString>>>& groups)
{
    NCloud::NProto::TWriteLogRecordRequest request;

    for (const auto& [firstPageNo, content]: groups) {
        auto& group = *request.AddPageGroups();
        group.SetFirstPageNo(firstPageNo);

        for (const auto& page: content) {
            group.AddContent(page);
        }
    }

    return request;
}

constexpr ui32 DefaultPageSize = 4096;

NCloud::NProto::TReadPagesRequest MakeReadRequest(
    const TVector<std::pair<ui64 /*firstPageNo*/, ui64 /*pageCount*/>>& refs,
    ui32 pageSize = DefaultPageSize)
{
    NCloud::NProto::TReadPagesRequest request;

    for (const auto& [firstPageNo, pageCount]: refs) {
        auto& ref = *request.AddPageGroupRefs();
        ref.SetFirstPageNo(firstPageNo);
        ref.SetPageCount(pageCount);
        ref.SetPageSize(pageSize);
    }

    return request;
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
        Device = CreateInMemoryDevice();
    }

    void WritePages(
        const TVector<std::pair<ui64, TVector<TString>>>& groups)
    {
        const auto response =
            Device->WritePages(MakeWriteRequest(groups)).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response.GetError().GetCode(),
            FormatError(response.GetError()));
    }

    NCloud::NProto::TReadPagesResponse ReadPagesResponse(
        const TVector<std::pair<ui64, ui64>>& refs,
        ui32 pageSize = DefaultPageSize)
    {
        auto response =
            Device->ReadPages(MakeReadRequest(refs, pageSize)).GetValueSync();

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

    Y_UNIT_TEST_F(ShouldWriteEveryPageGroupOfTheRequest, TFixture)
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

    Y_UNIT_TEST_F(ShouldZeroPagesAccordingToTheRequestedPageSize, TFixture)
    {
        const auto response = ReadPagesResponse({{10, 1}}, 512);

        UNIT_ASSERT_VALUES_EQUAL(1, response.PageGroupsSize());

        const auto& group = response.GetPageGroups(0);
        UNIT_ASSERT_VALUES_EQUAL(1, group.ContentSize());
        UNIT_ASSERT_VALUES_EQUAL(512, group.GetContent(0).size());
        UNIT_ASSERT_VALUES_EQUAL(ZeroedPage(512), group.GetContent(0));
    }

    Y_UNIT_TEST_F(ShouldReturnAPageGroupPerPageGroupRef, TFixture)
    {
        WritePages({{10, {"a"}}});

        // a ref without pages yields an empty page group

        UNIT_ASSERT_VALUES_EQUAL(
            "10:[a] 20:[]",
            ReadPages({{10, 1}, {20, 0}}));

        UNIT_ASSERT_VALUES_EQUAL("", ReadPages({}));
    }
}

}   // namespace NCloud::NJournalled
