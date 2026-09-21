#include "file_device.h"

#include "device_helpers.h"

#include <cloud/fastshard/journal/iface/device.h>

#include <cloud/storage/core/libs/aio/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/file_io_service.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/generic/algorithm.h>
#include <util/generic/size_literals.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/join.h>
#include <util/system/fstat.h>

namespace NCloud::NJournalled {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DefaultPageSize = 4_KB;
constexpr ui64 DefaultPageCount = 64;

// a page filled with the given character
TString Page(char c, ui32 pageSize = DefaultPageSize)
{
    return TString(pageSize, c);
}

TString ZeroedPage(ui32 pageSize = DefaultPageSize)
{
    return TString(pageSize, '\0');
}

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

// "a" for a page filled with 'a', "0" for a zeroed one, "?" for anything else
TString DescribePage(const TString& page)
{
    if (page.empty() || !AllOf(page, [&](char c) { return c == page[0]; })) {
        return "?";
    }

    return page[0] == '\0' ? "0" : TString(1, page[0]);
}

// "10:[a,b] 20:[c]"
TString DescribeGroups(const NCloud::NProto::TReadPagesResponse& response)
{
    TVector<TString> groups;

    for (const auto& group: response.GetPageGroups()) {
        TVector<TString> pages;
        for (const auto& page: group.GetContent()) {
            pages.push_back(DescribePage(page));
        }

        groups.push_back(TStringBuilder()
            << group.GetFirstPageNo()
            << ":[" << JoinSeq(",", pages) << "]");
    }

    return JoinSeq(" ", groups);
}

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    TTempDir TempDir;
    IFileIOServicePtr FileIO;
    IDevicePtr Device;

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        FileIO = CreateAIOService();
        FileIO->Start();

        Device = CreateDevice();
    }

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {
        Device.reset();
        FileIO->Stop();
    }

    TString FilePath() const
    {
        return TempDir.Path() / "device";
    }

    IDevicePtr CreateDevice(
        ui64 pageCount = DefaultPageCount,
        ui32 pageSize = DefaultPageSize)
    {
        return CreateFileDevice(FileIO, FilePath(), pageCount, pageSize);
    }

    ui64 FileSize() const
    {
        return TFileStat(FilePath()).Size;
    }

    NCloud::NProto::TError Write(
        const TVector<std::pair<ui64, TVector<TString>>>& groups)
    {
        return Device->WritePages(MakeRanges(groups)).GetValueSync();
    }

    void WritePages(const TVector<std::pair<ui64, TVector<TString>>>& groups)
    {
        const auto error = Write(groups);

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            error.GetCode(),
            FormatError(error));
    }

    // the pages read are grouped after the refs they have been read for
    NCloud::NProto::TReadPagesResponse Read(
        const TVector<std::pair<ui64, ui64>>& refs)
    {
        const auto rangeRefs = MakeRangeRefs(refs);

        const auto result = Device->ReadPages(rangeRefs).GetValueSync();
        if (HasError(result)) {
            return TErrorResponse(result.GetError());
        }

        return MakeReadPagesResponse(rangeRefs, result.GetResult());
    }

    NCloud::NProto::TReadPagesResponse ReadPagesResponse(
        const TVector<std::pair<ui64, ui64>>& refs)
    {
        auto response = Read(refs);

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

Y_UNIT_TEST_SUITE(TFileDeviceTest)
{
    Y_UNIT_TEST_F(ShouldReadWrittenPages, TFixture)
    {
        WritePages({{10, {Page('a'), Page('b'), Page('c')}}});

        UNIT_ASSERT_VALUES_EQUAL("10:[a,b,c]", ReadPages({{10, 3}}));

        // any subrange is readable as well

        UNIT_ASSERT_VALUES_EQUAL("11:[b]", ReadPages({{11, 1}}));
    }

    Y_UNIT_TEST_F(ShouldWriteEveryPageGroupOfTheRequest, TFixture)
    {
        WritePages({{10, {Page('a'), Page('b')}}, {20, {Page('c')}}});

        UNIT_ASSERT_VALUES_EQUAL(
            "10:[a,b] 20:[c]",
            ReadPages({{10, 2}, {20, 1}}));
    }

    Y_UNIT_TEST_F(ShouldOverwritePages, TFixture)
    {
        WritePages({{10, {Page('a'), Page('b'), Page('c')}}});

        // the new content overrides the previous one, page by page

        WritePages({{11, {Page('B')}}});

        UNIT_ASSERT_VALUES_EQUAL("10:[a,B,c]", ReadPages({{10, 3}}));

        // an overlapping write overrides the pages it covers only

        WritePages({{9, {Page('Z'), Page('A'), Page('B')}}});

        UNIT_ASSERT_VALUES_EQUAL(
            "9:[Z,A,B,c]",
            ReadPages({{9, 4}}));
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
                UNIT_ASSERT_VALUES_EQUAL(ZeroedPage(), content);
            }
        }

        // the pages around the written ones are zeroed as well

        WritePages({{11, {Page('b')}}});

        {
            const auto response = ReadPagesResponse({{10, 3}});

            UNIT_ASSERT_VALUES_EQUAL(1, response.PageGroupsSize());

            const auto& group = response.GetPageGroups(0);
            UNIT_ASSERT_VALUES_EQUAL(3, group.ContentSize());
            UNIT_ASSERT_VALUES_EQUAL(ZeroedPage(), group.GetContent(0));
            UNIT_ASSERT_VALUES_EQUAL(Page('b'), group.GetContent(1));
            UNIT_ASSERT_VALUES_EQUAL(ZeroedPage(), group.GetContent(2));
        }
    }

    Y_UNIT_TEST_F(ShouldReturnAPageGroupPerPageGroupRef, TFixture)
    {
        WritePages({{10, {Page('a')}}});

        // a ref without pages yields an empty page group

        UNIT_ASSERT_VALUES_EQUAL(
            "10:[a] 20:[]",
            ReadPages({{10, 1}, {20, 0}}));

        UNIT_ASSERT_VALUES_EQUAL("", ReadPages({}));
    }

    Y_UNIT_TEST_F(ShouldAcceptAnEmptyWriteRequest, TFixture)
    {
        WritePages({});
        WritePages({{10, {}}});
    }

    Y_UNIT_TEST_F(ShouldReadAndWriteTheWholeDevice, TFixture)
    {
        TVector<TString> pages;
        for (ui64 i = 0; i != DefaultPageCount; ++i) {
            pages.push_back(Page('a' + i % 26));
        }

        WritePages({{0, pages}});

        const auto response = ReadPagesResponse({{0, DefaultPageCount}});

        UNIT_ASSERT_VALUES_EQUAL(1, response.PageGroupsSize());

        const auto& group = response.GetPageGroups(0);
        UNIT_ASSERT_VALUES_EQUAL(DefaultPageCount, group.ContentSize());

        for (ui64 i = 0; i != DefaultPageCount; ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(pages[i], group.GetContent(i), i);
        }
    }

    Y_UNIT_TEST_F(ShouldKeepThePagesInTheFile, TFixture)
    {
        WritePages({{10, {Page('a'), Page('b')}}});

        // the pages are where they are supposed to be

        TString content = TFileInput(FilePath()).ReadAll();

        UNIT_ASSERT_VALUES_EQUAL(
            DefaultPageCount * DefaultPageSize,
            content.size());
        UNIT_ASSERT_VALUES_EQUAL(
            ZeroedPage(10 * DefaultPageSize),
            content.substr(0, 10 * DefaultPageSize));
        UNIT_ASSERT_VALUES_EQUAL(
            Page('a') + Page('b'),
            content.substr(10 * DefaultPageSize, 2 * DefaultPageSize));

        // and are still there when the device is reopened

        Device.reset();
        Device = CreateDevice();

        UNIT_ASSERT_VALUES_EQUAL("10:[a,b]", ReadPages({{10, 2}}));
    }

    Y_UNIT_TEST_F(ShouldCreateTheFileOfTheDeviceSize, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(
            DefaultPageCount * DefaultPageSize,
            FileSize());
    }

    Y_UNIT_TEST_F(ShouldGrowAShorterFile, TFixture)
    {
        Device.reset();

        TFileOutput(FilePath()).Write(Page('a') + Page('b'));

        Device = CreateDevice();

        UNIT_ASSERT_VALUES_EQUAL(
            DefaultPageCount * DefaultPageSize,
            FileSize());

        // the existing content is kept, the rest is zeroed

        UNIT_ASSERT_VALUES_EQUAL("0:[a,b]", ReadPages({{0, 2}}));

        const auto response = ReadPagesResponse({{2, 1}});
        UNIT_ASSERT_VALUES_EQUAL(
            ZeroedPage(),
            response.GetPageGroups(0).GetContent(0));
    }

    Y_UNIT_TEST_F(ShouldNotTruncateALongerFile, TFixture)
    {
        Device.reset();

        const ui64 pageCount = DefaultPageCount + 1;

        Device = CreateDevice(pageCount);
        WritePages({{pageCount - 1, {Page('x')}}});

        Device.reset();
        Device = CreateDevice(DefaultPageCount);

        UNIT_ASSERT_VALUES_EQUAL(pageCount * DefaultPageSize, FileSize());

        // the page beyond the device is not reachable through it though

        const auto response = Read({{pageCount - 1, 1}});
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_ARGUMENT,
            response.GetError().GetCode(),
            FormatError(response.GetError()));
    }

    Y_UNIT_TEST_F(ShouldRejectAPageOfTheWrongSizeOnWrite, TFixture)
    {
        for (ui32 size: {DefaultPageSize - 1, DefaultPageSize + 1, 0u}) {
            const auto error = Write({{10, {Page('a'), Page('b', size)}}});

            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                error.GetCode(),
                FormatError(error));
        }

        // nothing has been written

        const auto response = ReadPagesResponse({{10, 1}});
        UNIT_ASSERT_VALUES_EQUAL(
            ZeroedPage(),
            response.GetPageGroups(0).GetContent(0));
    }

    Y_UNIT_TEST_F(ShouldRejectPagesBeyondTheDevice, TFixture)
    {
        // the last page is writable and readable

        WritePages({{DefaultPageCount - 1, {Page('z')}}});

        UNIT_ASSERT_VALUES_EQUAL(
            "63:[z]",
            ReadPages({{DefaultPageCount - 1, 1}}));

        // an empty group right after it is fine as well

        WritePages({{DefaultPageCount, {}}});

        UNIT_ASSERT_VALUES_EQUAL("64:[]", ReadPages({{DefaultPageCount, 0}}));

        // anything beyond it is not

        const TVector<std::pair<ui64, ui64>> ranges {
            {DefaultPageCount - 1, 2},
            {DefaultPageCount, 1},
            {DefaultPageCount + 1, 0},
            {Max<ui64>(), 1},
            {Max<ui64>(), 0},
            {1, Max<ui64>()},
        };

        for (const auto& [firstPageNo, pageCount]: ranges) {
            const auto description = TStringBuilder()
                << "[" << firstPageNo << ", " << pageCount << "]";

            TVector<TString> pages;
            for (ui64 i = 0; i != Min<ui64>(pageCount, 2); ++i) {
                pages.push_back(Page('a'));
            }

            if (pages.size() == pageCount) {
                const auto error = Write({{firstPageNo, pages}});

                UNIT_ASSERT_VALUES_EQUAL_C(
                    E_ARGUMENT,
                    error.GetCode(),
                    description << ": " << FormatError(error));
            }

            const auto response = Read({{firstPageNo, pageCount}});

            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                response.GetError().GetCode(),
                description << ": " << FormatError(response.GetError()));
        }
    }

    Y_UNIT_TEST_F(ShouldRejectTheWholeRequestIfAnyGroupIsInvalid, TFixture)
    {
        const auto error = Write({
            {10, {Page('a')}},
            {DefaultPageCount, {Page('b')}},
        });

        UNIT_ASSERT_VALUES_EQUAL_C(
            E_ARGUMENT,
            error.GetCode(),
            FormatError(error));

        // the valid group has not been written either

        const auto response = ReadPagesResponse({{10, 1}});
        UNIT_ASSERT_VALUES_EQUAL(
            ZeroedPage(),
            response.GetPageGroups(0).GetContent(0));
    }

    Y_UNIT_TEST_F(ShouldThrowIfTheFileCanNotBeOpened, TFixture)
    {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            CreateFileDevice(
                FileIO,
                TempDir.Path() / "missing" / "device",
                DefaultPageCount,
                DefaultPageSize),
            yexception,
            "unable to open");
    }

    Y_UNIT_TEST_F(ShouldThrowOnAPageSizeUnfitForDirectIO, TFixture)
    {
        for (ui32 size: {0u, 512u, DefaultPageSize - 1, DefaultPageSize + 512})
        {
            UNIT_ASSERT_EXCEPTION_CONTAINS_C(
                CreateDevice(DefaultPageCount, size),
                yexception,
                "page size",
                size);
        }

        // any multiple of 4 KiB is fine

        CreateDevice(DefaultPageCount, 2 * DefaultPageSize);
    }
}

}   // namespace NCloud::NJournalled
