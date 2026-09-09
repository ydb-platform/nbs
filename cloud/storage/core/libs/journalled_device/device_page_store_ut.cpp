#include "device_page_store.h"

#include "device.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/buffer.h>

namespace NCloud::NJournalled {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 DefaultPageCount = 16;
constexpr ui32 DefaultPageSize = 4;

TVector<TBuffer> MakePages(const TVector<TString>& pages)
{
    TVector<TBuffer> buffers;
    buffers.reserve(pages.size());
    for (const auto& page: pages) {
        buffers.emplace_back(page.data(), page.size());
    }
    return buffers;
}

TString Join(const TVector<TBuffer>& pages)
{
    TStringBuilder sb;
    for (const auto& page: pages) {
        if (sb) {
            sb << "|";
        }
        sb << TStringBuf(page.Data(), page.Size());
    }
    return sb;
}

TVector<TPageRange> MakeRanges(const TVector<std::pair<ui64, ui64>>& ranges)
{
    TVector<TPageRange> pageRanges;
    for (const auto& [firstPageNo, pageCount]: ranges) {
        pageRanges.push_back(
            {.FirstPageNo = firstPageNo, .PageCount = pageCount});
    }
    return pageRanges;
}

// "<firstPageNo>x<pageCount>" per returned range
TString Describe(const TVector<TPageRange>& ranges)
{
    TStringBuilder sb;
    for (const auto& pageRange: ranges) {
        if (sb) {
            sb << ", ";
        }
        sb << pageRange.FirstPageNo << "x" << pageRange.PageCount;
    }
    return sb;
}

////////////////////////////////////////////////////////////////////////////////

struct TBrokenDevice final: public IDevice
{
    NThreading::TFuture<NCloud::NProto::TReadPagesResponse> ReadPages(
        NCloud::NProto::TReadPagesRequest request) override
    {
        Y_UNUSED(request);

        return NThreading::MakeFuture<NCloud::NProto::TReadPagesResponse>(
            TErrorResponse(E_IO, "device is broken"));
    }

    NThreading::TFuture<NCloud::NProto::TWriteLogRecordResponse> WritePages(
        NCloud::NProto::TWriteLogRecordRequest request) override
    {
        Y_UNUSED(request);

        return NThreading::MakeFuture<NCloud::NProto::TWriteLogRecordResponse>(
            TErrorResponse(E_IO, "device is broken"));
    }
};

// allocates the pages of a record and writes them
TVector<TPageRange> WriteRecord(
    const IDevicePageStorePtr& store,
    const TVector<TString>& pages)
{
    auto ranges = store->Allocate(pages.size());
    UNIT_ASSERT(pages.empty() || !ranges.empty());

    const auto error = store->Write(ranges, MakePages(pages)).GetValue();
    UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));

    return ranges;
}

// reads the pages straight from the device, bypassing the store
TString
ReadFromDevice(const IDevicePtr& device, ui64 firstPageNo, ui64 pageCount)
{
    NCloud::NProto::TReadPagesRequest request;
    auto& ref = *request.AddPageGroupRefs();
    ref.SetFirstPageNo(firstPageNo);
    ref.SetPageCount(pageCount);

    const auto response = device->ReadPages(std::move(request)).GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(
        S_OK,
        response.GetError().GetCode(),
        FormatError(response.GetError()));

    TStringBuilder sb;
    for (const auto& group: response.GetPageGroups()) {
        for (const auto& content: group.GetContent()) {
            if (sb) {
                sb << "|";
            }
            sb << content;
        }
    }

    return sb;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDevicePageStoreTest)
{
    Y_UNIT_TEST(ShouldAllocateFreePages)
    {
        auto store =
            CreateDevicePageStore(CreateInMemoryDevice(), 4, DefaultPageSize);

        UNIT_ASSERT_VALUES_EQUAL("0x2", Describe(store->Allocate(2)));

        // the allocated pages are busy, the next allocation goes past them
        UNIT_ASSERT_VALUES_EQUAL("2x1", Describe(store->Allocate(1)));
        UNIT_ASSERT_VALUES_EQUAL("3x1", Describe(store->Allocate(1)));

        // nothing is left
        UNIT_ASSERT(store->Allocate(1).empty());
    }

    Y_UNIT_TEST(ShouldNotAllocateMorePagesThanThereAre)
    {
        auto store =
            CreateDevicePageStore(CreateInMemoryDevice(), 4, DefaultPageSize);

        UNIT_ASSERT(store->Allocate(5).empty());

        // nothing has been taken
        UNIT_ASSERT_VALUES_EQUAL("0x4", Describe(store->Allocate(4)));
    }

    Y_UNIT_TEST(ShouldAllocateFragmentedFreeSpace)
    {
        auto store =
            CreateDevicePageStore(CreateInMemoryDevice(), 8, DefaultPageSize);

        UNIT_ASSERT_VALUES_EQUAL("0x8", Describe(store->Allocate(8)));

        // free two ranges apart from each other
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Free(MakeRanges({{1, 2}, {5, 1}})).GetCode());

        // the allocation spans them both, in the page order
        UNIT_ASSERT_VALUES_EQUAL("1x2, 5x1", Describe(store->Allocate(3)));
    }

    Y_UNIT_TEST(ShouldReadBackWhatWasWritten)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(),
            DefaultPageCount,
            DefaultPageSize);

        auto ranges = WriteRecord(store, {"aaaa", "bbbb"});
        UNIT_ASSERT_VALUES_EQUAL("0x2", Describe(ranges));

        UNIT_ASSERT_VALUES_EQUAL(
            "aaaa|bbbb",
            Join(store->Read(ranges).GetValue().GetResult()));
    }

    Y_UNIT_TEST(ShouldRejectPagesOfAWrongSize)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(),
            DefaultPageCount,
            DefaultPageSize);

        auto ranges = store->Allocate(2);

        for (const auto& pages:
             TVector<TVector<TString>>{
                 {"aaaa", "bbb"},     // too short
                 {"aaaa", "bbbbb"},   // too long
                 {"aaaa", ""}})       // empty
        {
            const auto error =
                store->Write(ranges, MakePages(pages)).GetValue();

            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_STRING_CONTAINS(
                error.GetMessage(),
                "the page size is 4");
        }

        // a write of the right size still goes through
        const auto error =
            store->Write(ranges, MakePages({"aaaa", "bbbb"})).GetValue();
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));

        UNIT_ASSERT_VALUES_EQUAL(
            "aaaa|bbbb",
            Join(store->Read(ranges).GetValue().GetResult()));
    }

    Y_UNIT_TEST(ShouldWriteIntoFragmentedRefs)
    {
        auto store =
            CreateDevicePageStore(CreateInMemoryDevice(), 8, DefaultPageSize);

        UNIT_ASSERT_VALUES_EQUAL("0x8", Describe(store->Allocate(8)));
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Free(MakeRanges({{1, 2}, {5, 1}})).GetCode());

        auto ranges = store->Allocate(3);
        UNIT_ASSERT_VALUES_EQUAL("1x2, 5x1", Describe(ranges));

        const auto error =
            store->Write(ranges, MakePages({"aaaa", "bbbb", "cccc"}))
                .GetValue();
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));

        // the pages follow the ranges, in order
        UNIT_ASSERT_VALUES_EQUAL(
            "aaaa|bbbb|cccc",
            Join(store->Read(ranges).GetValue().GetResult()));
    }

    Y_UNIT_TEST(ShouldRejectAWriteWithAWrongNumberOfPages)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(),
            DefaultPageCount,
            DefaultPageSize);

        auto ranges = store->Allocate(2);

        const auto tooMany =
            store->Write(ranges, MakePages({"aaaa", "bbbb", "cccc"}))
                .GetValue();
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, tooMany.GetCode());
        UNIT_ASSERT_STRING_CONTAINS(
            tooMany.GetMessage(),
            "the page ranges hold 2 pages, 3 given");

        const auto tooFew =
            store->Write(ranges, MakePages({"aaaa"})).GetValue();
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, tooFew.GetCode());
    }

    Y_UNIT_TEST(ShouldRejectAWriteIntoPagesThatAreNotAllocated)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(),
            DefaultPageCount,
            DefaultPageSize);

        const auto free =
            store->Write(MakeRanges({{0, 1}}), MakePages({"aaaa"})).GetValue();
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, free.GetCode());

        const auto beyond = store
                                ->Write(
                                    MakeRanges({{DefaultPageCount, 1}}),
                                    MakePages({"aaaa"}))
                                .GetValue();
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, beyond.GetCode());
    }

    Y_UNIT_TEST(ShouldRejectRefsForPagesThatAreNotThere)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(),
            DefaultPageCount,
            DefaultPageSize);

        WriteRecord(store, {"aaaa"});

        auto free = MakeRanges({{0, 2}});
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            store->Read(free).GetValue().GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, store->Free(free).GetCode());

        // the ranges beyond the device are rejected as ill-formed
        auto beyond = MakeRanges({{DefaultPageCount - 1, 2}});
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            store->Read(beyond).GetValue().GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            store->AllocateAt(beyond).GetCode());
    }

    Y_UNIT_TEST(ShouldAcceptRefsThatOnlyTouchTheFreeSpace)
    {
        auto store =
            CreateDevicePageStore(CreateInMemoryDevice(), 8, DefaultPageSize);

        UNIT_ASSERT_VALUES_EQUAL("0x8", Describe(store->Allocate(8)));

        // a hole in the middle: the pages 3 and 4
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Free(MakeRanges({{3, 2}})).GetCode());

        // a pageRange that ends right where the hole begins is busy all the way
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Read(MakeRanges({{0, 3}})).GetValue().GetError().GetCode());

        // and so is a pageRange that begins right where the hole ends
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Read(MakeRanges({{5, 3}})).GetValue().GetError().GetCode());

        // reaching into the hole from either side is not allowed, and the
        // first free page of the pageRange is reported
        const auto fromTheLeft =
            store->Read(MakeRanges({{2, 2}})).GetValue().GetError();
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, fromTheLeft.GetCode());
        UNIT_ASSERT_STRING_CONTAINS(
            fromTheLeft.GetMessage(),
            "page 3 is not busy");

        const auto fromTheRight =
            store->Read(MakeRanges({{4, 2}})).GetValue().GetError();
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, fromTheRight.GetCode());
        UNIT_ASSERT_STRING_CONTAINS(
            fromTheRight.GetMessage(),
            "page 4 is not busy");

        // as is a pageRange that spans the hole
        const auto spanning =
            store->Read(MakeRanges({{0, 8}})).GetValue().GetError();
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, spanning.GetCode());
        UNIT_ASSERT_STRING_CONTAINS(
            spanning.GetMessage(),
            "page 3 is not busy");
    }

    Y_UNIT_TEST(ShouldFreePages)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(),
            DefaultPageCount,
            DefaultPageSize);

        WriteRecord(store, {"aaaa", "bbbb", "cccc"});

        auto head = MakeRanges({{0, 2}});
        UNIT_ASSERT_VALUES_EQUAL(S_OK, store->Free(head).GetCode());

        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            store->Read(head).GetValue().GetError().GetCode());

        auto kept = MakeRanges({{2, 1}});
        UNIT_ASSERT_VALUES_EQUAL(
            "cccc",
            Join(store->Read(kept).GetValue().GetResult()));
    }

    Y_UNIT_TEST(ShouldNotFreeAnythingWhenARefIsRejected)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(),
            DefaultPageCount,
            DefaultPageSize);

        WriteRecord(store, {"aaaa", "bbbb"});

        auto reaching = MakeRanges({{0, 3}});
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            store->Free(reaching).GetCode());

        auto written = MakeRanges({{0, 2}});
        UNIT_ASSERT_VALUES_EQUAL(
            "aaaa|bbbb",
            Join(store->Read(written).GetValue().GetResult()));
    }

    Y_UNIT_TEST(ShouldReuseFreedPages)
    {
        auto store =
            CreateDevicePageStore(CreateInMemoryDevice(), 4, DefaultPageSize);

        auto ranges = WriteRecord(store, {"aaaa", "bbbb"});
        UNIT_ASSERT_VALUES_EQUAL("0x2", Describe(ranges));

        UNIT_ASSERT_VALUES_EQUAL(S_OK, store->Free(ranges).GetCode());

        // the freed pages are merged back into the free space
        UNIT_ASSERT_VALUES_EQUAL("0x4", Describe(store->Allocate(4)));
    }

    Y_UNIT_TEST(ShouldMergeTheFreedPagesWithTheirNeighbours)
    {
        auto store =
            CreateDevicePageStore(CreateInMemoryDevice(), 6, DefaultPageSize);

        UNIT_ASSERT_VALUES_EQUAL("0x6", Describe(store->Allocate(6)));

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Free(MakeRanges({{0, 2}})).GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Free(MakeRanges({{4, 2}})).GetCode());

        // the free space is fragmented until the hole between the two free
        // ranges is freed as well
        UNIT_ASSERT_VALUES_EQUAL("0x2, 4x2", Describe(store->Allocate(4)));

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Free(MakeRanges({{0, 2}, {4, 2}, {2, 2}})).GetCode());

        // and a single contiguous range once it is - the hole is merged with
        // the free range on either side of it
        UNIT_ASSERT_VALUES_EQUAL("0x6", Describe(store->Allocate(6)));
    }

    Y_UNIT_TEST(ShouldAllocateTheGivenPages)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(),
            DefaultPageCount,
            DefaultPageSize);

        // the pages of a restored record are taken out of the free space
        auto restored = MakeRanges({{0, 2}, {5, 1}});
        UNIT_ASSERT_VALUES_EQUAL(S_OK, store->AllocateAt(restored).GetCode());

        UNIT_ASSERT_VALUES_EQUAL("2x3, 6x1", Describe(store->Allocate(4)));
    }

    Y_UNIT_TEST(ShouldNotAllocateThePagesThatAreBusyAlready)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(),
            DefaultPageCount,
            DefaultPageSize);

        UNIT_ASSERT_VALUES_EQUAL("0x2", Describe(store->Allocate(2)));

        // a page in the middle of the pageRange is enough to reject it
        const auto busy = store->AllocateAt(MakeRanges({{1, 4}}));
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, busy.GetCode());
        UNIT_ASSERT_STRING_CONTAINS(
            busy.GetMessage(),
            "page 1 is busy already");

        const auto behind = store->AllocateAt(MakeRanges({{2, 2}, {1, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, behind.GetCode());
        UNIT_ASSERT_STRING_CONTAINS(
            behind.GetMessage(),
            "page 1 is busy already");

        // the ranges of a rejected call are not allocated, not even in part
        UNIT_ASSERT_VALUES_EQUAL("2x4", Describe(store->Allocate(4)));
    }

    Y_UNIT_TEST(ShouldKeepTheContentOfTheRejectedPages)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(),
            DefaultPageCount,
            DefaultPageSize);

        auto ranges = WriteRecord(store, {"aaaa", "bbbb"});

        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            store->AllocateAt(ranges).GetCode());

        // the rejected call leaves the pages alone
        UNIT_ASSERT_VALUES_EQUAL(
            "aaaa|bbbb",
            Join(store->Read(ranges).GetValue().GetResult()));
    }

    Y_UNIT_TEST(ShouldAcceptAnEmptyRequest)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(),
            DefaultPageCount,
            DefaultPageSize);

        UNIT_ASSERT(store->Allocate(0).empty());

        TVector<TPageRange> none;
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Write(none, {}).GetValue().GetCode());
        UNIT_ASSERT(store->Read(none).GetValue().GetResult().empty());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, store->Free(none).GetCode());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, store->AllocateAt(none).GetCode());
    }

    Y_UNIT_TEST(ShouldKeepThePagesOnTheDevice)
    {
        auto device = CreateInMemoryDevice();
        auto store =
            CreateDevicePageStore(device, DefaultPageCount, DefaultPageSize);

        auto ranges = WriteRecord(store, {"aaaa", "bbbb", "cccc"});
        UNIT_ASSERT_VALUES_EQUAL("0x3", Describe(ranges));

        // the pages are on the device, at the page numbers the store handed out
        UNIT_ASSERT_VALUES_EQUAL(
            "aaaa|bbbb|cccc",
            ReadFromDevice(device, 0, 3));
    }

    Y_UNIT_TEST(ShouldNotValidateTheRefsInTheTrustedMode)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(),
            DefaultPageCount,
            DefaultPageSize,
            EDevicePageStoreMode::Trusted);

        // nothing is allocated, yet the pages are written and read back
        auto ranges = MakeRanges({{0, 2}});
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Write(ranges, MakePages({"aaaa", "bbbb"}))
                .GetValue()
                .GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            "aaaa|bbbb",
            Join(store->Read(ranges).GetValue().GetResult()));

        // and freed
        UNIT_ASSERT_VALUES_EQUAL(S_OK, store->Free(ranges).GetCode());

        // the same store in the checked mode rejects all three
        auto checked = CreateDevicePageStore(
            CreateInMemoryDevice(),
            DefaultPageCount,
            DefaultPageSize);

        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            checked->Write(ranges, MakePages({"aaaa", "bbbb"}))
                .GetValue()
                .GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            checked->Read(ranges).GetValue().GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            checked->Free(ranges).GetCode());
    }

    Y_UNIT_TEST(ShouldValidateAllocateAtInTheTrustedMode)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(),
            DefaultPageCount,
            DefaultPageSize,
            EDevicePageStoreMode::Trusted);

        UNIT_ASSERT_VALUES_EQUAL("0x2", Describe(store->Allocate(2)));

        // AllocateAt is given the pages of a restored record, so it checks
        // them even here
        const auto busy = store->AllocateAt(MakeRanges({{1, 4}}));
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, busy.GetCode());
        UNIT_ASSERT_STRING_CONTAINS(
            busy.GetMessage(),
            "page 1 is busy already");

        const auto beyond =
            store->AllocateAt(MakeRanges({{DefaultPageCount, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, beyond.GetCode());

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->AllocateAt(MakeRanges({{5, 2}})).GetCode());
        UNIT_ASSERT_VALUES_EQUAL("2x3, 7x9", Describe(store->Allocate(12)));
    }

    Y_UNIT_TEST(ShouldTrackTheAllocationInTheTrustedMode)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(),
            4,
            DefaultPageSize,
            EDevicePageStoreMode::Trusted);

        auto ranges = store->Allocate(4);
        UNIT_ASSERT_VALUES_EQUAL("0x4", Describe(ranges));

        // the free space is exhausted, so nothing else can be allocated
        UNIT_ASSERT_VALUES_EQUAL("", Describe(store->Allocate(1)));

        UNIT_ASSERT_VALUES_EQUAL(S_OK, store->Free(ranges).GetCode());
        UNIT_ASSERT_VALUES_EQUAL("0x4", Describe(store->Allocate(4)));
    }

    Y_UNIT_TEST(ShouldReportTheDeviceWriteError)
    {
        auto store = CreateDevicePageStore(
            std::make_shared<TBrokenDevice>(),
            DefaultPageCount,
            DefaultPageSize);

        auto ranges = store->Allocate(2);
        UNIT_ASSERT_VALUES_EQUAL("0x2", Describe(ranges));

        UNIT_ASSERT_VALUES_EQUAL(
            E_IO,
            store->Write(ranges, MakePages({"aaaa", "bbbb"}))
                .GetValue()
                .GetCode());

        // the pages stay allocated, releasing them is up to the caller
        UNIT_ASSERT_VALUES_EQUAL("2x1", Describe(store->Allocate(1)));
    }

    Y_UNIT_TEST(ShouldReportTheDeviceReadError)
    {
        auto store = CreateDevicePageStore(
            std::make_shared<TBrokenDevice>(),
            DefaultPageCount,
            DefaultPageSize);

        auto ranges = store->Allocate(1);

        UNIT_ASSERT_VALUES_EQUAL(
            E_IO,
            store->Read(ranges).GetValue().GetError().GetCode());
    }
}

}   // namespace NCloud::NJournalled
