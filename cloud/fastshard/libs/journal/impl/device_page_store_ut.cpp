#include "device_page_store.h"

#include "memory_device.h"

#include <cloud/fastshard/libs/journal/iface/device.h>

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

TVector<TPageRangeRef> MakeRanges(const TVector<std::pair<ui64, ui64>>& ranges)
{
    TVector<TPageRangeRef> pageRanges;
    for (const auto& [firstPageNo, pageCount]: ranges) {
        pageRanges.push_back(
            {.FirstPageNo = firstPageNo, .PageCount = pageCount});
    }
    return pageRanges;
}

// "<firstPageNo>x<pageCount>" per returned range
TString Describe(const TVector<TPageRangeRef>& ranges)
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
    NThreading::TFuture<TResultOrError<TVector<TBuffer>>> ReadPages(
        TVector<TPageRangeRef> rangeRefs) override
    {
        Y_UNUSED(rangeRefs);

        return NThreading::MakeFuture<TResultOrError<TVector<TBuffer>>>(
            MakeError(E_IO, "device is broken"));
    }

    NThreading::TFuture<NCloud::NProto::TError> WritePages(
        TVector<TPageRange> ranges) override
    {
        Y_UNUSED(ranges);

        return NThreading::MakeFuture(MakeError(E_IO, "device is broken"));
    }

    NThreading::TFuture<NCloud::NProto::TError> ZeroPages(
        TVector<TPageRangeRef> ranges) override
    {
        Y_UNUSED(ranges);

        return NThreading::MakeFuture(MakeError(E_IO, "device is broken"));
    }
};

// allocates the pages of a record and writes them
TVector<TPageRangeRef> WriteRecord(
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
    const auto result = device
        ->ReadPages({{.FirstPageNo = firstPageNo, .PageCount = pageCount}})
        .GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(
        S_OK,
        result.GetError().GetCode(),
        FormatError(result.GetError()));

    return Join(result.GetResult());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDevicePageStoreTest)
{
    Y_UNIT_TEST(ShouldAllocateFreePages)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(DefaultPageSize),
            4,
            DefaultPageSize,
            EDevicePageStoreMode::Checked);

        UNIT_ASSERT_VALUES_EQUAL("0x2", Describe(store->Allocate(2)));

        // the allocated pages are busy, the next allocation goes past them
        UNIT_ASSERT_VALUES_EQUAL("2x1", Describe(store->Allocate(1)));
        UNIT_ASSERT_VALUES_EQUAL("3x1", Describe(store->Allocate(1)));

        // nothing is left
        UNIT_ASSERT(store->Allocate(1).empty());
    }

    Y_UNIT_TEST(ShouldNotAllocateMorePagesThanThereAre)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(DefaultPageSize),
            4,
            DefaultPageSize,
            EDevicePageStoreMode::Checked);

        UNIT_ASSERT(store->Allocate(5).empty());

        // nothing has been taken
        UNIT_ASSERT_VALUES_EQUAL("0x4", Describe(store->Allocate(4)));
    }

    Y_UNIT_TEST(ShouldAllocateFragmentedFreeSpace)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(DefaultPageSize),
            8,
            DefaultPageSize,
            EDevicePageStoreMode::Checked);

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
            CreateInMemoryDevice(DefaultPageSize),
            DefaultPageCount,
            DefaultPageSize,
            EDevicePageStoreMode::Checked);

        auto ranges = WriteRecord(store, {"aaaa", "bbbb"});
        UNIT_ASSERT_VALUES_EQUAL("0x2", Describe(ranges));

        UNIT_ASSERT_VALUES_EQUAL(
            "aaaa|bbbb",
            Join(store->Read(ranges).GetValue().GetResult()));
    }

    Y_UNIT_TEST(ShouldRejectPagesOfWrongSize)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(DefaultPageSize),
            DefaultPageCount,
            DefaultPageSize,
            EDevicePageStoreMode::Checked);

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
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(DefaultPageSize),
            8,
            DefaultPageSize,
            EDevicePageStoreMode::Checked);

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

    Y_UNIT_TEST(ShouldRejectWriteWithWrongNumberOfPages)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(DefaultPageSize),
            DefaultPageCount,
            DefaultPageSize,
            EDevicePageStoreMode::Checked);

        auto ranges = store->Allocate(2);

        const auto tooMany =
            store->Write(ranges, MakePages({"aaaa", "bbbb", "cccc"}))
                .GetValue();
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, tooMany.GetCode());

        const auto tooFew =
            store->Write(ranges, MakePages({"aaaa"})).GetValue();
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, tooFew.GetCode());
    }

    Y_UNIT_TEST(ShouldRejectWriteIntoPagesBeyondDevice)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(DefaultPageSize),
            DefaultPageCount,
            DefaultPageSize,
            EDevicePageStoreMode::Checked);

        // a write does not look at the allocation state of the pages
        const auto free =
            store->Write(MakeRanges({{0, 1}}), MakePages({"aaaa"})).GetValue();
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, free.GetCode(), FormatError(free));

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
            CreateInMemoryDevice(DefaultPageSize),
            DefaultPageCount,
            DefaultPageSize,
            EDevicePageStoreMode::Checked);

        WriteRecord(store, {"aaaa"});

        // a read does not look at the allocation state of the pages, a free
        // does
        auto free = MakeRanges({{0, 2}});
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
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

    Y_UNIT_TEST(ShouldRejectRefsThatIntersect)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(DefaultPageSize),
            DefaultPageCount,
            DefaultPageSize,
            EDevicePageStoreMode::Checked);

        // the page 1 is in both ranges
        const auto overlapping = MakeRanges({{0, 2}, {1, 2}});
        const auto pages = MakePages({"aaaa", "bbbb", "cccc", "dddd"});

        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            store->AllocateAt(overlapping).GetCode());

        // nothing is allocated by the rejected request
        UNIT_ASSERT_VALUES_EQUAL("0x4", Describe(store->Allocate(4)));

        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            store->Write(overlapping, pages).GetValue().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            store->Read(overlapping).GetValue().GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            store->Free(overlapping).GetCode());

        // the same range given twice intersects with itself
        const auto repeated = MakeRanges({{1, 1}, {1, 1}});
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            store->Read(repeated).GetValue().GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, store->Free(repeated).GetCode());

        // the ranges that only touch each other do not intersect
        const auto adjacent = MakeRanges({{0, 2}, {2, 2}});
        const auto written = store->Write(adjacent, pages).GetValue();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            written.GetCode(),
            FormatError(written));
        UNIT_ASSERT_VALUES_EQUAL(
            "aaaa|bbbb|cccc|dddd",
            Join(store->Read(adjacent).GetValue().GetResult()));

        // and nothing is freed by the rejected requests
        UNIT_ASSERT_VALUES_EQUAL(S_OK, store->Free(adjacent).GetCode());
    }

    Y_UNIT_TEST(ShouldAcceptRefsThatOnlyTouchFreeSpace)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(DefaultPageSize),
            8,
            DefaultPageSize,
            EDevicePageStoreMode::Checked);

        UNIT_ASSERT_VALUES_EQUAL("0x8", Describe(store->Allocate(8)));

        // a hole in the middle: the pages 3 and 4
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Free(MakeRanges({{3, 2}})).GetCode());

        // reaching into the hole from either side is not allowed
        const auto fromTheLeft = store->Free(MakeRanges({{2, 2}}));
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, fromTheLeft.GetCode());

        const auto fromTheRight = store->Free(MakeRanges({{4, 2}}));
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, fromTheRight.GetCode());

        // as is a pageRange that spans the hole
        const auto spanning = store->Free(MakeRanges({{0, 8}}));
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, spanning.GetCode());

        // a pageRange that ends right where the hole begins is busy all the way
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Free(MakeRanges({{0, 3}})).GetCode());

        // and so is a pageRange that begins right where the hole ends
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Free(MakeRanges({{5, 3}})).GetCode());
    }

    Y_UNIT_TEST(ShouldFreePages)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(DefaultPageSize),
            DefaultPageCount,
            DefaultPageSize,
            EDevicePageStoreMode::Checked);

        WriteRecord(store, {"aaaa", "bbbb", "cccc"});

        auto head = MakeRanges({{0, 2}});
        UNIT_ASSERT_VALUES_EQUAL(S_OK, store->Free(head).GetCode());

        // the pages are free now and cannot be freed again
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, store->Free(head).GetCode());

        auto kept = MakeRanges({{2, 1}});
        UNIT_ASSERT_VALUES_EQUAL(
            "cccc",
            Join(store->Read(kept).GetValue().GetResult()));
    }

    Y_UNIT_TEST(ShouldNotFreeAnythingWhenRefIsRejected)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(DefaultPageSize),
            DefaultPageCount,
            DefaultPageSize,
            EDevicePageStoreMode::Checked);

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
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(DefaultPageSize),
            4,
            DefaultPageSize,
            EDevicePageStoreMode::Checked);

        auto ranges = WriteRecord(store, {"aaaa", "bbbb"});
        UNIT_ASSERT_VALUES_EQUAL("0x2", Describe(ranges));

        UNIT_ASSERT_VALUES_EQUAL(S_OK, store->Free(ranges).GetCode());

        // the freed pages are merged back into the free space
        UNIT_ASSERT_VALUES_EQUAL("0x4", Describe(store->Allocate(4)));
    }

    Y_UNIT_TEST(ShouldMergeFreedPagesWithTheirNeighbours)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(DefaultPageSize),
            6,
            DefaultPageSize,
            EDevicePageStoreMode::Checked);

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

    Y_UNIT_TEST(ShouldAllocateGivenPages)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(DefaultPageSize),
            DefaultPageCount,
            DefaultPageSize,
            EDevicePageStoreMode::Checked);

        // the pages of a restored record are taken out of the free space
        auto restored = MakeRanges({{0, 2}, {5, 1}});
        UNIT_ASSERT_VALUES_EQUAL(S_OK, store->AllocateAt(restored).GetCode());

        UNIT_ASSERT_VALUES_EQUAL("2x3, 6x1", Describe(store->Allocate(4)));
    }

    Y_UNIT_TEST(ShouldNotAllocatePagesThatAreBusyAlready)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(DefaultPageSize),
            DefaultPageCount,
            DefaultPageSize,
            EDevicePageStoreMode::Checked);

        UNIT_ASSERT_VALUES_EQUAL("0x2", Describe(store->Allocate(2)));

        // a page in the middle of the pageRange is enough to reject it
        const auto busy = store->AllocateAt(MakeRanges({{1, 4}}));
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, busy.GetCode());

        const auto behind = store->AllocateAt(MakeRanges({{2, 2}, {1, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, behind.GetCode());

        // the ranges of a rejected call are not allocated, not even in part
        UNIT_ASSERT_VALUES_EQUAL("2x4", Describe(store->Allocate(4)));
    }

    Y_UNIT_TEST(ShouldKeepContentOfRejectedPages)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(DefaultPageSize),
            DefaultPageCount,
            DefaultPageSize,
            EDevicePageStoreMode::Checked);

        auto ranges = WriteRecord(store, {"aaaa", "bbbb"});

        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            store->AllocateAt(ranges).GetCode());

        // the rejected call leaves the pages alone
        UNIT_ASSERT_VALUES_EQUAL(
            "aaaa|bbbb",
            Join(store->Read(ranges).GetValue().GetResult()));
    }

    Y_UNIT_TEST(ShouldAcceptEmptyRequest)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(DefaultPageSize),
            DefaultPageCount,
            DefaultPageSize,
            EDevicePageStoreMode::Checked);

        UNIT_ASSERT(store->Allocate(0).empty());

        TVector<TPageRangeRef> none;
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Write(none, {}).GetValue().GetCode());
        UNIT_ASSERT(store->Read(none).GetValue().GetResult().empty());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, store->Free(none).GetCode());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, store->AllocateAt(none).GetCode());
    }

    Y_UNIT_TEST(ShouldKeepPagesOnDevice)
    {
        auto device = CreateInMemoryDevice(DefaultPageSize);
        auto store = CreateDevicePageStore(
            device,
            DefaultPageCount,
            DefaultPageSize,
            EDevicePageStoreMode::Checked);

        auto ranges = WriteRecord(store, {"aaaa", "bbbb", "cccc"});
        UNIT_ASSERT_VALUES_EQUAL("0x3", Describe(ranges));

        // the pages are on the device, at the page numbers the store handed out
        UNIT_ASSERT_VALUES_EQUAL(
            "aaaa|bbbb|cccc",
            ReadFromDevice(device, 0, 3));
    }

    Y_UNIT_TEST(ShouldNotValidateRefsInTrustedMode)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(DefaultPageSize),
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

        // the same store in the checked mode writes and reads the pages just
        // the same, but does not free what is not allocated
        auto checked = CreateDevicePageStore(
            CreateInMemoryDevice(DefaultPageSize),
            DefaultPageCount,
            DefaultPageSize,
            EDevicePageStoreMode::Checked);

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            checked->Write(ranges, MakePages({"aaaa", "bbbb"}))
                .GetValue()
                .GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            "aaaa|bbbb",
            Join(checked->Read(ranges).GetValue().GetResult()));
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            checked->Free(ranges).GetCode());
    }

    Y_UNIT_TEST(ShouldValidateAllocateAtInTrustedMode)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(DefaultPageSize),
            DefaultPageCount,
            DefaultPageSize,
            EDevicePageStoreMode::Trusted);

        UNIT_ASSERT_VALUES_EQUAL("0x2", Describe(store->Allocate(2)));

        // AllocateAt is given the pages of a restored record, so it checks
        // them even here
        const auto busy = store->AllocateAt(MakeRanges({{1, 4}}));
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, busy.GetCode());

        const auto beyond =
            store->AllocateAt(MakeRanges({{DefaultPageCount, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, beyond.GetCode());

        const auto overlapping =
            store->AllocateAt(MakeRanges({{5, 2}, {6, 2}}));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, overlapping.GetCode());

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->AllocateAt(MakeRanges({{5, 2}})).GetCode());
        UNIT_ASSERT_VALUES_EQUAL("2x3, 7x9", Describe(store->Allocate(12)));
    }

    Y_UNIT_TEST(ShouldTrackAllocationInTrustedMode)
    {
        auto store = CreateDevicePageStore(
            CreateInMemoryDevice(DefaultPageSize),
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

    Y_UNIT_TEST(ShouldReportDeviceWriteError)
    {
        auto store = CreateDevicePageStore(
            std::make_shared<TBrokenDevice>(),
            DefaultPageCount,
            DefaultPageSize,
            EDevicePageStoreMode::Checked);

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

    Y_UNIT_TEST(ShouldReportDeviceReadError)
    {
        auto store = CreateDevicePageStore(
            std::make_shared<TBrokenDevice>(),
            DefaultPageCount,
            DefaultPageSize,
            EDevicePageStoreMode::Checked);

        auto ranges = store->Allocate(1);

        UNIT_ASSERT_VALUES_EQUAL(
            E_IO,
            store->Read(ranges).GetValue().GetError().GetCode());
    }
}

}   // namespace NCloud::NJournalled
