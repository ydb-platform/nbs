#include "page_store.h"

#include "device.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/join.h>

namespace NCloud::NJournalled {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 DefaultPageCount = 16;
constexpr ui32 DefaultPageSize = 4;

TString Join(const TVector<TString>& pages)
{
    return JoinSeq("|", pages);
}

TVector<TPageGroupRef> MakeRefs(
    const TVector<std::pair<ui64, ui64>>& ranges)
{
    TVector<TPageGroupRef> refs;
    for (const auto& [firstPageNo, pageCount]: ranges) {
        refs.push_back({.FirstPageNo = firstPageNo, .PageCount = pageCount});
    }
    return refs;
}

// "<firstPageNo>x<pageCount>" per returned ref
TString Describe(const TVector<TPageGroupRef>& refs)
{
    TStringBuilder sb;
    for (const auto& ref: refs) {
        if (sb) {
            sb << ", ";
        }
        sb << ref.FirstPageNo << "x" << ref.PageCount;
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
TVector<TPageGroupRef> WriteRecord(
    const IPageStorePtr& store,
    const TVector<TString>& pages)
{
    auto refs = store->Allocate(pages.size());
    UNIT_ASSERT(pages.empty() || !refs.empty());

    const auto error =
        store->Write(refs, pages).GetValue();
    UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));

    return refs;
}

// reads the pages straight from the device, bypassing the store
TString ReadFromDevice(
    const IDevicePtr& device,
    ui64 firstPageNo,
    ui64 pageCount)
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

Y_UNIT_TEST_SUITE(TPageStoreTest)
{
    Y_UNIT_TEST(ShouldAllocateFreePages)
    {
        auto store = CreatePageStore(CreateInMemoryDevice(), 4, DefaultPageSize);

        UNIT_ASSERT_VALUES_EQUAL("0x2", Describe(store->Allocate(2)));

        // the allocated pages are busy, the next allocation goes past them
        UNIT_ASSERT_VALUES_EQUAL("2x1", Describe(store->Allocate(1)));
        UNIT_ASSERT_VALUES_EQUAL("3x1", Describe(store->Allocate(1)));

        // nothing is left
        UNIT_ASSERT(store->Allocate(1).empty());
    }

    Y_UNIT_TEST(ShouldNotAllocateMorePagesThanThereAre)
    {
        auto store = CreatePageStore(CreateInMemoryDevice(), 4, DefaultPageSize);

        UNIT_ASSERT(store->Allocate(5).empty());

        // nothing has been taken
        UNIT_ASSERT_VALUES_EQUAL("0x4", Describe(store->Allocate(4)));
    }

    Y_UNIT_TEST(ShouldAllocateFragmentedFreeSpace)
    {
        auto store = CreatePageStore(CreateInMemoryDevice(), 8, DefaultPageSize);

        UNIT_ASSERT_VALUES_EQUAL("0x8", Describe(store->Allocate(8)));

        // free two ranges apart from each other
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Free(MakeRefs({{1, 2}, {5, 1}})).GetCode());

        // the allocation spans them both, in the page order
        UNIT_ASSERT_VALUES_EQUAL("1x2, 5x1", Describe(store->Allocate(3)));
    }

    Y_UNIT_TEST(ShouldReadBackWhatWasWritten)
    {
        auto store = CreatePageStore(
            CreateInMemoryDevice(),
            DefaultPageCount,
            DefaultPageSize);

        auto refs = WriteRecord(store, {"aaaa", "bbbb"});
        UNIT_ASSERT_VALUES_EQUAL("0x2", Describe(refs));

        UNIT_ASSERT_VALUES_EQUAL(
            "aaaa|bbbb",
            Join(store->Read(refs).GetValue().GetResult()));
    }

    Y_UNIT_TEST(ShouldRejectPagesOfAWrongSize)
    {
        auto store = CreatePageStore(
            CreateInMemoryDevice(),
            DefaultPageCount,
            DefaultPageSize);

        auto refs = store->Allocate(2);

        for (const auto& pages: TVector<TVector<TString>>{
                {"aaaa", "bbb"},        // too short
                {"aaaa", "bbbbb"},      // too long
                {"aaaa", ""}})          // empty
        {
            const auto error =
                store->Write(refs, pages).GetValue();

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
            store->Write(refs, TVector<TString>({"aaaa", "bbbb"}))
                .GetValue();
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));

        UNIT_ASSERT_VALUES_EQUAL(
            "aaaa|bbbb",
            Join(store->Read(refs).GetValue().GetResult()));
    }

    Y_UNIT_TEST(ShouldWriteIntoFragmentedRefs)
    {
        auto store = CreatePageStore(CreateInMemoryDevice(), 8, DefaultPageSize);

        UNIT_ASSERT_VALUES_EQUAL("0x8", Describe(store->Allocate(8)));
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Free(MakeRefs({{1, 2}, {5, 1}})).GetCode());

        auto refs = store->Allocate(3);
        UNIT_ASSERT_VALUES_EQUAL("1x2, 5x1", Describe(refs));

        const auto error =
            store->Write(refs, TVector<TString>({"aaaa", "bbbb", "cccc"}))
                .GetValue();
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));

        // the pages follow the refs, in order
        UNIT_ASSERT_VALUES_EQUAL(
            "aaaa|bbbb|cccc",
            Join(store->Read(refs).GetValue().GetResult()));
    }

    Y_UNIT_TEST(ShouldRejectAWriteWithAWrongNumberOfPages)
    {
        auto store = CreatePageStore(
            CreateInMemoryDevice(),
            DefaultPageCount,
            DefaultPageSize);

        auto refs = store->Allocate(2);

        const auto tooMany =
            store->Write(refs, TVector<TString>({"aaaa", "bbbb", "cccc"}))
                .GetValue();
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, tooMany.GetCode());
        UNIT_ASSERT_STRING_CONTAINS(
            tooMany.GetMessage(),
            "the page group refs hold 2 pages, 3 given");

        const auto tooFew =
            store->Write(refs, TVector<TString>({"aaaa"})).GetValue();
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, tooFew.GetCode());
    }

    Y_UNIT_TEST(ShouldRejectAWriteIntoPagesThatAreNotAllocated)
    {
        auto store = CreatePageStore(
            CreateInMemoryDevice(),
            DefaultPageCount,
            DefaultPageSize);

        const auto free =
            store->Write(MakeRefs({{0, 1}}), TVector<TString>({"aaaa"}))
                .GetValue();
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, free.GetCode());

        const auto beyond = store->Write(
            MakeRefs({{DefaultPageCount, 1}}),
            TVector<TString>({"aaaa"})).GetValue();
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, beyond.GetCode());
    }

    Y_UNIT_TEST(ShouldRejectRefsForPagesThatAreNotThere)
    {
        auto store = CreatePageStore(
            CreateInMemoryDevice(),
            DefaultPageCount,
            DefaultPageSize);

        WriteRecord(store, {"aaaa"});

        auto free = MakeRefs({{0, 2}});
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            store->Read(free).GetValue().GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            store->Free(free).GetCode());

        // the refs beyond the device are rejected as ill-formed
        auto beyond = MakeRefs({{DefaultPageCount - 1, 2}});
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            store->Read(beyond).GetValue().GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            store->AllocateAt(beyond).GetCode());
    }

    Y_UNIT_TEST(ShouldAcceptRefsThatOnlyTouchTheFreeSpace)
    {
        auto store = CreatePageStore(CreateInMemoryDevice(), 8, DefaultPageSize);

        UNIT_ASSERT_VALUES_EQUAL("0x8", Describe(store->Allocate(8)));

        // a hole in the middle: the pages 3 and 4
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Free(MakeRefs({{3, 2}})).GetCode());

        // a ref that ends right where the hole begins is busy all the way
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Read(MakeRefs({{0, 3}}))
                .GetValue().GetError().GetCode());

        // and so is a ref that begins right where the hole ends
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Read(MakeRefs({{5, 3}}))
                .GetValue().GetError().GetCode());

        // reaching into the hole from either side is not allowed
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            store->Read(MakeRefs({{2, 2}}))
                .GetValue().GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            store->Read(MakeRefs({{4, 2}}))
                .GetValue().GetError().GetCode());

        // as is a ref that spans the hole
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            store->Read(MakeRefs({{0, 8}}))
                .GetValue().GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldFreePages)
    {
        auto store = CreatePageStore(
            CreateInMemoryDevice(),
            DefaultPageCount,
            DefaultPageSize);

        WriteRecord(store, {"aaaa", "bbbb", "cccc"});

        auto head = MakeRefs({{0, 2}});
        UNIT_ASSERT_VALUES_EQUAL(S_OK, store->Free(head).GetCode());

        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            store->Read(head).GetValue().GetError().GetCode());

        auto kept = MakeRefs({{2, 1}});
        UNIT_ASSERT_VALUES_EQUAL(
            "cccc",
            Join(store->Read(kept).GetValue().GetResult()));
    }

    Y_UNIT_TEST(ShouldNotFreeAnythingWhenARefIsRejected)
    {
        auto store = CreatePageStore(
            CreateInMemoryDevice(),
            DefaultPageCount,
            DefaultPageSize);

        WriteRecord(store, {"aaaa", "bbbb"});

        auto reaching = MakeRefs({{0, 3}});
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            store->Free(reaching).GetCode());

        auto written = MakeRefs({{0, 2}});
        UNIT_ASSERT_VALUES_EQUAL(
            "aaaa|bbbb",
            Join(store->Read(written).GetValue().GetResult()));
    }

    Y_UNIT_TEST(ShouldReuseFreedPages)
    {
        auto store = CreatePageStore(CreateInMemoryDevice(), 4, DefaultPageSize);

        auto refs = WriteRecord(store, {"aaaa", "bbbb"});
        UNIT_ASSERT_VALUES_EQUAL("0x2", Describe(refs));

        UNIT_ASSERT_VALUES_EQUAL(S_OK, store->Free(refs).GetCode());

        // the freed pages are merged back into the free space
        UNIT_ASSERT_VALUES_EQUAL("0x4", Describe(store->Allocate(4)));
    }

    Y_UNIT_TEST(ShouldAllocateTheGivenPages)
    {
        auto store = CreatePageStore(
            CreateInMemoryDevice(),
            DefaultPageCount,
            DefaultPageSize);

        // the pages of a restored record are taken out of the free space
        auto restored = MakeRefs({{0, 2}, {5, 1}});
        UNIT_ASSERT_VALUES_EQUAL(S_OK, store->AllocateAt(restored).GetCode());

        UNIT_ASSERT_VALUES_EQUAL("2x3, 6x1", Describe(store->Allocate(4)));
    }

    Y_UNIT_TEST(ShouldNotAllocateThePagesThatAreBusyAlready)
    {
        auto store = CreatePageStore(
            CreateInMemoryDevice(),
            DefaultPageCount,
            DefaultPageSize);

        UNIT_ASSERT_VALUES_EQUAL("0x2", Describe(store->Allocate(2)));

        // a page in the middle of the ref is enough to reject it
        const auto busy = store->AllocateAt(MakeRefs({{1, 4}}));
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, busy.GetCode());
        UNIT_ASSERT_STRING_CONTAINS(busy.GetMessage(), "page 1 is busy already");

        const auto behind = store->AllocateAt(MakeRefs({{2, 2}, {1, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, behind.GetCode());
        UNIT_ASSERT_STRING_CONTAINS(
            behind.GetMessage(),
            "page 1 is busy already");

        // the refs of a rejected call are not allocated, not even in part
        UNIT_ASSERT_VALUES_EQUAL("2x4", Describe(store->Allocate(4)));
    }

    Y_UNIT_TEST(ShouldKeepTheContentOfTheRejectedPages)
    {
        auto store = CreatePageStore(
            CreateInMemoryDevice(),
            DefaultPageCount,
            DefaultPageSize);

        auto refs = WriteRecord(store, {"aaaa", "bbbb"});

        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            store->AllocateAt(refs).GetCode());

        // the rejected call leaves the pages alone
        UNIT_ASSERT_VALUES_EQUAL(
            "aaaa|bbbb",
            Join(store->Read(refs).GetValue().GetResult()));
    }

    Y_UNIT_TEST(ShouldAcceptAnEmptyRequest)
    {
        auto store = CreatePageStore(
            CreateInMemoryDevice(),
            DefaultPageCount,
            DefaultPageSize);

        UNIT_ASSERT(store->Allocate(0).empty());

        TVector<TPageGroupRef> none;
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
        auto store = CreatePageStore(device, DefaultPageCount, DefaultPageSize);

        auto refs = WriteRecord(store, {"aaaa", "bbbb", "cccc"});
        UNIT_ASSERT_VALUES_EQUAL("0x3", Describe(refs));

        // the pages are on the device, at the page numbers the store handed out
        UNIT_ASSERT_VALUES_EQUAL(
            "aaaa|bbbb|cccc",
            ReadFromDevice(device, 0, 3));
    }

    Y_UNIT_TEST(ShouldReportTheDeviceWriteError)
    {
        auto store = CreatePageStore(
            std::make_shared<TBrokenDevice>(),
            DefaultPageCount,
            DefaultPageSize);

        auto refs = store->Allocate(2);
        UNIT_ASSERT_VALUES_EQUAL("0x2", Describe(refs));

        UNIT_ASSERT_VALUES_EQUAL(
            E_IO,
            store->Write(refs, TVector<TString>({"aaaa", "bbbb"}))
                .GetValue().GetCode());

        // the pages stay allocated, releasing them is up to the caller
        UNIT_ASSERT_VALUES_EQUAL("2x1", Describe(store->Allocate(1)));
    }

    Y_UNIT_TEST(ShouldReportTheDeviceReadError)
    {
        auto store = CreatePageStore(
            std::make_shared<TBrokenDevice>(),
            DefaultPageCount,
            DefaultPageSize);

        auto refs = store->Allocate(1);

        UNIT_ASSERT_VALUES_EQUAL(
            E_IO,
            store->Read(refs).GetValue().GetError().GetCode());
    }
}

}   // namespace NCloud::NJournalled
