#include "journal_store.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NJournalled {

namespace {

////////////////////////////////////////////////////////////////////////////////

TBuffer MakeBuffer(TStringBuf data)
{
    return TBuffer(data.data(), data.size());
}

TString ToString(const TBuffer& buffer)
{
    return TString(buffer.Data(), buffer.Size());
}

TString JoinKeys(const TSet<ui64>& keys)
{
    TStringBuilder sb;
    for (ui64 key: keys) {
        if (sb) {
            sb << "|";
        }
        sb << key;
    }
    return sb;
}

NCloud::NProto::TWriteLogRecordRequest MakeRequest(
    const TVector<TVector<TString>>& groups)
{
    NCloud::NProto::TWriteLogRecordRequest request;
    for (const auto& content: groups) {
        auto& group = *request.AddPageGroups();
        for (const auto& chunk: content) {
            group.AddContent(chunk);
        }
    }
    return request;
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

TString Join(const TVector<TBuffer>& buffers)
{
    TStringBuilder sb;
    for (const auto& buffer: buffers) {
        if (sb) {
            sb << "|";
        }
        sb << ToString(buffer);
    }
    return sb;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TInMemoryKeyBufferStoreTest)
{
    Y_UNIT_TEST(ShouldStartEmpty)
    {
        auto store = CreateInMemoryKeyBufferStore();

        UNIT_ASSERT(store->GetKeys().empty());
        UNIT_ASSERT_VALUES_EQUAL(
            E_NOT_FOUND,
            store->Read(1).GetValue().GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldInsertAndGet)
    {
        auto store = CreateInMemoryKeyBufferStore();

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Write(1, MakeBuffer("one")).GetValue().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Write(2, MakeBuffer("two")).GetValue().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(
            "one",
            ToString(store->Read(1).GetValue().GetResult()));
        UNIT_ASSERT_VALUES_EQUAL(
            "two",
            ToString(store->Read(2).GetValue().GetResult()));
    }

    Y_UNIT_TEST(ShouldOverwriteAnExistingKey)
    {
        auto store = CreateInMemoryKeyBufferStore();

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Write(1, MakeBuffer("first")).GetValue().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Write(1, MakeBuffer("second")).GetValue().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(
            "second",
            ToString(store->Read(1).GetValue().GetResult()));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            store->GetKeys().size());
    }

    Y_UNIT_TEST(ShouldEraseASingleKey)
    {
        auto store = CreateInMemoryKeyBufferStore();

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Write(1, MakeBuffer("one")).GetValue().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, store->EraseTo(1).GetValue().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_NOT_FOUND,
            store->Read(1).GetValue().GetError().GetCode());

        // removing what is not there reports that nothing was done
        UNIT_ASSERT_VALUES_EQUAL(S_FALSE, store->EraseTo(1).GetValue().GetCode());
    }

    Y_UNIT_TEST(ShouldEraseEveryKeyUpToTheGivenOne)
    {
        auto store = CreateInMemoryKeyBufferStore();

        for (ui64 key: {1, 3, 5, 7}) {
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                store->Write(key, MakeBuffer("x")).GetValue().GetCode());
        }

        // the bound itself is included, and it need not be a stored key
        UNIT_ASSERT_VALUES_EQUAL(S_OK, store->EraseTo(4).GetValue().GetCode());
        UNIT_ASSERT_VALUES_EQUAL("5|7", JoinKeys(store->GetKeys()));

        UNIT_ASSERT_VALUES_EQUAL(S_OK, store->EraseTo(5).GetValue().GetCode());
        UNIT_ASSERT_VALUES_EQUAL("7", JoinKeys(store->GetKeys()));
    }

    Y_UNIT_TEST(ShouldEraseNothingBelowTheLowestKey)
    {
        auto store = CreateInMemoryKeyBufferStore();

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Write(5, MakeBuffer("x")).GetValue().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(
            S_FALSE,
            store->EraseTo(4).GetValue().GetCode());
        UNIT_ASSERT_VALUES_EQUAL("5", JoinKeys(store->GetKeys()));

        // and on an empty store
        UNIT_ASSERT_VALUES_EQUAL(S_OK, store->EraseTo(5).GetValue().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            S_FALSE,
            store->EraseTo(Max<ui64>()).GetValue().GetCode());
    }

    Y_UNIT_TEST(ShouldEraseKeyZeroLikeAnyOther)
    {
        auto store = CreateInMemoryKeyBufferStore();

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Write(0, MakeBuffer("metadata")).GetValue().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Write(10, MakeBuffer("record")).GetValue().GetCode());

        // the store gives key 0 no special meaning
        UNIT_ASSERT_VALUES_EQUAL(S_OK, store->EraseTo(10).GetValue().GetCode());
        UNIT_ASSERT(store->GetKeys().empty());
    }

    Y_UNIT_TEST(ShouldReadKeysInAscendingOrder)
    {
        auto store = CreateInMemoryKeyBufferStore();

        for (ui64 key: {5, 1, 3}) {
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                store->Write(key, MakeBuffer("x")).GetValue().GetCode());
        }

        UNIT_ASSERT_VALUES_EQUAL(
            "1|3|5",
            JoinKeys(store->GetKeys()));
    }

    Y_UNIT_TEST(ShouldKeepAnIndependentCopyOfTheBuffer)
    {
        auto store = CreateInMemoryKeyBufferStore();

        auto buffer = MakeBuffer("original");
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Write(1, buffer).GetValue().GetCode());

        buffer.Clear();

        UNIT_ASSERT_VALUES_EQUAL(
            "original",
            ToString(store->Read(1).GetValue().GetResult()));
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TInMemoryPageStoreTest)
{
    Y_UNIT_TEST(ShouldAllocatePagesOnWrite)
    {
        auto store = CreateInMemoryPageStore();

        auto first = store->WritePageGroups(MakeRequest({{"aaaa", "bbbb"}}))
            .GetValue().GetResult();
        UNIT_ASSERT_VALUES_EQUAL("0x2", Describe(first));

        // the next write continues where the previous one stopped
        auto second = store->WritePageGroups(MakeRequest({{"cccc"}}))
            .GetValue().GetResult();
        UNIT_ASSERT_VALUES_EQUAL("2x1", Describe(second));

        // one ref per requested group
        auto third = store->WritePageGroups(MakeRequest({{"dddd"}, {"eeee"}}))
            .GetValue().GetResult();
        UNIT_ASSERT_VALUES_EQUAL("3x1, 4x1", Describe(third));
    }

    Y_UNIT_TEST(ShouldReadBackWhatWasWritten)
    {
        auto store = CreateInMemoryPageStore();

        auto written = store->WritePageGroups(MakeRequest({{"aaaa", "bbbb"}}))
            .GetValue().GetResult();

        auto refs = written;
        UNIT_ASSERT_VALUES_EQUAL(
            "aaaa|bbbb",
            Join(store->ReadPageGroups(refs).GetValue().GetResult()));
    }

    Y_UNIT_TEST(ShouldStoreOneEntryPerPageWhateverItsSize)
    {
        auto store = CreateInMemoryPageStore();

        // the store has no page size: an entry is a page, long or short
        auto written = store->WritePageGroups(
            MakeRequest({{"a", "bbbbbbbb", ""}})).GetValue().GetResult();
        UNIT_ASSERT_VALUES_EQUAL("0x3", Describe(written));

        auto refs = written;
        UNIT_ASSERT_VALUES_EQUAL(
            "a|bbbbbbbb|",
            Join(store->ReadPageGroups(refs).GetValue().GetResult()));
    }

    Y_UNIT_TEST(ShouldRejectRefsForPagesThatAreNotThere)
    {
        auto store = CreateInMemoryPageStore();

        UNIT_ASSERT(store->WritePageGroups(MakeRequest({{"aaaa"}})).GetValue().GetResult());

        auto beyond = MakeRefs({{0, 2}});
        UNIT_ASSERT_VALUES_EQUAL(
            E_NOT_FOUND,
            store->ReadPageGroups(beyond).GetValue().GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_NOT_FOUND,
            store->Free(beyond).GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_NOT_FOUND,
            store->MarkAsWritten(beyond).GetCode());
    }

    Y_UNIT_TEST(ShouldFreePages)
    {
        auto store = CreateInMemoryPageStore();

        auto written = store->WritePageGroups(MakeRequest({{"aaaa", "bbbb", "cccc"}}))
            .GetValue().GetResult();

        auto head = MakeRefs({{0, 2}});
        UNIT_ASSERT_VALUES_EQUAL(S_OK, store->Free(head).GetCode());

        UNIT_ASSERT_VALUES_EQUAL(
            E_NOT_FOUND,
            store->ReadPageGroups(head).GetValue().GetError().GetCode());

        auto kept = MakeRefs({{2, 1}});
        UNIT_ASSERT_VALUES_EQUAL(
            "cccc",
            Join(store->ReadPageGroups(kept).GetValue().GetResult()));
    }

    Y_UNIT_TEST(ShouldNotFreeAnythingWhenARefIsRejected)
    {
        auto store = CreateInMemoryPageStore();

        UNIT_ASSERT(
            store->WritePageGroups(MakeRequest({{"aaaa", "bbbb"}})).GetValue().GetResult());

        auto beyond = MakeRefs({{0, 3}});
        UNIT_ASSERT_VALUES_EQUAL(
            E_NOT_FOUND,
            store->Free(beyond).GetCode());

        auto written = MakeRefs({{0, 2}});
        UNIT_ASSERT_VALUES_EQUAL(
            "aaaa|bbbb",
            Join(store->ReadPageGroups(written).GetValue().GetResult()));
    }

    Y_UNIT_TEST(ShouldCheckThatMarkedPagesArePresent)
    {
        auto store = CreateInMemoryPageStore();

        auto written = store->WritePageGroups(MakeRequest({{"aaaa", "bbbb"}}))
            .GetValue().GetResult();

        auto refs = written;
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->MarkAsWritten(refs).GetCode());

        // the check leaves the store alone
        UNIT_ASSERT_VALUES_EQUAL(
            "aaaa|bbbb",
            Join(store->ReadPageGroups(refs).GetValue().GetResult()));
    }

    Y_UNIT_TEST(ShouldAcceptAnEmptyRequest)
    {
        auto store = CreateInMemoryPageStore();

        UNIT_ASSERT(store->WritePageGroups({}).GetValue().GetResult().empty());

        TVector<TPageGroupRef> none;
        UNIT_ASSERT(store->ReadPageGroups(none).GetValue().GetResult().empty());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, store->Free(none).GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->MarkAsWritten(none).GetCode());
    }

}

}   // namespace NCloud::NJournalled
