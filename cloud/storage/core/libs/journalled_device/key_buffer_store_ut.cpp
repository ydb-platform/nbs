#include "key_buffer_store.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/buffer.h>

namespace NCloud::NJournalled {

namespace {

////////////////////////////////////////////////////////////////////////////////

TBuffer MakeBuffer(TStringBuf data)
{
    return TBuffer(data.data(), data.size());
}

TString AsString(const TBuffer& buffer)
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
            AsString(store->Read(1).GetValue().GetResult()));
        UNIT_ASSERT_VALUES_EQUAL(
            "two",
            AsString(store->Read(2).GetValue().GetResult()));
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
            AsString(store->Read(1).GetValue().GetResult()));
        UNIT_ASSERT_VALUES_EQUAL(1, store->GetKeys().size());
    }

    Y_UNIT_TEST(ShouldEraseASingleKey)
    {
        auto store = CreateInMemoryKeyBufferStore();

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Write(1, MakeBuffer("one")).GetValue().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->EraseUpTo(1).GetValue().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_NOT_FOUND,
            store->Read(1).GetValue().GetError().GetCode());

        // removing what is not there reports that nothing was done
        UNIT_ASSERT_VALUES_EQUAL(
            S_FALSE,
            store->EraseUpTo(1).GetValue().GetCode());
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
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->EraseUpTo(4).GetValue().GetCode());
        UNIT_ASSERT_VALUES_EQUAL("5|7", JoinKeys(store->GetKeys()));

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->EraseUpTo(5).GetValue().GetCode());
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
            store->EraseUpTo(4).GetValue().GetCode());
        UNIT_ASSERT_VALUES_EQUAL("5", JoinKeys(store->GetKeys()));

        // and on an empty store
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->EraseUpTo(5).GetValue().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            S_FALSE,
            store->EraseUpTo(Max<ui64>()).GetValue().GetCode());
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
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->EraseUpTo(10).GetValue().GetCode());
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

        UNIT_ASSERT_VALUES_EQUAL("1|3|5", JoinKeys(store->GetKeys()));
    }

    Y_UNIT_TEST(ShouldKeepAnIndependentCopyOfTheBuffer)
    {
        auto store = CreateInMemoryKeyBufferStore();

        TBuffer buffer = MakeBuffer("original");
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Write(1, buffer).GetValue().GetCode());

        buffer.Clear();

        UNIT_ASSERT_VALUES_EQUAL(
            "original",
            AsString(store->Read(1).GetValue().GetResult()));
    }
}

}   // namespace NCloud::NJournalled
