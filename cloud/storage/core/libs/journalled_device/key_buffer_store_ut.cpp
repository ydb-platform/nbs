#include "key_buffer_store.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/string/builder.h>
#include <util/system/spinlock.h>

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

TKeyBuffers Restore(const IKeyBufferStorePtr& store)
{
    auto response = store->Restore().GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(
        S_OK,
        response.GetError().GetCode(),
        FormatError(response.GetError()));
    return response.ExtractResult();
}

// "<key>=<buffer>|..." sorted by key - the store promises no order of its own
TString Describe(TKeyBuffers buffers)
{
    SortBy(buffers, [] (const auto& keyBuffer) { return keyBuffer.first; });

    TStringBuilder sb;
    for (const auto& [key, buffer]: buffers) {
        if (sb) {
            sb << "|";
        }
        sb << key << "=" << AsString(buffer);
    }
    return sb;
}

void Write(const IKeyBufferStorePtr& store, ui64 key, TStringBuf data)
{
    const auto error = store->Write(key, MakeBuffer(data)).GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));
}

ui32 EraseBelow(const IKeyBufferStorePtr& store, ui64 key)
{
    const auto error = store->EraseBelow(key).GetValueSync();
    UNIT_ASSERT_C(!HasError(error), FormatError(error));
    return error.GetCode();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TInMemoryKeyBufferStoreTest)
{
    Y_UNIT_TEST(ShouldStartEmpty)
    {
        auto store = CreateInMemoryKeyBufferStore();

        UNIT_ASSERT(Restore(store).empty());
    }

    Y_UNIT_TEST(ShouldInsertAndGet)
    {
        auto store = CreateInMemoryKeyBufferStore();

        Write(store, 1, "one");
        Write(store, 2, "two");

        UNIT_ASSERT_VALUES_EQUAL("1=one|2=two", Describe(Restore(store)));
    }

    Y_UNIT_TEST(ShouldOverwriteAnExistingKey)
    {
        auto store = CreateInMemoryKeyBufferStore();

        Write(store, 1, "first");
        Write(store, 1, "second");

        UNIT_ASSERT_VALUES_EQUAL("1=second", Describe(Restore(store)));
    }

    Y_UNIT_TEST(ShouldEraseASingleKey)
    {
        auto store = CreateInMemoryKeyBufferStore();

        Write(store, 1, "one");

        UNIT_ASSERT_VALUES_EQUAL(S_OK, EraseBelow(store, 2));
        UNIT_ASSERT(Restore(store).empty());

        // removing what is not there reports that nothing was done
        UNIT_ASSERT_VALUES_EQUAL(S_FALSE, EraseBelow(store, 2));
    }

    Y_UNIT_TEST(ShouldEraseEveryKeyBelowTheGivenOne)
    {
        auto store = CreateInMemoryKeyBufferStore();

        for (ui64 key: {1, 3, 5, 7}) {
            Write(store, key, "x");
        }

        // the bound itself is kept, and it need not be a stored key
        UNIT_ASSERT_VALUES_EQUAL(S_OK, EraseBelow(store, 5));
        UNIT_ASSERT_VALUES_EQUAL("5=x|7=x", Describe(Restore(store)));

        UNIT_ASSERT_VALUES_EQUAL(S_OK, EraseBelow(store, 6));
        UNIT_ASSERT_VALUES_EQUAL("7=x", Describe(Restore(store)));
    }

    Y_UNIT_TEST(ShouldEraseNothingBelowTheLowestKey)
    {
        auto store = CreateInMemoryKeyBufferStore();

        Write(store, 5, "x");

        UNIT_ASSERT_VALUES_EQUAL(S_FALSE, EraseBelow(store, 5));
        UNIT_ASSERT_VALUES_EQUAL("5=x", Describe(Restore(store)));

        // and on an empty store
        UNIT_ASSERT_VALUES_EQUAL(S_OK, EraseBelow(store, 6));
        UNIT_ASSERT_VALUES_EQUAL(S_FALSE, EraseBelow(store, Max<ui64>()));
    }

    Y_UNIT_TEST(ShouldEraseKeyZeroLikeAnyOther)
    {
        auto store = CreateInMemoryKeyBufferStore();

        Write(store, 0, "metadata");
        Write(store, 10, "record");

        // the store gives key 0 no special meaning
        UNIT_ASSERT_VALUES_EQUAL(S_OK, EraseBelow(store, 11));
        UNIT_ASSERT(Restore(store).empty());
    }

    Y_UNIT_TEST(ShouldRefuseToWriteAnErasedKey)
    {
        auto store = CreateInMemoryKeyBufferStore();

        Write(store, 5, "x");
        UNIT_ASSERT_VALUES_EQUAL(S_OK, EraseBelow(store, 6));

        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            store->Write(5, MakeBuffer("x")).GetValueSync().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            store->Write(3, MakeBuffer("x")).GetValueSync().GetCode());

        Write(store, 6, "y");
        UNIT_ASSERT_VALUES_EQUAL("6=y", Describe(Restore(store)));
    }

    Y_UNIT_TEST(ShouldReadEveryKeyWhateverTheWriteOrder)
    {
        auto store = CreateInMemoryKeyBufferStore();

        for (ui64 key: {5, 1, 3}) {
            Write(store, key, "x");
        }

        UNIT_ASSERT_VALUES_EQUAL(3, Restore(store).size());
        UNIT_ASSERT_VALUES_EQUAL("1=x|3=x|5=x", Describe(Restore(store)));
    }

    Y_UNIT_TEST(ShouldKeepAnIndependentCopyOfTheBuffer)
    {
        auto store = CreateInMemoryKeyBufferStore();

        TBuffer buffer = MakeBuffer("original");
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            store->Write(1, buffer).GetValueSync().GetCode());

        buffer.Clear();

        UNIT_ASSERT_VALUES_EQUAL("1=original", Describe(Restore(store)));
    }
}

}   // namespace NCloud::NJournalled
