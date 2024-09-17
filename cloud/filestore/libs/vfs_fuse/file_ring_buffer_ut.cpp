#include "file_ring_buffer.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>
#include <util/system/tempfile.h>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString Dump(const TVector<TBrokenFileRingBufferEntry>& entries)
{
    TStringBuilder sb;

    for (ui32 i = 0; i < entries.size(); ++i) {
        if (i) {
            sb << ", ";
        }

        sb << "data=" << entries[i].Data
            << " ecsum=" << entries[i].ExpectedChecksum
            << " csum=" << entries[i].ActualChecksum;
    }

    return sb;
}

TString PopAll(TFileRingBuffer& rb)
{
    TStringBuilder sb;

    while (!rb.Empty()) {
        if (sb.Size()) {
            sb << ", ";
        }

        sb << rb.Front();
        rb.Pop();
    }

    return sb;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFileRingBufferTest)
{
    Y_UNIT_TEST(ShouldPushPop)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 64;
        const ui32 maxEntrySize = 10;
        TFileRingBuffer rb(f.GetName(), len, maxEntrySize);

        UNIT_ASSERT_VALUES_EQUAL(0, rb.Size());
        UNIT_ASSERT(rb.Empty());

        UNIT_ASSERT(!rb.Push("longvasya11"));   // too long
        UNIT_ASSERT(!rb.Push(""));              // empty
        UNIT_ASSERT(rb.Push("vasya"));
        UNIT_ASSERT(rb.Push("petya"));
        UNIT_ASSERT(rb.Push("vasya2"));
        UNIT_ASSERT(rb.Push("petya2"));
        UNIT_ASSERT(!rb.Push("vasya3"));        // out of space

        UNIT_ASSERT_VALUES_EQUAL("", Dump(rb.Validate()));
        UNIT_ASSERT_VALUES_EQUAL(4, rb.Size());
        UNIT_ASSERT_VALUES_EQUAL("vasya", rb.Front());
        rb.Pop();

        UNIT_ASSERT_VALUES_EQUAL("", Dump(rb.Validate()));
        UNIT_ASSERT_VALUES_EQUAL(3, rb.Size());
        UNIT_ASSERT(!rb.Push("vasya3"));

        UNIT_ASSERT_VALUES_EQUAL("petya", rb.Front());
        rb.Pop();

        UNIT_ASSERT_VALUES_EQUAL("", Dump(rb.Validate()));
        UNIT_ASSERT_VALUES_EQUAL(2, rb.Size());
        UNIT_ASSERT(rb.Push("vasya3"));

        UNIT_ASSERT_VALUES_EQUAL("", Dump(rb.Validate()));
        UNIT_ASSERT_VALUES_EQUAL(3, rb.Size());
        UNIT_ASSERT_VALUES_EQUAL("vasya2", rb.Front());
        rb.Pop();

        UNIT_ASSERT_VALUES_EQUAL("", Dump(rb.Validate()));
        UNIT_ASSERT_VALUES_EQUAL(2, rb.Size());
        UNIT_ASSERT_VALUES_EQUAL("petya2", rb.Front());
        rb.Pop();

        UNIT_ASSERT_VALUES_EQUAL("", Dump(rb.Validate()));
        UNIT_ASSERT_VALUES_EQUAL(1, rb.Size());
        UNIT_ASSERT_VALUES_EQUAL("vasya3", rb.Front());
        rb.Pop();

        UNIT_ASSERT_VALUES_EQUAL("", Dump(rb.Validate()));
        UNIT_ASSERT_VALUES_EQUAL(0, rb.Size());
        UNIT_ASSERT(rb.Empty());
    }

    Y_UNIT_TEST(ShouldRestore)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 64;
        const ui32 maxEntrySize = 10;
        auto rb = std::make_unique<TFileRingBuffer>(
            f.GetName(),
            len,
            maxEntrySize);

        UNIT_ASSERT(rb->Push("vasya"));
        UNIT_ASSERT(rb->Push("petya"));
        UNIT_ASSERT(rb->Push("vasya2"));
        UNIT_ASSERT(rb->Push("petya2"));
        rb->Pop();
        rb->Pop();
        UNIT_ASSERT(rb->Push("vasya3"));
        UNIT_ASSERT(rb->Push("xxx"));

        rb = std::make_unique<TFileRingBuffer>(
            f.GetName(),
            len,
            maxEntrySize);

        UNIT_ASSERT_VALUES_EQUAL("", Dump(rb->Validate()));
        UNIT_ASSERT_VALUES_EQUAL(4, rb->Size());

        UNIT_ASSERT_VALUES_EQUAL("vasya2, petya2, vasya3, xxx", PopAll(*rb));
    }

    Y_UNIT_TEST(ShouldValidate)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 64;
        const ui32 maxEntrySize = 10;
        TFileRingBuffer rb(f.GetName(), len, maxEntrySize);

        UNIT_ASSERT(rb.Push("vasya"));
        UNIT_ASSERT(rb.Push("petya"));
        UNIT_ASSERT(rb.Push("vasya2"));
        UNIT_ASSERT(rb.Push("petya2"));

        UNIT_ASSERT_VALUES_EQUAL("", Dump(rb.Validate()));
        TFileMap m(f.GetName(), TMemoryMapCommon::oRdWr);
        m.Map(0, len);
        char* data = static_cast<char*>(m.Ptr());
        data[10] = 'A';

        UNIT_ASSERT_VALUES_EQUAL(
            "data=invalid_entry_marker ecsum=0 csum=11034342",
            Dump(rb.Validate()));
    }
}

}   // namespace NCloud::NFileStore
