#include "file_ring_buffer.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/deque.h>
#include <util/generic/size_literals.h>
#include <util/random/random.h>
#include <util/system/filemap.h>
#include <util/system/tempfile.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString Dump(const TVector<TFileRingBuffer::TBrokenFileEntry>& entries)
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

TString Dump(const TVector<TString>& entries)
{
    TStringBuilder sb;

    for (ui32 i = 0; i < entries.size(); ++i) {
        if (i) {
            sb << ", ";
        }
        sb << entries[i];
    }

    return sb;
}

TString PopAll(TFileRingBuffer& rb)
{
    TStringBuilder sb;

    while (!rb.Empty()) {
        if (sb.size()) {
            sb << ", ";
        }

        sb << rb.Front();
        rb.PopFront();
    }

    return sb;
}

////////////////////////////////////////////////////////////////////////////////

struct TReferenceImplementation
{
    static constexpr ui32 EntryOverhead = 8;

    const ui32 MaxWeight;
    TDeque<TString> Q;
    ui32 ReadPos = 0;
    ui32 WritePos = 0;
    ui32 SlackSpace = 0;

    explicit TReferenceImplementation(ui32 maxWeight)
        : MaxWeight(maxWeight)
    {}

    bool PushBack(TStringBuf data)
    {
        if (data.empty() || data.size() > MaxWeight) {
            return false;
        }

        const ui32 sz = EntryOverhead + data.size();
        if (sz > MaxWeight) {
            return false;
        }

        if (!Empty()) {
            if (ReadPos < WritePos) {
                const auto avail = MaxWeight - WritePos;
                if (avail <= sz) {
                    if (ReadPos <= sz) {
                        // out of space
                        return false;
                    }

                    SlackSpace = avail;
                    WritePos = 0;
                }
            } else {
                const auto avail = ReadPos - WritePos;
                if (avail <= sz) {
                    // out of space
                    return false;
                }
            }
        }

        WritePos += sz;
        Q.emplace_back(data);
        return true;
    }

    TStringBuf Front() const
    {
        if (!Q) {
            return {};
        }

        return Q.front();
    }

    TStringBuf Back() const
    {
        if (!Q) {
            return {};
        }

        return Q.back();
    }

    void PopFront()
    {
        if (!Q) {
            return;
        }

        const ui32 sz = Q.front().size() + EntryOverhead;
        ReadPos += sz;
        if (MaxWeight - ReadPos <= SlackSpace) {
            UNIT_ASSERT_VALUES_EQUAL(SlackSpace, MaxWeight - ReadPos);
            if (ReadPos == WritePos) {
                WritePos = 0;
            }
            ReadPos = 0;
            SlackSpace = 0;
        }

        Q.pop_front();
    }

    bool Empty() const
    {
        return Q.empty();
    }

    ui32 Size() const
    {
        return Q.size();
    }

    auto Validate() const
    {
        return TVector<TFileRingBuffer::TBrokenFileEntry>();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFileRingBufferTest)
{
    TString GenerateData(ui32 sz)
    {
        TString s(sz, 0);
        for (ui32 i = 0; i < sz; ++i) {
            s[i] = 'a' + RandomNumber<char>('z' - 'a' + 1);
        }
        return s;
    }

    template <typename TRingBuffer>
    void DoTestShouldPushPop(TRingBuffer& rb)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, rb.Size());
        UNIT_ASSERT(rb.Empty());

        UNIT_ASSERT(!rb.PushBack(GenerateData(rb.Size())));   // too long
        UNIT_ASSERT(!rb.PushBack(""));              // empty

        UNIT_ASSERT(rb.PushBack("vasya"));
        UNIT_ASSERT_VALUES_EQUAL("vasya", rb.Back());

        UNIT_ASSERT(rb.PushBack("petya"));
        UNIT_ASSERT_VALUES_EQUAL("petya", rb.Back());

        UNIT_ASSERT(rb.PushBack("vasya2"));
        UNIT_ASSERT_VALUES_EQUAL("vasya2", rb.Back());

        UNIT_ASSERT(rb.PushBack("petya2"));
        UNIT_ASSERT(!rb.PushBack("vasya3"));        // out of space

        UNIT_ASSERT_VALUES_EQUAL("", Dump(rb.Validate()));
        UNIT_ASSERT_VALUES_EQUAL(4, rb.Size());
        UNIT_ASSERT_VALUES_EQUAL("vasya", rb.Front());
        rb.PopFront();

        UNIT_ASSERT_VALUES_EQUAL("", Dump(rb.Validate()));
        UNIT_ASSERT_VALUES_EQUAL(3, rb.Size());
        UNIT_ASSERT(!rb.PushBack("vasya3"));

        UNIT_ASSERT_VALUES_EQUAL("petya", rb.Front());
        rb.PopFront();

        UNIT_ASSERT_VALUES_EQUAL("", Dump(rb.Validate()));
        UNIT_ASSERT_VALUES_EQUAL(2, rb.Size());
        UNIT_ASSERT(rb.PushBack("vasya3"));

        UNIT_ASSERT_VALUES_EQUAL("", Dump(rb.Validate()));
        UNIT_ASSERT_VALUES_EQUAL(3, rb.Size());
        UNIT_ASSERT_VALUES_EQUAL("vasya2", rb.Front());
        rb.PopFront();

        UNIT_ASSERT_VALUES_EQUAL("", Dump(rb.Validate()));
        UNIT_ASSERT_VALUES_EQUAL(2, rb.Size());
        UNIT_ASSERT_VALUES_EQUAL("petya2", rb.Front());
        rb.PopFront();

        UNIT_ASSERT_VALUES_EQUAL("", Dump(rb.Validate()));
        UNIT_ASSERT_VALUES_EQUAL(1, rb.Size());
        UNIT_ASSERT_VALUES_EQUAL("vasya3", rb.Front());
        rb.PopFront();

        UNIT_ASSERT_VALUES_EQUAL("", Dump(rb.Validate()));
        UNIT_ASSERT_VALUES_EQUAL(0, rb.Size());
        UNIT_ASSERT(rb.Empty());
    }

    Y_UNIT_TEST(ShouldPushPop)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 64;
        TFileRingBuffer rb(f.GetName(), len);

        DoTestShouldPushPop(rb);
    }

    Y_UNIT_TEST(ShouldPushPopReferenceImplementation)
    {
        const ui32 len = 64;
        TReferenceImplementation rb(len);

        DoTestShouldPushPop(rb);
    }

    Y_UNIT_TEST(ShouldRestore)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 64;
        auto rb = std::make_unique<TFileRingBuffer>(
            f.GetName(),
            len);

        UNIT_ASSERT(rb->PushBack("vasya"));
        UNIT_ASSERT(rb->PushBack("petya"));
        UNIT_ASSERT(rb->PushBack("vasya2"));
        UNIT_ASSERT(rb->PushBack("petya2"));
        rb->PopFront();
        rb->PopFront();
        UNIT_ASSERT(rb->PushBack("vasya3"));
        UNIT_ASSERT(rb->PushBack("xxx"));

        rb = std::make_unique<TFileRingBuffer>(
            f.GetName(),
            len);

        UNIT_ASSERT_VALUES_EQUAL("", Dump(rb->Validate()));
        UNIT_ASSERT_VALUES_EQUAL(4, rb->Size());

        UNIT_ASSERT_VALUES_EQUAL("vasya2, petya2, vasya3, xxx", PopAll(*rb));
    }

    Y_UNIT_TEST(ShouldValidate)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 128;
        TFileRingBuffer rb(f.GetName(), len);

        UNIT_ASSERT(rb.PushBack("vasya"));
        UNIT_ASSERT(rb.PushBack("petya"));
        UNIT_ASSERT(rb.PushBack("vasya2"));
        UNIT_ASSERT(rb.PushBack("petya2"));

        UNIT_ASSERT_VALUES_EQUAL("", Dump(rb.Validate()));
        TFileMap m(f.GetName(), TMemoryMapCommon::oRdWr);
        m.Map(0, len);
        char* data = static_cast<char*>(m.Ptr());
        data[44] = 'A';

        UNIT_ASSERT_VALUES_EQUAL(
            "data=vasya ecsum=3387363649 csum=3387363646",
            Dump(rb.Validate()));
    }

    Y_UNIT_TEST(ShouldIgnoreSlackSpaceSmallerThanEntryHeader)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 64;
        TFileRingBuffer rb(f.GetName(), len);

        const ui32 entryHeaderSize = 8;
        const ui32 entryLen = 29;
        const ui32 entryDataLen = entryLen - entryHeaderSize;
        const TString data(entryDataLen + 1, 'a');
        const TString data2(entryDataLen, 'b');
        const TString data3(entryDataLen, 'c');

        UNIT_ASSERT(rb.PushBack(data));
        UNIT_ASSERT(rb.PushBack(data2));
        UNIT_ASSERT(!rb.PushBack(data3));
        rb.PopFront();
        UNIT_ASSERT(rb.PushBack(data3));

        /*
         * Buffer data:
         *  hhhhhhhhccccccccccccccccccccc0hhhhhhhhbbbbbbbbbbbbbbbbbbbbb00000
         */

        UNIT_ASSERT_VALUES_EQUAL("", Dump(rb.Validate()));
    }

    void DoRandomizedPushPopRestore(
        ui32 len,
        ui32 testBytes,
        ui32 testUpToEntrySize)
    {
        const auto f = TTempFileHandle();
        const double restoreProbability = 0.05;
        std::unique_ptr<TFileRingBuffer> rb;
        TReferenceImplementation ri(len);

        auto restore = [&] () {
            rb = std::make_unique<TFileRingBuffer>(
                f.GetName(),
                len);
        };

        restore();

        ui32 remainingBytes = testBytes;
        while (remainingBytes || !ri.Empty()) {
            const bool shouldPush = remainingBytes && RandomNumber<bool>();
            if (shouldPush) {
                const ui32 entrySize =
                    RandomNumber(Min(remainingBytes + 1, testUpToEntrySize));
                const auto data = GenerateData(entrySize);
                const auto maxAllocationSize = rb->GetMaxAllocationBytesCount();
                const bool pushed = ri.PushBack(data);
                UNIT_ASSERT_VALUES_EQUAL(pushed, rb->PushBack(data));
                if (pushed) {
                    UNIT_ASSERT_LE_C(
                        data.size(),
                        maxAllocationSize,
                        "Data size " << data.size()
                                     << " should be less or equal than "
                                        "GetMaxAllocationBytesCount "
                                     << maxAllocationSize
                                     << " for a successful PushBack");
                    UNIT_ASSERT_VALUES_EQUAL(ri.Back(), rb->Back());
                    remainingBytes -= entrySize;
                    // Cerr << "PUSH\t" << data << Endl;
                } else {
                    UNIT_ASSERT_C(
                        data.size() == 0 || data.size() > maxAllocationSize,
                        "Data size " << data.size()
                                     << " should be zero or greater than "
                                        "GetMaxAllocationBytesCount "
                                     << maxAllocationSize
                                     << " for a unsuccessful PushBack");
                }
            } else {
                UNIT_ASSERT_VALUES_EQUAL(ri.Back(), rb->Back());
                UNIT_ASSERT_VALUES_EQUAL(ri.Front(), rb->Front());
                // Cerr << "POP\t" << ri.Front() << Endl;
                ri.PopFront();
                rb->PopFront();
            }

            // Cerr << ri.Size() << " " << remainingBytes << Endl;

            if (RandomNumber<double>() < restoreProbability) {
                restore();
            }

            UNIT_ASSERT_VALUES_EQUAL(ri.Size(), rb->Size());
            UNIT_ASSERT_VALUES_EQUAL(ri.Empty(), rb->Empty());
            UNIT_ASSERT_VALUES_EQUAL("", Dump(rb->Validate()));
            UNIT_ASSERT(!rb->IsCorrupted());
        }
    }

    Y_UNIT_TEST(RandomizedPushPopRestore)
    {
        DoRandomizedPushPopRestore(1_MB, 16_MB, 5_KB);
    }

    Y_UNIT_TEST(RandomizedPushPopRestoreSmall)
    {
        DoRandomizedPushPopRestore(4_KB, 1_MB, 16);
    }

    Y_UNIT_TEST(ShouldFullyUtilizeCapacity)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 64;
        TFileRingBuffer rb(f.GetName(), len);

        const ui32 entryHeaderSize = 8;
        const ui32 entryLen = 32;
        const ui32 entryDataLen = entryLen - entryHeaderSize;
        const TString data(entryDataLen, 'a');
        const TString data2(entryDataLen, 'b');
        const TString data3(entryDataLen, 'c');
        const TString data4(entryDataLen, 'd');

        UNIT_ASSERT(rb.PushBack(data));
        UNIT_ASSERT(rb.PushBack(data2));
        UNIT_ASSERT(!rb.PushBack(data3));
        rb.PopFront();
        UNIT_ASSERT(!rb.PushBack(data3));
        rb.PopFront();
        UNIT_ASSERT(rb.PushBack(data3));
        UNIT_ASSERT(rb.PushBack(data4));
    }

    Y_UNIT_TEST(ShouldNotAccessMemoryOutsideMappedBuffer)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 32;
        TFileRingBuffer rb(f.GetName(), len);

        TFileMap m(f.GetName(), TMemoryMapCommon::oRdWr);
        m.Map(0, len + 40); // len + sizeof(THeader)
        char* data = static_cast<char*>(m.Ptr());
        data[len + 40] = 'A';

        UNIT_ASSERT(rb.PushBack("01234567"));
        UNIT_ASSERT(rb.PushBack("89abcde"));
        rb.PopFront();
        UNIT_ASSERT(rb.PushBack("01"));
        rb.PopFront();
        UNIT_ASSERT_VALUES_EQUAL("01", rb.Front());
    }

    struct TStateWithCorruptedEntryLength
    {
        const TTempFileHandle FileHandle;
        const ui32 Len = 32;
        TFileRingBuffer RingBuffer;

        explicit TStateWithCorruptedEntryLength(int newLength)
            : RingBuffer(FileHandle.GetName(), Len)
        {
            UNIT_ASSERT(RingBuffer.PushBack("aaa"));
            UNIT_ASSERT(RingBuffer.PushBack("bb"));

            TFileMap m(FileHandle.GetName(), TMemoryMapCommon::oRdWr);
            m.Map(0, Len + 40); // len + sizeof(THeader)
            char* data = static_cast<char*>(m.Ptr());
            UNIT_ASSERT_VALUES_EQUAL(2, data[51]);
            data[51] = newLength;
        }
    };

    Y_UNIT_TEST(ShouldSetIsCorruptedFlagWhenEntryLengthIsAltered)
    {
        for (int i = 0; i <= 32; i++) {
            TStateWithCorruptedEntryLength s(i);
            TFileRingBuffer rb(s.FileHandle.GetName(), s.Len);
            UNIT_ASSERT_VALUES_EQUAL(i != 2, rb.IsCorrupted());
        }
    }

    Y_UNIT_TEST(ShouldSetIsCorruptedFlagInVisitWhenEntryLengthIsAltered)
    {
        for (int i = 0; i <= 32; i++) {
            TStateWithCorruptedEntryLength s(i);
            UNIT_ASSERT(!s.RingBuffer.IsCorrupted());
            s.RingBuffer.Visit([] (ui32, TStringBuf) {});
            UNIT_ASSERT_VALUES_EQUAL(i != 2, s.RingBuffer.IsCorrupted());
        }
    }

    Y_UNIT_TEST(ShouldSetIsCorruptedFlagInValidateWhenEntryLengthIsAltered)
    {
        for (int i = 0; i <= 32; i++) {
            TStateWithCorruptedEntryLength s(i);
            UNIT_ASSERT(!s.RingBuffer.IsCorrupted());
            s.RingBuffer.Validate();
            UNIT_ASSERT_VALUES_EQUAL(i != 2, s.RingBuffer.IsCorrupted());
        }
    }

    Y_UNIT_TEST(ShouldProhibitPushBackInCorruptedState)
    {
        TStateWithCorruptedEntryLength good(2);
        TFileRingBuffer rb(good.FileHandle.GetName(), good.Len);
        UNIT_ASSERT(rb.PushBack("c"));

        TStateWithCorruptedEntryLength bad(1);
        TFileRingBuffer rb2(bad.FileHandle.GetName(), bad.Len);
        UNIT_ASSERT(!rb2.PushBack("c"));
    }

    Y_UNIT_TEST(ShouldNotFailOnCapacityChange)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 16;
        TFileRingBuffer rb(f.GetName(), len);
        TFileRingBuffer rb1(f.GetName(), len + 1);
        TFileRingBuffer rb2(f.GetName(), len - 1);

        UNIT_ASSERT_EQUAL(f.GetLength(), len + 40);
        UNIT_ASSERT(rb.PushBack("12345678"));
    }

    Y_UNIT_TEST(VisitAndPopBackShouldProduceSameResults)
    {
        for (int i = 0; i <= 32; i++) {
            TStateWithCorruptedEntryLength s(i);
            TFileRingBuffer rb(s.FileHandle.GetName(), s.Len);

            TVector<TString> afterVisit;
            rb.Visit([&] (ui32 checksum, TStringBuf entry)
            {
                Y_UNUSED(checksum);
                afterVisit.push_back(TString(entry));
            });

            TVector<TString> afterPopBack;
            while (!rb.Empty())
            {
                afterPopBack.push_back(TString(rb.Front()));
                rb.PopFront();
            }

            UNIT_ASSERT_EQUAL(Dump(afterVisit), Dump(afterPopBack));
        }
    }

    Y_UNIT_TEST(ShouldReportRawCapacity)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 42;
        TFileRingBuffer rb(f.GetName(), len);
        UNIT_ASSERT_EQUAL(len, rb.GetRawCapacity());
    }

    Y_UNIT_TEST(ShouldReportRawUsedBytesCount)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 36;
        TFileRingBuffer rb(f.GetName(), len);

        UNIT_ASSERT_EQUAL(0, rb.GetRawUsedBytesCount());
        UNIT_ASSERT(rb.PushBack("abcd"));   // 12 bytes
        UNIT_ASSERT_EQUAL(12, rb.GetRawUsedBytesCount());
        UNIT_ASSERT(rb.PushBack("efgh"));   // 12 bytes
        UNIT_ASSERT_EQUAL(24, rb.GetRawUsedBytesCount());
        UNIT_ASSERT(rb.PushBack("ijkl"));   // 12 bytes
        UNIT_ASSERT_EQUAL(36, rb.GetRawUsedBytesCount());
        rb.PopFront();
        UNIT_ASSERT_EQUAL(24, rb.GetRawUsedBytesCount());
        rb.PopFront();
        UNIT_ASSERT_EQUAL(12, rb.GetRawUsedBytesCount());
        rb.PopFront();
        UNIT_ASSERT_EQUAL(0, rb.GetRawUsedBytesCount());
    }

    Y_UNIT_TEST(ShouldReportMaxAllocationBytesCount)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 36;
        TFileRingBuffer rb(f.GetName(), len);

        // 36 - header size (8)
        UNIT_ASSERT_EQUAL(28, rb.GetMaxAllocationBytesCount());
        UNIT_ASSERT(rb.PushBack("abcd"));
        UNIT_ASSERT_EQUAL(16, rb.GetMaxAllocationBytesCount());
        UNIT_ASSERT(rb.PushBack("efgh"));
        UNIT_ASSERT_EQUAL(4, rb.GetMaxAllocationBytesCount());
        UNIT_ASSERT(rb.PushBack("ijkl"));
        UNIT_ASSERT_EQUAL(0, rb.GetMaxAllocationBytesCount());
        rb.PopFront();
        UNIT_ASSERT_EQUAL(3, rb.GetMaxAllocationBytesCount());
        rb.PopFront();
        UNIT_ASSERT_EQUAL(15, rb.GetMaxAllocationBytesCount());
        rb.PopFront();
        UNIT_ASSERT_EQUAL(28, rb.GetMaxAllocationBytesCount());
    }
}

}   // namespace NCloud
