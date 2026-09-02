#include "file_ring_buffer.h"
#include "file_ring_buffer_accessor.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/digest/crc32c/crc32c.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/deque.h>
#include <util/generic/size_literals.h>
#include <util/random/random.h>
#include <util/system/filemap.h>
#include <util/system/tempfile.h>

namespace NCloud {

using EVersion = EFileRingBufferVersion;

namespace {

////////////////////////////////////////////////////////////////////////////////

#define FILE_RING_BUFFER_TEST(name)                                            \
    void TestImpl##name(EVersion ver);                                         \
    Y_UNIT_TEST(name##V5)                                                      \
    {                                                                          \
        TestImpl##name(EVersion::V5);                                          \
    }                                                                          \
    Y_UNIT_TEST(name##V6)                                                      \
    {                                                                          \
        TestImpl##name(EVersion::V6);                                          \
    }                                                                          \
    void TestImpl##name(EVersion ver)                                          \
// FILE_RING_BUFFER_TEST

////////////////////////////////////////////////////////////////////////////////

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

TString Dump(TFileRingBuffer& rb)
{
    TVector<TString> entries;

    auto error = rb.Visit([&](ui32, ui32, TStringBuf entry)
                          { entries.push_back(TString(entry)); });

    UNIT_ASSERT(!HasError(error));

    return Dump(entries);
}

TString Dump(const TTempFileHandle& fh)
{
    TFileMap m(fh.GetName(), TMemoryMapCommon::oRdWr);
    m.Map(0, m.Length());
    TString res(m.Length(), 0);
    MemCopy(res.begin(), static_cast<const char*>(m.Ptr()), m.Length());
    return res;
}

TStringBuf Find(TFileRingBuffer& rb, TStringBuf entry)
{
    TStringBuf result;

    auto error = rb.Visit(
        [&](ui32, ui32, TStringBuf e)
        {
            if (e == entry) {
                result = e;
            }
        });

    UNIT_ASSERT(!HasError(error));

    return result;
}

TString PopAll(TFileRingBuffer& rb)
{
    TStringBuilder sb;

    while (!rb.Empty()) {
        if (sb.size()) {
            sb << ", ";
        }

        sb << rb.Front().Data;
        UNIT_ASSERT(rb.PopFront().Removed);
    }

    return sb;
}

////////////////////////////////////////////////////////////////////////////////

struct TReferenceImplementation
{
    static constexpr ui32 EntryOverhead = 8;

    const ui32 MaxWeight;
    const EVersion Version;

    TDeque<TString> Q;
    ui32 ReadPos = 0;
    ui32 WritePos = 0;
    ui32 SlackSpace = 0;

    TReferenceImplementation(ui32 maxWeight, EVersion version)
        : MaxWeight(maxWeight)
        , Version(version)
    {}

    TFileRingBuffer::TPushBackResult PushBack(TStringBuf data)
    {
        if (data.empty() || data.size() > MaxWeight) {
            return TFileRingBuffer::TPushBackResult(MakeError(E_ARGUMENT));
        }

        ui32 sz = EntryOverhead + data.size();
        if (Version >= EVersion::V6) {
            sz = AlignUp(sz, static_cast<ui32>(sizeof(ui64)));
        }

        if (sz > MaxWeight) {
            return TFileRingBuffer::TPushBackResult(false);
        }

        if (!Empty()) {
            if (ReadPos < WritePos) {
                const auto avail = MaxWeight - WritePos;
                if (avail < sz) {
                    if (ReadPos <= sz) {
                        // out of space
                        return TFileRingBuffer::TPushBackResult(false);
                    }

                    SlackSpace = avail;
                    WritePos = 0;
                }
            } else {
                const auto avail = ReadPos - WritePos;
                if (avail <= sz) {
                    // out of space
                    return TFileRingBuffer::TPushBackResult(false);
                }
            }
        }

        WritePos += sz;
        Q.emplace_back(data);
        return TFileRingBuffer::TPushBackResult(true);
    }

    TFileRingBuffer::TFrontResult Front() const
    {
        if (!Q) {
            return TFileRingBuffer::TFrontResult(TStringBuf{});
        }

        return TFileRingBuffer::TFrontResult(TStringBuf(Q.front()));
    }

    TFileRingBuffer::TPopFrontResult PopFront()
    {
        if (!Q) {
            return TFileRingBuffer::TPopFrontResult(false);
        }

        ui32 sz = Q.front().size() + EntryOverhead;
        if (Version >= EVersion::V6) {
            sz = AlignUp(sz, static_cast<ui32>(sizeof(ui64)));
        }

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

        if (Q.empty()) {
            ReadPos = 0;
            WritePos = 0;
        }

        return TFileRingBuffer::TPopFrontResult(true);
    }

    bool Empty() const
    {
        return Q.empty();
    }

    ui32 Size() const
    {
        return Q.size();
    }

    bool Validate() const
    {
        return true;
    }
};

TString Dump(const TReferenceImplementation& ri)
{
    TStringBuilder sb;
    for (const auto& entry: ri.Q) {
        if (!sb.empty()) {
            sb << ", ";
        }
        sb << entry;
    }
    return sb;
}

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

        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            rb.PushBack(GenerateData(rb.Size())).Error.GetCode());  // too big

        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            rb.PushBack("").Error.GetCode());   // empty

        UNIT_ASSERT(rb.PushBack("vasya").Pushed);
        UNIT_ASSERT(rb.PushBack("petya").Pushed);
        UNIT_ASSERT(rb.PushBack("vasya2").Pushed);
        UNIT_ASSERT(rb.PushBack("petya2").Pushed);
        UNIT_ASSERT(!rb.PushBack("vasya3").Pushed); // out of space

        UNIT_ASSERT(rb.Validate());
        UNIT_ASSERT_VALUES_EQUAL(4, rb.Size());
        UNIT_ASSERT_VALUES_EQUAL("vasya", rb.Front().Data);
        UNIT_ASSERT(rb.PopFront().Removed);

        UNIT_ASSERT(rb.Validate());
        UNIT_ASSERT_VALUES_EQUAL(3, rb.Size());
        UNIT_ASSERT(!rb.PushBack("vasya3").Pushed);

        UNIT_ASSERT_VALUES_EQUAL("petya", rb.Front().Data);
        UNIT_ASSERT(rb.PopFront().Removed);

        UNIT_ASSERT(rb.Validate());
        UNIT_ASSERT_VALUES_EQUAL(2, rb.Size());
        UNIT_ASSERT(rb.PushBack("vasya3").Pushed);

        UNIT_ASSERT(rb.Validate());
        UNIT_ASSERT_VALUES_EQUAL(3, rb.Size());
        UNIT_ASSERT_VALUES_EQUAL("vasya2", rb.Front().Data);
        UNIT_ASSERT(rb.PopFront().Removed);

        UNIT_ASSERT(rb.Validate());
        UNIT_ASSERT_VALUES_EQUAL(2, rb.Size());
        UNIT_ASSERT_VALUES_EQUAL("petya2", rb.Front().Data);
        UNIT_ASSERT(rb.PopFront().Removed);

        UNIT_ASSERT(rb.Validate());
        UNIT_ASSERT_VALUES_EQUAL(1, rb.Size());
        UNIT_ASSERT_VALUES_EQUAL("vasya3", rb.Front().Data);
        UNIT_ASSERT(rb.PopFront().Removed);

        UNIT_ASSERT(rb.Validate());
        UNIT_ASSERT_VALUES_EQUAL(0, rb.Size());
        UNIT_ASSERT(rb.Empty());
    }

    FILE_RING_BUFFER_TEST(ShouldPushPop)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 64;
        TFileRingBuffer rb(f.GetName(), len, 0, ver);

        DoTestShouldPushPop(rb);
    }

    FILE_RING_BUFFER_TEST(ShouldPushPopReferenceImplementation)
    {
        const ui32 len = 64;
        TReferenceImplementation rb(len, ver);

        DoTestShouldPushPop(rb);
    }

    FILE_RING_BUFFER_TEST(ShouldRestore)
    {
        const auto f = TTempFileHandle();
        const ui32 len = ver >= EVersion::V6 ? 80 : 64;
        auto rb = std::make_unique<TFileRingBuffer>(f.GetName(), len, 0, ver);

        UNIT_ASSERT(rb->PushBack("vasya").Pushed);
        UNIT_ASSERT(rb->PushBack("petya").Pushed);
        UNIT_ASSERT(rb->PushBack("vasya2").Pushed);
        UNIT_ASSERT(rb->PushBack("petya2").Pushed);
        UNIT_ASSERT(rb->PopFront().Removed);
        UNIT_ASSERT(rb->PopFront().Removed);
        UNIT_ASSERT(rb->PushBack("vasya3").Pushed);
        UNIT_ASSERT(rb->PushBack("xxx").Pushed);

        rb = std::make_unique<TFileRingBuffer>(f.GetName(), len, 0, ver);

        UNIT_ASSERT(rb->Validate());
        UNIT_ASSERT_VALUES_EQUAL(4, rb->Size());

        UNIT_ASSERT_VALUES_EQUAL("vasya2, petya2, vasya3, xxx", PopAll(*rb));
    }

    FILE_RING_BUFFER_TEST(ShouldValidate)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 128;
        TFileRingBuffer rb(f.GetName(), len, 0, ver);

        UNIT_ASSERT(rb.PushBack("vasya").Pushed);
        UNIT_ASSERT(rb.PushBack("petya").Pushed);
        UNIT_ASSERT(rb.PushBack("vasya2").Pushed);
        UNIT_ASSERT(rb.PushBack("petya2").Pushed);

        UNIT_ASSERT(rb.Validate());
        TFileMap m(f.GetName(), TMemoryMapCommon::oRdWr);
        m.Map(0, len);
        char* data = static_cast<char*>(m.Ptr());
        data[260] = 'A';

        UNIT_ASSERT(!rb.Validate());
    }

    Y_UNIT_TEST(ShouldIgnoreSlackSpaceSmallerThanEntryHeader)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 64;
        // In V6, having slack space smaller than entry header is not
        // possible due to 8-byte alignment
        TFileRingBuffer rb(f.GetName(), len, 0, EVersion::V5);

        const ui32 entryHeaderSize = 8;
        const ui32 entryLen = 29;
        const ui32 entryDataLen = entryLen - entryHeaderSize;
        const TString data(entryDataLen + 1, 'a');
        const TString data2(entryDataLen, 'b');
        const TString data3(entryDataLen, 'c');

        UNIT_ASSERT(rb.PushBack(data).Pushed);
        UNIT_ASSERT(rb.PushBack(data2).Pushed);
        UNIT_ASSERT(!rb.PushBack(data3).Pushed);
        UNIT_ASSERT(rb.PopFront().Removed);
        UNIT_ASSERT(rb.PushBack(data3).Pushed);

        /*
         * Buffer data:
         *  hhhhhhhhccccccccccccccccccccc0hhhhhhhhbbbbbbbbbbbbbbbbbbbbb00000
         */

        UNIT_ASSERT(rb.Validate());
    }

    void DoRandomizedPushPopRestore(
        ui32 len,
        ui32 testBytes,
        ui32 testUpToEntrySize,
        ui32 metadataSize,
        EVersion version)
    {
        const auto f = TTempFileHandle();
        const double restoreProbability = 0.05;
        std::unique_ptr<TFileRingBuffer> rb;
        TReferenceImplementation ri(len, version);

        auto restore = [&]()
        {
            rb = std::make_unique<TFileRingBuffer>(
                f.GetName(),
                len,
                metadataSize,
                version);
        };

        restore();

        ui32 remainingBytes = testBytes;
        while (remainingBytes || !ri.Empty()) {
            const bool shouldPush = remainingBytes && RandomNumber<bool>();
            if (shouldPush) {
                const ui32 entrySize =
                    RandomNumber(Min(remainingBytes, testUpToEntrySize)) + 1;
                const auto data = GenerateData(entrySize);
                const auto maxAllocationSize = rb->GetAvailableByteCount();
                const bool pushed = ri.PushBack(data).Pushed;
                UNIT_ASSERT_VALUES_EQUAL(pushed, rb->PushBack(data).Pushed);
                if (pushed) {
                    UNIT_ASSERT_LE_C(
                        data.size(),
                        maxAllocationSize,
                        "Data size " << data.size()
                                     << " should be less or equal than "
                                        "GetMaxAllocationBytesCount "
                                     << maxAllocationSize
                                     << " for a successful PushBack");
                    UNIT_ASSERT_VALUES_EQUAL(Dump(ri), Dump(*rb));
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
                UNIT_ASSERT_VALUES_EQUAL(Dump(ri), Dump(*rb));
                // Cerr << "POP\t" << ri.Front() << Endl;
                UNIT_ASSERT_VALUES_EQUAL(
                    ri.PopFront().Removed,
                    rb->PopFront().Removed);
            }

            // Cerr << ri.Size() << " " << remainingBytes << Endl;

            if (RandomNumber<double>() < restoreProbability) {
                restore();
            }

            UNIT_ASSERT_VALUES_EQUAL(ri.Size(), rb->Size());
            UNIT_ASSERT_VALUES_EQUAL(ri.Empty(), rb->Empty());
            UNIT_ASSERT(rb->Validate());
            UNIT_ASSERT(!rb->IsCorrupted());
        }
    }

    FILE_RING_BUFFER_TEST(ShouldNotWriteBeyondBufferWhenEmpty)
    {
        const auto f = TTempFileHandle();
        auto ri = TReferenceImplementation(64, ver);
        auto rb = TFileRingBuffer(f.GetName(), 64, 0, ver);

        TString data(36, 'a');

        UNIT_ASSERT(rb.PushBack(data).Pushed);
        UNIT_ASSERT(ri.PushBack(data).Pushed);

        UNIT_ASSERT(rb.PopFront().Removed);
        UNIT_ASSERT(ri.PopFront().Removed);

        UNIT_ASSERT(rb.PushBack(data).Pushed);
        UNIT_ASSERT(ri.PushBack(data).Pushed);

        UNIT_ASSERT(!rb.PushBack(data).Pushed);
        UNIT_ASSERT(!ri.PushBack(data).Pushed);
    }

    FILE_RING_BUFFER_TEST(RandomizedPushPopRestore)
    {
        DoRandomizedPushPopRestore(1_MB, 16_MB, 5_KB, 0, ver);
    }

    FILE_RING_BUFFER_TEST(RandomizedPushPopRestoreSmall)
    {
        DoRandomizedPushPopRestore(4_KB, 1_MB, 16, 0, ver);
    }

    FILE_RING_BUFFER_TEST(RandomizedPushPopRestoreWithMetadata)
    {
        DoRandomizedPushPopRestore(1_MB, 16_MB, 5_KB, 1_KB, ver);
    }

    FILE_RING_BUFFER_TEST(RandomizedPushPopRestoreSmallWithMetadata)
    {
        DoRandomizedPushPopRestore(4_KB, 1_MB, 16, 4, ver);
    }

    FILE_RING_BUFFER_TEST(ShouldFullyUtilizeCapacity)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 64;
        TFileRingBuffer rb(f.GetName(), len, 0, ver);

        const ui32 entryHeaderSize = 8;
        const ui32 entryLen = 32;
        const ui32 entryDataLen = entryLen - entryHeaderSize;
        const TString data(entryDataLen, 'a');
        const TString data2(entryDataLen, 'b');
        const TString data3(entryDataLen, 'c');
        const TString data4(entryDataLen, 'd');

        UNIT_ASSERT(rb.PushBack(data).Pushed);
        UNIT_ASSERT(rb.PushBack(data2).Pushed);
        UNIT_ASSERT(!rb.PushBack(data3).Pushed);
        UNIT_ASSERT(rb.PopFront().Removed);
        UNIT_ASSERT(!rb.PushBack(data3).Pushed);
        UNIT_ASSERT(rb.PopFront().Removed);
        UNIT_ASSERT(rb.PushBack(data3).Pushed);
        UNIT_ASSERT(rb.PushBack(data4).Pushed);
    }

    Y_UNIT_TEST(ShouldNotAccessMemoryOutsideMappedBuffer)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 32;
        TFileRingBuffer rb(f.GetName(), len, 0, EVersion::V5);

        TFileMap m(f.GetName(), TMemoryMapCommon::oRdWr);
        m.Map(0, len + 40); // len + sizeof(THeader)
        char* data = static_cast<char*>(m.Ptr());
        data[len + 40] = 'A';

        UNIT_ASSERT(rb.PushBack("01234567").Pushed);
        UNIT_ASSERT(rb.PushBack("89abcde").Pushed);
        UNIT_ASSERT(rb.PopFront().Removed);
        UNIT_ASSERT(rb.PushBack("01").Pushed);
        UNIT_ASSERT(rb.PopFront().Removed);
        UNIT_ASSERT_VALUES_EQUAL("01", rb.Front().Data);
    }

    struct TStateWithCorruptedEntryLength
    {
        const TTempFileHandle FileHandle;
        const ui32 Len = 32;
        TFileRingBuffer RingBuffer;

        explicit TStateWithCorruptedEntryLength(int newLength)
            : RingBuffer(FileHandle.GetName(), Len, 0, EVersion::V5)
        {
            UNIT_ASSERT(RingBuffer.PushBack("aaa").Pushed);
            UNIT_ASSERT(RingBuffer.PushBack("bb").Pushed);

            TFileMap m(FileHandle.GetName(), TMemoryMapCommon::oRdWr);
            m.Map(0, 256 + Len);
            char* data = static_cast<char*>(m.Ptr());
            UNIT_ASSERT_VALUES_EQUAL(2, data[267]);
            data[267] = newLength;
        }
    };

    Y_UNIT_TEST(ShouldSetIsCorruptedFlagWhenEntryLengthIsAltered)
    {
        for (int i = 0; i <= 32; i++) {
            TStateWithCorruptedEntryLength s(i);
            TFileRingBuffer rb(s.FileHandle.GetName(), s.Len, 0, EVersion::V5);
            UNIT_ASSERT_VALUES_EQUAL(i != 2, rb.IsCorrupted());
        }
    }

    Y_UNIT_TEST(ShouldSetIsCorruptedFlagInVisitWhenEntryLengthIsAltered)
    {
        for (int i = 0; i <= 32; i++) {
            TStateWithCorruptedEntryLength s(i);
            UNIT_ASSERT(!s.RingBuffer.IsCorrupted());
            UNIT_ASSERT_VALUES_EQUAL(
                i != 2,
                HasError(s.RingBuffer.Visit([](ui32, ui32, TStringBuf) {})));
            UNIT_ASSERT_VALUES_EQUAL(i != 2, s.RingBuffer.IsCorrupted());
        }
    }

    Y_UNIT_TEST(ShouldSetIsCorruptedFlagInValidateWhenEntryLengthIsAltered)
    {
        for (int i = 0; i <= 32; i++) {
            TStateWithCorruptedEntryLength s(i);
            UNIT_ASSERT(!s.RingBuffer.IsCorrupted());
            UNIT_ASSERT_VALUES_EQUAL(i == 2, s.RingBuffer.Validate());
            UNIT_ASSERT_VALUES_EQUAL(i != 2, s.RingBuffer.IsCorrupted());
        }
    }

    Y_UNIT_TEST(ShouldProhibitPushBackInCorruptedState)
    {
        TStateWithCorruptedEntryLength good(2);
        TFileRingBuffer rb(good.FileHandle.GetName(), good.Len, 0, EVersion::V5);
        UNIT_ASSERT(rb.PushBack("c").Pushed);

        TStateWithCorruptedEntryLength bad(1);
        TFileRingBuffer rb2(bad.FileHandle.GetName(), bad.Len, 0, EVersion::V5);
        UNIT_ASSERT(!rb2.PushBack("c").Pushed);
    }

    FILE_RING_BUFFER_TEST(ShouldNotFailOnCapacityChange)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 16;
        TFileRingBuffer rb(f.GetName(), len, 0, ver);
        TFileRingBuffer rb1(f.GetName(), len + 1, 0, ver);
        TFileRingBuffer rb2(f.GetName(), len - 1, 0, ver);

        UNIT_ASSERT_EQUAL(f.GetLength(), len + 256);
        UNIT_ASSERT(rb.PushBack("12345678").Pushed);
    }

    FILE_RING_BUFFER_TEST(ForbidModificationOfCorruptedBuffer)
    {
        TTempFileHandle f;
        TFileRingBuffer rb(f.GetName(), 36, 8, ver);

        auto ptr = rb.Alloc(3).AllocationPtr;
        UNIT_ASSERT(ptr != nullptr);
        ptr[0] = 'a';
        ptr[1] = 'b';
        ptr[2] = 'c';
        UNIT_ASSERT(!HasError(rb.Commit(ptr)));
        UNIT_ASSERT(rb.SetMetadata("123").Updated);

        rb.SetCorrupted();

        auto dump = Dump(f);

        UNIT_ASSERT(HasError(rb.Front().Error));
        UNIT_ASSERT_STRINGS_EQUAL(dump, Dump(f));

        int visitCount = 0;
        auto visitor = [&visitCount](ui32, ui32, TStringBuf) {
            visitCount++;
        };
        UNIT_ASSERT(HasError(rb.Visit(visitor)));
        UNIT_ASSERT_VALUES_EQUAL(0, visitCount);
        UNIT_ASSERT_STRINGS_EQUAL(dump, Dump(f));

        UNIT_ASSERT(HasError(rb.PopFront().Error));
        UNIT_ASSERT_STRINGS_EQUAL(dump, Dump(f));

        UNIT_ASSERT(HasError(rb.PushBack("1").Error));
        UNIT_ASSERT_STRINGS_EQUAL(dump, Dump(f));

        UNIT_ASSERT(HasError(rb.Alloc(1).Error));
        UNIT_ASSERT_STRINGS_EQUAL(dump, Dump(f));

        UNIT_ASSERT(HasError(rb.Free(ptr)));
        UNIT_ASSERT_STRINGS_EQUAL(dump, Dump(f));

        UNIT_ASSERT(HasError(rb.Commit(ptr)));
        UNIT_ASSERT_STRINGS_EQUAL(dump, Dump(f));

        UNIT_ASSERT(HasError(rb.GetTag(ptr).Error));
        UNIT_ASSERT_STRINGS_EQUAL(dump, Dump(f));

        UNIT_ASSERT(HasError(rb.SetTag(ptr, 1)));
        UNIT_ASSERT_STRINGS_EQUAL(dump, Dump(f));

        UNIT_ASSERT(HasError(rb.GetMetadata().Error));
        UNIT_ASSERT_STRINGS_EQUAL(dump, Dump(f));

        UNIT_ASSERT(HasError(rb.SetMetadata("x").Error));
        UNIT_ASSERT_STRINGS_EQUAL(dump, Dump(f));
    }

    struct TShouldDetectCorruptionOnPopFrontAndFreeBootstrap
    {
        TTempFileHandle TempFileHandle;
        TFileRingBuffer RingBuffer;
        char* frontAllocationPtr = nullptr;
        char* secondAllocationPtr = nullptr;

        TShouldDetectCorruptionOnPopFrontAndFreeBootstrap(EVersion ver)
            : RingBuffer(TempFileHandle.GetName(), 42, 0, ver)
        {
            frontAllocationPtr = RingBuffer.Alloc(3).AllocationPtr;
            UNIT_ASSERT(frontAllocationPtr != nullptr);
            frontAllocationPtr[0] = 'a';
            frontAllocationPtr[1] = 'b';
            frontAllocationPtr[2] = 'c';
            UNIT_ASSERT(!HasError(RingBuffer.Commit(frontAllocationPtr)));

            secondAllocationPtr = RingBuffer.Alloc(3).AllocationPtr;
            UNIT_ASSERT(secondAllocationPtr != nullptr);
            secondAllocationPtr[0] = 'd';
            secondAllocationPtr[1] = 'e';
            secondAllocationPtr[2] = 'f';
            UNIT_ASSERT(!HasError(RingBuffer.Commit(secondAllocationPtr)));
        }

        void Corrupt(ui64 ofs)
        {
            TFileMapFileRingBufferAccessor accessor(
                TempFileHandle.GetName(),
                EFileRingBufferAccessorValidationMode::Normal,
                TMemoryMapCommon::EOpenModeFlag::oRdWr);

            UNIT_ASSERT(!HasError(accessor.Map()));

            UNIT_ASSERT_VALUES_EQUAL(
                EFileRingBufferAccessorValidationStatus::Success,
                accessor.ValidateAndInitialize());

            auto eh = accessor.GetDataProcessor()->ReadEntryHeader(ofs);
            UNIT_ASSERT_VALUES_EQUAL(3, eh.DataSize);
            eh.DataSize = 1000;
            accessor.GetDataProcessor()->WriteEntryHeader(ofs, eh);
        }

        void CorruptFront()
        {
            Corrupt(0);
        }

        void CorruptSecond()
        {
            Corrupt(secondAllocationPtr - frontAllocationPtr);
        }
    };

    FILE_RING_BUFFER_TEST(ShouldDetectCorruptionOnPopFrontAndFree)
    {
        {
            TShouldDetectCorruptionOnPopFrontAndFreeBootstrap b(ver);
            b.CorruptFront();
            UNIT_ASSERT(HasError(b.RingBuffer.PopFront().Error));
        }

        {
            TShouldDetectCorruptionOnPopFrontAndFreeBootstrap b(ver);
            b.CorruptSecond();
            UNIT_ASSERT(HasError(b.RingBuffer.PopFront().Error));
        }

        {
            TShouldDetectCorruptionOnPopFrontAndFreeBootstrap b(ver);
            b.CorruptFront();
            UNIT_ASSERT(HasError(b.RingBuffer.Free(b.frontAllocationPtr)));
        }

        {
            TShouldDetectCorruptionOnPopFrontAndFreeBootstrap b(ver);
            b.CorruptSecond();
            UNIT_ASSERT(HasError(b.RingBuffer.Free(b.frontAllocationPtr)));
        }
    }

    FILE_RING_BUFFER_TEST(ShouldGetRawCapacity)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 42;
        TFileRingBuffer rb(f.GetName(), len, 0, ver);
        UNIT_ASSERT_EQUAL(len, rb.GetRawCapacity());
    }

    FILE_RING_BUFFER_TEST(ShouldGetRawUsedBytesCount)
    {
        const ui64 q = ver >= EVersion::V6 ? 16 : 12;

        const auto f = TTempFileHandle();
        const ui32 len = q * 3;
        TFileRingBuffer rb(f.GetName(), len, 0, ver);

        UNIT_ASSERT_VALUES_EQUAL(0, rb.GetRawUsedBytesCount());
        UNIT_ASSERT(rb.PushBack("abcd").Pushed);    // q bytes
        UNIT_ASSERT_VALUES_EQUAL(q, rb.GetRawUsedBytesCount());
        UNIT_ASSERT(rb.PushBack("efgh").Pushed);    // q bytes
        UNIT_ASSERT_VALUES_EQUAL(q * 2, rb.GetRawUsedBytesCount());
        UNIT_ASSERT(rb.PushBack("ijkl").Pushed);    // q bytes
        UNIT_ASSERT_VALUES_EQUAL(q * 3, rb.GetRawUsedBytesCount());
        UNIT_ASSERT(rb.PopFront().Removed);
        UNIT_ASSERT_VALUES_EQUAL(q * 2, rb.GetRawUsedBytesCount());
        UNIT_ASSERT(rb.PopFront().Removed);
        UNIT_ASSERT_VALUES_EQUAL(q, rb.GetRawUsedBytesCount());
        UNIT_ASSERT(rb.PopFront().Removed);
        UNIT_ASSERT_VALUES_EQUAL(0, rb.GetRawUsedBytesCount());
    }

    FILE_RING_BUFFER_TEST(ShouldGetMaxAllocationBytesCount)
    {
        // Adjustment due to different minimal slack space size
        const ui64 adj = ver >= EVersion::V6 ? 0 : 7;

        const auto f = TTempFileHandle();
        const ui32 len = 56;
        TFileRingBuffer rb(f.GetName(), len, 0, ver);

        // 56 - header size (8)
        UNIT_ASSERT_VALUES_EQUAL(48, rb.GetAvailableByteCount());
        UNIT_ASSERT(rb.PushBack("0123456789abcdef").Pushed);
        UNIT_ASSERT_VALUES_EQUAL(24, rb.GetAvailableByteCount());
        UNIT_ASSERT(rb.PushBack("ABCDEFGH").Pushed);
        UNIT_ASSERT_VALUES_EQUAL(8, rb.GetAvailableByteCount());
        UNIT_ASSERT(rb.PushBack("IJKLMNOP").Pushed);
        UNIT_ASSERT_VALUES_EQUAL(0, rb.GetAvailableByteCount());
        UNIT_ASSERT(rb.PopFront().Removed);
        UNIT_ASSERT_VALUES_EQUAL(8 + adj, rb.GetAvailableByteCount());
        UNIT_ASSERT(rb.PopFront().Removed);
        UNIT_ASSERT_VALUES_EQUAL(24 + adj, rb.GetAvailableByteCount());
        UNIT_ASSERT(rb.PopFront().Removed);
        UNIT_ASSERT_VALUES_EQUAL(48, rb.GetAvailableByteCount());
    }

    FILE_RING_BUFFER_TEST(ShouldGetVersion)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 36;
        TFileRingBuffer rb(f.GetName(), len, 0, ver);

        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(ver), rb.GetVersion());
    }

    FILE_RING_BUFFER_TEST(ShouldGetMaxObservedEntryByteCount)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 48;
        auto rb = std::make_unique<TFileRingBuffer>(f.GetName(), len, 0, ver);

        UNIT_ASSERT_VALUES_EQUAL(0, rb->GetMaxObservedEntryByteCount());
        UNIT_ASSERT(rb->PushBack("abcd").Pushed);
        UNIT_ASSERT_VALUES_EQUAL(4, rb->GetMaxObservedEntryByteCount());
        UNIT_ASSERT(rb->PushBack("ef").Pushed);
        UNIT_ASSERT_VALUES_EQUAL(4, rb->GetMaxObservedEntryByteCount());
        UNIT_ASSERT(rb->PushBack("ghijk").Pushed);
        UNIT_ASSERT_VALUES_EQUAL(5, rb->GetMaxObservedEntryByteCount());
        UNIT_ASSERT(!rb->PushBack("1234567890").Pushed);
        UNIT_ASSERT_VALUES_EQUAL(5, rb->GetMaxObservedEntryByteCount());

        rb = std::make_unique<TFileRingBuffer>(f.GetName(), len, 0, ver);
        UNIT_ASSERT_VALUES_EQUAL(5, rb->GetMaxObservedEntryByteCount());
    }

    FILE_RING_BUFFER_TEST(ShouldGetAndSetMetadata_ZeroMetadataCapacity)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 36;

        auto rb = std::make_unique<TFileRingBuffer>(f.GetName(), len, 0, ver);
        UNIT_ASSERT(rb->PushBack("AAA").Pushed);
        UNIT_ASSERT_VALUES_EQUAL("", rb->GetMetadata().Metadata);
        UNIT_ASSERT(!rb->SetMetadata("1").Updated);
        UNIT_ASSERT(rb->SetMetadata("").Updated);

        rb = std::make_unique<TFileRingBuffer>(f.GetName(), len, 0, ver);
        UNIT_ASSERT(!rb->SetMetadata("1").Updated);
        UNIT_ASSERT_VALUES_EQUAL("", rb->GetMetadata().Metadata);
        UNIT_ASSERT_VALUES_EQUAL("AAA", rb->Front().Data);
    }

    FILE_RING_BUFFER_TEST(ShouldGetAndSetMetadata_NonZeroMetadataCapacity)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 36;

        auto rb = std::make_unique<TFileRingBuffer>(f.GetName(), len, 8, ver);
        UNIT_ASSERT(rb->PushBack("AAA").Pushed);
        UNIT_ASSERT_VALUES_EQUAL("", rb->GetMetadata().Metadata);
        UNIT_ASSERT(rb->SetMetadata("1234").Updated);
        UNIT_ASSERT_VALUES_EQUAL("1234", rb->GetMetadata().Metadata);
        UNIT_ASSERT(!rb->SetMetadata("abcdefghij").Updated);
        UNIT_ASSERT_VALUES_EQUAL("1234", rb->GetMetadata().Metadata);
        UNIT_ASSERT(rb->SetMetadata("abc").Updated);
        UNIT_ASSERT_VALUES_EQUAL("abc", rb->GetMetadata().Metadata);

        rb = std::make_unique<TFileRingBuffer>(f.GetName(), len, 8, ver);
        UNIT_ASSERT_VALUES_EQUAL("abc", rb->GetMetadata().Metadata);
        UNIT_ASSERT(rb->SetMetadata("").Updated);
        UNIT_ASSERT_VALUES_EQUAL("", rb->GetMetadata().Metadata);
        UNIT_ASSERT_VALUES_EQUAL("AAA", rb->Front().Data);
    }

    FILE_RING_BUFFER_TEST(ShouldValidateMetadataWithCorruptedData)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 36;

        auto rb = std::make_unique<TFileRingBuffer>(f.GetName(), len, 8, ver);
        UNIT_ASSERT(rb->SetMetadata("1234").Updated);

        {
            // Corrupt metadata contents
            TFileMap m(f.GetName(), TMemoryMapCommon::oRdWr);
            m.Map(0, 256);
            char* data = static_cast<char*>(m.Ptr());
            data[68] ^= 1;
        }

        UNIT_ASSERT(!rb->Validate());

        rb = std::make_unique<TFileRingBuffer>(f.GetName(), len, 8, ver);
        UNIT_ASSERT(rb->IsCorrupted());
        UNIT_ASSERT(HasError(rb->GetMetadata().Error));
        UNIT_ASSERT(HasError(rb->SetMetadata("1234").Error));
    }

    FILE_RING_BUFFER_TEST(ShouldValidateMetadataWithCorruptedSize)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 36;

        auto rb = std::make_unique<TFileRingBuffer>(f.GetName(), len, 8, ver);
        UNIT_ASSERT(rb->SetMetadata("1234").Updated);

        {
            // Corrupt metadata length (set length > capacity)
            // Header validation will fail - setting metadata will not be
            // possible
            TFileMap m(f.GetName(), TMemoryMapCommon::oRdWr);
            m.Map(0, 256);
            char* data = static_cast<char*>(m.Ptr());
            data[64] = 100;
        }

        UNIT_ASSERT(!rb->Validate());

        rb = std::make_unique<TFileRingBuffer>(f.GetName(), len, 8, ver);
        UNIT_ASSERT(rb->IsCorrupted());
        UNIT_ASSERT(HasError(rb->GetMetadata().Error));
        UNIT_ASSERT(HasError(rb->SetMetadata("1234").Error));
    }

    FILE_RING_BUFFER_TEST(ShouldResizeMetadata)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 19;

        auto rb = std::make_unique<TFileRingBuffer>(f.GetName(), len, 1, ver);
        UNIT_ASSERT(rb->PushBack("ABCD").Pushed);
        UNIT_ASSERT(rb->SetMetadata("1").Updated);
        UNIT_ASSERT(!rb->SetMetadata("12").Updated);

        rb = std::make_unique<TFileRingBuffer>(f.GetName(), len, 4, ver);
        UNIT_ASSERT_STRINGS_EQUAL("ABCD", rb->Front().Data);
        UNIT_ASSERT_STRINGS_EQUAL("1", rb->GetMetadata().Metadata);
        UNIT_ASSERT(rb->SetMetadata("123").Updated);
        UNIT_ASSERT(!rb->SetMetadata("12345").Updated);

        rb = std::make_unique<TFileRingBuffer>(f.GetName(), len, 16, ver);
        UNIT_ASSERT_STRINGS_EQUAL("ABCD", rb->Front().Data);
        UNIT_ASSERT_STRINGS_EQUAL("123", rb->GetMetadata().Metadata);
        UNIT_ASSERT(rb->SetMetadata("123456789").Updated);
        UNIT_ASSERT(!rb->SetMetadata("1234567890abcdef!").Updated);

        rb = std::make_unique<TFileRingBuffer>(f.GetName(), len, 17, ver);
        UNIT_ASSERT_STRINGS_EQUAL("ABCD", rb->Front().Data);
        UNIT_ASSERT_STRINGS_EQUAL("123456789", rb->GetMetadata().Metadata);
        UNIT_ASSERT(!rb->SetMetadata("1234567890abcdefg!").Updated);

        rb = std::make_unique<TFileRingBuffer>(f.GetName(), len, 100, ver);
        UNIT_ASSERT_STRINGS_EQUAL("ABCD", rb->Front().Data);
        UNIT_ASSERT_STRINGS_EQUAL("123456789", rb->GetMetadata().Metadata);

        rb = std::make_unique<TFileRingBuffer>(f.GetName(), len, 10, ver);
        UNIT_ASSERT_STRINGS_EQUAL("ABCD", rb->Front().Data);
        UNIT_ASSERT_STRINGS_EQUAL("123456789", rb->GetMetadata().Metadata);
        UNIT_ASSERT(!rb->SetMetadata("1234567890!").Updated);

        // Cannot shrink metadata below current size
        // New metadata capacity = 9
        rb = std::make_unique<TFileRingBuffer>(f.GetName(), len, 8, ver);
        UNIT_ASSERT_STRINGS_EQUAL("ABCD", rb->Front().Data);
        UNIT_ASSERT_STRINGS_EQUAL("123456789", rb->GetMetadata().Metadata);
        UNIT_ASSERT(rb->SetMetadata("abcdefghi").Updated);
    }

    FILE_RING_BUFFER_TEST(ShouldResumeAbortedMetadataResize)
    {
        const auto f = TTempFileHandle();
        // entry header (8) + max entry data (4 or 8 depending on alignment)
        const ui32 len = ver >= EVersion::V6 ? 16 : 12;

        TString initial;
        TString resized;

        // Collect expected file contents
        {
            TFileRingBuffer rb(f.GetName(), len, 1, ver);
            UNIT_ASSERT(rb.PushBack("ABCD").Pushed);
            UNIT_ASSERT(rb.SetMetadata("1").Updated);
        }
        {
            TFileMap m(f.GetName(), TMemoryMapCommon::oRdWr);
            m.Map(0, m.Length());
            initial = TString(static_cast<char*>(m.Ptr()), m.Length());
        }
        {
            TFileRingBuffer rb(f.GetName(), len, 100, ver);
        }
        {
            TFileMap m(f.GetName(), TMemoryMapCommon::oRdWr);
            m.Map(0, m.Length());
            resized = TString(static_cast<char*>(m.Ptr()), m.Length());
        }

        // Simulate interruption during metadata resize
        // 1. Abort during copy to the temporary location
        const auto f1 = TTempFileHandle();
        {
            TFileMap m(f1.GetName(), TMemoryMapCommon::oRdWr);
            m.ResizeAndRemap(0, resized.length() + len);
            auto* data = static_cast<char*>(m.Ptr());
            MemCopy(data, initial.data(), initial.length());
            const auto* entryData = initial.data() + initial.length() - len;
            MemCopy(data + resized.length(), entryData, 3);
        }
        {
            TFileRingBuffer rb(f1.GetName(), len, 100, ver);
        }
        {
            TFileMap m(f1.GetName(), TMemoryMapCommon::oRdWr);
            m.Map(0, m.Length());
            UNIT_ASSERT_STRINGS_EQUAL(
                resized,
                TString(static_cast<char*>(m.Ptr()), m.Length()));
        }

        // 2. Abort during copy back to the final location
        const auto f2 = TTempFileHandle();
        {
            TFileMap m(f2.GetName(), TMemoryMapCommon::oRdWr);
            auto dataOffset = AlignUp(resized.length(), sizeof(ui64));
            m.ResizeAndRemap(0, dataOffset + len);
            auto* data = static_cast<char*>(m.Ptr());
            MemCopy(data, initial.data(), initial.length());
            const auto* entryData = initial.data() + initial.length() - len;
            MemCopy(data + dataOffset, entryData, len);
            MemCopy(data + dataOffset - len, entryData, 3);
            *reinterpret_cast<ui64*>(data + 40) = dataOffset;
        }
        {
            TFileRingBuffer rb(f2.GetName(), len, 100, ver);
        }
        {
            TFileMap m(f2.GetName(), TMemoryMapCommon::oRdWr);
            m.Map(0, m.Length());
            UNIT_ASSERT_STRINGS_EQUAL(
                resized,
                TString(static_cast<char*>(m.Ptr()), m.Length()));
        }
    }

    FILE_RING_BUFFER_TEST(ShouldSupportInPlaceAllocation)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 64;
        TFileRingBuffer rb(f.GetName(), len, 0, ver);

        TString data1 = "vasya";
        TString data2 = "ivan";

        auto alloc1 = rb.Alloc(data1.size());
        UNIT_ASSERT(!HasError(alloc1.Error));
        UNIT_ASSERT(alloc1.AllocationPtr != nullptr);
        data1.copy(alloc1.AllocationPtr, data1.size());
        UNIT_ASSERT(!HasError(rb.Commit(alloc1.AllocationPtr)));

        auto alloc2 = rb.Alloc(data2.size());
        UNIT_ASSERT(!HasError(alloc2.Error));
        UNIT_ASSERT(alloc2.AllocationPtr != nullptr);
        data2.copy(alloc2.AllocationPtr, data2.size());
        UNIT_ASSERT(!HasError(rb.Commit(alloc2.AllocationPtr)));

        UNIT_ASSERT_VALUES_EQUAL(data1, rb.Front().Data);
        UNIT_ASSERT(rb.PopFront().Removed);
        UNIT_ASSERT_VALUES_EQUAL(data2, rb.Front().Data);
        UNIT_ASSERT(rb.PopFront().Removed);
        UNIT_ASSERT_VALUES_EQUAL("", rb.Front().Data);
    }

    FILE_RING_BUFFER_TEST(ShouldNotAccessIncompleteAllocations)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 64;
        TFileRingBuffer rb(f.GetName(), len, 0, ver);

        TString data = "vasya";

        auto ptr = rb.Alloc(data.size()).AllocationPtr;
        UNIT_ASSERT(ptr != nullptr);
        data.copy(ptr, data.size());

        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, rb.Free(ptr).GetCode());
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, rb.GetTag(ptr).Error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, rb.SetTag(ptr, 0).GetCode());
    }

    FILE_RING_BUFFER_TEST(ShouldSupportMultipleIncompleteAllocations)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 128;
        TFileRingBuffer rb(f.GetName(), len, 0, ver);

        const TString data1 = "one";
        const TString data2 = "two";
        const TString data3 = "three";

        auto* ptr1 = rb.Alloc(data1.size()).AllocationPtr;
        auto* ptr2 = rb.Alloc(data2.size()).AllocationPtr;
        auto* ptr3 = rb.Alloc(data3.size()).AllocationPtr;

        UNIT_ASSERT(ptr1 != nullptr);
        UNIT_ASSERT(ptr2 != nullptr);
        UNIT_ASSERT(ptr3 != nullptr);

        data1.copy(ptr1, data1.size());
        data2.copy(ptr2, data2.size());
        data3.copy(ptr3, data3.size());

        UNIT_ASSERT_VALUES_EQUAL("", Dump(rb));

        // Allocations may be committed in a different order from allocation.
        UNIT_ASSERT(!HasError(rb.Commit(ptr2)));
        UNIT_ASSERT_VALUES_EQUAL("two", Dump(rb));

        UNIT_ASSERT(!HasError(rb.Commit(ptr1)));
        UNIT_ASSERT_VALUES_EQUAL("one, two", Dump(rb));

        // Removing committed entries must not discard an incomplete entry at
        // the front of the remaining allocation range.
        UNIT_ASSERT(!HasError(rb.Free(ptr1)));
        UNIT_ASSERT(!HasError(rb.Free(ptr2)));
        UNIT_ASSERT_VALUES_EQUAL("", Dump(rb));

        UNIT_ASSERT(!HasError(rb.Commit(ptr3)));
        UNIT_ASSERT_VALUES_EQUAL("three", Dump(rb));
        UNIT_ASSERT_VALUES_EQUAL(data3, rb.Front().Data);
    }

    FILE_RING_BUFFER_TEST(ShouldNotPopFrontIncompleteAllocation)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 128;
        TFileRingBuffer rb(f.GetName(), len, 0, ver);

        const TString data1 = "one";
        const TString data2 = "two";

        auto* ptr1 = rb.Alloc(data1.size()).AllocationPtr;
        auto* ptr2 = rb.Alloc(data2.size()).AllocationPtr;

        UNIT_ASSERT(ptr1 != nullptr);
        UNIT_ASSERT(ptr2 != nullptr);

        data1.copy(ptr1, data1.size());
        data2.copy(ptr2, data2.size());

        UNIT_ASSERT(!HasError(rb.Commit(ptr2)));

        UNIT_ASSERT_VALUES_EQUAL("two", Dump(rb));

        auto frontResult = rb.Front();
        UNIT_ASSERT(!HasError(frontResult.Error));
        UNIT_ASSERT(frontResult.Data.empty());

        auto popFrontResult = rb.PopFront();
        UNIT_ASSERT(!popFrontResult.Removed);
        UNIT_ASSERT(HasError(popFrontResult.Error));

        UNIT_ASSERT_VALUES_EQUAL("two", Dump(rb));

        UNIT_ASSERT(!HasError(rb.Commit(ptr1)));

        UNIT_ASSERT_VALUES_EQUAL("one, two", Dump(rb));
        UNIT_ASSERT_VALUES_EQUAL("one", rb.Front().Data);

        UNIT_ASSERT(rb.PopFront().Removed);

        UNIT_ASSERT_VALUES_EQUAL("two", Dump(rb));
        UNIT_ASSERT_VALUES_EQUAL("two", rb.Front().Data);
    }

    FILE_RING_BUFFER_TEST(ShouldCommitWithPrecomputedChecksum)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 64;
        const TString data = "precomputed checksum";
        const ui32 checksum = Crc32c(data.data(), data.size());
        const ui32 badChecksum = checksum ^ 1;

        {
            TFileRingBuffer rb(f.GetName(), len, 0, ver);
            auto alloc = rb.Alloc(data.size());

            UNIT_ASSERT(!HasError(alloc.Error));
            UNIT_ASSERT(alloc.AllocationPtr != nullptr);
            data.copy(alloc.AllocationPtr, data.size());
            UNIT_ASSERT(!HasError(rb.Commit(alloc.AllocationPtr, checksum)));

            ui32 visitedChecksum = 0;
            UNIT_ASSERT(!HasError(rb.Visit(
                [&](ui32 crc32, ui32, TStringBuf entry)
                {
                    visitedChecksum = crc32;
                    UNIT_ASSERT_VALUES_EQUAL(data, entry);
                })));
            UNIT_ASSERT_VALUES_EQUAL(checksum, visitedChecksum);
        }

        {
            // A caller-provided checksum must produce an entry that survives
            // validation when the ring buffer is reopened.
            TFileRingBuffer rb(f.GetName(), len, 0, ver);
            UNIT_ASSERT(!rb.IsCorrupted());
            UNIT_ASSERT_VALUES_EQUAL(data, rb.Front().Data);
        }

        // Bad checksum
        {
            TFileRingBuffer rb(f.GetName(), len, 0, ver);
            auto alloc = rb.Alloc(data.size());

            UNIT_ASSERT(!HasError(alloc.Error));
            UNIT_ASSERT(alloc.AllocationPtr != nullptr);
            data.copy(alloc.AllocationPtr, data.size());
            UNIT_ASSERT(!HasError(rb.Commit(alloc.AllocationPtr, badChecksum)));

            ui32 visitedChecksum = 0;
            UNIT_ASSERT(!HasError(rb.Visit(
                [&](ui32 crc32, ui32, TStringBuf entry)
                {
                    visitedChecksum = crc32;
                    UNIT_ASSERT_VALUES_EQUAL(data, entry);
                })));
            UNIT_ASSERT_VALUES_EQUAL(badChecksum, visitedChecksum);
        }

        {
            TFileRingBuffer rb(f.GetName(), len, 0, ver);
            UNIT_ASSERT(rb.IsCorrupted());
        }
    }

    FILE_RING_BUFFER_TEST(ShouldDropNotCommittedEntries)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 64;

        TString data1 = "vasya";
        TString data2 = "ivan";
        TString data3 = "peter";

        {
            TFileRingBuffer rb(f.GetName(), len, 0, ver);
            UNIT_ASSERT(rb.Empty());

            auto alloc1 = rb.Alloc(data1.size());
            UNIT_ASSERT_VALUES_EQUAL(1, rb.Size());
            UNIT_ASSERT(!rb.Empty());
            UNIT_ASSERT(!HasError(alloc1.Error));
            UNIT_ASSERT(alloc1.AllocationPtr != nullptr);
            data1.copy(alloc1.AllocationPtr, data1.size());

            auto alloc2 = rb.Alloc(data2.size());
            UNIT_ASSERT_VALUES_EQUAL(2, rb.Size());
            UNIT_ASSERT(!rb.Empty());
            UNIT_ASSERT(!HasError(alloc2.Error));
            UNIT_ASSERT(alloc2.AllocationPtr != nullptr);
            data2.copy(alloc2.AllocationPtr, data2.size());
            UNIT_ASSERT(!HasError(rb.Commit(alloc2.AllocationPtr)));
            UNIT_ASSERT_VALUES_EQUAL(2, rb.Size());

            auto alloc3 = rb.Alloc(data3.size());
            UNIT_ASSERT_VALUES_EQUAL(3, rb.Size());
            UNIT_ASSERT(!rb.Empty());
            UNIT_ASSERT(!HasError(alloc3.Error));
            UNIT_ASSERT(alloc3.AllocationPtr != nullptr);
            data3.copy(alloc3.AllocationPtr, data3.size());
        }

        {
            TFileRingBuffer rb(f.GetName(), len, 0, ver);
            UNIT_ASSERT_VALUES_EQUAL(1, rb.Size());
            UNIT_ASSERT(!rb.Empty());
            UNIT_ASSERT_VALUES_EQUAL(data2, rb.Front().Data);
            UNIT_ASSERT(rb.PopFront().Removed);

            UNIT_ASSERT_VALUES_EQUAL("", rb.Front().Data);

            UNIT_ASSERT(rb.Empty());
            auto alloc4 = rb.Alloc(data1.size());
            UNIT_ASSERT(!rb.Empty());
            UNIT_ASSERT(!HasError(alloc4.Error));
        }

        {
            TFileRingBuffer rb(f.GetName(), len, 0, ver);
            UNIT_ASSERT(rb.Empty());
        }
    }

    struct TRandomizedTestWithIncompleteAllocationsBootstrap
    {
        static constexpr ui64 BufferSize = 1024;
        static constexpr ui64 MaxAllocSize = 256;

        const EFileRingBufferVersion Version;

        TTempFileHandle FileHandle;
        std::unique_ptr<TFileRingBuffer> RingBuffer;

        struct TAllocation
        {
            TString Data;
            const char* Ptr = nullptr;
            bool Committed = false;
        };

        TVector<TAllocation> Allocations;
        size_t IncompleteAllocationCount = 0;

        TRandomizedTestWithIncompleteAllocationsBootstrap(
            EFileRingBufferVersion version)
            : Version(version)
        {
            Recreate();
        }

        void Recreate()
        {
            RingBuffer = std::make_unique<TFileRingBuffer>(
                FileHandle.GetName(),
                BufferSize,
                0,
                Version);

            UNIT_ASSERT(RingBuffer->Validate());
            UNIT_ASSERT(!RingBuffer->IsCorrupted());

            IncompleteAllocationCount = 0;

            EraseIf(
                Allocations,
                [](const TAllocation& alloc) { return !alloc.Committed; });

            size_t index = 0;

            auto visitResult = RingBuffer->Visit(
                [&](ui32 checksum, ui32 tag, TStringBuf entry)
                {
                    Y_UNUSED(checksum);
                    Y_UNUSED(tag);
                    UNIT_ASSERT(index < Allocations.size());
                    Allocations[index].Ptr = entry.data();
                    index++;
                });

            UNIT_ASSERT(!HasError(visitResult));
            UNIT_ASSERT_VALUES_EQUAL(Allocations.size(), index);

            Check();
        }

        void Alloc()
        {
            const auto size = RandomNumber(MaxAllocSize) + 1;
            auto data = GenerateData(static_cast<ui32>(size));
            auto allocation = RingBuffer->Alloc(size);
            UNIT_ASSERT(!HasError(allocation.Error));
            if (allocation.AllocationPtr) {
                data.copy(allocation.AllocationPtr, data.size());
                Allocations.push_back(
                    {std::move(data), allocation.AllocationPtr, false});
                ++IncompleteAllocationCount;
            }

            Check();
        }

        void Commit()
        {
            if (IncompleteAllocationCount == 0) {
                return;
            }

            const size_t index = GetRandomAllocation(false);
            const auto& allocation = Allocations[index];

            if (RandomNumber(2u) == 0) {
                const ui32 crc =
                    Crc32c(allocation.Data.data(), allocation.Data.size());
                UNIT_ASSERT(!HasError(RingBuffer->Commit(allocation.Ptr, crc)));
            } else {
                UNIT_ASSERT(!HasError(RingBuffer->Commit(allocation.Ptr)));
            }

            allocation.Committed = true;
            --IncompleteAllocationCount;

            Check();
        }

        void Free()
        {
            if (Allocations.size() == IncompleteAllocationCount) {
                return;
            }

            const size_t index = GetRandomAllocation(true);
            if (index != Allocations.size()) {
                UNIT_ASSERT(
                    !HasError(RingBuffer->Free(Allocations[index].Ptr)));
                Allocations.erase(Allocations.begin() + index);
            }

            Check();
        }

        size_t GetRandomAllocation(bool committed)
        {
            if (committed) {
                Y_ABORT_UNLESS(Allocations.size() > IncompleteAllocationCount);
            } else {
                Y_ABORT_UNLESS(IncompleteAllocationCount > 0);
            }

            size_t index = RandomNumber(Allocations.size());
            for (size_t i = 0; i < Allocations.size(); ++i) {
                if (Allocations[index].Committed == committed) {
                    return index;
                }
                index = (index + 1) % Allocations.size();
            }

            return index;
        }

        TString DumpReference()
        {
            TStringBuilder res;
            for (const auto& allocation: Allocations) {
                if (allocation.Committed) {
                    if (!res.empty()) {
                        res += ", ";
                    }
                    res += allocation.Data;
                }
            }
            return res;
        }

        void Check()
        {
            UNIT_ASSERT_VALUES_EQUAL(DumpReference(), Dump(*RingBuffer));
            UNIT_ASSERT_VALUES_EQUAL(Allocations.size(), RingBuffer->Size());
            UNIT_ASSERT_VALUES_EQUAL(Allocations.empty(), RingBuffer->Empty());
        }
    };

    FILE_RING_BUFFER_TEST(RandomizedTestWithIncompleteAllocations)
    {
        constexpr size_t numIterations = 10000;
        constexpr ui32 recreateProbability = 10;
        constexpr ui32 allocProbability = 30;
        constexpr ui32 commitProbability = 40;
        constexpr ui32 freeProbability = 20;

        TRandomizedTestWithIncompleteAllocationsBootstrap b(ver);

        for (size_t i = 0; i < numIterations; ++i) {
            const ui32 action = static_cast<ui32>(RandomNumber(
                recreateProbability + allocProbability + commitProbability +
                freeProbability));

            if (action < recreateProbability) {
                b.Recreate();
            } else if (action < recreateProbability + allocProbability) {
                b.Alloc();
            } else if (
                action <
                recreateProbability + allocProbability + commitProbability)
            {
                b.Commit();
            } else {
                b.Free();
            }
        }
    }

    FILE_RING_BUFFER_TEST(AllocShouldReturnExtendedStatus)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 64;
        TFileRingBuffer rb(f.GetName(), len, 0, ver);

        auto alloc1 = rb.Alloc(128);
        UNIT_ASSERT(alloc1.AllocationPtr == nullptr);
        UNIT_ASSERT(HasError(alloc1.Error));

        auto alloc2 = rb.Alloc(0);
        UNIT_ASSERT(alloc2.AllocationPtr == nullptr);
        UNIT_ASSERT(HasError(alloc2.Error));

        auto alloc3 = rb.Alloc(1);
        UNIT_ASSERT(alloc3.AllocationPtr != nullptr);
        UNIT_ASSERT(!HasError(alloc3.Error));
        UNIT_ASSERT(!HasError(rb.Commit(alloc3.AllocationPtr)));

        // Already committed
        UNIT_ASSERT(HasError(rb.Commit(alloc3.AllocationPtr)));

        rb.SetCorrupted();

        auto alloc4 = rb.Alloc(2);
        UNIT_ASSERT(alloc4.AllocationPtr == nullptr);
        UNIT_ASSERT(HasError(alloc4.Error));
    }

    FILE_RING_BUFFER_TEST(ShouldSupportRandomDeletion)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 64;
        auto rb = std::make_unique<TFileRingBuffer>(f.GetName(), len, 0, ver);

        TString data1 = "hello";
        TString data2 = "abc";
        TString data3 = "meow";
        TString data4 = "ok";

        auto alloc = [&](TString data)
        {
            auto res = rb->Alloc(data.size());
            UNIT_ASSERT(res.AllocationPtr != nullptr);
            data.copy(res.AllocationPtr, data.size());
            UNIT_ASSERT(!HasError(rb->Commit(res.AllocationPtr)));
            return res.AllocationPtr;
        };

        const char* alloc1 = alloc(data1);
        const char* alloc2 = alloc(data2);
        const char* alloc3 = alloc(data3);
        const char* alloc4 = alloc(data4);

        UNIT_ASSERT(!HasError(rb->Free(alloc2)));
        UNIT_ASSERT(HasError(rb->Free(alloc2)));
        UNIT_ASSERT(HasError(rb->Free(nullptr)));

        UNIT_ASSERT_VALUES_EQUAL("hello, meow, ok", Dump(*rb));
        UNIT_ASSERT_VALUES_EQUAL(3, rb->Size());

        // Recreate buffer - information about entry skip should be persisted
        rb = std::make_unique<TFileRingBuffer>(f.GetName(), len, 0, ver);

        UNIT_ASSERT_VALUES_EQUAL("hello, meow, ok", Dump(*rb));

        alloc3 = Find(*rb, "meow").data();
        UNIT_ASSERT(!HasError(rb->Free(alloc3)));

        UNIT_ASSERT_VALUES_EQUAL("hello, ok", Dump(*rb));
        UNIT_ASSERT_VALUES_EQUAL(2, rb->Size());

        rb = std::make_unique<TFileRingBuffer>(f.GetName(), len, 0, ver);

        alloc1 = Find(*rb, "hello").data();
        alloc4 = Find(*rb, "ok").data();

        UNIT_ASSERT(!HasError(rb->Free(alloc1)));

        UNIT_ASSERT_VALUES_EQUAL("ok", Dump(*rb));
        UNIT_ASSERT_VALUES_EQUAL(1, rb->Size());

        UNIT_ASSERT(!HasError(rb->Free(alloc4)));

        UNIT_ASSERT_VALUES_EQUAL("", Dump(*rb));
        UNIT_ASSERT_VALUES_EQUAL(0, rb->Size());

        rb = std::make_unique<TFileRingBuffer>(f.GetName(), len, 0, ver);

        UNIT_ASSERT_VALUES_EQUAL("", Dump(*rb));
        UNIT_ASSERT_VALUES_EQUAL(0, rb->Size());
    }

    FILE_RING_BUFFER_TEST(ShouldSupportFreeBetweenAllocAndCommit)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 36;
        auto rb = std::make_unique<TFileRingBuffer>(f.GetName(), len, 0, ver);

        const TString data = "efgh";

        UNIT_ASSERT(rb->PushBack("abcd").Pushed);   // 12 bytes

        auto alloc = rb->Alloc(4);
        UNIT_ASSERT(alloc.AllocationPtr != nullptr);
        data.copy(alloc.AllocationPtr, data.size());

        UNIT_ASSERT(rb->PopFront().Removed);

        UNIT_ASSERT(!HasError(rb->Commit(alloc.AllocationPtr)));

        UNIT_ASSERT_VALUES_EQUAL(data, rb->Front().Data);

        rb = std::make_unique<TFileRingBuffer>(f.GetName(), len, 0, ver);

        UNIT_ASSERT(!rb->IsCorrupted());
        UNIT_ASSERT_VALUES_EQUAL(data, rb->Front().Data);
    }

    Y_UNIT_TEST(ShouldSupportTags)
    {
        auto check = [](EVersion version)
        {
            const auto f = TTempFileHandle();
            const ui32 len = 36;

            auto rb =
                std::make_unique<TFileRingBuffer>(f.GetName(), len, 0, version);

            UNIT_ASSERT_VALUES_EQUAL(7, rb->GetMaxTag());

            const TString data1 = "abc";
            const TString data2 = "defg";

            auto* ptr1 = rb->Alloc(data1.size()).AllocationPtr;
            UNIT_ASSERT(ptr1 != nullptr);
            data1.copy(ptr1, data1.size());
            UNIT_ASSERT(!HasError(rb->Commit(ptr1)));

            auto* ptr2 = rb->Alloc(data2.size()).AllocationPtr;
            UNIT_ASSERT(ptr2 != nullptr);
            data2.copy(ptr2, data2.size());
            UNIT_ASSERT(!HasError(rb->Commit(ptr2)));

            UNIT_ASSERT_VALUES_EQUAL(0, rb->GetTag(ptr1).Tag);
            UNIT_ASSERT_VALUES_EQUAL(0, rb->GetTag(ptr2).Tag);

            UNIT_ASSERT(!HasError(rb->SetTag(ptr1, 1)));
            UNIT_ASSERT(!HasError(rb->SetTag(ptr2, 2)));

            // Tag value exceeds MaxTag
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, rb->SetTag(ptr1, 8).GetCode());

            // Invalid tag pointer
            UNIT_ASSERT_VALUES_EQUAL(
                E_ARGUMENT,
                rb->SetTag(ptr1 + 1, 0).GetCode());

            UNIT_ASSERT_VALUES_EQUAL(1, rb->GetTag(ptr1).Tag);
            UNIT_ASSERT_VALUES_EQUAL(2, rb->GetTag(ptr2).Tag);

            UNIT_ASSERT_VALUES_EQUAL(
                E_ARGUMENT,
                rb->GetTag(ptr1 + 1).Error.GetCode());

            // Recreate cache - old pointers become invalid
            rb =
                std::make_unique<TFileRingBuffer>(f.GetName(), len, 0, version);

            const auto* ptr3 = Find(*rb, TStringBuf(data1)).data();
            UNIT_ASSERT(ptr3 != nullptr);
            UNIT_ASSERT_VALUES_EQUAL(1, rb->GetTag(ptr3).Tag);

            const auto* ptr4 = Find(*rb, TStringBuf(data2)).data();
            UNIT_ASSERT(ptr4 != nullptr);
            UNIT_ASSERT_VALUES_EQUAL(2, rb->GetTag(ptr4).Tag);

            UNIT_ASSERT(rb->PopFront().Removed);
            UNIT_ASSERT(rb->PopFront().Removed);
            UNIT_ASSERT(rb->Empty());

            // Reuse entry
            auto* ptr5 = rb->Alloc(data1.size()).AllocationPtr;
            UNIT_ASSERT(ptr5 != nullptr);
            data1.copy(ptr5, data1.size());
            UNIT_ASSERT(!HasError(rb->Commit(ptr5)));

            UNIT_ASSERT_VALUES_EQUAL(0, rb->GetTag(ptr5).Tag);
        };

        check(EVersion::V5);
        check(EVersion::V6);
    }

    Y_UNIT_TEST(CheckAlignmentForVersionV6)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 36;

        TFileRingBuffer rb(f.GetName(), len, 0, EVersion::V6);

        auto ptr1 = rb.Alloc(2).AllocationPtr;
        UNIT_ASSERT(ptr1 != nullptr);
        ptr1[0] = 'a';
        ptr1[1] = 'b';
        UNIT_ASSERT(!HasError(rb.Commit(ptr1)));

        auto ptr2 = rb.Alloc(1).AllocationPtr;
        UNIT_ASSERT(ptr2 != nullptr);
        ptr2[0] = 'c';
        UNIT_ASSERT(!HasError(rb.Commit(ptr2)));

        UNIT_ASSERT(AlignDown(ptr1, sizeof(ui64)) == ptr1);
        UNIT_ASSERT(AlignDown(ptr2, sizeof(ui64)) == ptr2);
    }

    Y_UNIT_TEST(ShouldMigrate)
    {
        auto check = [](EVersion srcVersion,
                        EVersion dstVersion)
        {
            const auto f = TTempFileHandle();
            const ui32 len = 128;

            auto rb = std::make_unique<TFileRingBuffer>(
                f.GetName(),
                len,
                8,
                srcVersion);

            UNIT_ASSERT(rb->SetMetadata("abc").Updated);
            UNIT_ASSERT(rb->PushBack("123").Pushed);
            UNIT_ASSERT(rb->PushBack("4").Pushed);
            UNIT_ASSERT(rb->PushBack("xz").Pushed);

            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(srcVersion),
                rb->GetVersion());

            rb = std::make_unique<TFileRingBuffer>(
                f.GetName(),
                len,
                8,
                dstVersion);

            UNIT_ASSERT(rb->Validate());

            UNIT_ASSERT_VALUES_EQUAL("abc", rb->GetMetadata().Metadata);
            UNIT_ASSERT_VALUES_EQUAL("123, 4, xz", Dump(*rb));

            // New entries can be added only after the migration is completed
            UNIT_ASSERT(!rb->PushBack("!").Pushed);

            // Migration didn't happen - need to empty the buffer first
            UNIT_ASSERT_VALUES_EQUAL(0, rb->GetAvailableByteCount());

            UNIT_ASSERT(rb->PopFront().Removed);
            UNIT_ASSERT(rb->PopFront().Removed);
            UNIT_ASSERT(rb->PopFront().Removed);

            UNIT_ASSERT_LT(0, rb->GetAvailableByteCount());

            UNIT_ASSERT(rb->PushBack("!").Pushed);

            UNIT_ASSERT_VALUES_EQUAL("!", Dump(*rb));
            UNIT_ASSERT_VALUES_EQUAL("abc", rb->GetMetadata().Metadata);

            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(dstVersion),
                rb->GetVersion());

            UNIT_ASSERT(rb->Validate());
        };

        // Version upgrade
        check(EVersion::V5, EVersion::V6);

        // Version downgrade
        check(EVersion::V6, EVersion::V5);
    }

    Y_UNIT_TEST(ShouldValidateEmptyBufferWithUnalignedPos)
    {
        const auto f = TTempFileHandle();
        const ui32 len = 36;

        {
            TFileRingBuffer rb(f.GetName(), len, 0, EVersion::V5);
            UNIT_ASSERT(rb.PushBack("a").Pushed);
            UNIT_ASSERT(rb.PopFront().Removed);
        }

        {
            TFileMapFileRingBufferAccessor accessor(
                f.GetName(),
                EFileRingBufferAccessorValidationMode::Debug,
                TMemoryMapCommon::EOpenModeFlag::oRdWr);

            UNIT_ASSERT(!HasError(accessor.Map()));
            UNIT_ASSERT_VALUES_EQUAL(
                EFileRingBufferAccessorValidationStatus::Success,
                accessor.ValidateAndInitialize());

            auto* header = accessor.GetHeader();

            UNIT_ASSERT_VALUES_EQUAL(header->ReadPos, header->WritePos);
            UNIT_ASSERT_VALUES_UNEQUAL(0, header->ReadPos % sizeof(ui64));

            header->Version = EVersion::V6;
        }

        {
            TFileRingBuffer rb(f.GetName(), len, 0, EVersion::V6);
            UNIT_ASSERT(rb.PushBack("b").Pushed);
        }
    }
}

}   // namespace NCloud
