#include "file_ring_buffer.h"

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/generic/size_literals.h>
#include <util/stream/mem.h>
#include <util/system/compiler.h>
#include <util/system/filemap.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 VERSION = 3;
constexpr ui64 INVALID_POS = Max<ui64>();

////////////////////////////////////////////////////////////////////////////////

struct THeader
{
    ui32 Version = 0;
    ui64 Capacity = 0;
    ui64 ReadPos = 0;
    ui64 WritePos = 0;
    ui64 LastEntrySize = 0;
};

struct Y_PACKED TEntryHeader
{
private:
    ui32 DataSizeAndFlags = 0;
    ui32 Checksum = 0;

    static constexpr ui32 IncompleteFlag = 1U << 30U;
    static constexpr ui32 SkipFlag = 1U << 31U;
    static constexpr ui32 SizeMask = (1U << 30U) - 1;

public:
    ui32 GetChecksum() const
    {
        return Checksum;
    }

    void SetChecksum(ui32 checksum)
    {
        Checksum = checksum;
    }

    ui32 GetDataSize() const
    {
        return DataSizeAndFlags & SizeMask;
    }

    static ui64 GetMaxDataSize()
    {
        return SizeMask;
    }

    void InitSlackSpace()
    {
        DataSizeAndFlags = 0;
    }

    void InitIncompleteEntry(ui64 size)
    {
        auto maskedSize = static_cast<ui32>(size) & SizeMask;
        Y_ABORT_UNLESS(maskedSize == size);
        DataSizeAndFlags = maskedSize | IncompleteFlag;
    }

    bool HasIncompleteFlag() const
    {
        return (DataSizeAndFlags & IncompleteFlag) != 0;
    }

    void ClearIncompleteFlag()
    {
        DataSizeAndFlags &= ~IncompleteFlag;
    }

    bool HasSkipFlag() const
    {
        return (DataSizeAndFlags & SkipFlag) != 0;
    }

    void SetSkipFlag()
    {
        DataSizeAndFlags |= SkipFlag;
    }
};

//  Structure contract:
//
//  1. Empty buffer:
//    - ReadPos = WritePos = 0
//
//  2. Non-empty buffer:
//    - ReadPos != WritePos
//    - ReadPos points to the first byte of the first valid entry
//    - WritePos points to the first byte right after the last valid entry
//    - LastEntrySize is the size of the last valid entry
//
//  3. Valid entry:
//    - Size > 0, Flags: none
//    - The entry takes sizeof(TEntryHeader) + Size contiguous bytes in
//      the occupied part of the buffer
//
//  4. Occupied part of the buffer:
//     - ReadPos < WritePos: [ReadPos, WritePos)
//     - ReadPos > WritePos: [ReadPos, Capacity) + [0, WritePos)
//
//  5. Slack space entry marker:
//     - Size = 0
//     - Instructs to read the next entry at pos = 0
//     - Can appear only when ReadPos > WritePos in [ReadPos, Capacity) part
//
//  6. Implicit slack space marker:
//     - pos + sizeof(TEntryHeader) > Capacity
//
//  7. Incomplete entry:
//    - Size > 0, Flags: Incomplete
//    - It is not allowed to PopBack incomplete entries
//    - Incomplete entries become empty entries after restore
//
//  8. Empty entry:
//    - Size > 0, Flags: Skip

////////////////////////////////////////////////////////////////////////////////

class TEntriesData
{
private:
    char* Begin = nullptr;
    const char* End = nullptr;

    template <class T = char>
    T* GetPtr(ui64 pos, ui64 size = sizeof(T)) const
    {
        char* begin = Begin + pos;
        const char* end = begin + size;
        return Begin <= begin && begin <= end && end <= End
                   ? reinterpret_cast<T*>(begin)
                   : nullptr;
    }

public:
    TEntriesData() = default;

    TEntriesData(char* begin, const char* end)
    {
        Y_ABORT_UNLESS(begin != nullptr);
        Y_ABORT_UNLESS(end != nullptr);
        Y_ABORT_UNLESS(begin <= end);

        Begin = begin;
        End = end;
    }

    TEntryHeader* GetEntryHeader(ui64 pos) const
    {
        return GetPtr<TEntryHeader>(pos);
    }

    char* GetEntryData(const TEntryHeader* eh) const
    {
        Y_ABORT_UNLESS(eh != nullptr);
        Y_ABORT_UNLESS(eh->GetDataSize() != 0);

        const char* ptr = reinterpret_cast<const char*>(eh);
        ui64 pos = static_cast<ui64>(ptr - Begin) + sizeof(TEntryHeader);
        return GetPtr(pos, eh->GetDataSize());
    }

    TEntryHeader* GetEntryHeaderFromDataPtr(const char* ptr) const
    {
        ui64 pos = static_cast<ui64>(ptr - Begin) - sizeof(TEntryHeader);
        return GetEntryHeader(pos);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TEntryInfo
{
    ui64 ActualPos = 0;
    TEntryHeader* Header = nullptr;
    const char* Data = nullptr;

    bool HasValue() const
    {
        return Header != nullptr;
    }

    bool IsInvalid() const
    {
        return ActualPos == INVALID_POS;
    }

    TStringBuf GetData() const
    {
        return HasValue() ? TStringBuf(Data, Header->GetDataSize()) : TStringBuf();
    }

    ui64 GetEntrySize() const
    {
        return HasValue() ? sizeof(TEntryHeader) + Header->GetDataSize() : 0;
    }

    ui64 GetNextEntryPos() const
    {
        auto entrySize = GetEntrySize();
        return entrySize != 0 ? ActualPos + entrySize : INVALID_POS;
    }

    static TEntryInfo Create(
        ui64 pos,
        TEntryHeader* header,
        const char* data)
    {
        Y_ABORT_UNLESS(pos != INVALID_POS);
        Y_ABORT_UNLESS(header != nullptr);
        Y_ABORT_UNLESS(header->GetDataSize() > 0);
        Y_ABORT_UNLESS(data != nullptr);

        return TEntryInfo{.ActualPos = pos, .Header = header, .Data = data};
    }

    static TEntryInfo CreateEmpty(ui64 pos)
    {
        Y_ABORT_UNLESS(pos != INVALID_POS);

        return TEntryInfo{.ActualPos = pos};
    }

    static TEntryInfo CreateInvalid()
    {
        return TEntryInfo{.ActualPos = INVALID_POS};
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

class TFileRingBuffer::TImpl
{
private:
    TFileMap Map;

    TEntriesData Data;
    ui64 Count = 0;
    bool Corrupted = false;

private:
    THeader* Header()
    {
        return reinterpret_cast<THeader*>(Map.Ptr());
    }

    const THeader* Header() const
    {
        return reinterpret_cast<THeader*>(Map.Ptr());
    }

    TEntryInfo DoGetEntry(ui64 pos) const
    {
        if (pos > Header()->WritePos) {
            // This is valid only in the case:
            // ====W]....[R====
            //             ^- here
            if (Header()->ReadPos <= Header()->WritePos ||
                pos < Header()->ReadPos)
            {
                return TEntryInfo::CreateInvalid();
            }

            auto* eh = Data.GetEntryHeader(pos);
            if (eh != nullptr && eh->GetDataSize() != 0) {
                const auto* data = Data.GetEntryData(eh);
                return data != nullptr
                    ? TEntryInfo::Create(pos, eh, data)
                    : TEntryInfo::CreateInvalid();
            }
            pos = 0;
        }

        if (pos == Header()->WritePos) {
            return TEntryInfo::CreateEmpty(pos);
        }

        Y_ABORT_UNLESS(pos < Header()->WritePos);

        if (pos < Header()->ReadPos &&
            Header()->ReadPos <= Header()->WritePos)
        {
            return TEntryInfo::CreateInvalid();
        }

        auto* eh = Data.GetEntryHeader(pos);
        if (eh == nullptr || eh->GetDataSize() == 0) {
            return TEntryInfo::CreateInvalid();
        }

        const auto* data = Data.GetEntryData(eh);
        if (data == nullptr) {
            return TEntryInfo::CreateInvalid();
        }

        auto res = TEntryInfo::Create(pos, eh, data);
        if (res.GetNextEntryPos() > Header()->WritePos) {
            return TEntryInfo::CreateInvalid();
        }

        return res;
    }

    TEntryInfo GetEntry(ui64 pos) const
    {
        while (true) {
            auto e = DoGetEntry(pos);
            if (e.HasValue() && e.Header->HasSkipFlag()) {
                pos = e.GetNextEntryPos();
            } else {
                return e;
            }
        }
    }

    TEntryInfo GetFrontEntry() const
    {
        return GetEntry(Header()->ReadPos);
    }

    TEntryInfo GetNextEntry(const TEntryInfo& e) const
    {
        return e.HasValue()
            ? GetEntry(e.GetNextEntryPos())
            : TEntryInfo::CreateInvalid();
    }

    void SetCorrupted()
    {
        Corrupted = true;
    }

    void ValidateDataStructureAndDeleteIncompleteEntries()
    {
        TEntryInfo front = GetFrontEntry();
        TEntryInfo back = front;
        TEntryInfo cur = front;

        if (front.IsInvalid() || front.ActualPos != Header()->ReadPos) {
            SetCorrupted();
        }

        while (cur.HasValue() && cur.Header->HasIncompleteFlag()) {
            back = cur;
            cur = GetNextEntry(cur);
        }

        TEntryInfo first = cur;
        TEntryInfo last = cur;

        while (cur.HasValue()) {
            if (cur.Header->HasIncompleteFlag()) {
                cur.Header->SetSkipFlag();
            } else {
                Count++;
                last = cur;
            }
            back = cur;
            cur = GetNextEntry(cur);
        }

        if (cur.IsInvalid() || cur.ActualPos != Header()->WritePos) {
            SetCorrupted();
        }

        if (back.HasValue() && back.GetEntrySize() != Header()->LastEntrySize) {
            SetCorrupted();
        }

        if (first.HasValue()) {
            Y_ABORT_UNLESS(last.HasValue());
            Header()->ReadPos = first.ActualPos;
            Header()->WritePos = last.GetNextEntryPos();
            Header()->LastEntrySize = last.GetEntrySize();
        } else {
            Header()->ReadPos = 0;
            Header()->WritePos = 0;
            Header()->LastEntrySize = 0;
        }
    }

public:
    TImpl(const TString& filePath, ui64 capacity)
        : Map(filePath, TMemoryMapCommon::oRdWr)
    {
        Y_ABORT_UNLESS(sizeof(TEntryHeader) <= capacity);

        if (static_cast<ui64>(Map.Length()) < sizeof(THeader)) {
            const ui64 realSize = sizeof(THeader) + capacity;
            Map.ResizeAndRemap(0, realSize);
        } else {
            Map.Map(0, Map.Length());
        }

        if (Header()->Version) {
            Y_ABORT_UNLESS(
                sizeof(THeader) + Header()->Capacity <=
                static_cast<ui64>(Map.Length()));
            Y_ABORT_UNLESS(Header()->Version == VERSION);
        } else {
            Header()->Capacity = capacity;
            Header()->Version = VERSION;
        }

        auto* begin = static_cast<char*>(Map.Ptr()) + sizeof(THeader);
        Data = TEntriesData(begin, begin + capacity);

        ValidateDataStructureAndDeleteIncompleteEntries();
    }

public:
    bool PushBack(TStringBuf data)
    {
        char* ptr = nullptr;
        if (!AllocateBack(data.size(), &ptr)) {
            return false;
        }

        data.copy(ptr, data.size());
        CompleteAllocation(ptr);
        return true;
    }

    ui64 MaxAllocationSize() const
    {
        return Min(
            Header()->Capacity - sizeof(TEntryHeader),
            TEntryHeader::GetMaxDataSize());
    }

    bool AllocateBack(size_t size, char** ptr)
    {
        Y_ABORT_UNLESS(ptr != nullptr);
        *ptr = nullptr;

        if (IsCorrupted()) {
            // TODO: should return error code
            return false;
        }

        if (size == 0 || size > MaxAllocationSize()) {
            return false;
        }

        const auto sz = size + sizeof(TEntryHeader);
        auto writePos = Header()->WritePos;

        if (!Empty()) {
            // checking that we have a contiguous chunk of sz + 1 bytes
            // 1 extra byte is needed to distinguish between an empty buffer
            // and a buffer which is completely full
            if (Header()->ReadPos < Header()->WritePos) {
                // we have a single contiguous occupied region
                ui64 freeSpace = Header()->Capacity - Header()->WritePos;
                if (freeSpace < sz) {
                    if (Header()->ReadPos <= sz) {
                        // out of space
                        return false;
                    }
                    auto* eh = Data.GetEntryHeader(Header()->WritePos);
                    if (eh != nullptr) {
                        eh->InitSlackSpace();
                    }
                    writePos = 0;
                }
            } else {
                // we have two occupied regions
                ui64 freeSpace = Header()->ReadPos - Header()->WritePos;
                // there should remain free space between the occupied regions
                if (freeSpace <= sz) {
                    // out of space
                    return false;
                }
            }
        }

        auto* eh = Data.GetEntryHeader(writePos);
        eh->InitIncompleteEntry(size);

        *ptr = Data.GetEntryData(eh);
        Y_ABORT_UNLESS(*ptr);

        Header()->WritePos = writePos + sz;
        Header()->LastEntrySize = sz;
        ++Count;

        return true;
    }

    void CompleteAllocation(char* ptr)
    {
        auto* eh = Data.GetEntryHeaderFromDataPtr(ptr);

        Y_ABORT_UNLESS(eh != nullptr);
        Y_ABORT_UNLESS(eh->HasIncompleteFlag());

        eh->SetChecksum(Crc32c(ptr, eh->GetDataSize()));
        eh->ClearIncompleteFlag();
    }

    TStringBuf Front() const
    {
        auto e = GetFrontEntry();

        if (e.IsInvalid()) {
            // corruption
            // TODO: report?
            return {};
        }

        return e.GetData();
    }

    TStringBuf Back() const
    {
        if (Empty()) {
            return {};
        }

        if (Header()->WritePos < Header()->LastEntrySize) {
            // corruption
            // TODO: report?
            return {};
        }
        auto pos = Header()->WritePos - Header()->LastEntrySize;
        const auto e = GetEntry(pos);

        if (!e.HasValue() || e.ActualPos != pos ||
            e.GetNextEntryPos() != Header()->WritePos)
        {
            // corruption
            // TODO: report?
            return {};
        }

        return e.GetData();
    }

    void PopFront()
    {
        auto cur = GetFrontEntry();
        if (!cur.HasValue()) {
            return;
        }

        // Removing an incomplete entry will produce a dangling pointer
        // It indicates the problem in the logic in the calling code
        Y_ABORT_IF(cur.Header->HasIncompleteFlag());

        auto next = GetNextEntry(cur);
        if (next.HasValue()) {
            Header()->ReadPos = next.ActualPos;
        } else {
            Header()->ReadPos = 0;
            Header()->WritePos = 0;
        }

        --Count;
    }

    ui64 Size() const
    {
        return Count;
    }

    bool Empty() const
    {
        const bool result = Header()->ReadPos == Header()->WritePos;
        Y_DEBUG_ABORT_UNLESS(result == (Count == 0));
        return result;
    }

    auto ValidateEntriesChecksums()
    {
        TVector<TBrokenFileEntry> entries;

        Visit([&] (ui32 checksum, TStringBuf entry) {
            const ui32 actualChecksum = Crc32c(entry.data(), entry.size());
            if (actualChecksum != checksum) {
                entries.push_back({
                    TString(entry),
                    checksum,
                    actualChecksum});
            }
        });

        return entries;
    }

    void Visit(const TVisitor& visitor)
    {
        auto e = GetFrontEntry();

        while (e.HasValue()) {
            visitor(e.Header->GetChecksum(), e.GetData());
            e = GetNextEntry(e);
        }

        if (e.IsInvalid()) {
            SetCorrupted();
        }
    }

    bool IsCorrupted() const
    {
        return Corrupted;
    }
};

////////////////////////////////////////////////////////////////////////////////

TFileRingBuffer::TFileRingBuffer(
        const TString& filePath,
        ui64 capacity)
    : Impl(new TImpl(filePath, capacity))
{}

TFileRingBuffer::~TFileRingBuffer() = default;

bool TFileRingBuffer::PushBack(TStringBuf data)
{
    return Impl->PushBack(data);
}

ui64 TFileRingBuffer::MaxAllocationSize() const
{
    return Impl->MaxAllocationSize();
}

bool TFileRingBuffer::AllocateBack(size_t size, char** ptr)
{
    return Impl->AllocateBack(size, ptr);
}

void TFileRingBuffer::CompleteAllocation(char* ptr)
{
    Impl->CompleteAllocation(ptr);
}

TStringBuf TFileRingBuffer::Front() const
{
    return Impl->Front();
}

TStringBuf TFileRingBuffer::Back() const
{
    return Impl->Back();
}

void TFileRingBuffer::PopFront()
{
    Impl->PopFront();
}

ui64 TFileRingBuffer::Size() const
{
    return Impl->Size();
}

bool TFileRingBuffer::Empty() const
{
    return Impl->Empty();
}

TVector<TFileRingBuffer::TBrokenFileEntry> TFileRingBuffer::Validate()
{
    return Impl->ValidateEntriesChecksums();
}

void TFileRingBuffer::Visit(const TVisitor& visitor)
{
    Impl->Visit(visitor);
}

bool TFileRingBuffer::IsCorrupted() const
{
    return Impl->IsCorrupted();
}

}   // namespace NCloud
