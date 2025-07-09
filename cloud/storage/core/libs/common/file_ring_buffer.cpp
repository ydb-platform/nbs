#include "file_ring_buffer.h"

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/generic/size_literals.h>
#include <util/stream/mem.h>
#include <util/system/compiler.h>
#include <util/system/filemap.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 VERSION = 2;
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
    ui32 Size = 0;
    ui32 Checksum = 0;
};

//  Structure contract:
//
//  1. Empty buffer:
//    - ReadPos == WritePos = 0
//
//  2. Non-empty buffer:
//    - ReadPos != WritePos
//    - ReadPos points to the first byte of the first valid entry
//    - WritePos points to the first byte right after the last valid entry
//    - LastEntrySize is the size of the last valid entry
//
//  3. Valid entry:
//    - Size > 0
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

    char* DoGetEntryData(const TEntryHeader* eh) const
    {
        Y_ABORT_UNLESS(eh != nullptr);
        Y_ABORT_UNLESS(eh->Size != 0);

        ui64 pos = reinterpret_cast<const char*>(eh) - Begin;
        return GetPtr(pos + sizeof(eh), eh->Size);
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

    TEntryHeader* GetEntryHeader(ui64 pos)
    {
        return GetPtr<TEntryHeader>(pos);
    }

    const TEntryHeader* GetEntryHeader(ui64 pos) const
    {
        return GetPtr<TEntryHeader>(pos);
    }

    char* GetEntryData(TEntryHeader* eh)
    {
        return DoGetEntryData(eh);
    }

    const char* GetEntryData(const TEntryHeader* eh) const
    {
        return DoGetEntryData(eh);
    }

    void WriteEntry(ui64 pos, TStringBuf data)
    {
        auto* eh = GetEntryHeader(pos);
        Y_ABORT_UNLESS(eh != nullptr);

        eh->Size = data.size();
        eh->Checksum = Crc32c(data.data(), data.size());

        auto* dst = GetEntryData(eh);
        Y_ABORT_UNLESS(dst != nullptr);
        memcpy(dst, data.data(), data.size());
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TEntryInfo
{
    ui64 ActualPos = 0;
    const TEntryHeader* Header = nullptr;
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
        return HasValue() ? TStringBuf(Data, Header->Size) : TStringBuf();
    }

    ui64 GetNextEntryPos() const
    {
        return Header != nullptr && Header->Size > 0
            ? ActualPos + sizeof(TEntryHeader) + Header->Size
            : INVALID_POS;
    }

    static TEntryInfo Create(
        ui64 pos,
        const TEntryHeader* header,
        const char* data)
    {
        Y_ABORT_UNLESS(pos != INVALID_POS);
        Y_ABORT_UNLESS(header != nullptr);
        Y_ABORT_UNLESS(header->Size > 0);
        Y_ABORT_UNLESS(data != nullptr);

        return TEntryInfo{.ActualPos = pos, .Header = header, .Data = data};
    }

    static TEntryInfo CreateEmpty()
    {
        return TEntryInfo{};
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

    void SkipSlackSpace()
    {
        auto e = GetFrontEntry();

        if (e.HasValue()) {
            Header()->ReadPos = e.ActualPos;
        } else {
            Header()->ReadPos = 0;
            Header()->WritePos = 0;
        }
    }

    TEntryInfo GetEntry(ui64 pos) const
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

            const auto* eh = Data.GetEntryHeader(pos);
            if (eh != nullptr && eh->Size != 0) {
                const auto* data = Data.GetEntryData(eh);
                return data != nullptr
                    ? TEntryInfo::Create(pos, eh, data)
                    : TEntryInfo::CreateInvalid();
            }
            pos = 0;
        }

        if (pos == Header()->WritePos) {
            return TEntryInfo::CreateEmpty();
        }

        Y_ABORT_UNLESS(pos < Header()->WritePos);

        if (pos < Header()->ReadPos &&
            Header()->ReadPos <= Header()->WritePos)
        {
            return TEntryInfo::CreateInvalid();
        }

        const auto* eh = Data.GetEntryHeader(pos);
        if (eh == nullptr || eh->Size == 0) {
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

    TEntryInfo GetFrontEntry() const
    {
        return GetEntry(Header()->ReadPos);
    }

    void SetCorrupted()
    {
        Corrupted = true;
    }

public:
    TImpl(const TString& filePath, ui64 capacity)
        : Map(filePath, TMemoryMapCommon::oRdWr)
    {
        Y_ABORT_UNLESS(sizeof(TEntryHeader) <= capacity);

        const ui64 realSize = sizeof(THeader) + capacity;
        if (static_cast<ui64>(Map.Length()) < realSize) {
            Map.ResizeAndRemap(0, realSize);
        } else {
            Map.Map(0, realSize);
        }

        if (Header()->Version) {
            Y_ABORT_UNLESS(Header()->Capacity == capacity);
            Y_ABORT_UNLESS(Header()->Version == VERSION);
        } else {
            Header()->Capacity = capacity;
            Header()->Version = VERSION;
        }

        auto* begin = static_cast<char*>(Map.Ptr()) + sizeof(THeader);
        Data = TEntriesData(begin, begin + capacity);

        SkipSlackSpace();
        bool isConsistent = Visit([this] (ui32 checksum, TStringBuf entry) {
            Y_UNUSED(checksum);
            Y_UNUSED(entry);
            ++Count;
        });

        if (!isConsistent) {
            SetCorrupted();
        }
    }

public:
    bool PushBack(TStringBuf data)
    {
        if (IsCorrupted()) {
            // TODO: should return error code
            return false;
        }

        if (data.empty()) {
            return false;
        }

        const auto sz = data.size() + sizeof(TEntryHeader);
        if (sz > Header()->Capacity) {
            return false;
        }
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
                        eh->Size = 0;
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

        Data.WriteEntry(writePos, data);

        Header()->WritePos = writePos + sz;
        Header()->LastEntrySize = sz;
        ++Count;

        return true;
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
        auto e = GetFrontEntry();
        if (!e.HasValue()) {
            return;
        }

        Header()->ReadPos = e.GetNextEntryPos();
        --Count;

        SkipSlackSpace();
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

    auto Validate() const
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

    bool Visit(const TVisitor& visitor) const
    {
        ui64 pos = Header()->ReadPos;

        auto e = GetEntry(pos);

        while (e.HasValue()) {
            visitor(e.Header->Checksum, e.GetData());
            pos = e.GetNextEntryPos();
            e = GetEntry(pos);
        }

        return pos == Header()->WritePos && !e.IsInvalid();
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

TVector<TFileRingBuffer::TBrokenFileEntry> TFileRingBuffer::Validate() const
{
    return Impl->Validate();
}

void TFileRingBuffer::Visit(const TVisitor& visitor) const
{
    Impl->Visit(visitor);
}

bool TFileRingBuffer::IsCorrupted() const
{
    return Impl->IsCorrupted();
}

}   // namespace NCloud
