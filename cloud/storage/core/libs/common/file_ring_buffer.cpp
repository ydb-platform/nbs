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
        if (Header()->ReadPos == Header()->WritePos) {
            Header()->ReadPos = 0;
            Header()->WritePos = 0;
            return;
        }

        const auto* eh = Data.GetEntryHeader(Header()->ReadPos);
        if (eh == nullptr || eh->Size == 0) {
            Header()->ReadPos = 0;
        }
    }

    ui64 VisitEntry(const TVisitor& visitor, ui64 pos) const
    {
        const auto* eh = Data.GetEntryHeader(pos);
        if (eh == nullptr || eh->Size == 0) {
            return 0;
        }

        const auto* data = Data.GetEntryData(eh);
        if (data == nullptr) {
            return INVALID_POS;
        }
        visitor(eh->Checksum, {data, eh->Size});
        return pos + sizeof(TEntryHeader) + eh->Size;
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
        if (Empty()) {
            return {};
        }

        const auto* eh = Data.GetEntryHeader(Header()->ReadPos);
        if (eh == nullptr) {
            // corruption
            // TODO: report?
            return {};
        }

        const auto* data = Data.GetEntryData(eh);
        if (data == nullptr) {
            // corruption
            // TODO: report?
            return {};
        }

        return {data, eh->Size};
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

        const auto* eh =
            Data.GetEntryHeader(Header()->WritePos - Header()->LastEntrySize);
        const auto* data = eh ? Data.GetEntryData(eh) : nullptr;
        if (data == nullptr) {
            // corruption
            // TODO: report?
            return {};
        }

        return {data, eh->Size};
    }

    void PopFront()
    {
        auto data = Front();
        if (!data) {
            return;
        }

        Header()->ReadPos += sizeof(TEntryHeader) + data.size();
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
        while (pos > Header()->WritePos && pos != INVALID_POS) {
            pos = VisitEntry(visitor, pos);
        }

        while (pos < Header()->WritePos && pos != INVALID_POS) {
            pos = VisitEntry(visitor, pos);
            if (!pos) {
                // can happen if the buffer is corrupted
                return false;
            }
        }

        return pos == Header()->WritePos;
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
