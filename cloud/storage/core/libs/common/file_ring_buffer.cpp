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
constexpr TStringBuf INVALID_MARKER = "invalid_entry_marker";

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

void WriteEntry(IOutputStream& os, TStringBuf data)
{
    TEntryHeader eh;
    eh.Size = data.size();
    eh.Checksum = Crc32c(data.data(), data.size());
    os.Write(&eh, sizeof(eh));
    os.Write(data);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

class TFileRingBuffer::TImpl
{
private:
    TFileMap Map;

    char* Data = nullptr;
    ui64 Count = 0;
    const char* End = nullptr;

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

        const auto* b = Data + Header()->ReadPos;
        if (b + sizeof(TEntryHeader) > End) {
            Header()->ReadPos = 0;
            return;
        }

        const auto* eh = reinterpret_cast<const TEntryHeader*>(b);
        if (eh->Size == 0) {
            Header()->ReadPos = 0;
        }
    }

    ui64 VisitEntry(const TVisitor& visitor, ui64 pos) const
    {
        const auto* b = Data + pos;
        if (b + sizeof(TEntryHeader) > End) {
            // slack space smaller than TEntryHeader
            return 0;
        }

        const auto* eh = reinterpret_cast<const TEntryHeader*>(b);
        if (eh->Size == 0) {
            return 0;
        }

        TStringBuf entry(b + sizeof(TEntryHeader), eh->Size);
        if (entry.data() + entry.size() > End) {
            visitor(eh->Checksum, INVALID_MARKER);
            return INVALID_POS;
        }
        visitor(eh->Checksum, {b + sizeof(TEntryHeader), eh->Size});
        return pos + sizeof(TEntryHeader) + eh->Size;
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

        Data = static_cast<char*>(Map.Ptr()) + sizeof(THeader);
        End = Data + capacity;

        SkipSlackSpace();
        Visit([this] (ui32 checksum, TStringBuf entry) {
            Y_UNUSED(checksum);
            Y_UNUSED(entry);
            ++Count;
        });
    }

public:
    bool PushBack(TStringBuf data)
    {
        if (data.empty()) {
            return false;
        }

        const auto sz = data.size() + sizeof(TEntryHeader);
        if (sz > Header()->Capacity) {
            return false;
        }
        auto* ptr = Data + Header()->WritePos;

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

                    memset(ptr, 0, Min(sizeof(TEntryHeader), freeSpace));
                    ptr = Data;
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

        TMemoryOutput mo(ptr, sz);
        WriteEntry(mo, data);

        Header()->WritePos = ptr - Data + sz;
        Header()->LastEntrySize = sz;
        ++Count;

        return true;
    }

    TStringBuf Front() const
    {
        if (Empty()) {
            return {};
        }

        const auto* b = Data + Header()->ReadPos;
        if (b + sizeof(TEntryHeader) > End) {
            // corruption
            // TODO: report?
            return {};
        }

        const auto* eh = reinterpret_cast<const TEntryHeader*>(b);
        TStringBuf result{b + sizeof(TEntryHeader), eh->Size};
        if (result.data() + result.size() > End) {
            // corruption
            // TODO: report?
            return {};
        }

        return result;
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

        const auto* b = Data + Header()->WritePos - Header()->LastEntrySize;
        if (b + sizeof(TEntryHeader) > End) {
            // corruption
            // TODO: report?
            return {};
        }

        const auto* eh = reinterpret_cast<const TEntryHeader*>(b);
        TStringBuf result{b + sizeof(TEntryHeader), eh->Size};
        if (result.data() + result.size() > End) {
            // corruption
            // TODO: report?
            return {};
        }

        return result;
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

    void Visit(const TVisitor& visitor) const
    {
        ui64 pos = Header()->ReadPos;
        while (pos > Header()->WritePos && pos != INVALID_POS) {
            pos = VisitEntry(visitor, pos);
        }

        while (pos < Header()->WritePos && pos != INVALID_POS) {
            pos = VisitEntry(visitor, pos);
            if (!pos) {
                // can happen if the buffer is corrupted
                break;
            }
        }
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
    return Impl->Visit(visitor);
}

}   // namespace NCloud
