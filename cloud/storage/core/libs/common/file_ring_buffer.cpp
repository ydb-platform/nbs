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

class TBuffer
{
private:
    char* Data = nullptr;
    const char* End = nullptr;

public:
    void Init(char* data, const char* end)
    {
        Y_ABORT_UNLESS(data != nullptr);
        Y_ABORT_UNLESS(end != nullptr);
        Y_ABORT_UNLESS(data <= end);

        Data = data;
        End = end;
    }

    char* CheckPtr(char* ptr, ui64 size)
    {
        char* end = ptr + size;
        return Data <= ptr && ptr <= end && end <= End ? ptr : nullptr;
    }

    const char* CheckPtr(const char* ptr, ui64 size) const
    {
        const char* end = ptr + size;
        return Data <= ptr && ptr <= end && end <= End ? ptr : nullptr;
    }

    char* GetPtr(ui64 pos, ui64 size)
    {
        return CheckPtr(Data + pos, size);
    }

    const char* GetPtr(ui64 pos, ui64 size) const
    {
        return CheckPtr(Data + pos, size);
    }
};

struct TEntryInfo
{
    ui64 ActualPos = INVALID_POS;
    const TEntryHeader* Header = nullptr;
    const char* Data = nullptr;

    explicit operator bool() const
    {
        return ActualPos != INVALID_POS;
    }
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

    TBuffer Buffer;
    ui64 Count = 0;
    mutable bool Corrupted = false;

private:
    THeader* Header()
    {
        return reinterpret_cast<THeader*>(Map.Ptr());
    }

    const THeader* Header() const
    {
        return reinterpret_cast<THeader*>(Map.Ptr());
    }

    TEntryHeader* EntryHeader(ui64 pos)
    {
        char* ptr = Buffer.GetPtr(pos, sizeof(TEntryHeader));
        return reinterpret_cast<TEntryHeader*>(ptr);
    }

    const TEntryHeader* EntryHeader(ui64 pos) const
    {
        const char* ptr = Buffer.GetPtr(pos, sizeof(TEntryHeader));
        return reinterpret_cast<const TEntryHeader*>(ptr);
    }

    const char* EntryData(const TEntryHeader* eh) const
    {
        Y_ABORT_UNLESS(eh != nullptr);

        return Buffer.CheckPtr(
            reinterpret_cast<const char*>(eh) + sizeof(TEntryHeader),
            eh->Size);
    }

    static ui64 NextEntryPos(const TEntryInfo& e)
    {
        Y_ABORT_UNLESS(e);
        Y_ABORT_UNLESS(e.Header != nullptr);
        Y_ABORT_UNLESS(e.Header->Size > 0);

        return e.ActualPos + sizeof(TEntryHeader) + e.Header->Size;
    }

    TEntryInfo GetEntry(ui64 pos) const
    {
        if (pos > Header()->WritePos) {
            // This is valid only in the case:
            // ====W]....[R====
            //             ^- here
            Y_ABORT_UNLESS(
                Header()->ReadPos > Header()->WritePos &&
                pos >= Header()->ReadPos);

            const auto* eh = EntryHeader(pos);
            if (eh != nullptr && eh->Size != 0) {
                const auto* data = EntryData(eh);
                if (data == nullptr) {
                    SetCorrupted();
                    return {};
                }
                return {.ActualPos = pos, .Header = eh, .Data = data};
            } else {
                pos = 0;
            }
        }

        if (pos == Header()->WritePos) {
            return {};
        }

        Y_ABORT_IF(
            pos < Header()->ReadPos && Header()->ReadPos <= Header()->WritePos);

        const auto* eh = EntryHeader(pos);
        if (eh == nullptr || eh->Size == 0) {
            SetCorrupted();
            return {};
        }

        const auto* data = EntryData(eh);
        if (data == nullptr) {
            SetCorrupted();
            return {};
        }

        TEntryInfo res {.ActualPos = pos, .Header = eh, .Data = data};
        if (NextEntryPos(res) > Header()->WritePos) {
            SetCorrupted();
            return {};
        }

        return res;
    }

    void SetCorrupted() const
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

        auto* data = static_cast<char*>(Map.Ptr()) + sizeof(THeader);
        Buffer.Init(data, data + capacity);

        Visit([this] (ui32 checksum, TStringBuf entry) {
            Y_UNUSED(checksum);
            Y_UNUSED(entry);
            ++Count;
        });
    }

public:
    bool PushBack(TStringBuf data)
    {
        if (IsCorrupted()) {
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
                    auto* eh = EntryHeader(Header()->WritePos);
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
        } else {
            Header()->ReadPos = 0;
            Header()->WritePos = 0;
            writePos = 0;
        }

        char* ptr = Buffer.GetPtr(writePos, sz);
        Y_ABORT_UNLESS(ptr != nullptr);

        TMemoryOutput mo(ptr, sz);
        WriteEntry(mo, data);

        Header()->WritePos = writePos + sz;
        Header()->LastEntrySize = sz;
        ++Count;

        return true;
    }

    TStringBuf Front() const
    {
        auto e = GetEntry(Header()->ReadPos);
        return e ? TStringBuf(e.Data, e.Header->Size) : TStringBuf();
    }

    TStringBuf Back() const
    {
        if (Empty()) {
            return {};
        }

        if (Header()->WritePos < Header()->LastEntrySize) {
            SetCorrupted();
            return {};
        }

        auto pos = Header()->WritePos - Header()->LastEntrySize;
        const auto e = GetEntry(pos);
        if (!e || e.ActualPos != pos || NextEntryPos(e) != Header()->WritePos) {
            SetCorrupted();
            return {};
        }

        return {e.Data, e.Header->Size};
    }

    void PopFront()
    {
        auto e = GetEntry(Header()->ReadPos);
        if (!e) {
            return;
        }

        if (Count > 0) {
            Count--;
        } else {
            SetCorrupted();
        }

        e = GetEntry(NextEntryPos(e));
        if (e) {
            Header()->ReadPos = e.ActualPos;
            return;
        }

        Header()->ReadPos = 0;
        Header()->WritePos = 0;

        if (Count > 0) {
            Count = 0;
            SetCorrupted();
        }
    }

    ui64 Size() const
    {
        return Count;
    }

    bool Empty() const
    {
        const bool result = Header()->ReadPos == Header()->WritePos;
        if (result != (Count == 0)) {
            SetCorrupted();
        }
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
        auto e = GetEntry(Header()->ReadPos);
        ui64 pos = e ? e.ActualPos : 0;
        ui64 count = 0;

        if (Header()->ReadPos != pos) {
            SetCorrupted();
        }

        while (e) {
            count++;
            visitor(e.Header->Checksum, {e.Data, e.Header->Size});
            pos = NextEntryPos(e);
            e = GetEntry(pos);
        }

        if (Header()->WritePos != pos) {
            SetCorrupted();
        }

        if (Count != count) {
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
