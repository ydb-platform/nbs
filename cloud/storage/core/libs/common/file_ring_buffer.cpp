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

class TFileRingBufferData
{
private:
    char* Begin = nullptr;
    const char* End = nullptr;

    bool IsValidRegion(const char* ptr, ui64 size) const
    {
        const char* end = ptr + size;
        return Begin <= ptr && ptr <= end && end <= End;
    }

    char* CheckPtr(char* ptr, ui64 size)
    {
        return IsValidRegion(ptr, size) ? ptr : nullptr;
    }

    const char* CheckPtr(const char* ptr, ui64 size) const
    {
        return IsValidRegion(ptr, size) ? ptr : nullptr;
    }

    char* GetPtr(ui64 pos, ui64 size)
    {
        return CheckPtr(Begin + pos, size);
    }

    const char* GetPtr(ui64 pos, ui64 size) const
    {
        return CheckPtr(Begin + pos, size);
    }

public:
    void Init(char* begin, const char* end)
    {
        Y_ABORT_UNLESS(begin != nullptr);
        Y_ABORT_UNLESS(end != nullptr);
        Y_ABORT_UNLESS(begin <= end);

        Begin = begin;
        End = end;
    }

    TEntryHeader* GetEntryHeader(ui64 pos)
    {
        char* ptr = GetPtr(pos, sizeof(TEntryHeader));
        return reinterpret_cast<TEntryHeader*>(ptr);
    }

    const TEntryHeader* GetEntryHeader(ui64 pos) const
    {
        const char* ptr = GetPtr(pos, sizeof(TEntryHeader));
        return reinterpret_cast<const TEntryHeader*>(ptr);
    }

    char* GetEntryData(TEntryHeader* eh)
    {
        Y_ABORT_UNLESS(eh != nullptr);
        return CheckPtr(
            reinterpret_cast<char*>(eh) + sizeof(TEntryHeader),
            eh->Size);
    }

    const char* GetEntryData(const TEntryHeader* eh) const
    {
        Y_ABORT_UNLESS(eh != nullptr);
        return CheckPtr(
            reinterpret_cast<const char*>(eh) + sizeof(TEntryHeader),
            eh->Size);
    }

    void WriteEntry(ui64 pos, TStringBuf data)
    {
        auto eh = GetEntryHeader(pos);
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

    explicit operator bool() const
    {
        return Header != nullptr;
    }

    bool IsInvalid() const
    {
        return ActualPos == INVALID_POS;
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

    TFileRingBufferData Data;
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
            if (Header()->ReadPos <= Header()->WritePos ||
                pos < Header()->ReadPos)
            {
                return TEntryInfo::CreateInvalid();
            }

            const auto* eh = Data.GetEntryHeader(pos);
            if (eh != nullptr && eh->Size != 0) {
                const auto* data = Data.GetEntryData(eh);
                if (data == nullptr) {
                    return TEntryInfo::CreateInvalid();
                }
                return {.ActualPos = pos, .Header = eh, .Data = data};
            } else {
                pos = 0;
            }
        }

        if (pos == Header()->WritePos) {
            // End of buffer
            return {};
        }

        if (Header()->ReadPos <= Header()->WritePos &&
            pos < Header()->ReadPos)
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

        TEntryInfo res {.ActualPos = pos, .Header = eh, .Data = data};
        if (NextEntryPos(res) > Header()->WritePos) {
            return TEntryInfo::CreateInvalid();
        }

        return res;
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
        Data.Init(begin, begin + capacity);

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
        } else {
            Header()->ReadPos = 0;
            Header()->WritePos = 0;
            writePos = 0;
        }

        Data.WriteEntry(writePos, data);

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

        Y_DEBUG_ABORT_UNLESS(Header()->WritePos >= Header()->LastEntrySize);

        auto pos = Header()->WritePos - Header()->LastEntrySize;
        const auto e = GetEntry(pos);
        Y_DEBUG_ABORT_UNLESS(
            e && e.ActualPos == pos && NextEntryPos(e) == Header()->WritePos);

        return {e.Data, e.Header->Size};
    }

    void PopFront()
    {
        auto e = GetEntry(Header()->ReadPos);
        if (!e) {
            if (e.IsInvalid()) {
                SetCorrupted();
            }
            return;
        }

        if (Count == 0) {
            SetCorrupted();
            return;
        }
        Count--;

        e = GetEntry(NextEntryPos(e));
        if (e) {
            Header()->ReadPos = e.ActualPos;
            return;
        }

        if (e.IsInvalid()) {
            SetCorrupted();
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
        Y_DEBUG_ABORT_UNLESS(result == (Count == 0));
        return result;
    }

    auto Validate()
    {
        TVector<TBrokenFileEntry> entries;

        bool isConsistent = Visit([&] (ui32 checksum, TStringBuf entry) {
            const ui32 actualChecksum = Crc32c(entry.data(), entry.size());
            if (actualChecksum != checksum) {
                entries.push_back({
                    TString(entry),
                    checksum,
                    actualChecksum});
            }
        });

        if (!isConsistent) {
            SetCorrupted();
        }

        return entries;
    }

    bool Visit(const TVisitor& visitor) const
    {
        auto e = GetEntry(Header()->ReadPos);
        ui64 pos = e ? e.ActualPos : 0;
        ui64 count = 0;

        if (Header()->ReadPos != pos) {
            return false;
        }

        while (e) {
            count++;
            visitor(e.Header->Checksum, {e.Data, e.Header->Size});
            pos = NextEntryPos(e);
            e = GetEntry(pos);
        }

        if (e.IsInvalid()) {
            return false;
        }

        if (Header()->WritePos != pos) {
            return false;
        }

        if (Count != count) {
            return false;
        }

        return true;
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
