#include "file_ring_buffer.h"

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/generic/size_literals.h>
#include <util/stream/mem.h>
#include <util/system/align.h>
#include <util/system/compiler.h>
#include <util/system/filemap.h>

#include <span>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 VERSION_PREV = 2;
constexpr ui32 VERSION = 3;
constexpr ui64 INVALID_POS = Max<ui64>();

// Reserve some space after header so adding new fields will not require data
// migration
constexpr ui64 HeaderReserveSize = 256;

////////////////////////////////////////////////////////////////////////////////

struct THeaderPrev
{
    ui32 Version = 0;
    ui32 HeaderSize = 0;
    ui64 DataCapacity = 0;
    ui64 ReadPos = 0;
    ui64 WritePos = 0;
    ui64 LastEntrySize = 0;
};

struct THeader: THeaderPrev
{
    ui64 DataOffset = 0;
    ui64 MetadataCapacity = 0;
    ui64 MetadataOffset = 0;
    ui32 MetadataSize = 0;
    ui32 MetadataChecksum = 0;
};

static_assert(sizeof(THeader) <= HeaderReserveSize);

struct Y_PACKED TEntryHeader
{
    ui32 DataSize = 0;
    ui32 Checksum = 0;
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
    std::span<char> Data;

    template <class T = char>
    T* GetPtr(ui64 pos, ui64 size = sizeof(T)) const
    {
        return pos < Data.size() && size <= Data.size() - pos
                   ? reinterpret_cast<T*>(Data.data() + pos)
                   : nullptr;
    }

    char* DoGetEntryData(const TEntryHeader* eh) const
    {
        Y_ABORT_UNLESS(eh != nullptr);
        Y_ABORT_UNLESS(eh->DataSize != 0);

        ui64 pos = reinterpret_cast<const char*>(eh) - Data.data();
        return GetPtr(pos + sizeof(eh), eh->DataSize);
    }

public:
    TEntriesData() = default;

    explicit TEntriesData(std::span<char> data)
        : Data(data)
    {}

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

        eh->DataSize = data.size();
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
        return HasValue() ? TStringBuf(Data, Header->DataSize) : TStringBuf();
    }

    ui64 GetNextEntryPos() const
    {
        return Header != nullptr && Header->DataSize > 0
            ? ActualPos + sizeof(TEntryHeader) + Header->DataSize
            : INVALID_POS;
    }

    static TEntryInfo Create(
        ui64 pos,
        const TEntryHeader* header,
        const char* data)
    {
        Y_ABORT_UNLESS(pos != INVALID_POS);
        Y_ABORT_UNLESS(header != nullptr);
        Y_ABORT_UNLESS(header->DataSize > 0);
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

////////////////////////////////////////////////////////////////////////////////

THeader InitHeader(ui64 dataCapacity, ui64 metadataCapacity)
{
    THeader res;
    res.Version = VERSION;
    res.HeaderSize = sizeof(THeader);
    res.MetadataOffset = HeaderReserveSize;
    res.MetadataCapacity = metadataCapacity;
    res.DataOffset =
        AlignUp(res.MetadataOffset + res.MetadataCapacity, sizeof(ui64));
    res.DataCapacity = dataCapacity;
    return res;
}

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
            if (eh != nullptr && eh->DataSize != 0) {
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

        const auto* eh = Data.GetEntryHeader(pos);
        if (eh == nullptr || eh->DataSize == 0) {
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

    void ValidateStructure()
    {
        const ui64 mapLength = static_cast<ui64>(Map.Length());
        const auto& h = *Header();

        Y_ABORT_UNLESS(h.Version == VERSION);
        Y_ABORT_UNLESS(sizeof(THeader) == h.HeaderSize);
        Y_ABORT_UNLESS(h.HeaderSize <= h.MetadataOffset);
        Y_ABORT_UNLESS(h.MetadataOffset <= h.DataOffset);
        Y_ABORT_UNLESS(h.MetadataCapacity <= h.DataOffset - h.MetadataOffset);
        Y_ABORT_UNLESS(h.DataOffset <= mapLength);
        Y_ABORT_UNLESS(h.DataCapacity <= mapLength - h.DataOffset);
    }

    void ValidateDataStructure()
    {
        TEntryInfo front = GetFrontEntry();
        TEntryInfo back = front;
        TEntryInfo cur = front;

        if (front.IsInvalid() || front.ActualPos != Header()->ReadPos) {
            SetCorrupted();
        }

        while (cur.HasValue()) {
            Count++;
            back = cur;
            cur = GetNextEntry(cur);
        }

        if (cur.IsInvalid() || cur.ActualPos != Header()->WritePos) {
            SetCorrupted();
        }

        if (front.HasValue()) {
            Y_ABORT_UNLESS(back.HasValue());

            Header()->ReadPos = front.ActualPos;
            Header()->WritePos = back.GetNextEntryPos();

            auto lastEntrySize = back.GetNextEntryPos() - back.ActualPos;
            if (Header()->LastEntrySize != lastEntrySize) {
                SetCorrupted();
                Header()->LastEntrySize = lastEntrySize;
            }
        } else {
            Header()->ReadPos = 0;
            Header()->WritePos = 0;
            Header()->LastEntrySize = 0;
        }
    }

    void ResizeAndRemap(ui64 fileSize)
    {
        Map.ResizeAndRemap(0, fileSize);
    }

    std::span<char> GetMappedData(ui64 offset, ui64 size) const
    {
        const ui64 mapLength = static_cast<ui64>(Map.Length());
        Y_ABORT_UNLESS(offset <= mapLength);
        Y_ABORT_UNLESS(size <= mapLength - offset);
        return {static_cast<char*>(Map.Ptr()) + offset, size};
    }

    void CopyMappedData(ui64 destPos, ui64 srcPos, ui64 size)
    {
        auto src = GetMappedData(srcPos, size);
        auto dst = GetMappedData(destPos, size);

        // Copied data regions cannot overlap
        Y_ABORT_UNLESS(destPos + size <= srcPos || srcPos + size <= destPos);

        MemCopy(dst.data(), src.data(), size);
    }

    void Migrate(const THeader& header)
    {
        Y_ABORT_UNLESS(Header()->DataCapacity == header.DataCapacity);
        Y_ABORT_UNLESS(sizeof(THeaderPrev) <= header.DataOffset);

        const ui64 newFileSize = header.DataOffset + header.DataCapacity;

        // Make a copy of all data in the end of the file
        // Then copy it to the right place
        ResizeAndRemap(newFileSize + header.DataCapacity);

        if (Header()->HeaderSize == 0) {
            CopyMappedData(
                newFileSize,
                sizeof(THeaderPrev),
                header.DataCapacity);
            // Indicate that the data has been copied
            Header()->HeaderSize = sizeof(THeader);
        }

        Y_ABORT_UNLESS(Header()->HeaderSize == sizeof(THeader));
        CopyMappedData(header.DataOffset, newFileSize, header.DataCapacity);

        Header()->DataOffset = header.DataOffset;
        Header()->MetadataOffset = header.MetadataOffset;
        Header()->MetadataCapacity = header.MetadataCapacity;
        Header()->MetadataSize = 0;
        Header()->Version = VERSION;

        ResizeAndRemap(newFileSize);
    }

    void ResizeMetadata(ui64 desiredMetadataCapacity)
    {
        Header()->MetadataCapacity =
            Min(Header()->MetadataCapacity,
                static_cast<ui64>(Header()->MetadataSize));

        const ui64 newMetadataCapacity =
            Max(desiredMetadataCapacity, Header()->MetadataCapacity);

        const ui64 newDataOffset = AlignUp(
            Header()->MetadataOffset + newMetadataCapacity,
            sizeof(ui64));

        const ui64 newFileSize = newDataOffset + Header()->DataCapacity;

        if (Header()->DataOffset != newDataOffset &&
            Header()->DataOffset < newFileSize)
        {
            // Move data to the temporary place
            const ui64 tempDataOffset =
                Max(newFileSize, Header()->DataOffset + Header()->DataCapacity);

            ResizeAndRemap(tempDataOffset + Header()->DataCapacity);

            CopyMappedData(
                tempDataOffset,
                Header()->DataOffset,
                Header()->DataCapacity);

            Header()->DataOffset = tempDataOffset;
        }

        if (Header()->DataOffset != newDataOffset) {
            // Move data to the right place
            CopyMappedData(
                newDataOffset,
                Header()->DataOffset,
                Header()->DataCapacity);

            Header()->DataOffset = newDataOffset;
        }

        ResizeAndRemap(newFileSize);

        Header()->MetadataCapacity = newMetadataCapacity;
    }

public:
    TImpl(const TString& filePath, ui64 dataCapacity, ui64 metadataCapacity)
        : Map(filePath, TMemoryMapCommon::oRdWr)
    {
        Y_ABORT_UNLESS(sizeof(TEntryHeader) <= dataCapacity);

        if (static_cast<ui64>(Map.Length()) < sizeof(THeader)) {
            auto header = InitHeader(dataCapacity, metadataCapacity);
            Map.ResizeAndRemap(0, header.DataOffset + header.DataCapacity);
            *Header() = header;
        } else {
            Map.Map(0, Map.Length());
        }

        if (Header()->Version == VERSION_PREV) {
            auto header = InitHeader(Header()->DataCapacity, metadataCapacity);
            Migrate(header);
        }

        ValidateStructure();

        if (Header()->MetadataCapacity != metadataCapacity) {
            ResizeMetadata(metadataCapacity);
        }

        Data = TEntriesData(
            GetMappedData(Header()->DataOffset, Header()->DataCapacity));

        ValidateDataStructure();
    }

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
        if (sz > Header()->DataCapacity) {
            return false;
        }
        auto writePos = Header()->WritePos;

        if (!Empty()) {
            // checking that we have a contiguous chunk of sz + 1 bytes
            // 1 extra byte is needed to distinguish between an empty buffer
            // and a buffer which is completely full
            if (Header()->ReadPos < Header()->WritePos) {
                // we have a single contiguous occupied region
                ui64 freeSpace = Header()->DataCapacity - Header()->WritePos;
                if (freeSpace < sz) {
                    if (Header()->ReadPos <= sz) {
                        // out of space
                        return false;
                    }
                    auto* eh = Data.GetEntryHeader(Header()->WritePos);
                    if (eh != nullptr) {
                        eh->DataSize = 0;
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
        auto cur = GetFrontEntry();
        if (!cur.HasValue()) {
            return;
        }

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
            visitor(e.Header->Checksum, e.GetData());
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

    ui64 GetRawCapacity() const
    {
        return Header()->DataCapacity;
    }

    ui64 GetRawUsedBytesCount() const
    {
        ui64 res =
            Header()->ReadPos > Header()->WritePos ? Header()->DataCapacity : 0;

        return res + Header()->WritePos - Header()->ReadPos;
    }

    ui64 GetMaxAllocationBytesCount() const
    {
        if (IsCorrupted()) {
            return 0;
        }

        ui64 maxRawSize = 0;
        if (Empty()) {
            maxRawSize = Header()->DataCapacity;
        } else if (Header()->ReadPos <= Header()->WritePos) {
            maxRawSize = Header()->DataCapacity - Header()->WritePos;
            if (Header()->ReadPos > 0) {
                maxRawSize = Max(maxRawSize, Header()->ReadPos - 1);
            }
        } else {
            maxRawSize = Header()->ReadPos - Header()->WritePos - 1;
        }
        return maxRawSize > sizeof(TEntryHeader)
                   ? maxRawSize - sizeof(TEntryHeader)
                   : 0;
    }

    bool ValidateMetadata() const
    {
        auto data =
            GetMappedData(Header()->MetadataOffset, Header()->MetadataCapacity);

        return Header()->MetadataSize <= data.size() &&
               Crc32c(data.data(), Header()->MetadataSize) ==
                   Header()->MetadataChecksum;
    }

    TStringBuf GetMetadata() const
    {
        auto data =
            GetMappedData(Header()->MetadataOffset, Header()->MetadataCapacity);

        Y_ABORT_UNLESS(Header()->MetadataSize <= data.size());

        return {data.data(), Header()->MetadataSize};
    }

    bool SetMetadata(TStringBuf buf)
    {
        if (buf.size() > Header()->MetadataCapacity) {
            return false;
        }

        auto data =
            GetMappedData(Header()->MetadataOffset, Header()->MetadataCapacity);

        Header()->MetadataSize = buf.size();
        Header()->MetadataChecksum = Crc32c(buf.data(), buf.size());
        buf.copy(data.data(), buf.size());
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

TFileRingBuffer::TFileRingBuffer(
        const TString& filePath,
        ui64 dataCapacity,
        ui64 metadataCapacity)
    : Impl(new TImpl(filePath, dataCapacity, metadataCapacity))
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

ui64 TFileRingBuffer::GetRawCapacity() const
{
    return Impl->GetRawCapacity();
}

ui64 TFileRingBuffer::GetRawUsedBytesCount() const
{
    return Impl->GetRawUsedBytesCount();
}

ui64 TFileRingBuffer::GetMaxAllocationBytesCount() const
{
    return Impl->GetMaxAllocationBytesCount();
}

bool TFileRingBuffer::ValidateMetadata() const
{
    return Impl->ValidateMetadata();
}

TStringBuf TFileRingBuffer::GetMetadata() const
{
    return Impl->GetMetadata();
}

bool TFileRingBuffer::SetMetadata(TStringBuf data)
{
    return Impl->SetMetadata(data);
}

}   // namespace NCloud
