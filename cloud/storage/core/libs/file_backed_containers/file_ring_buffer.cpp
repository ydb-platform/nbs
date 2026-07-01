#include "file_ring_buffer.h"
#include "file_ring_buffer_format.h"

#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/generic/hash.h>
#include <util/generic/size_literals.h>
#include <util/stream/mem.h>
#include <util/string/builder.h>
#include <util/string/printf.h>
#include <util/system/align.h>
#include <util/system/compiler.h>
#include <util/system/filemap.h>

#include <span>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

using EVersion = EFileRingBufferVersion;
using THeader = TFileRingBufferHeader;
using TEntryHeader = TFileRingBufferEntryHeader;

constexpr ui64 INVALID_POS = Max<ui64>();

// Reserve some space after header so adding new fields will not require data
// migration
constexpr ui64 HeaderReserveSize = 256;

static_assert(sizeof(THeader) <= HeaderReserveSize);

////////////////////////////////////////////////////////////////////////////////

struct TFileRingBufferArgs
{
    TString FilePath;
    ui64 DataCapacity = 0;
    ui64 MetadataCapacity = 0;
    EFileRingBufferVersion Version = EFileRingBufferVersion::NotInitialized;
};

////////////////////////////////////////////////////////////////////////////////

struct TEntryInfo
{
    ui64 ActualPos = 0;
    TEntryHeader Header = {};
    const char* Data = nullptr;

    bool HasValue() const
    {
        return Header.DataSize != 0;
    }

    bool IsInvalid() const
    {
        return ActualPos == INVALID_POS;
    }

    bool GetFreeFlag() const
    {
        return Header.FreeFlag;
    }

    TStringBuf GetData() const
    {
        return HasValue() ? TStringBuf(Data, Header.DataSize)
                          : TStringBuf();
    }

    ui32 GetTag() const
    {
        return HasValue() ? Header.Tag : 0;
    }


    static TEntryInfo Create(
        ui64 pos,
        const TEntryHeader& header,
        const char* data)
    {
        Y_ABORT_UNLESS(pos != INVALID_POS);
        Y_ABORT_UNLESS(header.DataSize > 0);
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

THeader InitHeader(const TFileRingBufferArgs& args)
{
    THeader res;
    res.Version = args.Version;
    res.HeaderSize = sizeof(THeader);
    res.MetadataOffset = HeaderReserveSize;
    res.MetadataCapacity = args.MetadataCapacity;
    res.DataOffset =
        AlignUp(res.MetadataOffset + res.MetadataCapacity, sizeof(ui64));
    res.DataCapacity = args.DataCapacity;
    return res;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

class TFileRingBuffer::TImpl
{
private:
    const TFileRingBufferArgs Args;
    TFileMap Map;

    std::unique_ptr<IFileRingBufferDataProcessor> Data;
    TFileRingBufferCapabilities Capabilities;
    bool Corrupted = false;

    TEntryInfo CurrentAllocation = TEntryInfo::CreateInvalid();
    ui64 MaxObservedEntryByteCount = 0;

    // Map of non-free entries: data ptr -> pos
    THashMap<const void*, ui64> EntryMap;

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

            const auto eh = Data->ReadEntryHeader(pos);
            if (eh.DataSize != 0) {
                const auto* data = Data->GetEntryDataPtr(pos, eh.DataSize);
                if (data == nullptr) {
                    return TEntryInfo::CreateInvalid();
                }

                if (eh.FreeFlag && eh.DataChecksum != 0 &&
                    Capabilities.EntryHeaderIsCoveredByChecksum)
                {
                    return TEntryInfo::CreateInvalid();
                }

                return TEntryInfo::Create(pos, eh, data);
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

        const auto eh = Data->ReadEntryHeader(pos);
        if (eh.DataSize == 0) {
            return TEntryInfo::CreateInvalid();
        }

        const auto* data = Data->GetEntryDataPtr(pos, eh.DataSize);
        if (data == nullptr) {
            return TEntryInfo::CreateInvalid();
        }

        if (pos + Data->GetEntrySize(eh.DataSize) > Header()->WritePos) {
            return TEntryInfo::CreateInvalid();
        }

        if (eh.FreeFlag && eh.DataChecksum != 0 &&
            Capabilities.EntryHeaderIsCoveredByChecksum)
        {
            return TEntryInfo::CreateInvalid();
        }

        return TEntryInfo::Create(pos, eh, data);
    }

    TEntryInfo GetFrontEntry() const
    {
        return GetEntry(Header()->ReadPos);
    }

    TEntryInfo GetNextEntry(const TEntryInfo& e) const
    {
        return e.HasValue()
            ? GetEntry(e.ActualPos + Data->GetEntrySize(e.Header.DataSize))
            : TEntryInfo::CreateInvalid();
    }

    void CreateDataProcessor(EVersion version)
    {
        Data = CreateFileRingBufferDataProcessor(
            version,
            GetMappedData(Header()->DataOffset, Header()->DataCapacity));

        Capabilities = Data->GetCapabilities(true);
    }

    void ValidateStructure()
    {
        const ui64 mapLength = static_cast<ui64>(Map.Length());
        const auto& h = *Header();

        Y_ABORT_UNLESS(sizeof(THeader) == h.HeaderSize);
        Y_ABORT_UNLESS(h.HeaderSize <= h.MetadataOffset);
        Y_ABORT_UNLESS(h.MetadataOffset <= h.DataOffset);
        Y_ABORT_UNLESS(h.MetadataCapacity <= h.DataOffset - h.MetadataOffset);
        Y_ABORT_UNLESS(h.DataOffset <= mapLength);
        Y_ABORT_UNLESS(h.DataCapacity <= mapLength - h.DataOffset);
    }

    void ValidateDataStructure()
    {
        TEntryInfo cur = GetFrontEntry();

        if (cur.IsInvalid()) {
            SetCorrupted();
            return;
        }

        if (cur.ActualPos != Header()->ReadPos) {
            if (cur.ActualPos == 0 && Header()->WritePos == 0) {
                // Valid situation when Alloc was interrupted for empty buffer
                SyncAndUpdateReadPosition(0);
            } else {
                SetCorrupted();
                return;
            }
        }

        while (cur.HasValue()) {
            cur = GetNextEntry(cur);
        }

        if (cur.IsInvalid() || cur.ActualPos != Header()->WritePos) {
            SetCorrupted();
        }
    }

    void ResizeAndRemap(ui64 fileSize)
    {
        Map.ResizeAndRemap(0, fileSize);
        // File can be mapped to a different address - we should invalidate
        // the data processor working with the old address range
        Data.reset();
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

    bool IsMigrationNeeded() const
    {
        return Header()->Version != Args.Version;
    }

    void TryMigrate()
    {
        if (!IsMigrationNeeded() || IsCorrupted()) {
            return;
        }

        // Migration to any version can be performed when the buffer is empty
        if (Empty()) {
            Header()->Version = Args.Version;
            CreateDataProcessor(Args.Version);
            return;
        }

        // WriteBackCache needs functionality of V5+ (tags) for entries created
        // in older version (V4) - we need to make migration immediately to
        // at least V5 in order to provide this functionality.
        // Transition V4 -> V5 can be made without emptying the buffer.
        if (Header()->Version == EVersion::V4 && Args.Version > EVersion::V4) {
            // Parse entry headers using newer version
            CreateDataProcessor(EVersion::V5);

            // In the new version, the highest bits of the entry size are
            // treated as a tag value - need to ensure that they are not used
            VisitEntries([](const TEntryInfo& e)
                         { Y_ABORT_UNLESS(e.Header.Tag == 0); });

            Header()->Version = EVersion::V5;
        }
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

    void VisitEntries(auto&& visitor)
    {
        auto e = GetFrontEntry();

        while (e.HasValue()) {
            visitor(e);
            e = GetNextEntry(e);
        }

        if (e.IsInvalid()) {
            SetCorrupted();
        }
    }

    void EraseFreeEntriesFromFront()
    {
        auto front = GetFrontEntry();
        while (front.HasValue() && front.GetFreeFlag()) {
            front = GetNextEntry(front);
        }

        if (front.IsInvalid()) {
            SetCorrupted();
        } else {
            SyncAndUpdateReadPosition(front.ActualPos);
        }

        if (IsMigrationNeeded()) {
            TryMigrate();
        }
    }

    void WriteSlackSpaceMarker(ui64 pos)
    {
        Data->WriteEntryHeader(pos, {});
    }

    void SyncLoad()
    {
        // It is important to use the same shared memory address because
        // paired release-acquire operations guarantee ordering and visibility
        // of memory writes only when using the same atomic.

        auto readPos = __atomic_load_n(&Header()->ReadPos, __ATOMIC_ACQUIRE);
        auto writePos = __atomic_load_n(&Header()->WritePos, __ATOMIC_ACQUIRE);

        Y_UNUSED(readPos);
        Y_UNUSED(writePos);
    }

    // Ensures that the prior writes are visible before updating read position
    void SyncAndUpdateReadPosition(ui64 pos)
    {
        __atomic_store_n(&Header()->ReadPos, pos, __ATOMIC_RELEASE);
    }

    // Ensures that the prior writes are visible before updating write position
    void SyncAndUpdateWritePosition(ui64 pos)
    {
        __atomic_store_n(&Header()->WritePos, pos, __ATOMIC_RELEASE);
    }

    bool ValidateAccess(const char* name) const
    {
        if (IsCorrupted()) {
            ReportAccessToCorruptedFileRingBufferError(Sprintf(
                "An attempt to access an entry in a corrupted TFileRingBuffer "
                "from %s has been made",
                name));
            return false;
        }
        return true;
    }

public:
    explicit TImpl(const TFileRingBufferArgs& args)
        : Args(args)
        , Map(args.FilePath, TMemoryMapCommon::oRdWr)
    {
        Y_ABORT_UNLESS(
            IsSupportedFileRingBufferVersion(args.Version),
            "Unsupported requested FileRingBuffer version - %u",
            static_cast<ui32>(args.Version));

        if (static_cast<ui64>(Map.Length()) < sizeof(THeader)) {
            auto header = InitHeader(args);
            Map.ResizeAndRemap(0, header.DataOffset + header.DataCapacity);
            *Header() = header;
        } else {
            Map.Map(0, Map.Length());
        }

        Y_ABORT_UNLESS(
            IsSupportedFileRingBufferVersion(
                static_cast<EVersion>(Header()->Version)),
            "Unsupported current FileRingBuffer version - %u, file: %s",
            static_cast<ui32>(Header()->Version),
            args.FilePath.c_str());

        // We need to maintain data consistency on crash when another process
        // starts working with the same shared memory (memory-mapped file).
        //
        // We need to ensure that memory writes are observed in the correct
        // order - it is achieved by treating ReadPos and WritePos pointing
        // to shared memory as atomics and using acquire and release semantics.
        //
        // Note that concurrent access to the buffer is still not allowed and
        // should be managed at a higher level.
        SyncLoad();

        ValidateStructure();

        if (Header()->MetadataCapacity != Args.MetadataCapacity) {
            ResizeMetadata(Args.MetadataCapacity);
        }

        CreateDataProcessor(Header()->Version);

        ValidateDataStructure();

        VisitEntries(
            [&](const TEntryInfo& e)
            {
                if (!e.GetFreeFlag()) {
                    EntryMap[e.Data] = e.ActualPos;
                    MaxObservedEntryByteCount = Max<ui64>(
                        MaxObservedEntryByteCount,
                        e.Header.DataSize);
                }
            });

        if (!IsCorrupted()) {
            EraseFreeEntriesFromFront();
        }
    }

    bool PushBack(TStringBuf data)
    {
        if (!ValidateAccess("PushBack")) {
            return false;
        }

        auto allocationStatus = Alloc(data.size());
        if (HasError(allocationStatus) ||
            allocationStatus.GetResult() == nullptr)
        {
            return false;
        }

        data.copy(allocationStatus.GetResult(), data.size());

        return Commit();
    }

    TResultOrError<char*> Alloc(size_t size)
    {
        if (CurrentAllocation.HasValue()) {
            return MakeError(
                E_INVALID_STATE,
                "Previous allocation is not committed");
        }

        if (!ValidateAccess("Alloc")) {
            return MakeError(E_INVALID_STATE, "Buffer is corrupted");
        }

        if (size == 0) {
            return MakeError(
                E_ARGUMENT,
                "Zero size allocations are not allowed");
        }

        if (IsMigrationNeeded()) {
            // Return "storage is full" error.
            // Migration will happen when the buffer is emptied.
            return nullptr;
        }

        if (size > Capabilities.MaxAllocationByteCount) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder() << "Allocation data size (" << size
                                 << ") exceeds maximum allowed size ("
                                 << Capabilities.MaxAllocationByteCount << ")");
        }

        const auto sz = Data->GetEntrySize(size);
        if (sz > Header()->DataCapacity) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder() << "Allocation entry size (" << sz
                                 << ") exceeds DataCapacity ("
                                 << Header()->DataCapacity << ")");
        }
        auto writePos = Header()->WritePos;

        if (Empty()) {
            if (Header()->WritePos != 0) {
                // In order to fully utilize space when the buffer is empty,
                // we need to reset both ReadPos and WritePos to 0.
                //
                // We cannot do this atomically - we need to ensure that
                // the state can be restored from the intermediate state.

                WriteSlackSpaceMarker(Header()->WritePos);
                SyncAndUpdateWritePosition(0);
                SyncAndUpdateReadPosition(0);
                writePos = 0;
            }
        } else {
            // checking that we have a contiguous chunk of sz + 1 bytes
            // 1 extra byte is needed to distinguish between an empty buffer
            // and a buffer which is completely full
            if (Header()->ReadPos < Header()->WritePos) {
                // we have a single contiguous occupied region
                ui64 freeSpace = Header()->DataCapacity - Header()->WritePos;
                if (freeSpace < sz) {
                    if (Header()->ReadPos <= sz) {
                        // out of space
                        return nullptr;
                    }
                    WriteSlackSpaceMarker(Header()->WritePos);
                    writePos = 0;
                }
            } else {
                // we have two occupied regions
                ui64 freeSpace = Header()->ReadPos - Header()->WritePos;
                // there should remain free space between the occupied regions
                if (freeSpace <= sz) {
                    // out of space
                    return nullptr;
                }
            }
        }

        MaxObservedEntryByteCount =
            Max(MaxObservedEntryByteCount, size);

        char* ptr = Data->GetEntryDataPtr(writePos, size);
        Y_ABORT_UNLESS(ptr != nullptr);

        CurrentAllocation = TEntryInfo::Create(
            writePos,
            {.DataSize = static_cast<ui32>(size)},
            ptr);

        return ptr;
    }

    bool Commit()
    {
        if (!CurrentAllocation.HasValue()) {
            return false;
        }

        CurrentAllocation.Header.DataChecksum =
            Crc32c(CurrentAllocation.Data, CurrentAllocation.Header.DataSize);

        bool written = Data->WriteEntryHeader(
            CurrentAllocation.ActualPos,
            CurrentAllocation.Header);

        Y_ABORT_UNLESS(written);

        SyncAndUpdateWritePosition(
            CurrentAllocation.ActualPos +
            Data->GetEntrySize(CurrentAllocation.Header.DataSize));

        EntryMap[CurrentAllocation.Data] = CurrentAllocation.ActualPos;

        CurrentAllocation = TEntryInfo::CreateInvalid();
        return true;
    }

    bool Free(const void* ptr)
    {
        if (!ValidateAccess("Free")) {
            return false;
        }

        auto it = EntryMap.find(ptr);
        if (it == EntryMap.end()) {
            return false;
        }

        auto eh = Data->ReadEntryHeader(it->second);
        eh.DataChecksum = 0;
        eh.FreeFlag = true;
        Data->WriteEntryHeader(it->second, eh);

        EntryMap.erase(it);

        EraseFreeEntriesFromFront();

        return true;
    }

    ui32 GetMaxTag() const
    {
        return Capabilities.MaxTag;
    }

    ui32 GetTag(const void* ptr) const
    {
        if (!ValidateAccess("GetTag")) {
            return 0;
        }

        auto it = EntryMap.find(ptr);
        if (it == EntryMap.end()) {
            return 0;
        }

        auto eh = Data->ReadEntryHeader(it->second);
        return eh.Tag;
    }

    void SetTag(const void* ptr, ui32 tag)
    {
        if (!ValidateAccess("SetTag")) {
            return;
        }

        auto it = EntryMap.find(ptr);
        if (it == EntryMap.end()) {
            return;
        }

        auto eh = Data->ReadEntryHeader(it->second);
        eh.Tag = tag;
        Data->WriteEntryHeader(it->second, eh);
    }

    TStringBuf Front()
    {
        if (!ValidateAccess("Front")) {
            return {};
        }

        auto e = GetFrontEntry();

        if (e.IsInvalid()) {
            SetCorrupted();
            return {};
        }

        return e.GetData();
    }

    void PopFront()
    {
        if (!ValidateAccess("PopFront")) {
            return;
        }

        auto cur = GetFrontEntry();
        if (!cur.HasValue()) {
            return;
        }

        Free(cur.Data);
    }

    ui64 Size() const
    {
        return EntryMap.size();
    }

    bool Empty() const
    {
        const bool result = Header()->ReadPos == Header()->WritePos;
        Y_DEBUG_ABORT_UNLESS(result == (EntryMap.size() == 0));
        return result;
    }

    auto ValidateEntriesChecksums()
    {
        TVector<TBrokenFileEntry> entries;

        Visit([&] (ui32 checksum, ui32 tag, TStringBuf entry) {
            Y_UNUSED(tag);
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
        if (!ValidateAccess("Visit")) {
            return;
        }

        VisitEntries(
            [&](const TEntryInfo& e)
            {
                if (!e.GetFreeFlag()) {
                    visitor(e.Header.DataChecksum, e.GetTag(), e.GetData());
                }
            });
    }

    bool IsCorrupted() const
    {
        return Corrupted;
    }

    void SetCorrupted()
    {
        if (!Corrupted) {
            Corrupted = true;
            ReportFileRingBufferCorruptionDetectedError(
                "Corruption detected in FileRingBuffer, path: " +
                Map.GetFile().GetName());
        }
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

    ui32 GetVersion() const
    {
        return static_cast<ui32>(Header()->Version);
    }

    ui64 GetMaxObservedEntryByteCount() const
    {
        return MaxObservedEntryByteCount;
    }

    ui64 GetAvailableByteCount() const
    {
        if (IsCorrupted()) {
            return 0;
        }

        if (IsMigrationNeeded()) {
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

        return Data->GetMaxAllocationByteCount(maxRawSize);
    }

    ui64 GetMaxSupportedAllocationByteCount() const
    {
        if (IsCorrupted()) {
            return 0;
        }

        return Capabilities.MaxAllocationByteCount;
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
    ui64 metadataCapacity,
    EFileRingBufferVersion version)
    : Impl(new TImpl(
          {.FilePath = filePath,
           .DataCapacity = dataCapacity,
           .MetadataCapacity = metadataCapacity,
           .Version = version}))
{}

TFileRingBuffer::~TFileRingBuffer() = default;

bool TFileRingBuffer::PushBack(TStringBuf data)
{
    return Impl->PushBack(data);
}

TResultOrError<char*> TFileRingBuffer::Alloc(size_t size)
{
    return Impl->Alloc(size);
}

bool TFileRingBuffer::Commit()
{
    return Impl->Commit();
}

bool TFileRingBuffer::Free(const void* ptr)
{
    return Impl->Free(ptr);
}

ui32 TFileRingBuffer::GetMaxTag() const
{
    return Impl->GetMaxTag();
}

ui32 TFileRingBuffer::GetTag(const void* ptr) const
{
    return Impl->GetTag(ptr);
}

void TFileRingBuffer::SetTag(const void* ptr, ui32 tag)
{
    Impl->SetTag(ptr, tag);
}

TStringBuf TFileRingBuffer::Front()
{
    return Impl->Front();
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

void TFileRingBuffer::SetCorrupted()
{
    Impl->SetCorrupted();
}

ui64 TFileRingBuffer::GetRawCapacity() const
{
    return Impl->GetRawCapacity();
}

ui64 TFileRingBuffer::GetRawUsedBytesCount() const
{
    return Impl->GetRawUsedBytesCount();
}

ui32 TFileRingBuffer::GetVersion() const
{
    return Impl->GetVersion();
}

ui64 TFileRingBuffer::GetMaxObservedEntryByteCount() const
{
    return Impl->GetMaxObservedEntryByteCount();
}

ui64 TFileRingBuffer::GetAvailableByteCount() const
{
    return Impl->GetAvailableByteCount();
}

ui64 TFileRingBuffer::GetMaxSupportedAllocationByteCount() const
{
    return Impl->GetMaxSupportedAllocationByteCount();
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
