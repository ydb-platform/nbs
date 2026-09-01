#include "file_ring_buffer.h"
#include "file_ring_buffer_accessor.h"
#include "file_ring_buffer_format.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/size_literals.h>
#include <util/stream/mem.h>
#include <util/string/builder.h>
#include <util/string/printf.h>
#include <util/system/align.h>
#include <util/system/compiler.h>
#include <util/system/filemap.h>

#include <atomic>
#include <optional>

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

NProto::TError MakeBufferIsCorruptError()
{
    return MakeError(E_INVALID_STATE, "Buffer is corrupted");
}

NProto::TError MakeInvalidPointerError()
{
    return MakeError(E_ARGUMENT, "Invalid pointer");
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

class TFileRingBuffer::TImpl
{
private:
    const TFileRingBufferArgs Args;
    TFileMapFileRingBufferAccessor Accessor;
    std::atomic<bool> Corrupted = false;

    ui64 MaxObservedEntryByteCount = 0;

    // Map of allocated entries: data ptr -> pos
    THashMap<const void*, ui64> EntryMap;

private:
    THeader* Header()
    {
        return Accessor.GetHeader();
    }

    const THeader* Header() const
    {
        return Accessor.GetHeader();
    }

    IFileRingBufferDataProcessor* Data()
    {
        return Accessor.GetDataProcessor();
    }

    const IFileRingBufferDataProcessor* Data() const
    {
        return Accessor.GetDataProcessor();
    }

    const TFileRingBufferCapabilities& Capabilities() const
    {
        return Accessor.GetCapabilities();
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

            const auto eh = Data()->ReadEntryHeader(pos);
            if (eh.DataSize != 0) {
                const auto* data = Data()->GetEntryDataPtr(pos, eh.DataSize);
                if (data == nullptr) {
                    return TEntryInfo::CreateInvalid();
                }

                // When entry header is not written atomically, we cannot be
                // sure that the checksum is not stale
                if (eh.FreeFlag && eh.DataChecksum != 0 &&
                    Capabilities().EntryHeaderIsProcessedAtomically)
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

        const auto eh = Data()->ReadEntryHeader(pos);
        if (eh.DataSize == 0) {
            return TEntryInfo::CreateInvalid();
        }

        const auto* data = Data()->GetEntryDataPtr(pos, eh.DataSize);
        if (data == nullptr) {
            return TEntryInfo::CreateInvalid();
        }

        if (pos + Data()->GetEntrySize(eh.DataSize) > Header()->WritePos) {
            return TEntryInfo::CreateInvalid();
        }

        // When entry header is not written atomically, we cannot be
        // sure that the checksum is not stale
        if (eh.FreeFlag && eh.DataChecksum != 0 &&
            Capabilities().EntryHeaderIsProcessedAtomically)
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
            ? GetEntry(e.ActualPos + Data()->GetEntrySize(e.Header.DataSize))
            : TEntryInfo::CreateInvalid();
    }

    // Sets corrupted state if Data()->WriteEntryHeader is failed
    bool WriteEntryHeader(ui64 pos, const TFileRingBufferEntryHeader& header)
    {
        // A compiler-only fence is sufficient here because there is no
        // concurrent access to the memory and we just need to ensure
        // that a compiler does not reorder writes.
        std::atomic_signal_fence(std::memory_order_seq_cst);

        auto success = Data()->WriteEntryHeader(pos, header);
        if (!success) {
            SetCorrupted(
                TStringBuilder() << "Cannot write entry header at " << pos);
        }
        return success;
    }

    void CopyMappedData(ui64 destPos, ui64 srcPos, ui64 size)
    {
        auto src = Accessor.GetRawData(srcPos, size);
        auto dst = Accessor.GetRawData(destPos, size);

        // Copied data regions cannot overlap
        Y_ABORT_UNLESS(destPos + size <= srcPos || srcPos + size <= destPos);

        MemCopy(dst.data(), src.data(), size);
    }

    bool ResizeAndRemap(size_t newSize)
    {
        auto status = Accessor.ResizeAndRemap(newSize);
        if (HasError(status)) {
            SetCorrupted(FormatError(status));
            return false;
        }
        return true;
    }

    bool IsMigrationNeeded() const
    {
        return !IsCorrupted() && Header()->Version != Args.Version;
    }

    void TryMigrate()
    {
        if (!IsMigrationNeeded()) {
            return;
        }

        // Migration to any version can be performed when the buffer is empty
        if (Empty()) {
            SetReadAndWritePosToZeroForEmptyBuffer();
            Header()->Version = Args.Version;
            Validate();
        }
    }

    bool ResizeMetadata(ui64 desiredMetadataCapacity)
    {
        // We cannot shrink below the existing metadata size
        const ui64 newMetadataCapacity =
            Max(desiredMetadataCapacity,
                static_cast<ui64>(Header()->MetadataSize));

        if (Header()->MetadataCapacity == newMetadataCapacity) {
            return true;
        }

        Header()->MetadataCapacity = static_cast<ui64>(Header()->MetadataSize);

        const ui64 newDataOffset = AlignUp(
            Header()->MetadataOffset + newMetadataCapacity,
            sizeof(ui64));

        const ui64 newFileSize = newDataOffset + Header()->DataCapacity;

        if (Header()->DataOffset != newDataOffset &&
            Header()->DataOffset < newFileSize)
        {
            // Move data to the temporary place
            const ui64 tempDataOffset = AlignUp(
                Max(newFileSize, Header()->DataOffset + Header()->DataCapacity),
                sizeof(ui64));

            const ui64 tempFileSize = tempDataOffset + Header()->DataCapacity;

            if (!ResizeAndRemap(tempFileSize) || !Validate()) {
                return false;
            }

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

        if (!ResizeAndRemap(newFileSize) || !Validate()) {
            return false;
        }

        Header()->MetadataCapacity = newMetadataCapacity;

        return Validate();
    }

    void VisitEntries(auto&& visitor)
    {
        auto e = GetFrontEntry();

        while (e.HasValue()) {
            visitor(e);
            e = GetNextEntry(e);
        }

        if (e.IsInvalid()) {
            SetCorrupted("Invalid entry detected at VisitEntries");
        }
    }

    void EraseFreeEntriesFromFront()
    {
        auto front = GetFrontEntry();
        while (front.HasValue() && front.GetFreeFlag() &&
               !EntryMap.contains(front.Data))
        {
            front = GetNextEntry(front);
        }

        if (front.IsInvalid()) {
            SetCorrupted("Invalid front entry");
        } else {
            Header()->ReadPos = front.ActualPos;
        }

        if (IsMigrationNeeded()) {
            TryMigrate();
        }
    }

    void WriteSlackSpaceMarker(ui64 pos)
    {
        Data()->WriteEntryHeader(pos, {});
    }

    void SetReadAndWritePosToZeroForEmptyBuffer()
    {
        Y_ABORT_UNLESS(Header()->ReadPos == Header()->WritePos);
        if (Header()->WritePos != 0) {
            // Ensure that the state can be restored from the intermediate state
            WriteSlackSpaceMarker(Header()->WritePos);
            // A compiler-only fence is sufficient here because there is no
            // concurrent access to the memory and we just need to ensure
            // that a compiler does not reorder writes.
            std::atomic_signal_fence(std::memory_order_seq_cst);
            Header()->WritePos = 0;
            std::atomic_signal_fence(std::memory_order_seq_cst);
            Header()->ReadPos = 0;
            std::atomic_signal_fence(std::memory_order_seq_cst);
        }
    }

    bool ValidateAccess(const char* name) const
    {
        if (IsCorrupted() || Header() == nullptr || Data() == nullptr) {
            ReportAccessToCorruptedFileRingBufferError(Sprintf(
                "An attempt to access an entry in a corrupted or "
                "non-initialized TFileRingBuffer from %s has been made",
                name));
            return false;
        }
        return true;
    }

public:
    explicit TImpl(const TFileRingBufferArgs& args)
        : Args(args)
        , Accessor(
              args.FilePath,
              EFileRingBufferAccessorValidationMode::Normal,
              TMemoryMapCommon::EOpenModeFlag::oRdWr)
    {
        Y_ABORT_UNLESS(
            IsSupportedFileRingBufferVersion(args.Version),
            "Unsupported requested FileRingBuffer version - %u",
            static_cast<ui32>(args.Version));

        auto mapResult = Accessor.Map();
        if (HasError(mapResult)) {
            SetCorrupted(FormatError(mapResult));
            return;
        }

        auto status = Accessor.ValidateAndInitialize();

        switch (status) {
            case EFileRingBufferAccessorValidationStatus::NotInitialized: {
                auto header = InitHeader(args);

                if (!ResizeAndRemap(header.DataOffset + header.DataCapacity)) {
                    return;
                }

                Y_ABORT_UNLESS(
                    sizeof(TFileRingBufferHeader) <=
                    Accessor.GetRawData().size());

                *reinterpret_cast<TFileRingBufferHeader*>(
                    Accessor.GetRawData().data()) = header;

                if (!Validate()) {
                    return;
                }
                break;
            }
            case EFileRingBufferAccessorValidationStatus::Failed:
                SetCorrupted(FormatError(Accessor.GetLastValidationError()));
                return;

            case EFileRingBufferAccessorValidationStatus::Success:
                break;
        }

        if (Header()->MetadataCapacity != Args.MetadataCapacity) {
            if (!ResizeMetadata(Args.MetadataCapacity)) {
                // Corruption happened
                return;
            }
        }

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

    TPushBackResult PushBack(TStringBuf data)
    {
        if (!ValidateAccess("PushBack")) {
            return TPushBackResult(MakeBufferIsCorruptError());
        }

        auto allocationResult = Alloc(data.size());
        if (allocationResult.AllocationPtr == nullptr) {
            return TPushBackResult(allocationResult.Error);
        }

        data.copy(allocationResult.AllocationPtr, data.size());

        auto commitResult =
            Commit(allocationResult.AllocationPtr, std::nullopt);

        if (HasError(commitResult)) {
            return TPushBackResult(commitResult);
        }

        return TPushBackResult(true);
    }

    TAllocResult Alloc(size_t size)
    {
        if (!ValidateAccess("Alloc")) {
            return TAllocResult(MakeBufferIsCorruptError());
        }

        if (size == 0) {
            return TAllocResult(MakeError(
                E_ARGUMENT,
                "Zero size allocations are not allowed"));
        }

        if (IsMigrationNeeded()) {
            // Return "storage is full" error.
            // Migration will happen when the buffer is emptied.
            return TAllocResult(nullptr);
        }

        if (size > Capabilities().MaxAllocationByteCount) {
            return TAllocResult(MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "Allocation data size (" << size
                    << ") exceeds maximum allowed size ("
                    << Capabilities().MaxAllocationByteCount << ")"));
        }

        const auto sz = Data()->GetEntrySize(size);
        if (sz > Header()->DataCapacity) {
            return TAllocResult(MakeError(
                E_ARGUMENT,
                TStringBuilder() << "Allocation entry size (" << sz
                                 << ") exceeds DataCapacity ("
                                 << Header()->DataCapacity << ")"));
        }
        auto writePos = Header()->WritePos;

        if (Empty()) {
            if (Header()->WritePos != 0) {
                // In order to fully utilize space when the buffer is empty,
                // we need to reset read and write positions
                SetReadAndWritePosToZeroForEmptyBuffer();
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
                        return TAllocResult(nullptr);
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
                    return TAllocResult(nullptr);
                }
            }
        }

        MaxObservedEntryByteCount =
            Max(MaxObservedEntryByteCount, size);

        char* ptr = Data()->GetEntryDataPtr(writePos, size);
        if (ptr == nullptr) {
            SetCorrupted(
                TStringBuilder() << "Cannot access data buffer at " << writePos
                                 << ", size = " << size);
            return TAllocResult(MakeBufferIsCorruptError());
        }

        auto [_, inserted] = EntryMap.insert({ptr, writePos});
        if (!inserted) {
            SetCorrupted(
                TStringBuilder() << "Duplicate allocation at " << writePos);
            return TAllocResult(MakeBufferIsCorruptError());
        }

        auto headerWritten = WriteEntryHeader(
            writePos,
            {.DataSize = static_cast<ui32>(size),
             .DataChecksum = 0,
             .Tag = 0,
             .FreeFlag = true});

        if (!headerWritten) {
            return TAllocResult(MakeBufferIsCorruptError());
        }

        // A compiler-only fence is sufficient here because there is no
        // concurrent access to the memory and we just need to ensure
        // that a compiler does not reorder writes.
        std::atomic_signal_fence(std::memory_order_seq_cst);

        Header()->WritePos = writePos + sz;

        return TAllocResult(ptr);
    }

    NProto::TError Commit(const void* ptr, std::optional<ui32> crc32c)
    {
        if (!ValidateAccess("Commit")) {
            return MakeBufferIsCorruptError();
        }

        const auto it = EntryMap.find(ptr);
        if (it == EntryMap.end()) {
            return MakeInvalidPointerError();
        }

        ui64 pos = it->second;

        auto eh = Data()->ReadEntryHeader(pos);
        if (!eh.FreeFlag || eh.DataSize == 0 || eh.DataChecksum != 0 ||
            eh.Tag != 0)
        {
            SetCorrupted(
                TStringBuilder()
                << "Invalid header for incomplete allocation at " << pos);
            return MakeBufferIsCorruptError();
        }

        if (!crc32c) {
            if (ptr != Data()->GetEntryDataPtr(pos, eh.DataSize)) {
                SetCorrupted(
                    TStringBuilder()
                    << "Invalid data pointer for incomplete allocation at "
                    << pos);
                return MakeBufferIsCorruptError();
            }
            crc32c = Crc32c(ptr, eh.DataSize);
        }

        eh.DataChecksum = *crc32c;
        eh.FreeFlag = false;

        if (!WriteEntryHeader(pos, eh)) {
            return MakeBufferIsCorruptError();
        }

        return {};
    }

    NProto::TError Free(const void* ptr)
    {
        if (!ValidateAccess("Free")) {
            return MakeBufferIsCorruptError();
        }

        auto it = EntryMap.find(ptr);
        if (it == EntryMap.end()) {
            return MakeInvalidPointerError();
        }

        auto pos = it->second;

        auto eh = Data()->ReadEntryHeader(pos);

        if (eh.FreeFlag) {
            // Releasing incomplete entries is not allowed
            return MakeInvalidPointerError();
        }

        if (eh.DataSize == 0) {
            SetCorrupted(
                TStringBuilder() << "Invalid header for allocation at " << pos);
            return MakeBufferIsCorruptError();
        }

        eh.DataChecksum = 0;
        eh.FreeFlag = true;

        if (!WriteEntryHeader(pos, eh)) {
            return MakeBufferIsCorruptError();
        }

        EntryMap.erase(it);

        EraseFreeEntriesFromFront();

        if (IsCorrupted()) {
            // EraseFreeEntriesFromFront() may set IsCorrupted flag
            return MakeBufferIsCorruptError();
        }

        return {};
    }

    ui32 GetMaxTag() const
    {
        return Capabilities().MaxTag;
    }

    TGetTagResult GetTag(const void* ptr) const
    {
        if (!ValidateAccess("GetTag")) {
            return TGetTagResult(MakeBufferIsCorruptError());
        }

        auto it = EntryMap.find(ptr);
        if (it == EntryMap.end()) {
            return TGetTagResult(MakeInvalidPointerError());
        }

        auto pos = it->second;

        auto eh = Data()->ReadEntryHeader(pos);
        if (eh.FreeFlag) {
            return TGetTagResult(MakeInvalidPointerError());
        }

        return TGetTagResult(eh.Tag);
    }

    NProto::TError SetTag(const void* ptr, ui32 tag)
    {
        if (!ValidateAccess("SetTag")) {
            return MakeBufferIsCorruptError();
        }

        auto it = EntryMap.find(ptr);
        if (it == EntryMap.end()) {
            return MakeInvalidPointerError();
        }

        auto pos = it->second;

        if (tag > Capabilities().MaxTag) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder() << "Tag value (" << tag
                                 << ") exceeds maximum allowed value ("
                                 << Capabilities().MaxTag << ")");
        }

        auto eh = Data()->ReadEntryHeader(pos);
        if (eh.FreeFlag) {
            return MakeInvalidPointerError();
        }

        eh.Tag = tag;

        bool written = WriteEntryHeader(pos, eh);
        if (!written) {
            return MakeBufferIsCorruptError();
        }

        return {};
    }

    TFrontResult Front()
    {
        if (!ValidateAccess("Front")) {
            return TFrontResult(MakeBufferIsCorruptError());
        }

        auto e = GetFrontEntry();

        if (e.IsInvalid()) {
            SetCorrupted("Invalid front entry");
            return TFrontResult(MakeBufferIsCorruptError());
        }

        if (!e.HasValue() || e.GetFreeFlag()) {
            return TFrontResult(TStringBuf());
        }

        return TFrontResult(e.GetData());
    }

    TPopFrontResult PopFront()
    {
        if (!ValidateAccess("PopFront")) {
            return TPopFrontResult(MakeBufferIsCorruptError());
        }

        auto e = GetFrontEntry();

        if (e.IsInvalid()) {
            SetCorrupted("Invalid front entry");
            return TPopFrontResult(MakeBufferIsCorruptError());
        }

        if (!e.HasValue()) {
            return TPopFrontResult(false);
        }

        auto status = Free(e.Data);

        if (HasError(status)) {
            return TPopFrontResult(status);
        }

        return TPopFrontResult(true);
    }

    ui64 Size() const
    {
        return EntryMap.size();
    }

    bool Empty() const
    {
        if (Header() == nullptr) {
            return true;
        }

        const bool result = Header()->ReadPos == Header()->WritePos;
        Y_DEBUG_ABORT_UNLESS(result == (EntryMap.size() == 0));
        return result;
    }

    bool Validate()
    {
        auto status = Accessor.ValidateAndInitialize();
        if (status != EFileRingBufferAccessorValidationStatus::Success) {
            SetCorrupted(FormatError(Accessor.GetLastValidationError()));
            return false;
        }
        return !IsCorrupted();
    }

    NProto::TError Visit(const TVisitor& visitor)
    {
        if (!ValidateAccess("Visit")) {
            return MakeBufferIsCorruptError();
        }

        VisitEntries(
            [&](const TEntryInfo& e)
            {
                if (!e.GetFreeFlag()) {
                    visitor(e.Header.DataChecksum, e.GetTag(), e.GetData());
                }
            });

        if (IsCorrupted()) {
            // VisitEntries may set IsCorrupted flag during entry enumeration
            return MakeBufferIsCorruptError();
        }

        return {};
    }

    bool IsCorrupted() const
    {
        return Corrupted.load(std::memory_order_relaxed);
    }

    void SetCorrupted(const TString& message)
    {
        auto prevValue = Corrupted.exchange(true);
        if (!prevValue) {
            ReportFileRingBufferCorruptionDetectedError(
                "Corruption detected in FileRingBuffer, path: " +
                Args.FilePath + ", message: " + message);
        }
    }

    ui64 GetRawCapacity() const
    {
        return Header() != nullptr ? Header()->DataCapacity : 0;
    }

    ui64 GetRawUsedBytesCount() const
    {
        if (Header() == nullptr) {
            return 0;
        }

        ui64 res =
            Header()->ReadPos > Header()->WritePos ? Header()->DataCapacity : 0;

        return res + Header()->WritePos - Header()->ReadPos;
    }

    ui32 GetVersion() const
    {
        return Header() != nullptr ? static_cast<ui32>(Header()->Version) : 0;
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

        return Data()->GetMaxAllocationByteCount(maxRawSize);
    }

    ui64 GetMaxSupportedAllocationByteCount() const
    {
        if (IsCorrupted()) {
            return 0;
        }

        return Capabilities().MaxAllocationByteCount;
    }

    TGetMetadataResult GetMetadata()
    {
        if (!ValidateAccess("GetMetadata")) {
            return TGetMetadataResult(MakeBufferIsCorruptError());
        }

        auto data = Accessor.GetRawMetadata();

        if (Header()->MetadataSize > data.size()) {
            SetCorrupted("Invalid MetadataSize");
            return TGetMetadataResult(MakeBufferIsCorruptError());
        }

        return TGetMetadataResult({data.data(), Header()->MetadataSize});
    }

    TSetMetadataResult SetMetadata(TStringBuf buf)
    {
        if (!ValidateAccess("SetMetadata")) {
            return TSetMetadataResult(MakeBufferIsCorruptError());
        }

        auto data = Accessor.GetRawMetadata();

        if (buf.size() > data.size()) {
            return TSetMetadataResult(false);
        }

        Header()->MetadataSize = buf.size();
        Header()->MetadataChecksum = Crc32c(buf.data(), buf.size());
        buf.copy(data.data(), buf.size());
        return TSetMetadataResult(true);
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

TFileRingBuffer::TPushBackResult TFileRingBuffer::PushBack(TStringBuf data)
{
    return Impl->PushBack(data);
}

TFileRingBuffer::TAllocResult TFileRingBuffer::Alloc(size_t size)
{
    return Impl->Alloc(size);
}

NProto::TError TFileRingBuffer::Commit(const void* ptr)
{
    return Impl->Commit(ptr, std::nullopt);
}

NProto::TError TFileRingBuffer::Commit(const void* ptr, ui32 crc32c)
{
    return Impl->Commit(ptr, crc32c);
}

NProto::TError TFileRingBuffer::Free(const void* ptr)
{
    return Impl->Free(ptr);
}

ui32 TFileRingBuffer::GetMaxTag() const
{
    return Impl->GetMaxTag();
}

TFileRingBuffer::TGetTagResult TFileRingBuffer::GetTag(const void* ptr) const
{
    return Impl->GetTag(ptr);
}

NProto::TError TFileRingBuffer::SetTag(const void* ptr, ui32 tag)
{
    return Impl->SetTag(ptr, tag);
}

TFileRingBuffer::TFrontResult TFileRingBuffer::Front()
{
    return Impl->Front();
}

TFileRingBuffer::TPopFrontResult TFileRingBuffer::PopFront()
{
    return Impl->PopFront();
}

ui64 TFileRingBuffer::Size() const
{
    return Impl->Size();
}

bool TFileRingBuffer::Empty() const
{
    return Impl->Empty();
}

bool TFileRingBuffer::Validate()
{
    return Impl->Validate();
}

NProto::TError TFileRingBuffer::Visit(const TVisitor& visitor)
{
    return Impl->Visit(visitor);
}

bool TFileRingBuffer::IsCorrupted() const
{
    return Impl->IsCorrupted();
}

void TFileRingBuffer::SetCorrupted()
{
    Impl->SetCorrupted("");
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

TFileRingBuffer::TGetMetadataResult TFileRingBuffer::GetMetadata() const
{
    return Impl->GetMetadata();
}

TFileRingBuffer::TSetMetadataResult TFileRingBuffer::SetMetadata(TStringBuf data)
{
    return Impl->SetMetadata(data);
}

}   // namespace NCloud
