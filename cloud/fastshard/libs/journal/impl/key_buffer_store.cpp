#include "key_buffer_store.h"

#include "device_page_store.h"

#include <cloud/fastshard/libs/journal/iface/device.h>

#include <cloud/storage/core/libs/common/future_helper.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/utility.h>
#include <util/string/builder.h>

#include <cstddef>
#include <cstring>
#include <functional>
#include <mutex>
#include <optional>
#include <type_traits>
#include <utility>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TInMemoryKeyBufferStore final: public IKeyBufferStore
{
private:
    std::mutex Lock;
    TMap<ui64, TBuffer> Buffers;
    TBuffer MetaBuffer;
    ui64 ErasedBelowKey = 0;

public:
    TFuture<TRestoreResult> Restore() override
    {
        TRestoredBuffers buffers;

        {
            std::lock_guard lock(Lock);

            buffers.MetaBuffer = MetaBuffer;
            buffers.KeyBuffers.reserve(Buffers.size());
            for (const auto& [key, buffer]: Buffers) {
                buffers.KeyBuffers.push_back({.Key = key, .Buffer = buffer});
            }
        }

        return MakeFuture<TRestoreResult>(std::move(buffers));
    }

    TFuture<NProto::TError> Write(ui64 key, TBuffer buffer) override
    {
        {
            std::lock_guard lock(Lock);

            if (key < ErasedBelowKey) {
                return MakeFuture(MakeError(
                    E_ARGUMENT,
                    TStringBuilder() << "key " << key << " is erased"));
            }

            Buffers[key] = std::move(buffer);
        }
        return MakeFuture(MakeError(S_OK));
    }

    TFuture<NProto::TError> WriteMeta(TBuffer buffer) override
    {
        {
            std::lock_guard lock(Lock);
            MetaBuffer = std::move(buffer);
        }
        return MakeFuture(MakeError(S_OK));
    }

    TFuture<NProto::TError> EraseBelow(ui64 key) override
    {
        std::lock_guard lock(Lock);

        if (key <= ErasedBelowKey) {
            return MakeFuture(MakeError(S_FALSE));
        }

        ErasedBelowKey = key;
        Buffers.erase(Buffers.begin(), Buffers.lower_bound(key));
        return MakeFuture(MakeError(S_OK));
    }
};

////////////////////////////////////////////////////////////////////////////////
// The on-device format of TDeviceKeyBufferStore.

constexpr ui64 SuperblockMagic = 0x4B4253'53555042ULL;   // KBS'SUPB
constexpr ui64 PageMagic = 0x4B4253'50414745ULL;         // KBS'PAGE
constexpr ui32 StoreFormatVersion = 1;

constexpr ui64 SuperblockSlotCount = 2;

// A page holds EntryCount entries, each behind a TEntryHeader, and carries a
// checksum over everything but the Crc field.
struct TPageHeader
{
    ui64 Magic = 0;
    ui32 Crc = 0;
    ui32 Version = 0;
    ui32 EntryCount = 0;
    ui32 Reserved = 0;
};

struct TEntryHeader
{
    ui64 Key = 0;
    ui64 Seq = 0;
    ui32 PayloadSize = 0;
    ui32 Reserved = 0;
};

// A superblock page holds the erased bound and the metadata buffer, which
// follows the header, and carries a checksum over everything but the Crc
// field.
struct TSuperblockHeader
{
    ui64 Magic = 0;
    ui64 Seq = 0;
    ui32 Crc = 0;
    ui32 Version = 0;
    ui64 ErasedBelowKey = 0;
    ui32 MetaSize = 0;
    ui32 Reserved = 0;
};

constexpr ui32 PageHeaderSize = sizeof(TPageHeader);
constexpr ui32 EntryHeaderSize = sizeof(TEntryHeader);
constexpr ui32 SuperblockHeaderSize = sizeof(TSuperblockHeader);

static_assert(PageHeaderSize == 24);
static_assert(EntryHeaderSize == 24);
static_assert(SuperblockHeaderSize == 40);

// The headers are copied to and from the pages as raw bytes, so they must
// be trivially copyable and must not contain padding.
static_assert(std::is_trivially_copyable_v<TPageHeader>);
static_assert(std::is_trivially_copyable_v<TEntryHeader>);
static_assert(std::is_trivially_copyable_v<TSuperblockHeader>);
static_assert(std::has_unique_object_representations_v<TPageHeader>);
static_assert(std::has_unique_object_representations_v<TEntryHeader>);
static_assert(std::has_unique_object_representations_v<TSuperblockHeader>);

constexpr ui64 MaxPagesPerReadRequest = 1024;

struct TEntry
{
    ui64 Key = 0;
    ui64 Seq = 0;
    TStringBuf Data;
};

struct TSuperblock
{
    ui64 Seq = 0;
    ui64 ErasedBelowKey = 0;
    TBuffer MetaBuffer;
};

NProto::TError ValidatePageSize(ui32 pageSize)
{
    if (pageSize < PageHeaderSize + EntryHeaderSize ||
        pageSize < SuperblockHeaderSize)
    {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "the page size " << pageSize
                             << " is too small for the store");
    }

    return MakeError(S_OK);
}

ui32 GetPageHeaderSize()
{
    return PageHeaderSize;
}

ui32 GetSerializedEntrySize(ui32 entryBufferSize)
{
    return EntryHeaderSize + entryBufferSize;
}

// The checksum of a page covers everything but the Crc field itself.
ui32 PageCrc(TStringBuf page)
{
    constexpr size_t crcOffset = offsetof(TPageHeader, Crc);
    constexpr size_t afterCrc = crcOffset + sizeof(TPageHeader::Crc);

    ui32 crc = Crc32c(page.data(), crcOffset);
    return Crc32cExtend(crc, page.data() + afterCrc, page.size() - afterCrc);
}

// Lays out |entries| in a page of |pageSize| bytes.
TResultOrError<TBuffer> MakePageBuffer(
    ui32 pageSize,
    const TVector<TEntry>& entries)
{
    TPageHeader header = {
        .Magic = PageMagic,
        .Version = StoreFormatVersion,
        .EntryCount = static_cast<ui32>(entries.size()),
    };

    TBuffer page(pageSize);
    page.Append(reinterpret_cast<const char*>(&header), sizeof(header));

    for (const auto& entry: entries) {
        if (page.Size() + EntryHeaderSize + entry.Data.size() > pageSize) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "the entry of " << entry.Data.size()
                    << " bytes under key " << entry.Key
                    << " does not fit the page of " << pageSize << " bytes");
        }

        TEntryHeader entryHeader = {
            .Key = entry.Key,
            .Seq = entry.Seq,
            .PayloadSize = static_cast<ui32>(entry.Data.size()),
        };

        page.Append(
            reinterpret_cast<const char*>(&entryHeader),
            sizeof(entryHeader));
        page.Append(entry.Data.data(), entry.Data.size());
    }

    page.Fill('\0', pageSize - page.Size());

    const ui32 crc = PageCrc({page.Data(), page.Size()});
    memcpy(page.Data() + offsetof(TPageHeader, Crc), &crc, sizeof(crc));

    return page;
}

// Returns the entries of a valid page, nothing for a page that holds anything
// else.
TVector<TEntry> ParsePageBuffer(TStringBuf page, ui32 pageSize)
{
    if (page.size() != pageSize) {
        return {};
    }

    TPageHeader header;
    memcpy(&header, page.data(), sizeof(header));

    if (header.Magic != PageMagic || header.Version != StoreFormatVersion ||
        header.Crc != PageCrc(page) || header.EntryCount == 0)
    {
        return {};
    }

    TVector<TEntry> entries;
    entries.reserve(header.EntryCount);

    ui64 offset = PageHeaderSize;
    for (ui32 i = 0; i < header.EntryCount; ++i) {
        if (page.size() - offset < EntryHeaderSize) {
            return {};
        }

        TEntryHeader entryHeader;
        memcpy(&entryHeader, page.data() + offset, sizeof(entryHeader));
        offset += EntryHeaderSize;

        if (page.size() - offset < entryHeader.PayloadSize) {
            return {};
        }

        entries.push_back({
            .Key = entryHeader.Key,
            .Seq = entryHeader.Seq,
            .Data = page.SubStr(offset, entryHeader.PayloadSize),
        });
        offset += entryHeader.PayloadSize;
    }

    return entries;
}

// The checksum of a superblock page covers everything but the Crc field
// itself.
ui32 SuperblockCrc(TStringBuf page)
{
    constexpr size_t crcOffset = offsetof(TSuperblockHeader, Crc);
    constexpr size_t afterCrc = crcOffset + sizeof(TSuperblockHeader::Crc);

    ui32 crc = Crc32c(page.data(), crcOffset);
    return Crc32cExtend(crc, page.data() + afterCrc, page.size() - afterCrc);
}

ui32 GetSuperblockMetaCapacity(ui32 pageSize)
{
    if (pageSize < SuperblockHeaderSize) {
        return 0;
    }

    return pageSize - SuperblockHeaderSize;
}

// Lays out the metadata buffer behind the header, fills in the magic, the
// version and the checksum. The buffer must fit the page.
TResultOrError<TBuffer> MakeSuperblockPage(
    const TSuperblock& superblock,
    ui32 pageSize)
{
    const auto& meta = superblock.MetaBuffer;

    if (pageSize < SuperblockHeaderSize + meta.Size()) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "The metadata buffer of " << meta.Size()
                             << " bytes does not fit the superblock page of "
                             << pageSize << " bytes");
    }

    TSuperblockHeader header = {
        .Magic = SuperblockMagic,
        .Seq = superblock.Seq,
        .Version = StoreFormatVersion,
        .ErasedBelowKey = superblock.ErasedBelowKey,
        .MetaSize = static_cast<ui32>(meta.Size()),
    };

    TBuffer page(pageSize);
    page.Append(reinterpret_cast<const char*>(&header), sizeof(header));
    page.Append(meta.Data(), meta.Size());
    page.Fill('\0', pageSize - page.Size());

    const ui32 crc = SuperblockCrc({page.Data(), page.Size()});
    memcpy(page.Data() + offsetof(TSuperblockHeader, Crc), &crc, sizeof(crc));

    return page;
}

// Returns the contents of a valid superblock page, nothing for a page that
// holds anything else.
std::optional<TSuperblock> ParseSuperblockPage(TStringBuf page, ui32 pageSize)
{
    if (page.size() != pageSize) {
        return std::nullopt;
    }

    TSuperblockHeader header;
    memcpy(&header, page.data(), sizeof(header));

    if (header.Magic != SuperblockMagic ||
        header.Version != StoreFormatVersion ||
        header.MetaSize > pageSize - SuperblockHeaderSize ||
        header.Crc != SuperblockCrc(page))
    {
        return std::nullopt;
    }

    TStringBuf meta = page.SubStr(SuperblockHeaderSize, header.MetaSize);

    return TSuperblock{
        .Seq = header.Seq,
        .ErasedBelowKey = header.ErasedBelowKey,
        .MetaBuffer = TBuffer(meta.data(), meta.size()),
    };
}

bool IsEmptyPage(TStringBuf page, ui32 pageSize)
{
    return page.size() == pageSize && !page[0] &&
           !memcmp(page.data(), page.data() + 1, page.size() - 1);
}

////////////////////////////////////////////////////////////////////////////////

// Stores the buffers on a device of PageCount pages of PageSize bytes.
//
// The first SuperblockSlotCount pages are the superblock slots, the rest hold
// entries. A page holds a TPageHeader, then EntryCount entries, each a
// TEntryHeader with the key, the seq of the write and the payload size
// followed by the payload. Every page carries a checksum. A buffer never
// spans pages, so it has to fit a page together with the headers. The store
// writes a single entry per page, a page that holds several of them is
// restored all the same.
//
// Each write and erase takes the next seq from a counter that only grows,
// also across restores. A write goes to a free page and the previous copy
// of the key is released after the new one has landed, so a torn rewrite
// keeps the old copy. Freed pages are not wiped, stale entries are told apart
// by their seq.
//
// The superblock holds the erased bound and the metadata buffer. EraseBelow
// and WriteMeta each write the whole superblock to the slot next to the
// current one, so a torn superblock write keeps the previous contents. Only
// one superblock write may be in flight at a time. The erased keys are
// refused right away, the pages are released once the superblock has landed.
//
// Restore reads the whole device, drops the keys below the bound of the
// newest superblock and keeps the highest seq of every key found in the valid
// pages. The pages of the winners are marked allocated, the seq counter
// resumes past the highest seq seen.

class TDeviceKeyBufferStore final
    : public IKeyBufferStore
    , public std::enable_shared_from_this<TDeviceKeyBufferStore>
{
private:
    // Where the live copy of a key is.
    struct TStoredKey
    {
        ui64 Seq = 0;
        ui64 PageNo = 0;
    };

    const IDevicePtr Device;
    const ui64 PageCount;
    const ui32 PageSize;
    const IDevicePageStorePtr Pages;

    TLog Log;

    std::mutex Lock;

    bool RestoreStarted = false;
    bool Restored = false;
    ui64 NextSeq = 1;
    TMap<ui64, TStoredKey> Keys;

    // The superblock the device holds.
    TSuperblock Superblock;

    bool SuperblockWriteInFlight = false;
    ui64 NextSuperblockSlot = 0;

    std::atomic_bool ReadOnly = false;

public:
    TDeviceKeyBufferStore(
        ILoggingServicePtr logging,
        IDevicePtr device,
        ui64 pageCount,
        ui32 pageSize);

    TFuture<TRestoreResult> Restore() override;

    TFuture<NProto::TError> Write(ui64 key, TBuffer buffer) override;

    TFuture<NProto::TError> WriteMeta(TBuffer buffer) override;

    TFuture<NProto::TError> EraseBelow(ui64 key) override;

private:
    TRestoreResult RestoreFromPages(const TVector<TStringBuf>& pages);

    // Applies |update| to a copy of the superblock and writes it to the next
    // slot.
    TFuture<NProto::TError> WriteSuperblock(
        const std::function<NProto::TError(TSuperblock&)>& update);

    NProto::TError OnEntryWritten(
        ui64 key,
        ui64 seq,
        ui64 pageNo,
        const TFuture<NProto::TError>& future);

    NProto::TError OnSuperblockWritten(
        TSuperblock superblock,
        const TFuture<NProto::TError>& future);
};

////////////////////////////////////////////////////////////////////////////////

TDeviceKeyBufferStore::TDeviceKeyBufferStore(
    ILoggingServicePtr logging,
    IDevicePtr device,
    ui64 pageCount,
    ui32 pageSize)
    : Device(std::move(device))
    , PageCount(pageCount)
    , PageSize(pageSize)
    , Pages(CreateDevicePageStore(Device, PageCount, PageSize))
    , Log(logging->CreateLog("BLOCKSTORE_JOURNALLED_DEVICE"))
{}

TFuture<IKeyBufferStore::TRestoreResult> TDeviceKeyBufferStore::Restore()
{
    if (PageCount <= SuperblockSlotCount) {
        return MakeFuture<TRestoreResult>(MakeError(
            E_ARGUMENT,
            TStringBuilder() << "the store needs more than "
                             << SuperblockSlotCount << " pages"));
    }

    auto error = ValidatePageSize(PageSize);
    if (HasError(error)) {
        return MakeFuture<TRestoreResult>(std::move(error));
    }

    {
        std::lock_guard lock(Lock);

        if (RestoreStarted) {
            return MakeFuture<TRestoreResult>(MakeError(
                E_INVALID_STATE,
                "the store is being restored already"));
        }
        RestoreStarted = true;
    }

    // the scan goes straight to the device - the page store knows
    // nothing about the pages before the restore
    TVector<TFuture<TResultOrError<TVector<TBuffer>>>> futures;

    for (ui64 offset = 0; offset < PageCount; offset += MaxPagesPerReadRequest)
    {
        TPageRangeRef ref{
            .FirstPageNo = offset,
            .PageCount = Min(MaxPagesPerReadRequest, PageCount - offset)};

        futures.push_back(Device->ReadPages({ref}));
    }

    return WaitAll(futures).Apply(
        [self = shared_from_this(),
         futures = std::move(futures)](const TFuture<void>&)
        {
            TVector<TVector<TBuffer>> reads;
            reads.reserve(futures.size());

            TVector<TStringBuf> pages;
            pages.reserve(self->PageCount);

            for (auto& future: futures) {
                auto result = UnsafeExtractValue(future);
                if (HasError(result)) {
                    return TRestoreResult(result.GetError());
                }

                const auto& read = reads.emplace_back(result.ExtractResult());
                for (const auto& page: read) {
                    pages.emplace_back(page.Data(), page.Size());
                }
            }

            if (pages.size() != self->PageCount) {
                return TRestoreResult(MakeError(
                    E_INVALID_STATE,
                    TStringBuilder()
                        << "the device returned " << pages.size()
                        << " pages, expected " << self->PageCount));
            }

            return self->RestoreFromPages(pages);
        });
}

TFuture<NProto::TError> TDeviceKeyBufferStore::Write(ui64 key, TBuffer buffer)
{
    if (ReadOnly.load()) {
        return MakeFuture(MakeError(E_INVALID_STATE, "read only mode"));
    }

    // an entry never spans pages
    if (buffer.Size() > PageSize ||
        GetPageHeaderSize() +
                GetSerializedEntrySize(static_cast<ui32>(buffer.Size())) >
            PageSize)
    {
        return MakeFuture(MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "the buffer of " << buffer.Size() << " bytes under key "
                << key << " does not fit the page of " << PageSize
                << " bytes"));
    }

    ui64 seq = 0;

    {
        std::lock_guard lock(Lock);

        if (!Restored) {
            return MakeFuture(
                MakeError(E_INVALID_STATE, "the store is not restored"));
        }

        if (key < Superblock.ErasedBelowKey) {
            return MakeFuture(MakeError(
                E_ARGUMENT,
                TStringBuilder() << "key " << key << " is erased"));
        }

        seq = NextSeq++;
    }

    TVector<TEntry> entries;
    entries.push_back({
        .Key = key,
        .Seq = seq,
        .Data = TStringBuf(buffer.Data(), buffer.Size()),
    });

    auto result = MakePageBuffer(PageSize, entries);
    if (HasError(result)) {
        return MakeFuture(result.GetError());
    }

    TVector<TBuffer> pages;
    pages.push_back(result.ExtractResult());

    auto locations = Pages->Allocate(1);
    if (locations.empty()) {
        return MakeFuture(MakeError(
            E_REJECTED,
            TStringBuilder() << "not enough free pages to store "
                             << buffer.Size() << " bytes under key " << key));
    }

    const ui64 pageNo = locations[0].FirstPageNo;

    return Pages->Write(locations, pages)
        .Apply([self = shared_from_this(), key, seq, pageNo](
                   const TFuture<NProto::TError>& future)
               { return self->OnEntryWritten(key, seq, pageNo, future); });
}

TFuture<NProto::TError> TDeviceKeyBufferStore::WriteMeta(TBuffer buffer)
{
    const ui64 capacity = GetSuperblockMetaCapacity(PageSize);
    if (buffer.Size() > capacity) {
        return MakeFuture(MakeError(
            E_ARGUMENT,
            TStringBuilder() << "the metadata buffer of " << buffer.Size()
                             << " bytes does not fit the superblock page, "
                             << capacity << " bytes are available"));
    }

    return WriteSuperblock(
        [&buffer](TSuperblock& superblock)
        {
            superblock.MetaBuffer = std::move(buffer);
            return MakeError(S_OK);
        });
}

TFuture<NProto::TError> TDeviceKeyBufferStore::EraseBelow(ui64 key)
{
    return WriteSuperblock(
        [key](TSuperblock& superblock)
        {
            if (key <= superblock.ErasedBelowKey) {
                return MakeError(S_FALSE);
            }

            superblock.ErasedBelowKey = key;
            return MakeError(S_OK);
        });
}

TFuture<NProto::TError> TDeviceKeyBufferStore::WriteSuperblock(
    const std::function<NProto::TError(TSuperblock&)>& update)
{
    if (ReadOnly.load()) {
        return MakeFuture(MakeError(E_INVALID_STATE, "read only mode"));
    }

    TSuperblock superblock;
    ui64 slot = 0;

    {
        std::lock_guard lock(Lock);

        if (!Restored) {
            return MakeFuture(
                MakeError(E_INVALID_STATE, "the store is not restored"));
        }

        superblock = Superblock;
        if (auto error = update(superblock); error.GetCode() != S_OK) {
            return MakeFuture(std::move(error));
        }

        if (SuperblockWriteInFlight) {
            return MakeFuture(
                MakeError(E_REJECTED, "A superblock write is in progress"));
        }

        superblock.Seq = NextSeq++;
        slot = NextSuperblockSlot;

        SuperblockWriteInFlight = true;
    }

    auto result = MakeSuperblockPage(superblock, PageSize);
    if (HasError(result)) {
        std::lock_guard lock(Lock);
        SuperblockWriteInFlight = false;
        return MakeFuture(result.GetError());
    }

    TVector<TBuffer> pages;
    pages.push_back(result.ExtractResult());

    return Pages->Write({{.FirstPageNo = slot, .PageCount = 1}}, pages)
        .Apply(
            [self = shared_from_this(), superblock = std::move(superblock)](
                const TFuture<NProto::TError>& future) mutable
            {
                return self->OnSuperblockWritten(std::move(superblock), future);
            });
}

IKeyBufferStore::TRestoreResult TDeviceKeyBufferStore::RestoreFromPages(
    const TVector<TStringBuf>& pages)
{
    // the superblock slots are never given to an entry
    auto error = Pages->AllocateAt(
        {{.FirstPageNo = 0, .PageCount = SuperblockSlotCount}});
    if (HasError(error)) {
        return error;
    }

    ui64 maxSeq = 0;

    std::optional<TSuperblock> superblock;
    ui64 superblockSlot = SuperblockSlotCount;

    for (ui64 slot = 0; slot < SuperblockSlotCount; ++slot) {
        auto parsed = ParseSuperblockPage(pages[slot], PageSize);
        if (!parsed) {
            continue;
        }

        maxSeq = Max(maxSeq, parsed->Seq);
        if (!superblock || superblock->Seq < parsed->Seq) {
            superblock = std::move(*parsed);
            superblockSlot = slot;
        }
    }

    if (!superblock) {
        // no bound has been persisted yet - an empty slot tells this apart
        // from a store that has lost its superblocks, a dirty one holds a
        // torn superblock and takes the next write
        for (ui64 slot = 0; slot < SuperblockSlotCount; ++slot) {
            if (IsEmptyPage(pages[slot], PageSize)) {
                superblockSlot = slot;
            }
        }
    }

    if (superblockSlot == SuperblockSlotCount) {
        return MakeError(E_INVALID_STATE, "all superblock slots are dirty");
    }

    // the newest copy of a key wins
    struct TWinner
    {
        ui64 PageNo = 0;
        TEntry Entry;
    };

    THashMap<ui64, TWinner> winners;

    for (ui64 i = SuperblockSlotCount; i < pages.size(); ++i) {
        for (const auto& entry: ParsePageBuffer(pages[i], PageSize)) {
            maxSeq = Max(maxSeq, entry.Seq);

            if (superblock && entry.Key < superblock->ErasedBelowKey) {
                continue;
            }

            auto [it, inserted] = winners.try_emplace(
                entry.Key,
                TWinner{.PageNo = i, .Entry = entry});

            if (!inserted && it->second.Entry.Seq < entry.Seq) {
                it->second = {.PageNo = i, .Entry = entry};
            }
        }
    }

    TVector<TKeyBuffer> buffers;
    buffers.reserve(winners.size());
    TMap<ui64, TStoredKey> keys;
    TVector<TPageRangeRef> pagesToAllocate;

    for (const auto& [key, winner]: winners) {
        const auto& data = winner.Entry.Data;
        buffers.push_back({
            .Key = key,
            .Buffer = TBuffer(data.data(), data.size()),
        });

        keys[key] = {.Seq = winner.Entry.Seq, .PageNo = winner.PageNo};
        pagesToAllocate.push_back(
            {.FirstPageNo = winner.PageNo, .PageCount = 1});
    }

    error = Pages->AllocateAt(pagesToAllocate);
    if (HasError(error)) {
        return error;
    }

    TBuffer metaBuffer;

    {
        std::lock_guard lock(Lock);

        Restored = true;
        NextSeq = maxSeq + 1;
        Keys = std::move(keys);

        if (superblock) {
            Superblock = std::move(*superblock);
        }
        NextSuperblockSlot = (superblockSlot + 1) % SuperblockSlotCount;

        metaBuffer = Superblock.MetaBuffer;
    }

    return TRestoredBuffers{
        .MetaBuffer = std::move(metaBuffer),
        .KeyBuffers = std::move(buffers),
    };
}

NProto::TError TDeviceKeyBufferStore::OnEntryWritten(
    ui64 key,
    ui64 seq,
    ui64 pageNo,
    const TFuture<NProto::TError>& future)
{
    auto error = future.GetValue();
    if (HasError(error)) {
        auto freeError = Pages->Free({{.FirstPageNo = pageNo, .PageCount = 1}});
        if (HasError(freeError)) {
            ReadOnly.store(true);
            STORAGE_ERROR(
                "failed to free the page of the failed write of key "
                << key << ": " << FormatError(freeError));
        }
        return error;
    }

    TVector<TPageRangeRef> stalePages;

    {
        std::lock_guard lock(Lock);

        if (key < Superblock.ErasedBelowKey) {
            // erased while being written - a restore would drop it
            stalePages.push_back({.FirstPageNo = pageNo, .PageCount = 1});
            error = MakeError(
                E_REJECTED,
                TStringBuilder()
                    << "key " << key << " was erased while being written");
        } else {
            auto [it, inserted] = Keys.try_emplace(key);
            auto& stored = it->second;

            if (inserted || stored.Seq < seq) {
                if (!inserted) {
                    stalePages.push_back(
                        {.FirstPageNo = stored.PageNo, .PageCount = 1});
                }

                stored = {.Seq = seq, .PageNo = pageNo};
            } else {
                // a newer write of the key has landed already
                stalePages.push_back({.FirstPageNo = pageNo, .PageCount = 1});
            }
        }
    }

    if (!stalePages.empty()) {
        auto freeError = Pages->Free(stalePages);
        if (HasError(freeError)) {
            ReadOnly.store(true);
            STORAGE_ERROR(
                "failed to free the stale pages of key "
                << key << ": " << FormatError(freeError));
        }
    }

    return error;
}

NProto::TError TDeviceKeyBufferStore::OnSuperblockWritten(
    TSuperblock superblock,
    const TFuture<NProto::TError>& future)
{
    auto error = future.GetValue();
    TVector<TPageRangeRef> pagesToFree;

    {
        std::lock_guard lock(Lock);

        SuperblockWriteInFlight = false;

        if (HasError(error)) {
            return error;
        }

        Superblock = std::move(superblock);
        NextSuperblockSlot = (NextSuperblockSlot + 1) % SuperblockSlotCount;

        auto end = Keys.lower_bound(Superblock.ErasedBelowKey);
        for (auto it = Keys.begin(); it != end; ++it) {
            pagesToFree.push_back(
                {.FirstPageNo = it->second.PageNo, .PageCount = 1});
        }
        Keys.erase(Keys.begin(), end);
    }

    if (!pagesToFree.empty()) {
        auto freeError = Pages->Free(pagesToFree);
        if (HasError(freeError)) {
            ReadOnly.store(true);
            STORAGE_ERROR(
                "failed to free the pages erased below key "
                << superblock.ErasedBelowKey << ": " << FormatError(freeError));
        }
    }

    return MakeError(S_OK);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IKeyBufferStorePtr CreateInMemoryKeyBufferStore()
{
    return std::make_shared<TInMemoryKeyBufferStore>();
}

IKeyBufferStorePtr CreateDeviceKeyBufferStore(
    ILoggingServicePtr logging,
    IDevicePtr device,
    ui64 pageCount,
    ui32 pageSize)
{
    return std::make_shared<TDeviceKeyBufferStore>(
        std::move(logging),
        std::move(device),
        pageCount,
        pageSize);
}

}   // namespace NCloud::NJournalled
