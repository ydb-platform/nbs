#include "key_buffer_store.h"

#include "device.h"
#include "device_page_store.h"

#include <cloud/storage/core/libs/common/future_helper.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/digest/multi.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/utility.h>
#include <util/generic/ymath.h>
#include <util/string/builder.h>
#include <util/system/spinlock.h>
#include <util/system/yassert.h>

#include <cstddef>
#include <cstring>
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
    TAdaptiveLock Lock;
    TMap<ui64, TBuffer> Buffers;
    ui64 ErasedBelowKey = 0;

public:
    TFuture<TRestoreResult> Restore() override
    {
        TVector<TKeyBuffer> buffers;

        with_lock (Lock) {
            buffers.reserve(Buffers.size());
            for (const auto& [key, buffer]: Buffers) {
                buffers.push_back({.Key = key, .Buffer = buffer});
            }
        }

        return MakeFuture<TRestoreResult>(std::move(buffers));
    }

    TFuture<NCloud::NProto::TError> Write(ui64 key, TBuffer buffer) override
    {
        with_lock (Lock) {
            if (key < ErasedBelowKey) {
                return MakeFuture(MakeError(
                    E_ARGUMENT,
                    TStringBuilder() << "key " << key << " is erased"));
            }

            Buffers[key] = std::move(buffer);
        }
        return MakeFuture(MakeError(S_OK));
    }

    TFuture<NCloud::NProto::TError> EraseBelow(ui64 key) override
    {
        with_lock (Lock) {
            ErasedBelowKey = Max(ErasedBelowKey, key);

            auto end = Buffers.lower_bound(key);
            if (end == Buffers.begin()) {
                return MakeFuture(MakeError(S_FALSE));
            }

            Buffers.erase(Buffers.begin(), end);
            return MakeFuture(MakeError(S_OK));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////
// The on-device format of TDeviceKeyBufferStore.

constexpr ui64 SuperblockMagic = 0x4B4253'53555042ULL;   // KBS'SUPB
constexpr ui64 EntryMagic = 0x4B4253'454E5452ULL;        // KBS'ENTR
constexpr ui32 StoreFormatVersion = 1;

constexpr ui64 SuperblockSlotCount = 2;

struct TEntryHeader
{
    ui64 Magic = 0;
    ui64 Seq = 0;
    ui64 Key = 0;
    ui64 PayloadSize = 0;
    ui32 Version = 0;
    ui32 ChunkIndex = 0;
    ui32 ChunkCount = 0;
    ui32 Crc = 0;
};

struct TSuperblock
{
    ui64 Magic = 0;
    ui64 Seq = 0;
    ui64 ErasedBelowKey = 0;
    ui32 Version = 0;
    ui32 Crc = 0;
};

constexpr ui32 EntryHeaderSize = sizeof(TEntryHeader);
constexpr ui32 SuperblockSize = sizeof(TSuperblock);

static_assert(EntryHeaderSize == 48);
static_assert(SuperblockSize == 32);

// The headers are copied to and from the pages as raw bytes, so they must
// be trivially copyable and must not contain padding.
static_assert(std::is_trivially_copyable_v<TEntryHeader>);
static_assert(std::is_trivially_copyable_v<TSuperblock>);
static_assert(std::has_unique_object_representations_v<TEntryHeader>);
static_assert(std::has_unique_object_representations_v<TSuperblock>);

constexpr ui64 MaxPagesPerReadRequest = 1024;

struct TEntryPage
{
    TEntryHeader Header;
    TStringBuf Chunk;
};

// Identifies the version of a key written under a particular seq.
struct TEntryId
{
    ui64 Key = 0;
    ui64 Seq = 0;

    bool operator==(const TEntryId&) const = default;
};

struct TEntryIdHash
{
    size_t operator()(const TEntryId& id) const
    {
        return MultiHash(id.Key, id.Seq);
    }
};

ui64 ChunkCountFor(ui64 payloadSize, ui64 chunkCapacity)
{
    return Max<ui64>(1, CeilDiv(payloadSize, chunkCapacity));
}

ui64 ChunkSizeOf(const TEntryHeader& header, ui64 chunkCapacity)
{
    if (header.ChunkIndex + 1 < header.ChunkCount) {
        return chunkCapacity;
    }
    return header.PayloadSize - chunkCapacity * header.ChunkIndex;
}

TBuffer MakeEntryPage(TEntryHeader header, TStringBuf chunk, ui32 pageSize)
{
    header.Magic = EntryMagic;
    header.Version = StoreFormatVersion;
    header.Crc = Crc32c(&header, offsetof(TEntryHeader, Crc));
    header.Crc = Crc32cExtend(header.Crc, chunk.data(), chunk.size());

    TBuffer page(pageSize);
    page.Append(reinterpret_cast<const char*>(&header), sizeof(header));
    page.Append(chunk.data(), chunk.size());
    page.Fill('\0', pageSize - page.Size());

    return page;
}

// Returns the header and the chunk of a valid entry page, nothing for a page
// that holds anything else.
std::optional<TEntryPage> ParseEntryPage(TStringBuf page, ui32 pageSize)
{
    if (page.size() != pageSize) {
        return std::nullopt;
    }

    TEntryHeader header;
    memcpy(&header, page.data(), sizeof(header));

    if (header.Magic != EntryMagic || header.Version != StoreFormatVersion) {
        return std::nullopt;
    }

    const ui64 chunkCapacity = pageSize - EntryHeaderSize;
    if (header.ChunkIndex >= header.ChunkCount ||
        header.ChunkCount !=
            ChunkCountFor(header.PayloadSize, chunkCapacity))
    {
        return std::nullopt;
    }

    const ui64 chunkSize = ChunkSizeOf(header, chunkCapacity);
    TStringBuf chunk = page.SubStr(EntryHeaderSize, chunkSize);

    ui32 expectedCrc = Crc32c(page.data(), offsetof(TEntryHeader, Crc));
    expectedCrc = Crc32cExtend(expectedCrc, chunk.data(), chunk.size());

    if (header.Crc != expectedCrc) {
        return std::nullopt;
    }

    return TEntryPage{.Header = header, .Chunk = chunk};
}

// Fills in the magic, the version and the checksum of the header.
TBuffer MakeSuperblockPage(TSuperblock header, ui32 pageSize)
{
    header.Magic = SuperblockMagic;
    header.Version = StoreFormatVersion;
    header.Crc = Crc32c(&header, offsetof(TSuperblock, Crc));

    TBuffer page(pageSize);
    page.Append(reinterpret_cast<const char*>(&header), sizeof(header));
    page.Fill('\0', pageSize - page.Size());

    return page;
}

std::optional<TSuperblock> ParseSuperblockPage(TStringBuf page, ui32 pageSize)
{
    if (page.size() != pageSize) {
        return std::nullopt;
    }

    TSuperblock header;
    memcpy(&header, page.data(), sizeof(header));

    if (header.Magic != SuperblockMagic ||
        header.Version != StoreFormatVersion ||
        header.Crc != Crc32c(page.data(), offsetof(TSuperblock, Crc)))
    {
        return std::nullopt;
    }

    return header;
}

bool IsEmptyPage(TStringBuf page, ui32 pageSize)
{
    return page.size() == pageSize && !page[0] &&
           !memcmp(page.data(), page.data() + 1, page.size() - 1);
}

// Merges the consecutive page numbers into ranges.
TVector<TPageRange> ToPageRanges(const TVector<ui64>& pageNos)
{
    TVector<TPageRange> ranges;

    for (ui64 pageNo: pageNos) {
        if (!ranges.empty() &&
            ranges.back().FirstPageNo + ranges.back().PageCount == pageNo)
        {
            ++ranges.back().PageCount;
            continue;
        }

        ranges.push_back({.FirstPageNo = pageNo, .PageCount = 1});
    }

    return ranges;
}

////////////////////////////////////////////////////////////////////////////////

// Stores the buffers on a device of PageCount pages of PageSize bytes.
//
// The first SuperblockSlotCount pages are the superblock slots, the rest hold
// entries. A buffer is split into chunks of ChunkCapacity bytes, one page per
// chunk behind a TEntryHeader with the key, the seq of the write, the chunk
// index and the chunk count. Every page carries a checksum.
//
// Each write and erase takes the next seq from a counter that only grows,
// also across restores. A write goes to free pages only and the previous copy
// of the key is released after the new one has landed, so a torn rewrite
// keeps the old copy. Freed pages are not wiped, stale entries are told apart
// by their seq.
//
// EraseBelow persists the bound in a superblock written to the slot next to
// the current one, so a torn superblock write keeps the previous bound. The
// erased keys are refused right away, the pages are released once the
// superblock has landed. Only one erase may be in flight at a time.
//
// Restore reads the whole device, groups the valid pages into candidates by
// (key, seq), drops the incomplete ones and the keys below the bound of the
// newest superblock, and keeps the highest seq of every key. The pages of the
// winners are marked allocated, the seq counter resumes past the highest seq
// seen.

class TDeviceKeyBufferStore final
    : public IKeyBufferStore
    , public std::enable_shared_from_this<TDeviceKeyBufferStore>
{
private:
    struct TEntry
    {
        ui64 Seq = 0;
        TVector<TPageRange> Locations;
    };

    const IDevicePtr Device;
    const ui64 PageCount;
    const ui32 PageSize;
    const ui64 ChunkCapacity;
    const IDevicePageStorePtr Pages;

    TLog Log;

    TAdaptiveLock Lock;

    bool RestoreStarted = false;
    bool Restored = false;
    ui64 NextSeq = 1;
    TMap<ui64, TEntry> Entries;

    // The erased bound the device holds.
    ui64 ErasedBelowKey = 0;
    // The bound of the last erase requested - the keys below it are refused
    // right away, whether the erase has been persisted or not.
    ui64 RequestedErasedBelowKey = 0;

    bool EraseInFlight = false;
    ui64 NextSuperblockSlot = 0;

    std::atomic_bool ReadOnly = false;

public:
    TDeviceKeyBufferStore(
        ILoggingServicePtr logging,
        IDevicePtr device,
        ui64 pageCount,
        ui32 pageSize);

    TFuture<TRestoreResult> Restore() override;

    TFuture<NCloud::NProto::TError> Write(ui64 key, TBuffer buffer) override;

    TFuture<NCloud::NProto::TError> EraseBelow(ui64 key) override;

private:
    TRestoreResult RestoreFromPages(const TVector<TString>& pages);

    NCloud::NProto::TError OnEntryWritten(
        ui64 key,
        ui64 seq,
        const TVector<TPageRange>& locations,
        const TFuture<NCloud::NProto::TError>& future);

    NCloud::NProto::TError OnSuperblockWritten(
        ui64 key,
        const TFuture<NCloud::NProto::TError>& future);
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
    , ChunkCapacity(pageSize - EntryHeaderSize)
    , Pages(CreateDevicePageStore(Device, PageCount, PageSize))
    , Log(logging->CreateLog("KEY_BUFFER_STORE"))
{
    // the superblock slots are never given to an entry
    auto error = Pages->AllocateAt(
        {{.FirstPageNo = 0, .PageCount = SuperblockSlotCount}});
    Y_ABORT_UNLESS(!HasError(error), "%s", FormatError(error).c_str());
}

TFuture<IKeyBufferStore::TRestoreResult> TDeviceKeyBufferStore::Restore()
{
    with_lock (Lock) {
        if (RestoreStarted) {
            return MakeFuture<TRestoreResult>(MakeError(
                E_INVALID_STATE,
                "the store is being restored already"));
        }
        RestoreStarted = true;
    }

    // the scan goes straight to the device - the page store knows
    // nothing about the pages before the restore
    TVector<TFuture<NCloud::NProto::TReadPagesResponse>> futures;

    for (ui64 offset = 0; offset < PageCount; offset += MaxPagesPerReadRequest)
    {
        NCloud::NProto::TReadPagesRequest request;
        auto& ref = *request.AddPageGroupRefs();
        ref.SetFirstPageNo(offset);
        ref.SetPageCount(Min(MaxPagesPerReadRequest, PageCount - offset));
        ref.SetPageSize(PageSize);

        futures.push_back(Device->ReadPages(std::move(request)));
    }

    return WaitAll(futures).Apply(
        [self = shared_from_this(),
         futures = std::move(futures)](const TFuture<void>&)
        {
            TVector<TString> pages;
            pages.reserve(self->PageCount);

            for (auto& future: futures) {
                auto response = UnsafeExtractValue(future);
                if (HasError(response)) {
                    return TRestoreResult(response.GetError());
                }

                for (auto& group: *response.MutablePageGroups()) {
                    for (auto& content: *group.MutableContent()) {
                        pages.push_back(std::move(content));
                    }
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

TFuture<NCloud::NProto::TError> TDeviceKeyBufferStore::Write(
    ui64 key,
    TBuffer buffer)
{
    if (ReadOnly.load()) {
        return MakeFuture(MakeError(E_INVALID_STATE, "read only mode"));
    }

    ui64 seq = 0;

    with_lock (Lock) {
        if (!Restored) {
            return MakeFuture(
                MakeError(E_INVALID_STATE, "the store is not restored"));
        }

        if (key < RequestedErasedBelowKey) {
            return MakeFuture(MakeError(
                E_ARGUMENT,
                TStringBuilder() << "key " << key << " is erased"));
        }

        seq = NextSeq++;
    }

    const ui64 chunkCount = ChunkCountFor(buffer.Size(), ChunkCapacity);

    auto locations = Pages->Allocate(chunkCount);
    if (locations.empty()) {
        return MakeFuture(MakeError(
            E_REJECTED,
            TStringBuilder() << "not enough free pages to store "
                             << buffer.Size() << " bytes under key " << key));
    }

    TVector<TBuffer> pages;
    pages.reserve(chunkCount);

    TStringBuf payload(buffer.Data(), buffer.Size());
    for (ui64 i = 0; i < chunkCount; ++i) {
        TEntryHeader header = {
            .Seq = seq,
            .Key = key,
            .PayloadSize = buffer.Size(),
            .ChunkIndex = static_cast<ui32>(i),
            .ChunkCount = static_cast<ui32>(chunkCount),
        };

        pages.push_back(MakeEntryPage(
            header,
            payload.SubStr(i * ChunkCapacity, ChunkCapacity),
            PageSize));
    }

    return Pages->Write(locations, pages)
        .Apply([self = shared_from_this(), key, seq, locations](
                   const TFuture<NCloud::NProto::TError>& future)
               { return self->OnEntryWritten(key, seq, locations, future); });
}

TFuture<NCloud::NProto::TError> TDeviceKeyBufferStore::EraseBelow(ui64 key)
{
    if (ReadOnly.load()) {
        return MakeFuture(MakeError(E_INVALID_STATE, "read only mode"));
    }

    TSuperblock superblock;
    ui64 slot = 0;

    with_lock (Lock) {
        if (!Restored) {
            return MakeFuture(
                MakeError(E_INVALID_STATE, "the store is not restored"));
        }

        if (key <= ErasedBelowKey) {
            return MakeFuture(MakeError(S_FALSE));
        }

        if (EraseInFlight) {
            return MakeFuture(MakeError(
                E_REJECTED,
                TStringBuilder()
                    << "an erase below key " << RequestedErasedBelowKey
                    << " is in progress"));
        }

        EraseInFlight = true;
        RequestedErasedBelowKey = key;

        superblock = {.Seq = NextSeq++, .ErasedBelowKey = key};
        slot = NextSuperblockSlot;
    }

    TVector<TBuffer> pages;
    pages.push_back(MakeSuperblockPage(superblock, PageSize));

    return Pages->Write({{.FirstPageNo = slot, .PageCount = 1}}, pages)
        .Apply([self = shared_from_this(),
                key](const TFuture<NCloud::NProto::TError>& future)
               { return self->OnSuperblockWritten(key, future); });
}

IKeyBufferStore::TRestoreResult TDeviceKeyBufferStore::RestoreFromPages(
    const TVector<TString>& pages)
{
    struct TCandidate
    {
        ui32 ChunkCount = 0;
        ui64 PayloadSize = 0;
        bool Broken = false;
        TVector<std::optional<TStringBuf>> Chunks;
        TVector<ui64> PageNos;
    };

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
            superblock = *parsed;
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

    // the pages of a single (key, seq) entry
    THashMap<TEntryId, TCandidate, TEntryIdHash> candidates;

    for (ui64 i = SuperblockSlotCount; i < pages.size(); ++i) {
        auto parsed = ParseEntryPage(pages[i], PageSize);
        if (!parsed) {
            continue;
        }

        const auto& [header, chunk] = *parsed;
        maxSeq = Max(maxSeq, header.Seq);

        auto& candidate =
            candidates[TEntryId{.Key = header.Key, .Seq = header.Seq}];
        if (candidate.Chunks.empty()) {
            candidate.ChunkCount = header.ChunkCount;
            candidate.PayloadSize = header.PayloadSize;
            candidate.Chunks.resize(header.ChunkCount);
            candidate.PageNos.resize(header.ChunkCount);
        }

        if (candidate.ChunkCount != header.ChunkCount ||
            candidate.PayloadSize != header.PayloadSize ||
            candidate.Chunks[header.ChunkIndex])
        {
            candidate.Broken = true;
            continue;
        }

        candidate.Chunks[header.ChunkIndex] = chunk;
        candidate.PageNos[header.ChunkIndex] = i;
    }

    // the newest intact candidate of a key wins
    struct TWinner
    {
        ui64 Seq = 0;
        const TCandidate* Candidate = nullptr;
    };

    THashMap<ui64, TWinner> winners;

    for (const auto& [id, candidate]: candidates) {
        if (superblock && id.Key < superblock->ErasedBelowKey) {
            continue;
        }

        if (candidate.Broken) {
            continue;
        }

        const bool complete = AllOf(
            candidate.Chunks,
            [](const auto& chunk) { return chunk.has_value(); });

        if (!complete) {
            continue;
        }

        auto& winner = winners[id.Key];
        if (!winner.Candidate || winner.Seq < id.Seq) {
            winner = {.Seq = id.Seq, .Candidate = &candidate};
        }
    }

    TVector<TKeyBuffer> buffers;
    buffers.reserve(winners.size());
    TMap<ui64, TEntry> entries;

    for (const auto& [key, winner]: winners) {
        TBuffer buffer(winner.Candidate->PayloadSize);
        for (const auto& chunk: winner.Candidate->Chunks) {
            buffer.Append(chunk->data(), chunk->size());
        }

        auto locations = ToPageRanges(winner.Candidate->PageNos);
        auto error = Pages->AllocateAt(locations);
        if (HasError(error)) {
            return error;
        }

        entries[key] = {
            .Seq = winner.Seq,
            .Locations = std::move(locations),
        };
        buffers.push_back({.Key = key, .Buffer = std::move(buffer)});
    }

    with_lock (Lock) {
        Restored = true;
        NextSeq = maxSeq + 1;
        Entries = std::move(entries);

        if (superblock) {
            ErasedBelowKey = superblock->ErasedBelowKey;
        }
        RequestedErasedBelowKey = ErasedBelowKey;
        NextSuperblockSlot = (superblockSlot + 1) % SuperblockSlotCount;
    }

    return std::move(buffers);
}

NCloud::NProto::TError TDeviceKeyBufferStore::OnEntryWritten(
    ui64 key,
    ui64 seq,
    const TVector<TPageRange>& locations,
    const TFuture<NCloud::NProto::TError>& future)
{
    auto error = future.GetValue();
    if (HasError(error)) {
        auto freeError = Pages->Free(locations);
        if (HasError(freeError)) {
            ReadOnly.store(true);
            STORAGE_ERROR(
                "failed to free the pages of the failed write of key "
                << key << ": " << FormatError(freeError));
        }
        return error;
    }

    TVector<TPageRange> stalePages;

    with_lock (Lock) {
        if (key < RequestedErasedBelowKey) {
            // erased while being written - a restore would drop it
            stalePages = locations;
            error = MakeError(
                E_REJECTED,
                TStringBuilder()
                    << "key " << key << " was erased while being written");
        } else {
            auto& entry = Entries[key];
            if (entry.Seq < seq) {
                stalePages = std::exchange(entry.Locations, locations);
                entry.Seq = seq;
            } else {
                // a newer write of the key has landed already
                stalePages = locations;
            }
        }
    }

    if (!stalePages.empty()) {
        auto freeError = Pages->Free(stalePages);
        if (HasError(freeError)) {
            ReadOnly.store(true);
            STORAGE_ERROR(
                "failed to free the stale pages of key " << key << ": "
                << FormatError(freeError));
        }
    }

    return error;
}

NCloud::NProto::TError TDeviceKeyBufferStore::OnSuperblockWritten(
    ui64 key,
    const TFuture<NCloud::NProto::TError>& future)
{
    auto error = future.GetValue();
    TVector<TPageRange> pagesToFree;
    bool erasedAny = false;

    with_lock (Lock) {
        EraseInFlight = false;

        if (HasError(error)) {
            return error;
        }

        ErasedBelowKey = key;
        NextSuperblockSlot = (NextSuperblockSlot + 1) % SuperblockSlotCount;

        auto end = Entries.lower_bound(key);
        for (auto it = Entries.begin(); it != end; ++it) {
            for (const auto& location: it->second.Locations) {
                pagesToFree.push_back(location);
            }
        }

        erasedAny = end != Entries.begin();
        Entries.erase(Entries.begin(), end);
    }

    if (!pagesToFree.empty()) {
        auto freeError = Pages->Free(pagesToFree);
        if (HasError(freeError)) {
            ReadOnly.store(true);
            STORAGE_ERROR(
                "failed to free the pages erased below key " << key << ": "
                << FormatError(freeError));
        }
    }

    return MakeError(erasedAny ? S_OK : S_FALSE);
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
    Y_ABORT_UNLESS(
        pageCount > SuperblockSlotCount,
        "the store needs more than %" PRIu64 " pages",
        SuperblockSlotCount);
    Y_ABORT_UNLESS(
        pageSize > EntryHeaderSize && pageSize >= SuperblockSize,
        "the page size %" PRIu32 " is too small for the store",
        pageSize);

    return std::make_shared<TDeviceKeyBufferStore>(
        std::move(logging),
        std::move(device),
        pageCount,
        pageSize);
}

}   // namespace NCloud::NJournalled
