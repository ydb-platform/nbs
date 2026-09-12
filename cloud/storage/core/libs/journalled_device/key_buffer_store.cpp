#include "key_buffer_store.h"

#include "device.h"
#include "device_page_store.h"

#include <cloud/storage/core/libs/common/future_helper.h>

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/utility.h>
#include <util/generic/ylimits.h>
#include <util/stream/buffer.h>
#include <util/stream/mem.h>
#include <util/string/builder.h>
#include <util/system/spinlock.h>
#include <util/system/yassert.h>
#include <util/ysaveload.h>

#include <optional>
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
        TVector<std::pair<ui64, TBuffer>> buffers;

        with_lock (Lock) {
            buffers.reserve(Buffers.size());
            for (const auto& [key, buffer]: Buffers) {
                buffers.emplace_back(key, buffer);
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

// Magic, Seq, Key, PayloadSize, Version, PageIndex, PageCount, Crc. The
// checksum covers the fields before it and the chunk that follows.
constexpr ui32 EntryHeaderSize = 4 * sizeof(ui64) + 4 * sizeof(ui32);

// Magic, Seq, ErasedBelowKey, Version, Crc. The checksum covers the fields
// before it.
constexpr ui32 SuperblockSize = 3 * sizeof(ui64) + 2 * sizeof(ui32);

constexpr ui64 MaxPagesPerReadRequest = 1024;

struct TEntryHeader
{
    ui64 Seq = 0;
    ui64 Key = 0;
    ui64 PayloadSize = 0;
    ui32 PageIndex = 0;
    ui32 PageCount = 0;
};

struct TSuperblock
{
    ui64 Seq = 0;
    ui64 ErasedBelowKey = 0;
};

ui64 EntryPageCountFor(ui64 payloadSize, ui64 chunkCapacity)
{
    return Max<ui64>(1, (payloadSize + chunkCapacity - 1) / chunkCapacity);
}

ui64 ChunkSizeOf(const TEntryHeader& header, ui64 chunkCapacity)
{
    if (header.PageIndex + 1 < header.PageCount) {
        return chunkCapacity;
    }
    return header.PayloadSize - chunkCapacity * header.PageIndex;
}

TBuffer
MakeEntryPage(const TEntryHeader& header, TStringBuf chunk, ui32 pageSize)
{
    TBuffer page(pageSize);
    TBufferOutput out(page);

    Save(&out, EntryMagic);
    Save(&out, header.Seq);
    Save(&out, header.Key);
    Save(&out, header.PayloadSize);
    Save(&out, StoreFormatVersion);
    Save(&out, header.PageIndex);
    Save(&out, header.PageCount);

    ui32 crc = Crc32c(page.Data(), page.Size());
    crc = Crc32cExtend(crc, chunk.data(), chunk.size());
    Save(&out, crc);

    out.Write(chunk.data(), chunk.size());
    page.Fill('\0', pageSize - page.Size());

    return page;
}

// Returns the header and the chunk of a valid entry page, nothing for a page
// that holds anything else.
std::optional<std::pair<TEntryHeader, TStringBuf>> ParseEntryPage(
    TStringBuf page,
    ui32 pageSize)
{
    if (page.size() != pageSize) {
        return std::nullopt;
    }

    TMemoryInput in(page.data(), page.size());

    ui64 magic = 0;
    ui32 version = 0;
    ui32 crc = 0;
    TEntryHeader header;

    Load(&in, magic);
    Load(&in, header.Seq);
    Load(&in, header.Key);
    Load(&in, header.PayloadSize);
    Load(&in, version);
    Load(&in, header.PageIndex);
    Load(&in, header.PageCount);
    Load(&in, crc);

    if (magic != EntryMagic || version != StoreFormatVersion) {
        return std::nullopt;
    }

    const ui64 chunkCapacity = pageSize - EntryHeaderSize;
    if (header.PageIndex >= header.PageCount ||
        header.PageCount !=
            EntryPageCountFor(header.PayloadSize, chunkCapacity))
    {
        return std::nullopt;
    }

    const ui64 chunkSize = ChunkSizeOf(header, chunkCapacity);
    TStringBuf chunk = page.SubStr(EntryHeaderSize, chunkSize);

    ui32 expectedCrc = Crc32c(page.data(), EntryHeaderSize - sizeof(ui32));
    expectedCrc = Crc32cExtend(expectedCrc, chunk.data(), chunk.size());

    if (crc != expectedCrc) {
        return std::nullopt;
    }

    return std::make_pair(header, chunk);
}

TBuffer MakeSuperblockPage(const TSuperblock& superblock, ui32 pageSize)
{
    TBuffer page(pageSize);
    TBufferOutput out(page);

    Save(&out, SuperblockMagic);
    Save(&out, superblock.Seq);
    Save(&out, superblock.ErasedBelowKey);
    Save(&out, StoreFormatVersion);

    ui32 crc = Crc32c(page.Data(), page.Size());
    Save(&out, crc);

    page.Fill('\0', pageSize - page.Size());

    return page;
}

std::optional<TSuperblock> ParseSuperblockPage(TStringBuf page, ui32 pageSize)
{
    if (page.size() != pageSize) {
        return std::nullopt;
    }

    TMemoryInput in(page.data(), page.size());

    ui64 magic = 0;
    ui32 version = 0;
    ui32 crc = 0;
    TSuperblock superblock;

    Load(&in, magic);
    Load(&in, superblock.Seq);
    Load(&in, superblock.ErasedBelowKey);
    Load(&in, version);
    Load(&in, crc);

    if (magic != SuperblockMagic || version != StoreFormatVersion ||
        crc != Crc32c(page.data(), SuperblockSize - sizeof(ui32)))
    {
        return std::nullopt;
    }

    return superblock;
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

public:
    TDeviceKeyBufferStore(IDevicePtr device, ui64 pageCount, ui32 pageSize);

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
    IDevicePtr device,
    ui64 pageCount,
    ui32 pageSize)
    : Device(std::move(device))
    , PageCount(pageCount)
    , PageSize(pageSize)
    , ChunkCapacity(pageSize - EntryHeaderSize)
    , Pages(CreateDevicePageStore(Device, PageCount, PageSize))
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

    const ui64 pageCount = EntryPageCountFor(buffer.Size(), ChunkCapacity);
    if (pageCount > Max<ui32>()) {
        return MakeFuture(MakeError(
            E_ARGUMENT,
            TStringBuilder() << "a buffer of " << buffer.Size()
                             << " bytes is too large for the store"));
    }

    auto locations = Pages->Allocate(pageCount);
    if (locations.empty()) {
        return MakeFuture(MakeError(
            E_REJECTED,
            TStringBuilder() << "not enough free pages to store "
                             << buffer.Size() << " bytes under key " << key));
    }

    TVector<TBuffer> pages;
    pages.reserve(pageCount);

    TStringBuf payload(buffer.Data(), buffer.Size());
    for (ui64 i = 0; i < pageCount; ++i) {
        TEntryHeader header = {
            .Seq = seq,
            .Key = key,
            .PayloadSize = buffer.Size(),
            .PageIndex = static_cast<ui32>(i),
            .PageCount = static_cast<ui32>(pageCount),
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
        NextSuperblockSlot = (NextSuperblockSlot + 1) % SuperblockSlotCount;
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
        ui32 PageCount = 0;
        ui64 PayloadSize = 0;
        bool Broken = false;
        TVector<std::optional<TStringBuf>> Chunks;
        TVector<ui64> PageNos;
    };

    ui64 maxSeq = 0;

    std::optional<TSuperblock> superblock;
    ui64 superblockSlot = 0;

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

    // the pages of a single (key, seq) entry
    THashMap<std::pair<ui64, ui64>, TCandidate> candidates;

    for (ui64 i = SuperblockSlotCount; i < pages.size(); ++i) {
        auto parsed = ParseEntryPage(pages[i], PageSize);
        if (!parsed) {
            continue;
        }

        const auto& [header, chunk] = *parsed;
        maxSeq = Max(maxSeq, header.Seq);

        auto& candidate = candidates[std::pair(header.Key, header.Seq)];
        if (candidate.Chunks.empty()) {
            candidate.PageCount = header.PageCount;
            candidate.PayloadSize = header.PayloadSize;
            candidate.Chunks.resize(header.PageCount);
            candidate.PageNos.resize(header.PageCount);
        }

        if (candidate.PageCount != header.PageCount ||
            candidate.PayloadSize != header.PayloadSize ||
            candidate.Chunks[header.PageIndex])
        {
            candidate.Broken = true;
            continue;
        }

        candidate.Chunks[header.PageIndex] = chunk;
        candidate.PageNos[header.PageIndex] = i;
    }

    // the newest intact candidate of a key wins
    struct TWinner
    {
        ui64 Seq = 0;
        const TCandidate* Candidate = nullptr;
    };

    THashMap<ui64, TWinner> winners;

    for (const auto& [keyAndSeq, candidate]: candidates) {
        const auto& [key, seq] = keyAndSeq;

        if (superblock && key < superblock->ErasedBelowKey) {
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

        auto& winner = winners[key];
        if (!winner.Candidate || winner.Seq < seq) {
            winner = {.Seq = seq, .Candidate = &candidate};
        }
    }

    TVector<std::pair<ui64, TBuffer>> buffers;
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
        buffers.emplace_back(key, std::move(buffer));
    }

    with_lock (Lock) {
        Restored = true;
        NextSeq = maxSeq + 1;
        Entries = std::move(entries);

        if (superblock) {
            ErasedBelowKey = superblock->ErasedBelowKey;
            NextSuperblockSlot = (superblockSlot + 1) % SuperblockSlotCount;
        }
        RequestedErasedBelowKey = ErasedBelowKey;
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
        Y_UNUSED(Pages->Free(locations));
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
        Y_UNUSED(Pages->Free(stalePages));
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
        Y_UNUSED(Pages->Free(pagesToFree));
    }

    return MakeError(erasedAny ? S_OK : S_FALSE);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IKeyBufferStorePtr CreateInMemoryKeyBufferStore()
{
    return std::make_shared<TInMemoryKeyBufferStore>();
}

IKeyBufferStorePtr
CreateDeviceKeyBufferStore(IDevicePtr device, ui64 pageCount, ui32 pageSize)
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
        std::move(device),
        pageCount,
        pageSize);
}

}   // namespace NCloud::NJournalled
