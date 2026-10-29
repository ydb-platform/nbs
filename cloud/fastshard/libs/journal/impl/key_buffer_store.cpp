#include "key_buffer_store.h"

#include "key_buffer_store_format.h"

#include <cloud/fastshard/libs/journal/iface/device.h>

#include <cloud/storage/core/libs/common/future_helper.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/utility.h>
#include <util/string/builder.h>

#include <atomic>
#include <cstring>
#include <functional>
#include <mutex>
#include <optional>
#include <utility>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 InvalidPageNo = Max<ui64>();
constexpr ui64 SuperblockSlotCount = 2;

TStringBuf AsStringBuf(const TBuffer& buffer)
{
    return {buffer.Data(), buffer.Size()};
}

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

struct TAppendablePage
{
    ui64 PageNo = InvalidPageNo;
    TVector<TEntry> Entries;
    TBuffer PageBuffer;

    ui32 DataSize = 0;  // useful data size
    ui64 MaxKey = 0;
    ui64 MinKey = Max<ui64>();
};

using TAppendablePagePtr = std::shared_ptr<TAppendablePage>;

////////////////////////////////////////////////////////////////////////////////

struct IPageManager
{
    virtual ~IPageManager() = default;

    virtual void AddEmptyPage(ui64 pageNo) = 0;
    virtual ui64 PopEmptyPage() = 0;

    virtual void AddAppendablePage(TAppendablePagePtr page) = 0;
    virtual TAppendablePagePtr PopAppendablePage(ui64 size) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TWriteController
{
private:
    IPageManager& PageManager;
    const TEntry NewEntry;
    const TBuffer Buffer;
    const ui32 PageSize;

    TAppendablePagePtr OldPage;
    TAppendablePage NewPage;

    bool Written = false;

public:
    TWriteController(
        IPageManager& pageManager,
        TEntry entry,
        TBuffer buffer,
        ui32 pageSize)
        : PageManager(pageManager)
        , NewEntry(std::move(entry))
        , Buffer(std::move(buffer))
        , PageSize(pageSize)
    {}

    ~TWriteController()
    {
        if (Written) {
            if (OldPage) {
                PageManager.AddEmptyPage(OldPage->PageNo);
            }

            PageManager.AddAppendablePage(
                std::make_shared<TAppendablePage>(std::move(NewPage)));
        } else {
            PageManager.AddEmptyPage(NewPage.PageNo);
            PageManager.AddAppendablePage(std::move(OldPage));
        }
    }

    TResultOrError<TPageRange> GeneratePageRange()
    {
        NewPage.PageNo = PageManager.PopEmptyPage();
        if (NewPage.PageNo == InvalidPageNo) {
            return MakeError(
                E_REJECTED,
                TStringBuilder()
                    << "Not enough free pages to store " << NewEntry.Data.size()
                    << " bytes under key " << NewEntry.Key);
        }

        NewPage.Entries.clear();
        NewPage.MaxKey = NewEntry.Key;
        NewPage.MinKey = NewEntry.Key;

        OldPage = PageManager.PopAppendablePage(NewEntry.Data.size());
        if (OldPage) {
            NewPage.Entries = OldPage->Entries;
            NewPage.MaxKey = Max(NewPage.MaxKey, OldPage->MaxKey);
            NewPage.MinKey = Min(NewPage.MinKey, OldPage->MinKey);
        }

        NewPage.Entries.push_back(NewEntry);

        auto error = MakePageBuffer(
            PageSize,
            NewPage.Entries,
            &NewPage.PageBuffer,
            &NewPage.DataSize);

        if (HasError(error)) {
            return error;
        }

        TPageRange pageRange;
        pageRange.FirstPageNo = NewPage.PageNo;
        pageRange.Pages.push_back(NewPage.PageBuffer);
        return pageRange;
    }

    ui64 Key() const
    {
        return NewEntry.Key;
    }

    void SetAsWritten()
    {
        Written = true;
    }
};

using TWriteControllerPtr = std::shared_ptr<TWriteController>;

////////////////////////////////////////////////////////////////////////////////

class TPageManager: private IPageManager
{
private:
    static constexpr size_t MaxAppendablePageCount = 128;

    const ui32 PageSize;
    const ui32 MinEntrySize;

    std::mutex Lock;

    // everything below is guarded by Lock
    ui64 ErasedBelowKey = 0;
    TDeque<ui64 /* pageNo */> EmptyPages;
    TMultiMap<ui64 /* availableSize */, TAppendablePagePtr> AppendablePages;
    TMultiMap<ui64 /* maxKey */, ui64 /* pageNo */> FullPages;

public:
    TPageManager(ui32 pageSize, ui32 minKeyBufferSize)
        : PageSize(pageSize)
        , MinEntrySize(GetSerializedEntrySize(minKeyBufferSize))
    {}

    void Init(
        ui64 firstPage,
        ui64 pageCount,
        ui64 erasedBelowKey,
        THashMap<ui64 /* pageNo */, TVector<TEntry>> pageEntries,
        TVector<TBuffer> pages)
    {
        {
            std::lock_guard lock(Lock);
            ErasedBelowKey = erasedBelowKey;
        }

        for (ui64 i = 0; i < pageCount; ++i) {
            ui64 pageNo = firstPage + i;
            if (pageEntries.contains(pageNo)) {
                continue;
            }

            AddEmptyPage(pageNo);
        }

        for (auto& [pageNo, entries]: pageEntries) {
            auto appendablePage = std::make_shared<TAppendablePage>();
            appendablePage->PageNo = pageNo;
            appendablePage->Entries = std::move(entries);
            appendablePage->PageBuffer = std::move(pages[pageNo]);

            RefreshPage(*appendablePage, erasedBelowKey);
            AddAppendablePage(std::move(appendablePage));
        }
    }

    TWriteControllerPtr CreateWriteController(TEntry entry, TBuffer buffer)
    {
        return std::make_shared<TWriteController>(
            static_cast<IPageManager&>(*this),
            std::move(entry),
            std::move(buffer),
            PageSize);
    }

    void EraseBelowKey(ui64 key)
    {
        // Drop the entries below the bound from the appendable pages that
        // straddle it - a walk over their entries, which frees their room.
        static constexpr bool FilterPages = true;

        TVector<TAppendablePagePtr> dirtyPages;
        DoEraseBelowKey(key, FilterPages ? &dirtyPages : nullptr);

        for (auto& page: dirtyPages) {
            RefreshPage(*page, key);
            AddAppendablePage(std::move(page));
        }
    }

protected:
    // Frees the pages below |key| and takes out the appendable pages that
    // straddle it, for the caller to put back refreshed.
    void DoEraseBelowKey(ui64 key, TVector<TAppendablePagePtr>* outDirtyPages)
    {
        std::lock_guard lock(Lock);

        ErasedBelowKey = key;

        for (auto it = AppendablePages.begin(); it != AppendablePages.end();) {
            auto& page = it->second;
            if (page->MaxKey < key) {
                EmptyPages.push_back(page->PageNo);
                it = AppendablePages.erase(it);
                continue;
            }

            if (outDirtyPages && page->MinKey < key) {
                outDirtyPages->push_back(std::move(page));
                it = AppendablePages.erase(it);
                continue;
            }

            ++it;
        }

        auto end = FullPages.lower_bound(key);
        for (auto it = FullPages.begin(); it != end;) {
            EmptyPages.push_back(it->second);
            it = FullPages.erase(it);
        }
    }

    void AddEmptyPage(ui64 pageNo) override
    {
        if (pageNo == InvalidPageNo) {
            return;
        }

        std::lock_guard lock(Lock);
        EmptyPages.push_back(pageNo);
    }

    ui64 PopEmptyPage() override
    {
        std::lock_guard lock(Lock);

        if (EmptyPages.empty()) {
            return InvalidPageNo;
        }

        ui64 page = EmptyPages.back();
        EmptyPages.pop_back();
        return page;
    }

    void AddAppendablePage(TAppendablePagePtr page) override
    {
        if (!page || page->PageNo == InvalidPageNo) {
            return;
        }

        std::lock_guard lock(Lock);

        if (page->Entries.empty()) {
            EmptyPages.push_back(page->PageNo);
            return;
        }

        if (PageSize <= page->DataSize ||
            PageSize - page->DataSize < MinEntrySize)
        {
            FullPages.emplace(page->MaxKey, page->PageNo);
            return;
        }

        const ui64 availableSize = PageSize - page->DataSize;

        if (AppendablePages.size() >= MaxAppendablePageCount) {
            if (availableSize < AppendablePages.begin()->first) {
                FullPages.emplace(page->MaxKey, page->PageNo);
                return;
            }

            const auto& firstPage = *AppendablePages.begin()->second;
            FullPages.emplace(firstPage.MaxKey, firstPage.PageNo);
            AppendablePages.erase(AppendablePages.begin());
        }

        AppendablePages.emplace(availableSize, std::move(page));
    }

    TAppendablePagePtr PopAppendablePage(ui64 entryBufferSize) override
    {
        std::lock_guard lock(Lock);

        auto it = AppendablePages.lower_bound(
            GetSerializedEntrySize(entryBufferSize));

        if (it == AppendablePages.end()) {
            return nullptr;
        }

        auto page = std::move(it->second);
        AppendablePages.erase(it);
        return page;
    }

private:
    // Drops the erased entries of the page and recomputes the bounds and the
    // data size - the entries left keep pointing into the page buffer, which
    // stays as it is until the page is rewritten.
    static void RefreshPage(TAppendablePage& page, ui64 erasedBelowKey)
    {
        page.MaxKey = 0;
        page.MinKey = Max<ui64>();
        page.DataSize = GetPageHeaderSize();

        for (auto it = page.Entries.begin(); it != page.Entries.end();) {
            const auto& entry = *it;
            if (entry.Key < erasedBelowKey) {
                it = page.Entries.erase(it);
                continue;
            }
            ++it;

            page.MaxKey = Max(page.MaxKey, entry.Key);
            page.MinKey = Min(page.MinKey, entry.Key);
            page.DataSize += GetSerializedEntrySize(entry.Data.Size());
        }
    }
};

constexpr ui64 MaxPagesPerReadRequest = 1024;

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
// TEntryHeader with the key, the seq of the write and the payload size followed
// by the payload. A write appends the entry to a copy of a page with enough
// room (or to a fresh page), writes the copy to an empty page and releases the
// original. Every page carries a checksum.

class TDeviceKeyBufferStore final
    : public IKeyBufferStore
    , public std::enable_shared_from_this<TDeviceKeyBufferStore>
{
private:
    const IDevicePtr Device;
    TPageManager PageManager;
    const ui64 PageCount;
    const ui32 PageSize;
    const ui32 MinKeyBufferSize;

    TLog Log;

    std::mutex Lock;

    bool RestoreStarted = false;
    bool Restored = false;
    ui64 NextSeq = 1;

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
        ui32 pageSize,
        ui32 minKeyBufferSize);

    TFuture<TRestoreResult> Restore() override;

    TFuture<NProto::TError> Write(ui64 key, TBuffer buffer) override;

    TFuture<NProto::TError> WriteMeta(TBuffer buffer) override;

    TFuture<NProto::TError> EraseBelow(ui64 key) override;

private:
    TRestoreResult RestoreFromPages(TVector<TBuffer> pages);

    // Applies |update| to a copy of the superblock and writes it to the next
    // slot.
    TFuture<NProto::TError> WriteSuperblock(
        const std::function<NProto::TError(TSuperblock&)>& update);

    NProto::TError OnEntryWritten(
        TWriteControllerPtr writeController,
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
    ui32 pageSize,
    ui32 minKeyBufferSize)
    : Device(std::move(device))
    , PageManager(pageSize, minKeyBufferSize)
    , PageCount(pageCount)
    , PageSize(pageSize)
    , MinKeyBufferSize(minKeyBufferSize)
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

    auto error = ValidatePageSize(PageSize, MinKeyBufferSize);
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
            TVector<TBuffer> pages;
            pages.reserve(self->PageCount);

            for (auto& future: futures) {
                auto result = UnsafeExtractValue(future);
                if (HasError(result)) {
                    return TRestoreResult(result.GetError());
                }

                auto readPages = result.ExtractResult();
                for (auto& readPage: readPages) {
                    pages.emplace_back(std::move(readPage));
                }
            }

            if (pages.size() != self->PageCount) {
                return TRestoreResult(MakeError(
                    E_INVALID_STATE,
                    TStringBuilder()
                        << "the device returned " << pages.size()
                        << " pages, expected " << self->PageCount));
            }

            return self->RestoreFromPages(std::move(pages));
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

    TStringBuf bufferRef(buffer.Data(), buffer.Size());
    auto writeController = PageManager.CreateWriteController(
        TEntry{
            .Key = key,
            .Seq = seq,
            .Data = bufferRef,
        },
        std::move(buffer));

    auto result = writeController->GeneratePageRange();
    if (HasError(result)) {
        return MakeFuture(result.GetError());
    }
    auto pageRange = result.ExtractResult();

    TVector<TPageRange> ranges;
    ranges.push_back(std::move(pageRange));

    auto future = Device->WritePages(std::move(ranges));

    return future.Apply(
        [self = shared_from_this(),
         controller = std::move(writeController)](const auto& future)
        { return self->OnEntryWritten(std::move(controller), future); });
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
        return MakeFuture(OnSuperblockWritten(
            std::move(superblock),
            MakeFuture(result.GetError())));
    }

    auto superblockPage = result.ExtractResult();

    TVector<TPageRange> ranges;
    ranges.push_back({
        .FirstPageNo = slot,
        .Pages = {std::move(superblockPage)},
    });

    return Device->WritePages(std::move(ranges))
        .Apply([self = shared_from_this(), superblock = std::move(superblock)](
                   const TFuture<NProto::TError>& future) mutable
               {
                   return self->OnSuperblockWritten(
                       std::move(superblock),
                       future);
               });
}

IKeyBufferStore::TRestoreResult TDeviceKeyBufferStore::RestoreFromPages(
    TVector<TBuffer> pages)
{
    ui64 maxSeq = 0;

    std::optional<TSuperblock> superblock;
    ui64 superblockSlot = SuperblockSlotCount;

    for (ui64 slot = 0; slot < SuperblockSlotCount; ++slot) {
        auto parsed = ParseSuperblockPage(AsStringBuf(pages[slot]), PageSize);
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
            if (IsEmptyPage(AsStringBuf(pages[slot]), PageSize)) {
                superblockSlot = slot;
            }
        }
    }

    if (superblockSlot == SuperblockSlotCount) {
        return MakeError(E_INVALID_STATE, "all superblock slots are dirty");
    }

    struct TCandidate
    {
        ui64 PageNo = 0;
        ui64 PageSeq = 0;
        TEntry Entry;
    };

    THashMap<ui64 /*key*/, TCandidate> winners;

    for (ui64 i = SuperblockSlotCount; i < pages.size(); ++i) {
        auto entries = ParsePageBuffer(AsStringBuf(pages[i]), PageSize);
        if (entries.empty()) {
            continue;
        }

        ui64 pageSeq = entries.back().Seq;

        for (ui64 j = 0; j < entries.size(); ++j) {
            const auto& entry = entries[j];
            maxSeq = Max(maxSeq, entry.Seq);

            if (superblock && entry.Key < superblock->ErasedBelowKey) {
                continue;
            }

            TCandidate candidate = {
                .PageNo = i,
                .PageSeq = pageSeq,
                .Entry = entry,
            };

            auto [it, inserted] = winners.emplace(entry.Key, candidate);

            if (!inserted && (it->second.Entry.Seq < candidate.Entry.Seq ||
                              (it->second.Entry.Seq == candidate.Entry.Seq &&
                               it->second.PageSeq < candidate.PageSeq)))
            {
                it->second = candidate;
            }
        }
    }

    THashMap<ui64 /* pageNo */, TVector<TEntry>> pageEntries;
    TVector<TKeyBuffer> buffers;
    buffers.reserve(winners.size());

    for (const auto& [key, winner]: winners) {
        pageEntries[winner.PageNo].push_back(winner.Entry);

        const auto& buffer = winner.Entry.Data;
        buffers.push_back({
            .Key = key,
            .Buffer = TBuffer(buffer.data(), buffer.size()),
        });
    }

    PageManager.Init(
        SuperblockSlotCount,
        pages.size() - SuperblockSlotCount,
        superblock ? superblock->ErasedBelowKey : 0,
        std::move(pageEntries),
        std::move(pages));

    TBuffer metaBuffer;

    {
        std::lock_guard lock(Lock);

        if (superblock) {
            Superblock = std::move(*superblock);
        }
        NextSuperblockSlot = (superblockSlot + 1) % SuperblockSlotCount;

        Restored = true;
        NextSeq = maxSeq + 1;

        metaBuffer = Superblock.MetaBuffer;
    }

    return TRestoredBuffers{
        .MetaBuffer = std::move(metaBuffer),
        .KeyBuffers = std::move(buffers),
    };
}

NProto::TError TDeviceKeyBufferStore::OnEntryWritten(
    TWriteControllerPtr writeController,
    const TFuture<NProto::TError>& future)
{
    auto error = future.GetValue();

    {
        std::lock_guard lock(Lock);

        if (!HasError(error)) {
            const ui64 key = writeController->Key();
            if (key < Superblock.ErasedBelowKey) {
                // erased while being written - a restore would drop it
                error = MakeError(
                    E_REJECTED,
                    TStringBuilder()
                        << "Key " << key << " was erased while being written");
            } else {
                writeController->SetAsWritten();
            }
        }

        // hands the pages back to the manager, under the lock so that an
        // erase landing at the same time sees them
        writeController.reset();
    }

    return error;
}

NProto::TError TDeviceKeyBufferStore::OnSuperblockWritten(
    TSuperblock superblock,
    const TFuture<NProto::TError>& future)
{
    std::lock_guard lock(Lock);

    SuperblockWriteInFlight = false;

    if (auto error = future.GetValue(); HasError(error)) {
        return error;
    }

    Superblock = std::move(superblock);
    NextSuperblockSlot = (NextSuperblockSlot + 1) % SuperblockSlotCount;

    PageManager.EraseBelowKey(Superblock.ErasedBelowKey);
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
    ui32 pageSize,
    ui32 minKeyBufferSize)
{
    return std::make_shared<TDeviceKeyBufferStore>(
        std::move(logging),
        std::move(device),
        pageCount,
        pageSize,
        minKeyBufferSize);
}

}   // namespace NCloud::NJournalled
