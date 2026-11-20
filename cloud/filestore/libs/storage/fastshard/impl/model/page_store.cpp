#include "page_store.h"

#include <silk/fibers/mutex.h>
#include <silk/util/logger.h>

#include <library/cpp/cache/cache.h>

#include <util/generic/scope.h>
#include <util/string/builder.h>

#include <mutex>

namespace NCloud::NFileStore::NStorage::NFastShard {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TPage
{
    TBuffer Content;

    //
    // Lsn is used for additional correctness checks and for the Committed
    // page it is not strictly equal to commit Lsn but is actually <= it.
    //
    // This happens because when we ReadForLookup we might actually read a
    // version of the page which is newer than the last Lsn for which we
    // already got commit confirmation.
    //
    // For Dirty pages we know the exact Lsn.
    //

    ui64 Lsn = 0;
};

enum class EPageState
{
    NotLoaded,
    Loading,
    Loaded,
};

struct TPageVersions
{
    TPage Committed;
    TVector<TPage> Dirty;
    EPageState State = EPageState::NotLoaded;

    TVector<TPage>::iterator FindAtLsn(ui64 lsn)
    {
        auto* it = std::lower_bound(
            Dirty.begin(),
            Dirty.end(),
            lsn,
            [] (const TPage& page, ui64 lsn) {
                return page.Lsn < lsn;
            });
        if (it == Dirty.end() || it->Lsn != lsn) {
            return Dirty.end();
        }

        return it;
    }

    [[nodiscard]] ui64 LastLsn() const
    {
        if (Dirty) {
            return Dirty.back().Lsn;
        }

        return Committed.Lsn;
    }

    [[nodiscard]] TBuffer LastContent() const
    {
        if (Dirty) {
            return Dirty.back().Content;
        }

        return Committed.Content;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TPageCache
{
public:
    static constexpr ui64 InvalidPageNo = Max<ui64>();

    class TSlot
    {
    private:
        TPageVersions PV;
        ui64 PageNo = InvalidPageNo;
        ui32 RefCount = 0;
        bool Accessed = false;

        friend class TPageCache;

    public:
        TPageVersions& AccessPV()
        {
            return PV;
        }
    };

private:
    using TLookupMap = THashMap<ui64, TSlot*>;
    TLookupMap PageNo2Slot;
    TDeque<TSlot> Slots;
    ui64 ClockHand = 0;

    ui64 FirstPinnedPageNo = 0;
    ui64 PinnedPageCount = 0;

public:
    explicit TPageCache(ui64 maxPages)
        : Slots(maxPages)
    {}

public:
    NProto::TError Find(ui64 pageNo, TSlot** slot)
    {
        TLookupMap::insert_ctx insertCtx{};
        auto it = PageNo2Slot.find(pageNo, insertCtx);
        if (it == PageNo2Slot.end()) {
            auto error = AcquireSlot(slot);
            if (HasError(error)) {
                return error;
            }

            (*slot)->PageNo = pageNo;
            PageNo2Slot.insert_direct(std::make_pair(pageNo, *slot), insertCtx);
        } else {
            *slot = it->second;
        }

        (*slot)->Accessed = true;
        return {};
    }

    NProto::TError Access(ui64 pageNo, TSlot** slot)
    {
        auto error = Find(pageNo, slot);
        if (HasError(error)) {
            return error;
        }

        // XXX make sure that on Commit/Rollback we decrement refcount by the
        // same number as we incremented it
        //
        // we can access the same page multiple times during a single op
        //
        // or just do not pin accessed slots at all

        ++(*slot)->RefCount;
        return {};
    }

    static void Release(TSlot& slot)
    {
        Y_ABORT_UNLESS(slot.RefCount > 0);
        --slot.RefCount;
    }

    NProto::TError PinRange(ui64 firstPageNo, ui64 pageCount)
    {
        if (pageCount >= Slots.size()) {
            return MakeError(E_ARGUMENT, TStringBuilder() << "pageCount ("
                << pageCount << ") >= Slots.size() (" << Slots.size() << ")");
        }

        FirstPinnedPageNo = firstPageNo;
        PinnedPageCount = pageCount;
        return {};
    }

    void Clear()
    {
        for (auto& s: Slots) {
            Y_ABORT_UNLESS(
                s.RefCount == 0,
                "acquired slot in Clear: %lu, %lu",
                s.PageNo,
                s.RefCount);

            s = {};
        }

        PageNo2Slot.clear();
    }

private:
    NProto::TError AcquireSlot(TSlot** slot)
    {
        for (;;) {
            const ui64 j = ClockHand++ % Slots.size();
            auto& s = Slots[j];

            if (s.PageNo == InvalidPageNo) {
                Y_ABORT_UNLESS(s.RefCount == 0);
                *slot = &s;
                return {};
            }

            if (s.RefCount) {
                continue;
            }

            if (FirstPinnedPageNo <= s.PageNo
                    && s.PageNo < FirstPinnedPageNo + PinnedPageCount)
            {
                continue;
            }

            if (!s.Accessed) {
                PageNo2Slot.erase(s.PageNo);
                s.PageNo = InvalidPageNo;
                s.PV = {};
                *slot = &s;
                return {};
            }

            s.Accessed = false;
        }

        return MakeError(E_REJECTED, "could not acquire slot");
    }
};

////////////////////////////////////////////////////////////////////////////////

class TPageStore: public IPageStore
{
private:
    IStorageGroupPtr Storage;

protected:
    const TPageStoreConfig Config;

private:
    mutable TPageCache PageCache;
    mutable silk::FiberMutex Mutex;

    ui64 LastCommittedLsn = 0;
    ui64 Lsn = 0;
    ui64 LastLinkedLsn = 0;

public:
    TPageStore(IStorageGroupPtr storage, const TPageStoreConfig& config)
        : Storage(std::move(storage))
        , Config(config)
        , PageCache(Config.MaxCachedPages)
    {}

public:
    ui64 GetPageSize() const override
    {
        return Config.PageSize;
    }

    ui64 AllocateLsn() override;
    ui64 LinkRecord(ui64 lsn) override;
    void InitLastLsn(ui64 lsn) override;
    void CommitPages(ui64 lsn, const TVector<ui64>& pages) override;
    void RollbackPages(ui64 lsn, const TVector<ui64>& pages) override;
    NProto::TError WritePage(
        ui64 lsn,
        ui64 pageNo,
        TBuffer page,
        TVector<TPageGroup>& logRecord) override;
    NProto::TError ReadPageForLookup(ui64 pageNo, TBuffer* page) const override;
    NProto::TError
    ReadPageForUpdate(ui64 lsn, ui64 pageNo, TBuffer* page) const override;
    NProto::TError SetPinnedRange(ui64 firstPageNo, ui64 pageCount) override;
    void Clear() override;

private:
    NProto::TError DoReadPage(
        TPageCache::TSlot& cacheSlot,
        ui64 lsn,
        ui64 pageNo,
        TBuffer* page) const;
};

////////////////////////////////////////////////////////////////////////////////

ui64 TPageStore::AllocateLsn()
{
    std::lock_guard g(Mutex);
    return ++Lsn;
}

ui64 TPageStore::LinkRecord(ui64 lsn)
{
    std::lock_guard g(Mutex);
    Y_ABORT_UNLESS(
        lsn > LastLinkedLsn,
        "lsn %lu is not above the last linked one %lu",
        lsn,
        LastLinkedLsn);

    const ui64 prevLsn = LastLinkedLsn;
    LastLinkedLsn = lsn;
    return prevLsn;
}

void TPageStore::InitLastLsn(ui64 lsn)
{
    std::lock_guard g(Mutex);
    Y_ABORT_UNLESS(
        !Lsn,
        "lsn counter is already initialized: %lu, new value: %lu",
        Lsn,
        lsn);
    LastCommittedLsn = lsn;
    Lsn = lsn;
    LastLinkedLsn = lsn;
}

void TPageStore::CommitPages(ui64 lsn, const TVector<ui64>& pages)
{
    std::lock_guard g(Mutex);
    for (const ui64 pageNo: pages) {
        TPageCache::TSlot* cacheSlot;
        auto error = PageCache.Find(pageNo, &cacheSlot);

        //
        // If we are trying to commit something, it should be pinned in cache.
        //

        Y_ABORT_UNLESS(
            !HasError(error),
            "failed to access slot: %s",
            FormatError(error).c_str());

        Y_ABORT_UNLESS(cacheSlot);

        Y_DEFER {
            PageCache.Release(*cacheSlot);
        };

        auto& pv = cacheSlot->AccessPV();

        auto* it = pv.FindAtLsn(lsn);

        if (lsn <= pv.Committed.Lsn) {
            //
            // We might have received a commit notification for a later log
            // record already - this version has been erased then. Or the page
            // has been loaded after a later log record had been committed -
            // the loaded version includes this one, so this version is
            // obsolete and has to be erased along with the older ones.
            //

            if (it != pv.Dirty.end()) {
                pv.Dirty.erase(pv.Dirty.begin(), ++it);
            }

            continue;
        }

        //
        // This lsn must be present among the dirty ones because whenever we
        // apply a later commit we update the Committed version as well so we
        // should hit the previous if condition in case of log record commit
        // reordering.
        //

        Y_ABORT_UNLESS(it != pv.Dirty.end());

        //
        // Everything up to this lsn is committed. Reflecting this in the cache.
        //

        pv.Committed = std::move(*it);
        pv.Committed.Lsn = lsn;
        pv.State = EPageState::Loaded;
        pv.Dirty.erase(pv.Dirty.begin(), ++it);
    }

    LastCommittedLsn = Max(LastCommittedLsn, lsn);
}

void TPageStore::RollbackPages(ui64 lsn, const TVector<ui64>& pages)
{
    std::lock_guard g(Mutex);
    for (const ui64 pageNo: pages) {
        TPageCache::TSlot* cacheSlot;
        auto error = PageCache.Find(pageNo, &cacheSlot);

        //
        // Commit failed. The page should be pinned and no later lsn should be
        // marked as committed.
        //

        Y_ABORT_UNLESS(
            !HasError(error),
            "failed to access slot: %s",
            FormatError(error).c_str());

        Y_ABORT_UNLESS(cacheSlot);

        Y_DEFER {
            PageCache.Release(*cacheSlot);
        };

        auto& pv = cacheSlot->AccessPV();

        Y_ABORT_UNLESS(pv.Committed.Lsn < lsn);

        //
        // For the same reason the page must be present among the dirty ones.
        //

        auto* it = pv.FindAtLsn(lsn);
        Y_ABORT_UNLESS(it != pv.Dirty.end());

        //
        // We'll probably trigger full shard reinit in such cases later but for
        // now let's simply delete the rolled back version.
        //

        pv.Dirty.erase(it);
    }
}

NProto::TError TPageStore::WritePage(
    ui64 lsn,
    ui64 pageNo,
    TBuffer page,
    TVector<TPageGroup>& logRecord)
{
    std::lock_guard g(Mutex);
    TPageCache::TSlot* cacheSlot;
    auto error = PageCache.Access(pageNo, &cacheSlot);
    if (HasError(error)) {
        return error;
    }

    //
    // Linear search is ok because we don't expect to have more than a couple
    // page groups in log-record.
    //

    bool found = false;

    for (auto& pg: logRecord) {
        const ui64 endPageNo = pg.FirstPageNo + pg.Content.size();
        if (pg.FirstPageNo <= pageNo && endPageNo > pageNo) {
            pg.Content[pageNo - pg.FirstPageNo] = page;
            found = true;
        }
    }

    if (!found) {
        bool added = false;
        if (logRecord.size()) {
            const ui64 nextPageNo =
                logRecord.back().FirstPageNo + logRecord.back().Content.size();
            if (nextPageNo == pageNo) {
                logRecord.back().Content.push_back(page);
                added = true;
            }
        }

        if (!added) {
            logRecord.push_back(
                {.FirstPageNo = pageNo, .Content = TVector<TBuffer>({page})});
        }
    }

    //
    // Updating page cache.
    //

    auto& pv = cacheSlot->AccessPV();
    if (pv.State == EPageState::Loaded) {
        Y_ABORT_UNLESS(
            pv.Committed.Lsn < lsn,
            "p.Committed.Lsn=%lu, lsn=%lu, pageNo=%lu",
            pv.Committed.Lsn,
            lsn,
            pageNo);
    }

    if (pv.Dirty) {
        Y_ABORT_UNLESS(
            pv.Dirty.back().Lsn <= lsn,
            "p.Dirty.back().Lsn=%lu, lsn=%lu, pageNo=%lu",
            pv.Dirty.back().Lsn,
            lsn,
            pageNo);

        if (pv.Dirty.back().Lsn == lsn) {
            pv.Dirty.back().Content = std::move(page);
            return {};
        }
    }

    pv.Dirty.push_back({.Content = std::move(page), .Lsn = lsn});
    return {};
}

void TPageStore::Clear()
{
    std::lock_guard g(Mutex);
    PageCache.Clear();
}

NProto::TError TPageStore::ReadPageForLookup(ui64 pageNo, TBuffer* page) const
{
    page->Clear();

    TPageCache::TSlot* cacheSlot;
    {
        std::lock_guard g(Mutex);

        auto error = PageCache.Access(pageNo, &cacheSlot);
        if (HasError(error)) {
            return error;
        }

        Y_ABORT_UNLESS(cacheSlot);
        auto& pv = cacheSlot->AccessPV();

        switch (pv.State) {
            case EPageState::NotLoaded: {
                pv.State = EPageState::Loading;
                pv.Committed.Lsn = LastCommittedLsn;
                break;
            }

            case EPageState::Loading: {
                return MakeError(E_REJECTED, TStringBuilder() << "page "
                    << pageNo << " is loading");
            }

            case EPageState::Loaded: {
                *page = pv.Committed.Content;
                return {};
            }
        }
    }

    return DoReadPage(*cacheSlot, LastCommittedLsn, pageNo, page);
}

NProto::TError TPageStore::ReadPageForUpdate(
    ui64 lsn,
    ui64 pageNo,
    TBuffer* page) const
{
    page->Clear();

    TPageCache::TSlot* cacheSlot;
    {
        std::lock_guard g(Mutex);

        auto error = PageCache.Access(pageNo, &cacheSlot);
        if (HasError(error)) {
            return error;
        }

        Y_ABORT_UNLESS(cacheSlot);
        auto& pv = cacheSlot->AccessPV();

        switch (pv.State) {
            case EPageState::NotLoaded: {
                if (pv.Dirty.empty()) {
                    //
                    // Nothing to return, need to load the page.
                    //

                    pv.State = EPageState::Loading;
                    pv.Committed.Lsn = LastCommittedLsn;
                }

                break;
            }

            case EPageState::Loading: {
                return MakeError(E_REJECTED, TStringBuilder() << "page "
                    << pageNo << " is loading");
            }

            case EPageState::Loaded: {
                break;
            }
        }

        if (pv.State != EPageState::Loading) {
            //
            // We should not be trying to work with a page version older
            // than the last dirty one.
            //

            Y_ABORT_UNLESS(
                pv.LastLsn() <= lsn,
                "cachedPage->second.LastLsn()=%lu, lsn=%lu, pageNo=%lu",
                pv.LastLsn(),
                lsn,
                pageNo);

            *page = pv.LastContent();
            return {};
        }
    }

    return DoReadPage(*cacheSlot, LastCommittedLsn, pageNo, page);
}

NProto::TError TPageStore::SetPinnedRange(ui64 firstPageNo, ui64 pageCount)
{
    return PageCache.PinRange(firstPageNo, pageCount);
}

NProto::TError TPageStore::DoReadPage(
    TPageCache::TSlot& cacheSlot,
    ui64 lsn,
    ui64 pageNo,
    TBuffer* page) const
{
    Y_DEFER
    {
        std::lock_guard g(Mutex);

        //
        // A parallel write may have completed and replaced the Committed page
        // version already - in this case we don't need to do anything.
        // Otherwise we need to update the page.
        //

        auto& pv = cacheSlot.AccessPV();
        if (pv.State != EPageState::Loaded) {
            if (page->Empty()) {
                pv.State = EPageState::NotLoaded;
            } else {
                pv.Committed.Content = *page;
                pv.State = EPageState::Loaded;
            }
        }
    };

    if (!Storage) {
        return MakeError(E_NOT_FOUND);
    }

    TVector<TPageGroupRef> pageGroupRefs = {{
        .FirstPageNo = pageNo,
        .PageCount = 1,
    }};

    TVector<TPageGroup> pageGroups;

    auto error =
        Storage->ReadPages({} /* headers */, pageGroupRefs, &pageGroups);
    if (HasError(error)) {
        return error;
    }

    if (pageGroups.size() != 1) {
        return MakeError(
            E_BADMSG,
            TStringBuilder() << "lsn=" << lsn << ", pageNo=" << pageNo
                << ", unexpected pg count: " << pageGroups.size());
    }

    auto& rpg = pageGroups[0];
    if (rpg.Content.size() != 1) {
        return MakeError(
            E_BADMSG,
            TStringBuilder() << "lsn=" << lsn << ", pageNo=" << pageNo
                << "unexpected page count: " << rpg.Content.size());
    }

    if (rpg.Content[0].Size() < Config.PageSize) {
        return MakeError(
            E_BADMSG,
            TStringBuilder() << "lsn=" << lsn << ", pageNo=" << pageNo
                << "unexpected page size: " << rpg.Content[0].Size());
    }

    *page = std::move(rpg.Content[0]);
    return {};
}

////////////////////////////////////////////////////////////////////////////////

class TMemPageStore: public TPageStore
{
public:
    explicit TMemPageStore(const TPageStoreConfig& config)
        : TPageStore(nullptr /* storage */, config)
    {}

    NProto::TError
        ReadPageForUpdate(ui64 lsn, ui64 pageNo, TBuffer* page) const override
    {
        auto error = TPageStore::ReadPageForUpdate(lsn, pageNo, page);
        if (error.GetCode() == E_NOT_FOUND) {
            page->Clear();
            page->Fill(0, GetPageSize());
            return {};
        }

        return error;
    }

    NProto::TError ReadPageForLookup(ui64 pageNo, TBuffer* page) const override
    {
        auto error = TPageStore::ReadPageForLookup(pageNo, page);
        if (error.GetCode() == E_NOT_FOUND) {
            page->Clear();
            page->Fill(0, GetPageSize());
            return {};
        }

        return error;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IPageStorePtr CreatePageStore(
    IStorageGroupPtr storage,
    const TPageStoreConfig& config)
{
    return std::make_shared<TPageStore>(std::move(storage), config);
}

IPageStorePtr CreateMemPageStore(const TPageStoreConfig& config)
{
    return std::make_shared<TMemPageStore>(config);
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
