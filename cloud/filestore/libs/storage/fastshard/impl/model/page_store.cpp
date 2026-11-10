#include "page_store.h"

#include <silk/fibers/mutex.h>
#include <silk/util/logger.h>

#include <util/generic/scope.h>
#include <util/string/builder.h>

#include <mutex>

namespace NCloud::NFileStore::NStorage::NFastShard {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TPageStore: public IPageStore
{
private:
    IStorageGroupPtr Storage;

protected:
    const ui64 PageSize;

private:
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

    struct TPageVersions
    {
        TPage Committed;
        TVector<TPage> Dirty;
        bool IsLoading = false;

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

    // TODO(#5895): eviction strategy + size limit
    using TPageCache = THashMap<ui64, TPageVersions>;
    mutable TPageCache PageCache;
    mutable silk::FiberMutex Mutex;

    ui64 LastCommittedLsn = 0;
    ui64 Lsn = 0;
    ui64 LastLinkedLsn = 0;

public:
    TPageStore(IStorageGroupPtr storage, ui64 pageSize)
        : Storage(std::move(storage))
        , PageSize(pageSize)
    {}

public:
    ui64 GetPageSize() const override
    {
        return PageSize;
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
    void Clear() override;

private:
    NProto::TError DoReadPage(
        TPageCache::iterator& cachedPage,
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
        auto* pageVersions = PageCache.FindPtr(pageNo);

        //
        // If we are trying to commit something, it should be pinned in cache.
        //

        Y_ABORT_UNLESS(pageVersions);

        if (lsn <= pageVersions->Committed.Lsn) {
            //
            // We might have received a commit notification for a later log
            // record already.
            //

            continue;
        }

        auto* it = pageVersions->FindAtLsn(lsn);

        //
        // This lsn must be present among the dirty ones because whenever we
        // apply a later commit we update the Committed version as well so we
        // should hit the previous if condition in case of log record commit
        // reordering.
        //

        Y_ABORT_UNLESS(it != pageVersions->Dirty.end());

        //
        // Everything up to this lsn is committed. Reflecting this in the cache.
        //

        pageVersions->Committed = std::move(*it);
        pageVersions->Committed.Lsn = lsn;
        pageVersions->Dirty.erase(pageVersions->Dirty.begin(), ++it);
    }

    LastCommittedLsn = Max(LastCommittedLsn, lsn);
}

void TPageStore::RollbackPages(ui64 lsn, const TVector<ui64>& pages)
{
    std::lock_guard g(Mutex);
    for (const ui64 pageNo: pages) {
        auto* pageVersions = PageCache.FindPtr(pageNo);

        //
        // Commit failed. The page should be pinned and no later lsn should be
        // marked as committed.
        //

        Y_ABORT_UNLESS(pageVersions);
        Y_ABORT_UNLESS(pageVersions->Committed.Lsn < lsn);

        //
        // For the same reason the page must be present among the dirty ones.
        //

        auto* it = pageVersions->FindAtLsn(lsn);
        Y_ABORT_UNLESS(it != pageVersions->Dirty.end());

        //
        // We'll probably trigger full shard reinit in such cases later but for
        // now let's simply delete the rolled back version.
        //

        pageVersions->Dirty.erase(it);
    }
}

NProto::TError TPageStore::WritePage(
    ui64 lsn,
    ui64 pageNo,
    TBuffer page,
    TVector<TPageGroup>& logRecord)
{
    std::lock_guard g(Mutex);
    auto& p = PageCache[pageNo];

    //
    // If we're writing the page, it should either not exist yet or be already
    // loaded. And when we implement eviction, we should implement pinning for
    // the pages that are being processed as well.
    //

    Y_ABORT_UNLESS(!p.IsLoading);

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

    if (p.Committed.Lsn) {
        Y_ABORT_UNLESS(
            p.Committed.Lsn < lsn,
            "p.Committed.Lsn=%lu, lsn=%lu, pageNo=%lu",
            p.Committed.Lsn,
            lsn,
            pageNo);
    }

    if (p.Dirty) {
        Y_ABORT_UNLESS(
            p.Dirty.back().Lsn <= lsn,
            "p.Dirty.back().Lsn=%lu, lsn=%lu, pageNo=%lu",
            p.Dirty.back().Lsn,
            lsn,
            pageNo);

        if (p.Dirty.back().Lsn == lsn) {
            p.Dirty.back().Content = std::move(page);
            return {};
        }
    }

    p.Dirty.push_back({.Content = std::move(page), .Lsn = lsn});
    return {};
}

void TPageStore::Clear()
{
    std::lock_guard g(Mutex);
    for ([[maybe_unused]] const auto& [pageNo, page]: PageCache) {
        Y_ABORT_UNLESS(!page.Dirty, "dirty page in Clear: %lu", pageNo);
    }
    PageCache.clear();
}

NProto::TError TPageStore::ReadPageForLookup(ui64 pageNo, TBuffer* page) const
{
    page->Clear();

    TPageCache::iterator cachedPage;
    {
        std::lock_guard g(Mutex);

        TPageCache::insert_ctx insertCtx{};
        cachedPage = PageCache.find(pageNo, insertCtx);
        if (cachedPage != PageCache.end()) {
            if (cachedPage->second.IsLoading) {
                return MakeError(E_REJECTED, TStringBuilder() << "page "
                    << pageNo << " is loading");
            }

            *page = cachedPage->second.Committed.Content;
            if (page->Empty()) {
                page->Fill(0, PageSize);
            }

            return {};
        }

        cachedPage = PageCache.insert_direct(
            std::make_pair(
                pageNo,
                TPageVersions{
                    .Committed = {.Content = {}, .Lsn = LastCommittedLsn},
                    .Dirty = {},
                    .IsLoading = true,
                }),
            insertCtx);
    }

    return DoReadPage(cachedPage, LastCommittedLsn, pageNo, page);
}

NProto::TError TPageStore::ReadPageForUpdate(
    ui64 lsn,
    ui64 pageNo,
    TBuffer* page) const
{
    page->Clear();

    TPageCache::iterator cachedPage;
    {
        std::lock_guard g(Mutex);

        TPageCache::insert_ctx insertCtx{};
        cachedPage = PageCache.find(pageNo, insertCtx);
        if (cachedPage != PageCache.end()) {
            if (cachedPage->second.IsLoading) {
                return MakeError(E_REJECTED, TStringBuilder() << "page "
                    << pageNo << " is loading");
            }

            //
            // We should not be trying to work with a page version older than
            // the last dirty one.
            //

            Y_ABORT_UNLESS(
                cachedPage->second.LastLsn() <= lsn,
                "cachedPage->second.LastLsn()=%lu, lsn=%lu, pageNo=%lu",
                cachedPage->second.LastLsn(),
                lsn,
                pageNo);

            *page = cachedPage->second.LastContent();
            return {};
        }

        cachedPage = PageCache.insert_direct(
            std::make_pair(
                pageNo,
                TPageVersions{
                    .Committed = {.Content = {}, .Lsn = lsn},
                    .Dirty = {},
                    .IsLoading = true,
                }),
            insertCtx);
    }

    return DoReadPage(cachedPage, LastCommittedLsn, pageNo, page);
}

NProto::TError TPageStore::DoReadPage(
    TPageCache::iterator& cachedPage,
    ui64 lsn,
    ui64 pageNo,
    TBuffer* page) const
{
    Y_DEFER
    {
        std::lock_guard g(Mutex);

        if (page->Empty()) {
            PageCache.erase(cachedPage);
        } else {
            cachedPage->second.Committed.Content = *page;
            cachedPage->second.IsLoading = false;
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

    if (rpg.Content[0].Size() < PageSize) {
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
    explicit TMemPageStore(ui64 pageSize)
        : TPageStore(nullptr /* storage */, pageSize)
    {}

    NProto::TError
        ReadPageForUpdate(ui64 lsn, ui64 pageNo, TBuffer* page) const override
    {
        auto error = TPageStore::ReadPageForUpdate(lsn, pageNo, page);
        if (error.GetCode() == E_NOT_FOUND) {
            page->Clear();
            page->Fill(0, PageSize);
            return {};
        }

        return error;
    }

    NProto::TError ReadPageForLookup(ui64 pageNo, TBuffer* page) const override
    {
        auto error = TPageStore::ReadPageForLookup(pageNo, page);
        if (error.GetCode() == E_NOT_FOUND) {
            page->Clear();
            page->Fill(0, PageSize);
            return {};
        }

        return error;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IPageStorePtr CreatePageStore(IStorageGroupPtr storage, ui64 pageSize)
{
    return std::make_shared<TPageStore>(std::move(storage), pageSize);
}

IPageStorePtr CreateMemPageStore(ui64 pageSize)
{
    return std::make_shared<TMemPageStore>(pageSize);
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
