#include "page_store.h"

#include <cloud/storage/core/libs/common/error.h>

#include <silk/fibers/mutex.h>
#include <silk/util/logger.h>

#include <util/generic/scope.h>
#include <util/string/builder.h>

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
        TString Content;
        ui64 Lsn = 0;
        bool Dirty = false;
    };

    // TODO(#5895): eviction strategy + size limit
    using TPageCache = THashMap<ui64, TPage>;
    mutable TPageCache PageCache;
    mutable silk::FiberMutex Mutex;

    // TODO(#5895): properly initialize this
    ui64 Lsn = 0;

public:
    TPageStore(IStorageGroupPtr storage, ui64 pageSize)
        : Storage(std::move(storage))
        , PageSize(pageSize)
    {}

public:
    ui64 AllocateLsn() override;
    void CommitPages(const TVector<ui64>& pages) override;
    void RollbackPages(const TVector<ui64>& pages) override;
    NProto::TError WritePage(
        ui64 lsn,
        ui64 pageNo,
        TString page,
        TVector<TPageGroup>& logRecord) override;
    NProto::TError
    ReadPage(ui64 lsn, ui64 pageNo, TString* page) const override;
};

////////////////////////////////////////////////////////////////////////////////

ui64 TPageStore::AllocateLsn()
{
    std::lock_guard g(Mutex);
    return ++Lsn;
}

void TPageStore::CommitPages(const TVector<ui64>& pages)
{
    std::lock_guard g(Mutex);
    for (const ui64 pageNo: pages) {
        auto* page = PageCache.FindPtr(pageNo);
        Y_ABORT_UNLESS(page);
        page->Dirty = false;
    }
}

void TPageStore::RollbackPages(const TVector<ui64>& pages)
{
    std::lock_guard g(Mutex);
    for (const ui64 pageNo: pages) {
        auto it = PageCache.find(pageNo);
        Y_ABORT_UNLESS(it != PageCache.end());
        PageCache.erase(it);
    }
}

NProto::TError TPageStore::WritePage(
    ui64 lsn,
    ui64 pageNo,
    TString page,
    TVector<TPageGroup>& logRecord)
{
    std::lock_guard g(Mutex);
    auto& p = PageCache[pageNo];

    if (p.Dirty && p.Lsn != 0 && p.Lsn != lsn) {
        //
        // Doing it in the simplest way possible - just rejecting concurrent ops
        // for each page. We can implement tracking different page versions for
        // different lsns in the future if needed.
        //

        return MakeError(
            E_REJECTED,
            TStringBuilder()
                << "dirty page (" << pageNo << ") with different lsn (" << p.Lsn
                << " != " << lsn << ")");
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
        logRecord.push_back(
            {.FirstPageNo = pageNo, .Content = TVector<TString>({page})});
    }

    //
    // Updating page cache.
    //

    if (!p.Dirty) {
        Y_ABORT_UNLESS(p.Lsn <= lsn);
    }

    p = {.Content = std::move(page), .Lsn = lsn, .Dirty = true};

    return {};
}

NProto::TError TPageStore::ReadPage(ui64 lsn, ui64 pageNo, TString* page) const
{
    page->clear();

    TPageCache::iterator cachedPage;
    {
        std::lock_guard g(Mutex);

        TPageCache::insert_ctx insertCtx;
        cachedPage = PageCache.find(pageNo, insertCtx);
        if (cachedPage != PageCache.end()) {
            //
            // See the comment for WritePage() E_REJECTED error.
            //

            if (cachedPage->second.Dirty && cachedPage->second.Lsn != lsn) {
                return MakeError(
                    E_REJECTED,
                    TStringBuilder()
                        << "dirty page (" << pageNo << ") with different lsn ("
                        << cachedPage->second.Lsn << " != " << lsn << ")");
            }

            *page = cachedPage->second.Content;
            return {};
        }

        cachedPage = PageCache.insert_direct(
            std::make_pair(
                pageNo,
                TPage{.Content = {}, .Lsn = lsn, .Dirty = true}),
            insertCtx);
    }

    Y_DEFER
    {
        std::lock_guard g(Mutex);

        if (page->empty()) {
            PageCache.erase(cachedPage);
        } else {
            cachedPage->second.Content = *page;
            cachedPage->second.Dirty = false;
        }
    };

    if (!Storage) {
        return MakeError(E_NOT_FOUND);
    }

    NProto::TReadPagesRequest request;
    auto* pg = request.AddPageGroupRefs();
    pg->SetFirstPageNo(pageNo);
    pg->SetPageCount(1);
    pg->SetPageSize(PageSize);

    TVector<TPageGroupRef> pageGroupRefs = {{
        .FirstPageNo = pageNo,
        .PageCount = 1,
        .PageSize = PageSize,
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
            TStringBuilder() << "unexpected pg count: " << pageGroups.size());
    }

    auto& rpg = pageGroups[0];
    if (rpg.Content.size() != 1) {
        return MakeError(
            E_BADMSG,
            TStringBuilder()
                << "unexpected page count: " << rpg.Content.size());
    }

    if (rpg.Content[0].size() < PageSize) {
        return MakeError(
            E_BADMSG,
            TStringBuilder()
                << "unexpected page size: " << rpg.Content[0].size());
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

    NProto::TError ReadPage(ui64 lsn, ui64 pageNo, TString* page) const override
    {
        auto error = TPageStore::ReadPage(lsn, pageNo, page);
        if (error.GetCode() == E_NOT_FOUND) {
            *page = TString(PageSize, 0);
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
