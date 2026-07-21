#include "page_store.h"

#include <cloud/storage/core/libs/common/error.h>

#include <silk/util/logger.h>

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
        bool Dirty = false;
    };

    // TODO: eviction strategy + size limit
    mutable THashMap<ui64, TPage> PageCache;

public:
    TPageStore(
            IStorageGroupPtr storage,
            ui64 pageSize)
        : Storage(std::move(storage))
        , PageSize(pageSize)
    {
    }

public:
    void CommitPages(const TVector<ui64>& pages) override;
    void RollbackPages(const TVector<ui64>& pages) override;
    void WritePage(
        ui64 pageNo,
        TString page,
        TVector<TPageGroup>& logRecord) override;
    NProto::TError ReadPage(ui64 pageNo, TString* page) const override;
};

////////////////////////////////////////////////////////////////////////////////

void TPageStore::CommitPages(const TVector<ui64>& pages)
{
    for (const ui64 pageNo: pages) {
        auto* page = PageCache.FindPtr(pageNo);
        Y_ABORT_UNLESS(page);
        page->Dirty = false;
    }
}

void TPageStore::RollbackPages(const TVector<ui64>& pages)
{
    for (const ui64 pageNo: pages) {
        auto it = PageCache.find(pageNo);
        Y_ABORT_UNLESS(it != PageCache.end());
        PageCache.erase(it);
    }
}

void TPageStore::WritePage(
    ui64 pageNo,
    TString page,
    TVector<TPageGroup>& logRecord)
{
    logRecord.push_back({
        .FirstPageNo = pageNo,
        .Content = TVector<TString>({page})
    });
    PageCache[pageNo] = {.Content = std::move(page), .Dirty = true};
}

NProto::TError TPageStore::ReadPage(ui64 pageNo, TString* page) const
{
    if (const auto* ptr = PageCache.FindPtr(pageNo)) {
        // TODO: block reader upon dirty page read attempt instead of erroring
        if (ptr->Dirty) {
            return MakeError(
                E_REJECTED,
                TStringBuilder() << "dirty page: " << pageNo);
        }

        *page = ptr->Content;
        return {};
    }

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

    auto error = Storage->ReadPages(
        {} /* headers */,
        pageGroupRefs,
        &pageGroups);
    if (HasError(error)) {
        return error;
    }

    if (pageGroups.size() != 1) {
        return MakeError(
            E_BADMSG,
            TStringBuilder() << "unexpected pg count: "
                << pageGroups.size());
    }

    auto& rpg = pageGroups[0];
    if (rpg.Content.size() != 1) {
        return MakeError(
            E_BADMSG,
            TStringBuilder() << "unexpected page count: "
                << rpg.Content.size());
    }

    if (rpg.Content[0].size() < PageSize) {
        return MakeError(
            E_BADMSG,
            TStringBuilder() << "unexpected page size: "
                << rpg.Content[0].size());
    }

    *page = std::move(rpg.Content[0]);
    PageCache[pageNo] = {.Content = *page, .Dirty = false};
    return {};
}

////////////////////////////////////////////////////////////////////////////////

class TMemPageStore: public TPageStore
{
public:
    explicit TMemPageStore(ui64 pageSize)
        : TPageStore(nullptr /* storage */, pageSize)
    {
    }

    NProto::TError ReadPage(ui64 pageNo, TString* page) const override
    {
        auto error = TPageStore::ReadPage(pageNo, page);
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
