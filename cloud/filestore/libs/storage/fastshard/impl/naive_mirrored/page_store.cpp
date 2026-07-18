#include "page_store.h"

#include <cloud/storage/core/libs/common/error.h>

#include <silk/util/logger.h>

#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

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
    NProto::TWriteLogRecordRequest& logRecord)
{
    auto* pg = logRecord.AddPageGroups();
    pg->SetFirstPageNo(pageNo);
    pg->AddContent(page);
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

    NProto::TReadPagesRequest request;
    auto* pg = request.AddPageGroupRefs();
    pg->SetFirstPageNo(pageNo);
    pg->SetPageCount(1);
    pg->SetPageSize(PageSize);

    auto response = Storage->ReadPages(request);
    if (HasError(response.GetError())) {
        return response.GetError();
    }

    if (response.PageGroupsSize() != 1) {
        return MakeError(
            E_BADMSG,
            TStringBuilder() << "unexpected pg count: "
                << response.PageGroupsSize());
    }

    auto& rpg = *response.MutablePageGroups(0);
    if (rpg.ContentSize() != 1) {
        return MakeError(
            E_BADMSG,
            TStringBuilder() << "unexpected page count: "
                << rpg.ContentSize());
    }

    if (rpg.GetContent(0).size() < PageSize) {
        return MakeError(
            E_BADMSG,
            TStringBuilder() << "unexpected page size: "
                << rpg.GetContent(0).size());
    }

    *page = std::move(*rpg.MutableContent(0));
    PageCache[pageNo] = {.Content = *page, .Dirty = false};
    return {};
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
