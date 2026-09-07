#pragma once

#include <cloud/filestore/libs/service/error.h>
#include <cloud/filestore/libs/storage/fastshard/sn/quorum/storage_group.h>

#include <util/generic/buffer.h>

#include <memory>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 PageSize = 4_KB;

////////////////////////////////////////////////////////////////////////////////

class IPageStore
{
public:
    virtual ~IPageStore() = default;

    virtual ui64 AllocateLsn() = 0;
    virtual void CommitPages(const TVector<ui64>& pages) = 0;
    virtual void RollbackPages(const TVector<ui64>& pages) = 0;
    [[nodiscard]] virtual NProto::TError WritePage(
        ui64 lsn,
        ui64 pageNo,
        TBuffer page,
        TVector<TPageGroup>& logRecord) = 0;
    [[nodiscard]] virtual NProto::TError
    ReadPage(ui64 lsn, ui64 pageNo, TBuffer* page) const = 0;
};

using IPageStorePtr = std::shared_ptr<IPageStore>;

////////////////////////////////////////////////////////////////////////////////

struct TWriteContext
{
    NProto::TDeviceRequestHeaders Headers;
    TVector<TPageGroup> PageGroups;
    ui64 Lsn = 0;
    bool PagesCollected = false;
};

inline TVector<ui64> CollectPages(TWriteContext& writeContext)
{
    TVector<ui64> pages;
    for (const auto& pg: writeContext.PageGroups) {
        for (ui64 i = 0; i < pg.Content.size(); ++i) {
            pages.push_back(pg.FirstPageNo + i);
        }
    }

    writeContext.PagesCollected = true;
    return pages;
}

class TWriteContextGuard
{
private:
    TWriteContext& Context;
    IPageStore& Store;

public:
    TWriteContextGuard(TWriteContext& context, IPageStore& store)
        : Context(context)
        , Store(store)
    {
    }

    ~TWriteContextGuard()
    {
        if (!Context.PagesCollected) {
            auto pages = CollectPages(Context);
            Store.RollbackPages(pages);
        }

        // TODO(#5895) - notify storage that this Lsn was skipped
    }

    // Must be called after shard mutex is taken. Otherwise concurrent shard ops
    // can race and cause PageStore updates which are not Lsn-ordered.
    void Init()
    {
        Context.Lsn = Store.AllocateLsn();
    }
};

////////////////////////////////////////////////////////////////////////////////

IPageStorePtr CreatePageStore(IStorageGroupPtr storage, ui64 pageSize);
IPageStorePtr CreateMemPageStore(ui64 pageSize);

}   // namespace NCloud::NFileStore::NStorage::NFastShard
