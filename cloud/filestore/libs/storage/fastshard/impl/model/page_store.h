#pragma once

#include <cloud/fastshard/libs/storage_group/storage_group.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/buffer.h>

#include <memory>

namespace NCloud::NFileStore::NStorage::NFastShard {

using NCloud::NFastShard::IStorageGroup;
using NCloud::NFastShard::IStorageGroupPtr;
using NCloud::NFastShard::TLsnLink;
using NCloud::NFastShard::TPageGroup;
using NCloud::NFastShard::TPageGroupRef;

////////////////////////////////////////////////////////////////////////////////

class IPageStore
{
public:
    virtual ~IPageStore() = default;

    // Initializes lsn counter
    virtual void InitLastLsn(ui64 lsn) = 0;
    // Allocates next write lsn
    virtual ui64 AllocateLsn() = 0;
    // Exchanges w last successful write lsn
    virtual ui64 LinkRecord(ui64 lsn) = 0;

    [[nodiscard]] virtual ui64 GetPageSize() const = 0;
    virtual void CommitPages(ui64 lsn, const TVector<ui64>& pages) = 0;
    virtual void RollbackPages(ui64 lsn, const TVector<ui64>& pages) = 0;
    [[nodiscard]] virtual NProto::TError WritePage(
        ui64 lsn,
        ui64 pageNo,
        TBuffer page,
        TVector<TPageGroup>& logRecord) = 0;
    [[nodiscard]] virtual NProto::TError
    ReadPageForLookup(ui64 pageNo, TBuffer* page) const = 0;
    [[nodiscard]] virtual NProto::TError
    ReadPageForUpdate(ui64 lsn, ui64 pageNo, TBuffer* page) const = 0;

    /**
     * Forbids eviction of the pages in the supplied range. Intended to be used
     * for metadata pages.
     *
     * @param firstPageNo - the start of the range.
     * @param pageCount - the size of the range.
     */
    [[nodiscard]] virtual NProto::TError SetPinnedRange(
        ui64 firstPageNo,
        ui64 pageCount) = 0;

    /**
     * Drops all cached pages. Only committed pages may be cached at the
     * moment of the call.
     */
    virtual void Clear() = 0;
};

using IPageStorePtr = std::shared_ptr<IPageStore>;

////////////////////////////////////////////////////////////////////////////////

struct TWriteContext
{
    NProto::TDeviceRequestHeaders Headers;
    TVector<TPageGroup> PageGroups;
    ui64 Lsn = 0;
    ui64 PrevLsn = 0;
    bool PagesCollected = false;
    bool RolledBack = false;

    TLsnLink GetLink() const
    {
        return {
            .Lsn = Lsn,
            .PrevLsn = PrevLsn,
        };
    }
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
        Rollback();
    }

    // Must be called after shard mutex is taken. Otherwise concurrent shard ops
    // can race and cause PageStore updates which are not Lsn-ordered.
    void Init()
    {
        Context.Lsn = Store.AllocateLsn();
    }

    // Must be called before releasing Init mutex in case of successful
    // operation to chains it for write
    void Link()
    {
        Context.PrevLsn = Store.LinkRecord(Context.Lsn);
    }

    // Should be called explicitly for any pre-commit rollback. This should be
    // done while still holding all the required locks because otherwise a
    // concurrently running operation may read the pages that are never going
    // to be committed.
    void Rollback()
    {
        if (!Context.PagesCollected && !Context.RolledBack) {
            auto pages = CollectPages(Context);
            Store.RollbackPages(Context.Lsn, pages);
            Context.RolledBack = true;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TPageStoreConfig
{
    ui64 PageSize;
    ui64 MaxCachedPages;
};

IPageStorePtr CreatePageStore(
    IStorageGroupPtr storage,
    const TPageStoreConfig& config);
IPageStorePtr CreateMemPageStore(const TPageStoreConfig& config);

}   // namespace NCloud::NFileStore::NStorage::NFastShard
