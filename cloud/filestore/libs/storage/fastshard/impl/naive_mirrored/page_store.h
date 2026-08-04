#pragma once

#include <cloud/filestore/libs/service/error.h>
#include <cloud/filestore/libs/storage/fastshard/sn/quorum/storage_group.h>

#include <memory>

namespace NCloud::NFileStore::NStorage::NFastShard {

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
        TString page,
        TVector<TPageGroup>& logRecord) = 0;
    [[nodiscard]] virtual NProto::TError
    ReadPage(ui64 lsn, ui64 pageNo, TString* page) const = 0;
};

using IPageStorePtr = std::shared_ptr<IPageStore>;

////////////////////////////////////////////////////////////////////////////////

IPageStorePtr CreatePageStore(IStorageGroupPtr storage, ui64 pageSize);
IPageStorePtr CreateMemPageStore(ui64 pageSize);

}   // namespace NCloud::NFileStore::NStorage::NFastShard
