#pragma once

#include <cloud/filestore/libs/service/error.h>
#include <cloud/filestore/libs/storage/fastshard/sn/quorum/storage_group.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

class TPageStore
{
private:
    IStorageGroupPtr Storage;
    const ui64 PageSize;

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
    void CommitPages(const TVector<ui64>& pages);
    void RollbackPages(const TVector<ui64>& pages);
    void WritePage(
        ui64 pageNo,
        TString page,
        NProto::TWriteLogRecordRequest& logRecord);
    NProto::TError ReadPage(ui64 pageNo, TString* page) const;
};

using TPageStorePtr = std::shared_ptr<TPageStore>;

}   // namespace NCloud::NFileStore::NStorage::NFastShard
