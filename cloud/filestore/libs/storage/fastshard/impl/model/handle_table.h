#pragma once

#include "page_store.h"
#include "persistent_hash_table.h"

#include <cloud/filestore/libs/service/error.h>
#include <cloud/filestore/libs/storage/fastshard/iface/fs.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////
// handle table layout

constexpr ui64 HandleSlotSize = 16;

struct THandleSlot
{
    ui64 Handle;
    ui64 NodeId;
};

static_assert(sizeof(THandleSlot) <= HandleSlotSize);

////////////////////////////////////////////////////////////////////////////////

class THandleTable
{
private:
    static constexpr ui64 SlotsPerPage = 256;
    static_assert(SlotsPerPage * HandleSlotSize <= PageSize);

    using THt = TPersistentHashTable<ui64, THandleSlot>;
    std::unique_ptr<THt> Slots;

public:
    ui64 Init(ui64 handlesPerGroup, ui64 firstPageNo, IPageStorePtr pageStore);

    [[nodiscard]] ui64 GetSlotCount() const
    {
        return Slots->GetSlotCount();
    }

    NProto::TError AllocateHandle(ui64* handle) const;

    NProto::TError Put(THandleSlot v, TWriteContext& writeContext);

    NProto::TError Delete(ui64 handle, TWriteContext& writeContext);

    NProto::TError Get(ui64 handle, ui64* nodeId) const;

    [[nodiscard]] NProto::TError CollectStats(
        TFileSystemShardStats* stats) const;
};

}   // namespace NCloud::NFileStore::NStorage::NFastShard
