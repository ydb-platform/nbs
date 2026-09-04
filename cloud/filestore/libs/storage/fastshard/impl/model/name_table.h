#pragma once

#include "page_store.h"
#include "persistent_hash_table.h"

#include <cloud/filestore/libs/service/error.h>
#include <cloud/filestore/libs/storage/fastshard/iface/fs.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////
// name table layout

constexpr ui64 NameSlotSize = 48;
constexpr ui32 NameCapacity = 36;
constexpr ui64 NameTableSize = 512_MB;

struct TNameTableSlot
{
    char Name[NameCapacity];
    ui64 NodeId;
};

static_assert(sizeof(TNameTableSlot) <= NameSlotSize);

////////////////////////////////////////////////////////////////////////////////

class TNameTable
{
private:
    static constexpr ui64 SlotsPerPage = PageSize / NameSlotSize;
    static_assert(SlotsPerPage * NameSlotSize <= PageSize);

    using THt = TPersistentHashTable<TStringBuf, TNameTableSlot>;
    TNameTableSlot Tombstone{};
    std::unique_ptr<THt> Slots;

public:
    ui64 Init(ui64 nodesPerGroup, ui64 firstPageNo, IPageStorePtr pageStore);

    [[nodiscard]] ui64 GetSlotCount() const
    {
        return Slots->GetSlotCount();
    }

    NProto::TError
    Put(const TString& name, ui64 nodeId, TWriteContext& writeContext);

    NProto::TError Delete(const TString& name, TWriteContext& writeContext);

    NProto::TError Get(const TString& name, ui64* nodeId) const;

    [[nodiscard]] NProto::TError CollectStats(
        TFileSystemShardStats* stats) const;
};

}   // namespace NCloud::NFileStore::NStorage::NFastShard
