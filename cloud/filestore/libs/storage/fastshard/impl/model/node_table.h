#pragma once

#include "page_store.h"
#include "persistent_hash_table.h"

#include <cloud/filestore/libs/service/error.h>
#include <cloud/filestore/libs/storage/fastshard/iface/fs.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////
// inode table layout

constexpr ui64 NodeSlotSize = 96;   // bigger than the current slot struct - in
                                    // order not to drop all data if we decide
                                    // to add something to the slot struct
constexpr ui64 NodeTableSize = 512_MB;

struct TNodeTableSlot
{
    ui64 Id;
    ui32 Type;
    ui32 Mode;
    ui32 Uid;
    ui32 Gid;
    ui64 ATime;
    ui64 MTime;
    ui64 CTime;
    ui64 Size;
    ui32 Links;
};

static_assert(sizeof(TNodeTableSlot) <= NodeSlotSize);
static_assert(NodeSlotSize % alignof(TNodeTableSlot) == 0);

////////////////////////////////////////////////////////////////////////////////

NProto::TNodeAttr Convert(const TNodeTableSlot& slot);
TNodeTableSlot Convert(const NProto::TNodeAttr& attr);

////////////////////////////////////////////////////////////////////////////////
// This data structure is a PoC, it's not really efficient, can be easily
// optimized.

class TNodeTable
{
private:
    static constexpr ui64 SlotsPerPage = 42;
    static_assert(SlotsPerPage * NodeSlotSize <= PageSize);

    using THt = TPersistentHashTable<ui64, TNodeTableSlot>;
    std::unique_ptr<THt> Slots;

public:
    ui64 Init(ui64 nodesPerGroup, ui64 firstPageNo, IPageStorePtr pageStore);

    [[nodiscard]] ui64 GetSlotCount() const
    {
        return Slots->GetSlotCount();
    }

    NProto::TError AllocateNodeId(ui64* nodeId) const;

    NProto::TError ResizeNode(
        ui64 nodeId,
        ui64 newSize,
        TWriteContext& writeContext);

    NProto::TError UpdateNode(
        ui64 nodeId,
        ui32 flags,
        const NProto::TSetNodeAttrRequest::TUpdate& update,
        NProto::TNodeAttr* attr,
        TVector<ui64>* pagesToDeallocate,
        TWriteContext& writeContext);

    NProto::TError PutNode(
        const NProto::TNodeAttr& attr,
        TWriteContext& writeContext);

    NProto::TError DeleteNode(
        ui64 nodeId,
        TWriteContext& writeContext,
        TNodeTableSlot* slot);

    NProto::TError GetNode(ui64 nodeId, NProto::TNodeAttr* attr) const;

    [[nodiscard]] NProto::TError CollectStats(
        TFileSystemShardStats* stats) const;
};

}   // namespace NCloud::NFileStore::NStorage::NFastShard
