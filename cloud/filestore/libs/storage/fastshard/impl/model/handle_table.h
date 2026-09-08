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

constexpr ui64 NodeHandlesSlotSize = 16;

struct TNodeHandlesSlot
{
    ui64 NodeId;
    ui64 HandleCount;
};

static_assert(sizeof(TNodeHandlesSlot) <= NodeHandlesSlotSize);

////////////////////////////////////////////////////////////////////////////////

class THandleTable
{
private:
    static constexpr ui64 HandleSlotsPerPage = 256;
    static_assert(HandleSlotsPerPage * HandleSlotSize <= PageSize);
    static constexpr ui64 NodeHandlesSlotsPerPage = 256;
    static_assert(NodeHandlesSlotsPerPage * NodeHandlesSlotSize <= PageSize);

    using THandles = TPersistentHashTable<ui64, THandleSlot>;
    std::unique_ptr<THandles> Handles;
    using TNodeId2HandleCount = TPersistentHashTable<ui64, TNodeHandlesSlot>;
    std::unique_ptr<TNodeId2HandleCount> NodeId2HandleCount;

public:
    ui64 Init(
        ui64 nodesPerGroup,
        ui64 handlesPerGroup,
        ui64 firstPageNo,
        IPageStorePtr pageStore);

    [[nodiscard]] ui64 GetSlotCount() const
    {
        return Handles->GetSlotCount() + NodeId2HandleCount->GetSlotCount();
    }

    NProto::TError AllocateHandle(ui64* handle) const;

    NProto::TError Put(THandleSlot handle, TWriteContext& writeContext);

    NProto::TError Delete(
        ui64 handle,
        TWriteContext& writeContext,
        TNodeHandlesSlot* nodeHandles);

    NProto::TError Get(ui64 handle, TNodeHandlesSlot* nodeHandles) const;
    NProto::TError GetNodeId(ui64 handle, ui64* nodeId) const;
    NProto::TError GetNodeHandleCount(ui64 nodeId, ui64* handleCount) const;

    [[nodiscard]] NProto::TError CollectStats(
        TFileSystemShardStats* stats) const;
};

}   // namespace NCloud::NFileStore::NStorage::NFastShard
