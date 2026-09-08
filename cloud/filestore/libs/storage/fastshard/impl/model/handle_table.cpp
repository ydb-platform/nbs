#include "handle_table.h"

#include "helpers.h"

#include <cloud/filestore/libs/storage/model/utils.h>

#include <util/random/random.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

ui64 THandleTable::Init(
    ui64 nodesPerGroup,
    ui64 handlesPerGroup,
    ui64 firstPageNo,
    IPageStorePtr pageStore)
{
    ui64 totalPageCount = 0;
    {
        const ui64 pageCount = RoundUp(handlesPerGroup, HandleSlotsPerPage)
            / HandleSlotsPerPage;
        THandleSlot tombstone{};
        tombstone.Handle = Max<ui64>();
        Handles = std::make_unique<THandles>(
            firstPageNo,
            pageCount,
            PageSize,
            HandleSlotSize,
            tombstone,
            pageStore,
            [](const THandleSlot& s) -> ui64 { return s.Handle; },
            [](const ui64& handle) -> ui64 {
                return CityHash64(
                    reinterpret_cast<const char*>(&handle),
                    sizeof(handle));
            });

        totalPageCount += pageCount;
        firstPageNo += pageCount;
    }

    {
        const ui64 pageCount = RoundUp(nodesPerGroup, NodeHandlesSlotsPerPage)
            / NodeHandlesSlotsPerPage;
        TNodeHandlesSlot tombstone{};
        tombstone.NodeId = Max<ui64>();
        NodeId2HandleCount = std::make_unique<TNodeId2HandleCount>(
            firstPageNo,
            pageCount,
            PageSize,
            NodeHandlesSlotSize,
            tombstone,
            std::move(pageStore),
            [](const TNodeHandlesSlot& s) -> ui64 { return s.NodeId; },
            [](const ui64& nodeId) -> ui64 {
                return CityHash64(
                    reinterpret_cast<const char*>(&nodeId),
                    sizeof(nodeId));
            });

        totalPageCount += pageCount;
        firstPageNo += pageCount;
    }

    return totalPageCount;
}

NProto::TError THandleTable::AllocateHandle(ui64* handle) const
{
    while (true) {
        *handle = ShardedId(RandomNumber<ui64>(), 0 /* shardNo */);

        ui64 slotNo = 0;
        THandleSlot slot{};
        auto error = Handles->Get(0 /* lsn */, *handle, &slot, &slotNo);

        if (!HasError(error)) {
            continue;
        }

        if (error.GetCode() == E_FS_NOENT) {
            return {};
        }

        return error;
    }
}

NProto::TError THandleTable::Put(
    THandleSlot handle,
    TWriteContext& writeContext)
{
    auto error = Handles->Put(
        writeContext.Lsn,
        handle,
        writeContext.PageGroups);
    if (HasError(error)) {
        return error;
    }

    TNodeHandlesSlot nodeHandles{};
    ui64 slotNo = 0;
    error = NodeId2HandleCount->Get(
        writeContext.Lsn,
        handle.NodeId,
        &nodeHandles,
        &slotNo);
    if (error.GetCode() == E_FS_NOENT) {
        nodeHandles.NodeId = handle.NodeId;
        nodeHandles.HandleCount = 1;
        return NodeId2HandleCount->Put(
            writeContext.Lsn,
            nodeHandles,
            writeContext.PageGroups);
    }

    if (HasError(error)) {
        return error;
    }

    ++nodeHandles.HandleCount;
    return NodeId2HandleCount->Update(
        writeContext.Lsn,
        nodeHandles,
        slotNo,
        writeContext.PageGroups);
}

NProto::TError THandleTable::Delete(
    ui64 handle,
    TWriteContext& writeContext,
    TNodeHandlesSlot* nodeHandles)
{
    THandleSlot slot{};
    auto error = Handles->Delete(
        writeContext.Lsn,
        handle,
        &slot,
        writeContext.PageGroups);

    if (error.GetCode() == E_FS_NOENT) {
        return ErrorInvalidHandle(handle);
    }

    ui64 slotNo = 0;
    error = NodeId2HandleCount->Get(
        writeContext.Lsn,
        slot.NodeId,
        nodeHandles,
        &slotNo);
    if (HasError(error)) {
        return error;
    }

    --nodeHandles->HandleCount;

    if (nodeHandles->HandleCount) {
        error = NodeId2HandleCount->Update(
            writeContext.Lsn,
            *nodeHandles,
            slotNo,
            writeContext.PageGroups);
    } else {
        TNodeHandlesSlot dummy{};
        error = NodeId2HandleCount->Delete(
            writeContext.Lsn,
            nodeHandles->NodeId,
            &dummy,
            writeContext.PageGroups);
    }

    return error;
}

NProto::TError THandleTable::Get(
    ui64 handle,
    TNodeHandlesSlot* nodeHandles) const
{
    ui64 slotNo = 0;
    THandleSlot slot{};
    auto error = Handles->Get(0 /* lsn */, handle, &slot, &slotNo);
    if (error.GetCode() == E_FS_NOENT) {
        return ErrorInvalidHandle(handle);
    }

    if (HasError(error)) {
        return error;
    }

    error = NodeId2HandleCount->Get(
        0 /* lsn */,
        slot.NodeId,
        nodeHandles,
        &slotNo);
    if (HasError(error)) {
        return error;
    }

    return {};
}

NProto::TError THandleTable::GetNodeId(ui64 handle, ui64* nodeId) const
{
    ui64 slotNo = 0;
    THandleSlot slot{};
    auto error = Handles->Get(0 /* lsn */, handle, &slot, &slotNo);
    if (error.GetCode() == E_FS_NOENT) {
        return ErrorInvalidHandle(handle);
    }

    if (HasError(error)) {
        return error;
    }

    *nodeId = slot.NodeId;
    return {};
}

NProto::TError THandleTable::GetNodeHandleCount(
    ui64 nodeId,
    ui64* handleCount) const
{
    TNodeHandlesSlot nodeHandles{};
    ui64 slotNo = 0;
    auto error =
        NodeId2HandleCount->Get(0 /* lsn */, nodeId, &nodeHandles, &slotNo);

    if (error.GetCode() == E_FS_NOENT) {
        *handleCount = 0;
        return {};
    }

    if (HasError(error)) {
        return error;
    }

    *handleCount = nodeHandles.HandleCount;
    return {};
}

[[nodiscard]] NProto::TError THandleTable::CollectStats(
    TFileSystemShardStats* stats) const
{
    TPersistentHashTableStats slotStats;
    auto e = Handles->CollectStats(&slotStats);
    if (HasError(e)) {
        return e;
    }

    stats->TotalHandleCount = slotStats.SlotCount;
    stats->UsedHandleCount = slotStats.ValueCount;
    return {};
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
