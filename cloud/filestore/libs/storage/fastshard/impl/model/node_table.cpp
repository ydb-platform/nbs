#include "node_table.h"

#include "helpers.h"

#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/storage/model/utils.h>

#include <util/random/random.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

NProto::TNodeAttr Convert(const TNodeTableSlot& slot)
{
    NProto::TNodeAttr attr;
    attr.SetId(slot.Id);
    attr.SetType(slot.Type);
    attr.SetMode(slot.Mode);
    attr.SetUid(slot.Uid);
    attr.SetGid(slot.Gid);
    attr.SetATime(slot.ATime);
    attr.SetMTime(slot.MTime);
    attr.SetCTime(slot.CTime);
    attr.SetSize(slot.Size);
    attr.SetLinks(slot.Links);
    return attr;
}

TNodeTableSlot Convert(const NProto::TNodeAttr& attr)
{
    TNodeTableSlot slot{};
    slot.Id = attr.GetId();
    slot.Type = attr.GetType();
    slot.Mode = attr.GetMode();
    slot.Uid = attr.GetUid();
    slot.Gid = attr.GetGid();
    slot.ATime = attr.GetATime();
    slot.MTime = attr.GetMTime();
    slot.CTime = attr.GetCTime();
    slot.Size = attr.GetSize();
    slot.Links = attr.GetLinks();
    return slot;
}

////////////////////////////////////////////////////////////////////////////////

ui64 TNodeTable::Init(
    ui64 nodesPerGroup,
    ui64 firstPageNo,
    IPageStorePtr pageStore)
{
    const ui64 pageCount =
        Min(RoundUp(nodesPerGroup, SlotsPerPage),
            (NodeTableSize / PageSize) * SlotsPerPage) /
        SlotsPerPage;
    const TNodeTableSlot tombstone{.Id = Max<ui64>()};
    Slots = std::make_unique<THt>(
        firstPageNo,
        pageCount,
        PageSize,
        NodeSlotSize,
        tombstone,
        std::move(pageStore),
        [](const TNodeTableSlot& s) -> ui64 { return s.Id; },
        [](const ui64& nodeId) -> ui64 {
            return CityHash64(
                reinterpret_cast<const char*>(&nodeId),
                sizeof(nodeId));
        });

    return pageCount;
}

NProto::TError TNodeTable::AllocateNodeId(ui64* nodeId) const
{
    while (true) {
        *nodeId = ShardedId(RandomNumber<ui64>(), 0 /* shardNo */);
        NProto::TNodeAttr attr;
        auto error = GetNode(*nodeId, &attr);
        if (!HasError(error)) {
            continue;
        }

        if (error.GetCode() == E_FS_NOENT) {
            return {};
        }

        return error;
    }
}

NProto::TError TNodeTable::ResizeNode(
    ui64 nodeId,
    ui64 newSize,
    TWriteContext& writeContext)
{
    TNodeTableSlot slot{};
    ui64 slotNo = 0;
    auto error = Slots->Get(writeContext.Lsn, nodeId, &slot, &slotNo);
    if (HasError(error)) {
        return error;
    }

    slot.Size = Max(slot.Size, newSize);

    return Slots
        ->Update(writeContext.Lsn, slot, slotNo, writeContext.PageGroups);
}

NProto::TError TNodeTable::UpdateNode(
    ui64 nodeId,
    ui32 flags,
    const NProto::TSetNodeAttrRequest::TUpdate& update,
    NProto::TNodeAttr* attr,
    TVector<ui64>* pagesToDeallocate,
    TWriteContext& writeContext)
{
    TNodeTableSlot slot{};
    ui64 slotNo = 0;
    auto error = Slots->Get(writeContext.Lsn, nodeId, &slot, &slotNo);
    if (HasError(error)) {
        return error;
    }

    if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_MODE)) {
        slot.Mode = update.GetMode();
    }
    if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_UID)) {
        slot.Uid = update.GetUid();
    }
    if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_GID)) {
        slot.Gid = update.GetGid();
    }
    if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_ATIME)) {
        slot.ATime = update.GetATime();
    }
    if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_MTIME)) {
        slot.MTime = update.GetMTime();
    }
    if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_CTIME)) {
        slot.CTime = update.GetCTime();
    }
    if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE)) {
        if (slot.Size > update.GetSize()) {
            for (ui64 offset = AlignUp<ui64>(update.GetSize(), PageSize);
                    offset < slot.Size; offset += PageSize)
            {
                const ui64 pageNo = offset / PageSize;
                pagesToDeallocate->push_back(pageNo);
            }
        }

        slot.Size = update.GetSize();
    }

    error = Slots->Update(
        writeContext.Lsn,
        slot,
        slotNo,
        writeContext.PageGroups);
    if (HasError(error)) {
        return error;
    }

    *attr = Convert(slot);
    return {};
}

NProto::TError TNodeTable::PutNode(
    const NProto::TNodeAttr& attr,
    TWriteContext& writeContext)
{
    auto slot = Convert(attr);
    return Slots->Put(writeContext.Lsn, slot, writeContext.PageGroups);
}

NProto::TError TNodeTable::DeleteNode(
    ui64 nodeId,
    TWriteContext& writeContext,
    TNodeTableSlot* slot)
{
    return Slots
        ->Delete(writeContext.Lsn, nodeId, slot, writeContext.PageGroups);
}

NProto::TError TNodeTable::GetNode(ui64 nodeId, NProto::TNodeAttr* attr) const
{
    TNodeTableSlot slot{};
    ui64 slotNo = 0;
    auto error = Slots->Get(0 /* lsn */, nodeId, &slot, &slotNo);
    if (HasError(error)) {
        return error;
    }

    *attr = Convert(slot);
    return {};
}

[[nodiscard]] NProto::TError TNodeTable::CollectStats(
    TFileSystemShardStats* stats) const
{
    TPersistentHashTableStats slotStats;
    auto e = Slots->CollectStats(&slotStats);
    if (HasError(e)) {
        return e;
    }

    stats->TotalNodeCount = slotStats.SlotCount;
    stats->UsedNodeCount = slotStats.ValueCount;
    return {};
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
