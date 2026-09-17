#include "name_table.h"

#include "helpers.h"

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

ui64 TNameTable::Init(
    ui64 nodesPerGroup,
    ui64 firstPageNo,
    IPageStorePtr pageStore)
{
    const ui64 pageSize = pageStore->GetPageSize();
    ui64 totalPageCount = 0;
    {
        const ui64 pageCount = FormatPage.Init(firstPageNo, pageStore);

        totalPageCount += pageCount;
        firstPageNo += pageCount;
    }

    {
        const ui64 slotsPerPage = pageSize / NameSlotSize;
        const ui64 pageCount =
            Min(RoundUp(nodesPerGroup, slotsPerPage),
                (NameTableSize / pageSize) * slotsPerPage) /
            slotsPerPage;
        // Tombstone key needs to be different from an empty slot key
        memset(Tombstone.Name, 1, NameCapacity - 1);
        Tombstone.NodeId = Max<ui64>();
        Slots = std::make_unique<THt>(
            firstPageNo,
            pageCount,
            pageSize,
            NameSlotSize,
            Tombstone,
            std::move(pageStore),
            [](const TNameTableSlot& s) -> TStringBuf
            { return {s.Name, strlen(s.Name)}; },
            [](const TStringBuf& name) -> ui64
            { return CityHash64(name.data(), name.size()); });

        totalPageCount += pageCount;
        firstPageNo += pageCount;
    }

    return totalPageCount;
}

NProto::TError TNameTable::Put(
    const TString& name,
    ui64 nodeId,
    TWriteContext& writeContext)
{
    if (name.size() >= NameCapacity) {
        return ErrorNameTooLong(name);
    }

    TNameTableSlot slot{};
    memcpy(slot.Name, name.data(), name.size());
    memset(slot.Name + name.size(), 0, NameCapacity - name.size());
    slot.NodeId = nodeId;
    return Slots->Put(writeContext.Lsn, slot, writeContext.PageGroups);
}

NProto::TError TNameTable::Delete(
    const TString& name,
    TWriteContext& writeContext)
{
    TNameTableSlot slot{};
    return Slots
        ->Delete(writeContext.Lsn, name, &slot, writeContext.PageGroups);
}

NProto::TError TNameTable::Get(const TString& name, ui64* nodeId) const
{
    ui64 slotNo = 0;
    TNameTableSlot slot{};
    auto error = Slots->Get(0 /* lsn */, name.c_str(), &slot, &slotNo);
    if (HasError(error)) {
        return error;
    }

    *nodeId = slot.NodeId;
    return {};
}

[[nodiscard]] NProto::TError TNameTable::CollectStats(
    TFileSystemShardStats* stats) const
{
    TPersistentHashTableStats slotStats;
    auto e = Slots->CollectStats(&slotStats);
    if (HasError(e)) {
        return e;
    }

    stats->TotalNameCount = slotStats.SlotCount;
    stats->UsedNameCount = slotStats.ValueCount;
    return {};
}

NProto::TError TNameTable::CheckFormat(TWriteContext& writeContext)
{
    return FormatPage.RegisterStart(
        NameTableLayoutMinVersion,
        NameTableLayoutVersion,
        writeContext);
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
