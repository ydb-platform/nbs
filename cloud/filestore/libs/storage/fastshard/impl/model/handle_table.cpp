#include "handle_table.h"

#include "helpers.h"

#include <cloud/filestore/libs/storage/model/utils.h>

#include <util/random/random.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

ui64 THandleTable::Init(
    ui64 handlesPerGroup,
    ui64 firstPageNo,
    IPageStorePtr pageStore)
{
    const ui64 pageCount =
        RoundUp(handlesPerGroup, SlotsPerPage) / SlotsPerPage;
    THandleSlot tombstone{};
    tombstone.Handle = Max<ui64>();
    Slots = std::make_unique<THt>(
        firstPageNo,
        pageCount,
        PageSize,
        HandleSlotSize,
        tombstone,
        std::move(pageStore),
        [](const THandleSlot& s) -> ui64 { return s.Handle; },
        [](const ui64& handle) -> ui64 {
            return CityHash64(
                reinterpret_cast<const char*>(&handle),
                sizeof(handle));
        });

    return pageCount;
}

NProto::TError THandleTable::AllocateHandle(ui64* handle) const
{
    while (true) {
        *handle = ShardedId(RandomNumber<ui64>(), 0 /* shardNo */);

        ui64 slotNo = 0;
        THandleSlot slot{};
        auto error = Slots->Get(0 /* lsn */, *handle, &slot, &slotNo);

        if (!HasError(error)) {
            continue;
        }

        if (error.GetCode() == E_FS_NOENT) {
            return {};
        }

        return error;
    }
}

NProto::TError THandleTable::Put(THandleSlot v, TWriteContext& writeContext)
{
    return Slots->Put(writeContext.Lsn, v, writeContext.PageGroups);
}

NProto::TError THandleTable::Delete(ui64 handle, TWriteContext& writeContext)
{
    THandleSlot slot{};
    auto error = Slots->Delete(
        writeContext.Lsn,
        handle,
        &slot,
        writeContext.PageGroups);

    if (error.GetCode() == E_FS_NOENT) {
        error = ErrorInvalidHandle(handle);
    }

    return error;
}

NProto::TError THandleTable::Get(ui64 handle, ui64* nodeId) const
{
    ui64 slotNo = 0;
    THandleSlot slot{};
    auto error = Slots->Get(0 /* lsn */, handle, &slot, &slotNo);
    if (error.GetCode() == E_FS_NOENT) {
        return ErrorInvalidHandle(handle);
    }

    if (HasError(error)) {
        return error;
    }

    *nodeId = slot.NodeId;
    return {};
}

[[nodiscard]] NProto::TError THandleTable::CollectStats(
    TFileSystemShardStats* stats) const
{
    TPersistentHashTableStats slotStats;
    auto e = Slots->CollectStats(&slotStats);
    if (HasError(e)) {
        return e;
    }

    stats->TotalHandleCount = slotStats.SlotCount;
    stats->UsedHandleCount = slotStats.ValueCount;
    return {};
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
