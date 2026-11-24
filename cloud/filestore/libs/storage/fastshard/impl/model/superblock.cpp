#include "superblock.h"

#include <cloud/filestore/libs/storage/model/utils.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

/* static */ ui32 TSuperBlock::CalcRequiredPageCount()
{
    ui64 totalPageCount = 0;

    //
    // FormatPage.
    //

    totalPageCount += 1;

    //
    // Content.
    //

    totalPageCount += 1;

    return totalPageCount;
}

ui64 TSuperBlock::Init(ui64 firstPageNo, IPageStorePtr pageStore)
{
    TDescriptionBuilder debuilder("SuperBlock");

    ui64 totalPageCount = 0;

    FormatPage.Init(firstPageNo, pageStore);

    totalPageCount += 1;
    firstPageNo += 1;

    {
        debuilder.RegisterOffset("Content", firstPageNo);
        PageNo = firstPageNo;

        totalPageCount += 1;
        firstPageNo += 1;
    }

    Description = debuilder.Build();
    PageStore = std::move(pageStore);

    return totalPageCount;
}

NProto::TError TSuperBlock::AccessSlot(
    ui64 lsn,
    TBuffer& page,
    TSuperBlockSlot* slot)
{
    auto error = PageStore->ReadPageForUpdate(lsn, PageNo, &page);
    if (HasError(error)) {
        return error;
    }

    Y_ABORT_UNLESS(page.Size() == PageStore->GetPageSize());

    char* slotPtr = reinterpret_cast<char*>(slot);
    memcpy(slotPtr, page.Data(), sizeof(*slot));
    const bool isEmpty =
        *slotPtr == 0 && memcmp(slotPtr, slotPtr + 1, sizeof(*slot) - 1) == 0;
    if (isEmpty) {
        slot->LastNodeId = 1;
        slot->LastHandleId = 1;
    }

    if (slot->LastNodeId >= ShardedId(Max<ui64>(), 0 /* shardNo */)) {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder() << "NodeId overflow: " << slot->LastNodeId);
    }

    if (slot->LastHandleId >= ShardedId(Max<ui64>(), 0 /* shardNo */)) {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder() << "HandleId overflow: " << slot->LastHandleId);
    }

    return {};
}

NProto::TError TSuperBlock::AllocateNodeId(
    TWriteContext& writeContext,
    ui64* nodeId)
{
    TBuffer page;
    TSuperBlockSlot slot;
    auto error = AccessSlot(writeContext.Lsn, page, &slot);
    if (HasError(error)) {
        return error;
    }

    *nodeId = ++slot.LastNodeId;

    page.Clear();
    page.Resize(PageStore->GetPageSize());

    memcpy(page.Data(), reinterpret_cast<char*>(&slot), sizeof(slot));

    return PageStore->WritePage(
        writeContext.Lsn,
        PageNo,
        page,
        writeContext.PageGroups);
}

NProto::TError TSuperBlock::AllocateHandleId(
    TWriteContext& writeContext,
    ui64* handleId)
{
    TBuffer page;
    TSuperBlockSlot slot;
    auto error = AccessSlot(writeContext.Lsn, page, &slot);
    if (HasError(error)) {
        return error;
    }

    *handleId = ++slot.LastHandleId;

    page.Clear();
    page.Resize(PageStore->GetPageSize());

    memcpy(page.Data(), reinterpret_cast<char*>(&slot), sizeof(slot));

    return PageStore->WritePage(
        writeContext.Lsn,
        PageNo,
        page,
        writeContext.PageGroups);
}

NProto::TError TSuperBlock::CollectStats(TFileSystemShardStats* stats) const
{
    TBuffer page;
    auto error = PageStore->ReadPageForLookup(PageNo, &page);
    if (HasError(error)) {
        return error;
    }

    Y_ABORT_UNLESS(page.Size() == PageStore->GetPageSize());

    TSuperBlockSlot slot;
    char* slotPtr = reinterpret_cast<char*>(&slot);
    memcpy(slotPtr, page.Data(), sizeof(slot));
    stats->LastNodeId = slot.LastNodeId;
    stats->LastHandleId = slot.LastHandleId;

    return {};
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
