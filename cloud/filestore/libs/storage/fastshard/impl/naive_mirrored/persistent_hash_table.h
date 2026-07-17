#pragma once

#include <cloud/filestore/libs/service/error.h>
#include <cloud/filestore/libs/storage/fastshard/sn/quorum/storage_group.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/digest/city.h>
#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

template <typename TKey, typename TValue>
class TPersistentHashTable
{
private:
    const ui64 FirstPageNo;
    const ui64 PageCount;
    const ui64 PageSize;
    const ui64 SlotCount;
    const ui64 SlotSize;
    const ui64 SlotsPerPage;
    const TValue Tombstone;
    IStorageGroupPtr Storage;

    using TMakeKey = std::function<TKey(const TValue&)>;
    TMakeKey MakeKey;

    using THash = std::function<ui64(const TKey&)>;
    THash Hash;

    ui64 SlotPointer = 0;

public:
    TPersistentHashTable(
            ui64 firstPageNo,
            ui64 pageCount,
            ui64 pageSize,
            ui64 slotCount,
            ui64 slotSize,
            const TValue& tombstone,
            IStorageGroupPtr storage,
            TMakeKey makeKey,
            THash hash)
        : FirstPageNo(firstPageNo)
        , PageCount(pageCount)
        , PageSize(pageSize)
        , SlotCount(slotCount)
        , SlotSize(slotSize)
        , SlotsPerPage(PageSize / SlotSize)
        , Tombstone(tombstone)
        , Storage(std::move(storage))
        , MakeKey(std::move(makeKey))
        , Hash(std::move(hash))
    {
        Y_ABORT_UNLESS(SlotCount * SlotSize <= PageCount * PageSize);
        Y_ABORT_UNLESS(sizeof(TValue) <= SlotSize);
    }

public:
    NProto::TError Put(
        const TValue& v,
        NProto::TWriteLogRecordRequest& logRecord)
    {
        auto k = MakeKey(v);
        TValue existing{};
        ui64 slotNo = 0;
        auto error = FindSlot(k, &existing, &slotNo);
        if (HasError(error)) {
            return error;
        }

        return DoPut(v, slotNo, logRecord);
    }

    NProto::TError Update(
        const TValue& v,
        const ui64 slotNo,
        NProto::TWriteLogRecordRequest& logRecord)
    {
        return DoPut(v, slotNo, logRecord);
    }

    NProto::TError Get(const TKey& k, TValue* v, ui64* slotNo) const
    {
        return FindSlot(k, v, slotNo);
    }

private:
    void WritePage(
        ui64 slotNo,
        TString page,
        NProto::TWriteLogRecordRequest& logRecord)
    {
        const ui64 pageNo = slotNo / SlotsPerPage;

        auto* pg = logRecord.AddPageGroups();
        pg->SetFirstPageNo(pageNo);
        pg->AddContent(std::move(page));
    }

    NProto::TError ReadPage(ui64 slotNo, TString* page) const
    {
        const ui64 pageNo = slotNo / SlotsPerPage;

        // TODO: page cache
        // and actually use a proper page-storage abstraction on top of storage
        // groups

        NProto::TReadPagesRequest request;
        auto* pg = request.AddPageGroupRefs();
        pg->SetFirstPageNo(pageNo);
        pg->SetPageCount(1);
        pg->SetPageSize(PageSize);

        auto response = Storage->ReadPages(request);
        if (HasError(response.GetError())) {
            return response.GetError();
        }

        if (response.PageGroupsSize() != 1) {
            return MakeError(
                E_BADMSG,
                TStringBuilder() << "unexpected pg count: "
                    << response.PageGroupsSize());
        }

        auto& rpg = *response.MutablePageGroups(0);
        if (rpg.ContentSize() != 1) {
            return MakeError(
                E_BADMSG,
                TStringBuilder() << "unexpected page count: "
                    << rpg.ContentSize());
        }

        if (rpg.GetContent(0).size() < PageSize) {
            return MakeError(
                E_BADMSG,
                TStringBuilder() << "unexpected page size: "
                    << rpg.GetContent(0).size());
        }

        *page = std::move(*rpg.MutableContent(0));
        return {};
    }

    NProto::TError LookupSlot(ui64 slotNo, TValue* v) const
    {
        TString page;
        auto error = ReadPage(slotNo, &page);
        if (HasError(error)) {
            return error;
        }

        const ui32 relSlotNo = slotNo % SlotsPerPage;
        const char* ptr = page.data() + relSlotNo * SlotSize;
        if (ptr[0] == 0 && memcmp(ptr, ptr + 1, sizeof(TValue) - 1)) {
            return MakeError(S_FALSE);
        }

        memcpy(v, ptr, sizeof(TValue));
        return {};
    }

    NProto::TError FindSlot(const TKey& k, TValue* v, ui64* slotNo) const
    {
        const ui64 h = Hash(k);
        const ui64 firstSlotNo = h % SlotCount;
        *slotNo = firstSlotNo;
        while (true) {
            auto error = LookupSlot(*slotNo, v);
            if (HasError(error)) {
                return error;
            }

            if (error.GetCode() == S_FALSE) {
                break;
            }

            if (k == MakeKey(*v)) {
                return {};
            }

            *slotNo = (*slotNo + 1) % SlotCount;
            if (*slotNo == firstSlotNo) {
                break;
            }
        }

        return MakeError(E_NOT_FOUND);
    }

    NProto::TError DoPut(
        const TValue& v,
        ui64 slotNo,
        NProto::TWriteLogRecordRequest& logRecord)
    {
        TString page;
        auto error = ReadPage(slotNo, &page);
        if (HasError(error)) {
            return error;
        }

        const ui32 relSlotNo = slotNo % SlotsPerPage;
        char* ptr = page.begin() + relSlotNo * SlotSize;
        memcpy(ptr, &v, sizeof(TValue));

        WritePage(slotNo, std::move(page), logRecord);
        return {};
    }

    NProto::TError AllocateSlot(ui64* slotNo)
    {
        *slotNo = SlotPointer;
        while (true) {
            TValue v{};
            auto error = LookupSlot(*slotNo, &v);
            if (HasError(error)) {
                return error;
            }

            if (error.GetCode() == S_FALSE) {
                break;
            }

            *slotNo = (*slotNo + 1) % SlotCount;
            if (*slotNo == SlotPointer) {
                return MakeError(E_FS_OUT_OF_SPACE, "no free node slot");
            }
        }

        SlotPointer = (*slotNo + 1) % SlotCount;
        return {};
    }
};

}   // namespace NCloud::NFileStore::NStorage::NFastShard
