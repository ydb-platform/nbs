#pragma once

#include "page_store.h"

#include <cloud/filestore/libs/service/error.h>

#include <cloud/storage/core/libs/common/error.h>

#include <silk/util/logger.h>

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
    TPageStorePtr PageStore;

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
            TPageStorePtr pageStore,
            TMakeKey makeKey,
            THash hash)
        : FirstPageNo(firstPageNo)
        , PageCount(pageCount)
        , PageSize(pageSize)
        , SlotCount(slotCount)
        , SlotSize(slotSize)
        , SlotsPerPage(PageSize / SlotSize)
        , Tombstone(tombstone)
        , PageStore(std::move(pageStore))
        , MakeKey(std::move(makeKey))
        , Hash(std::move(hash))
    {
        Y_ABORT_UNLESS(SlotCount * SlotSize <= PageCount * PageSize);
        Y_ABORT_UNLESS(sizeof(TValue) <= SlotSize);
    }

public:
    NProto::TError Put(const TValue& v, TVector<TPageGroup>& pageGroups)
    {
        auto k = MakeKey(v);
        TValue existing{};
        ui64 slotNo = 0;
        auto error = AllocateSlot(k, &existing, &slotNo);
        if (HasError(error)) {
            return error;
        }

        return DoPut(v, slotNo, pageGroups);
    }

    NProto::TError Update(
        const TValue& v,
        const ui64 slotNo,
        TVector<TPageGroup>& pageGroups)
    {
        return DoPut(v, slotNo, pageGroups);
    }

    NProto::TError Get(const TKey& k, TValue* v, ui64* slotNo) const
    {
        return FindSlot(k, v, slotNo);
    }

private:
    void WritePage(
        ui64 slotNo,
        TString page,
        TVector<TPageGroup>& pageGroups)
    {
        const ui64 pageNo = FirstPageNo + slotNo / SlotsPerPage;
        PageStore->WritePage(pageNo, std::move(page), pageGroups);
    }

    NProto::TError ReadPage(ui64 slotNo, TString* page) const
    {
        const ui64 pageNo = FirstPageNo + slotNo / SlotsPerPage;
        return PageStore->ReadPage(pageNo, page);
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
        if (ptr[0] == 0 && memcmp(ptr, ptr + 1, sizeof(TValue) - 1) == 0) {
            return MakeError(S_FALSE);
        }

        memcpy(v, ptr, sizeof(TValue));
        return {};
    }

    NProto::TError AllocateSlot(const TKey& k, TValue* v, ui64* slotNo) const
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
                return MakeError(E_FS_EXIST);
            }

            *slotNo = (*slotNo + 1) % SlotCount;
            if (*slotNo == firstSlotNo) {
                return MakeError(E_FS_OUT_OF_SPACE, "no free node slot");
            }
        }

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

        return MakeError(E_FS_NOENT);
    }

    NProto::TError DoPut(
        const TValue& v,
        ui64 slotNo,
        TVector<TPageGroup>& pageGroups)
    {
        TString page;
        auto error = ReadPage(slotNo, &page);
        if (HasError(error)) {
            return error;
        }

        const ui32 relSlotNo = slotNo % SlotsPerPage;
        char* ptr = page.begin() + relSlotNo * SlotSize;
        memcpy(ptr, &v, sizeof(TValue));

        WritePage(slotNo, std::move(page), pageGroups);

        SILK_DEBUG(
            "pht DoPut: slotNo=%lu, logRecordPGs=%lu",
            slotNo,
            pageGroups.size());

        return {};
    }
};

}   // namespace NCloud::NFileStore::NStorage::NFastShard
