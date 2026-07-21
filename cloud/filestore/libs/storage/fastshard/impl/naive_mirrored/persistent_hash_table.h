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
    const TKey TombstoneKey;
    IPageStorePtr PageStore;

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
            IPageStorePtr pageStore,
            TMakeKey makeKey,
            THash hash)
        : FirstPageNo(firstPageNo)
        , PageCount(pageCount)
        , PageSize(pageSize)
        , SlotCount(slotCount)
        , SlotSize(slotSize)
        , SlotsPerPage(PageSize / SlotSize)
        , Tombstone(tombstone)
        , TombstoneKey(makeKey(tombstone))
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
        // TODO: verify that there's no key change
        return DoPut(v, slotNo, pageGroups);
    }

    NProto::TError Get(const TKey& k, TValue* v, ui64* slotNo) const
    {
        return FindSlot(k, v, slotNo);
    }

    NProto::TError Delete(
        const TKey& k,
        TValue* v,
        TVector<TPageGroup>& pageGroups)
    {
        ui64 slotNo = 0;
        auto error = FindSlot(k, v, &slotNo);
        if (HasError(error)) {
            return error;
        }

        return DoDelete(slotNo, pageGroups);
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

            if (TombstoneKey == MakeKey(*v)) {
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

    struct TSlotIterator
    {
        const IPageStore& PageStore;
        const ui64 FirstPageNo;
        const ui64 SlotSize;
        const ui64 SlotCount;
        const ui64 SlotsPerPage;
        TString Page;
        bool Dirty = false;
        ui64 SlotNo;

        struct TDirtyPage
        {
            TString Content;
            ui64 PageNo = 0;
        };
        TVector<TDirtyPage> DirtyPages;

        TSlotIterator(
                IPageStore& pageStore,
                ui64 firstPageNo,
                ui64 slotSize,
                ui64 slotCount,
                ui64 slotsPerPage,
                ui64 slotNo)
            : PageStore(pageStore)
            , FirstPageNo(firstPageNo)
            , SlotSize(slotSize)
            , SlotCount(slotCount)
            , SlotsPerPage(slotsPerPage)
            , SlotNo(slotNo)
        {
        }

        NProto::TError Init()
        {
            const ui64 pageNo = FirstPageNo + SlotNo / SlotsPerPage;
            return PageStore.ReadPage(pageNo, &Page);
        }

        ui64 CurrentPageNo() const
        {
            return FirstPageNo + SlotNo / SlotsPerPage;
        }

        NProto::TError ToPrevSlot()
        {
            const ui64 pageNo = CurrentPageNo();
            if (SlotNo == 0) {
                SlotNo = SlotCount;
            }

            --SlotNo;
            const ui64 newPageNo = CurrentPageNo();
            if (newPageNo != pageNo) {
                if (Dirty) {
                    DirtyPages.push_back({
                        .Content = std::move(Page),
                        .PageNo = pageNo,
                    });
                }

                Dirty = false;
                auto error = PageStore.ReadPage(pageNo, &Page);
                if (HasError(error)) {
                    return error;
                }
            }

            return {};
        }

        const TValue& Get() const
        {
            const ui64 offsetInPage = (SlotNo % SlotsPerPage) * SlotSize;
            return *reinterpret_cast<const TValue*>(Page.data() + offsetInPage);
        }

        void Write(const char* data)
        {
            const ui64 offsetInPage = (SlotNo % SlotsPerPage) * SlotSize;
            memcpy(Page.begin() + offsetInPage, data, SlotSize);
            Dirty = true;
        }

        void Clear()
        {
            const ui64 offsetInPage = (SlotNo % SlotsPerPage) * SlotSize;
            memset(Page.begin() + offsetInPage, 0, SlotSize);
            Dirty = true;
        }

        TVector<TDirtyPage> Finish()
        {
            if (Dirty) {
                DirtyPages.push_back({
                    .Content = std::move(Page),
                    .PageNo = CurrentPageNo(),
                });
            }

            return std::move(DirtyPages);
        }
    };

    ui64 CalcSlotNo(const TKey& k) const
    {
        return Hash(k) % SlotCount;
    }

    ui64 CalcSlotNo(const TValue& v) const
    {
        return CalcSlotNo(MakeKey(v));
    }

    NProto::TError DoDelete(ui64 slotNo, TVector<TPageGroup>& pageGroups)
    {
        const ui64 nextSlotNo = (slotNo + 1) % SlotCount;

        TSlotIterator it(
            *PageStore,
            FirstPageNo,
            SlotSize,
            SlotsPerPage,
            nextSlotNo);

        //
        // Lookup next slot page - we need to check whether it's misplaced.
        //

        auto error = it.Init();
        if (HasError(error)) {
            return error;
        }

        const bool isNextSlotMisplaced = CalcSlotNo(it.Get()) != nextSlotNo;

        //
        // Moving back to the current slot.
        //

        error = it.ToPrevSlot();
        if (HasError(error)) {
            return error;
        }

        if (isNextSlotMisplaced) {
            //
            // The next slot contains a value which is misplaced due to hash
            // collisions. We need to write a tombstone - otherwise we won't
            // be able to find that value.
            //

            it.Write(reinterpret_cast<const char*>(&Tombstone));
        } else {
            //
            // The next slot contains a properly placed value - we can just
            // clear the current slot.
            //

            it.Clear();

            //
            // Trying to remove a consecutive range of tombstones preceding this
            // slot.
            //

            error = it.ToPrevSlot();
            if (HasError(error)) {
                return error;
            }

            while (true) {
                const auto curK = MakeKey(it.Get());
                if (curK != TombstoneKey) {
                    // not a tombstone
                    break;
                }

                if (CalcSlotNo(curK) == it.SlotNo) {
                    // not misplaced
                    break;
                }

                it.Clear();

                error = it.ToPrevSlot();
                if (HasError(error)) {
                    return error;
                }
            }
        }

        //
        // Writing the changes.
        //

        auto toWrite = it.Finish();
        for (auto& dp: toWrite) {
            PageStore->WritePage(dp.PageNo, std::move(dp.Content), pageGroups);
        }

        SILK_DEBUG(
            "pht DoDelete: slotNo=%lu, logRecordPGs=%lu",
            slotNo,
            pageGroups.size());

        return {};
    }
};

}   // namespace NCloud::NFileStore::NStorage::NFastShard
