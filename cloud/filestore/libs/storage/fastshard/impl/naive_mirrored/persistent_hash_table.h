#pragma once

#include "page_store.h"

#include <cloud/filestore/libs/service/error.h>

#include <cloud/storage/core/libs/common/error.h>

#include <silk/util/logger.h>

#include <util/digest/city.h>
#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

struct TPersistentHashTableStats
{
    ui64 SlotCount = 0;
    ui64 ValueCount = 0;
    ui64 MisplacedValueCount = 0;
    ui64 TombstoneCount = 0;
};

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

private:
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

        NProto::TError ReloadPageIfNeeded(
            const ui64 pageNo,
            const ui64 newPageNo)
        {
            if (pageNo == newPageNo) {
                return {};
            }

            if (Dirty) {
                DirtyPages.push_back({
                    .Content = std::move(Page),
                    .PageNo = pageNo,
                });
            }

            Dirty = false;
            return PageStore.ReadPage(newPageNo, &Page);
        }

        NProto::TError ToPrevSlot()
        {
            const ui64 pageNo = CurrentPageNo();
            if (SlotNo == 0) {
                SlotNo = SlotCount;
            }

            --SlotNo;
            const ui64 newPageNo = CurrentPageNo();
            return ReloadPageIfNeeded(pageNo, newPageNo);
        }

        NProto::TError ToNextSlot()
        {
            const ui64 pageNo = CurrentPageNo();
            SlotNo = (SlotNo + 1) % SlotCount;
            const ui64 newPageNo = CurrentPageNo();
            return ReloadPageIfNeeded(pageNo, newPageNo);
        }

        const char* GetRaw() const
        {
            const ui64 offsetInPage = (SlotNo % SlotsPerPage) * SlotSize;
            return Page.data() + offsetInPage;
        }

        const TValue& Get() const
        {
            return *reinterpret_cast<const TValue*>(GetRaw());
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

    NProto::TError CollectStats(TPersistentHashTableStats* stats) const
    {
        TSlotIterator it(
            *PageStore,
            FirstPageNo,
            SlotSize,
            SlotCount,
            SlotsPerPage,
            0 /* slotNo */);

        auto error = it.Init();
        if (HasError(error)) {
            return error;
        }

        *stats = {};
        while (!stats->SlotCount || it.SlotNo != 0) {
            const char* raw = it.GetRaw();
            const bool isEmpty = IsEmpty(raw);
            const bool isTombstone = MakeKey(it.Get()) == TombstoneKey;

            ++stats->SlotCount;
            if (!isEmpty) {
                if (isTombstone) {
                    ++stats->TombstoneCount;
                } else {
                    ++stats->ValueCount;

                    if (CalcSlotNo(it.Get()) != it.SlotNo) {
                        ++stats->MisplacedValueCount;
                    }
                }
            }

            error = it.ToNextSlot();
            if (HasError(error)) {
                return error;
            }
        }

        return {};
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

    bool LookupSlot(const char* slotData, TValue* v) const
    {
        if (IsEmpty(slotData)) {
            return false;
        }

        memcpy(v, slotData, sizeof(TValue));
        return true;
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
        return LookupSlot(ptr, v) ? MakeError(S_OK) : MakeError(S_FALSE);
    }

    NProto::TError AllocateSlot(const TKey& k, TValue* v, ui64* slotNo) const
    {
        const ui64 h = Hash(k);
        const ui64 firstSlotNo = h % SlotCount;

        TSlotIterator it(
            *PageStore,
            FirstPageNo,
            SlotSize,
            SlotCount,
            SlotsPerPage,
            firstSlotNo);

        auto error = it.Init();
        if (HasError(error)) {
            return error;
        }

        while (true) {
            bool success = LookupSlot(it.GetRaw(), v);
            if (!success) {
                break;
            }

            if (TombstoneKey == MakeKey(*v)) {
                break;
            }

            if (k == MakeKey(*v)) {
                return MakeError(E_FS_EXIST);
            }

            error = it.ToNextSlot();
            if (HasError(error)) {
                return error;
            }

            if (it.SlotNo == firstSlotNo) {
                return MakeError(E_FS_OUT_OF_SPACE, "no free node slot");
            }
        }

        *slotNo = it.SlotNo;
        return {};
    }

    NProto::TError FindSlot(const TKey& k, TValue* v, ui64* slotNo) const
    {
        const ui64 h = Hash(k);
        const ui64 firstSlotNo = h % SlotCount;

        TSlotIterator it(
            *PageStore,
            FirstPageNo,
            SlotSize,
            SlotCount,
            SlotsPerPage,
            firstSlotNo);

        auto error = it.Init();
        if (HasError(error)) {
            return error;
        }

        while (true) {
            bool success = LookupSlot(it.GetRaw(), v);
            if (!success) {
                break;
            }

            if (k == MakeKey(*v)) {
                *slotNo = it.SlotNo;
                return {};
            }

            auto error = it.ToNextSlot();
            if (HasError(error)) {
                return error;
            }

            if (it.SlotNo == firstSlotNo) {
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

    ui64 CalcSlotNo(const TKey& k) const
    {
        return Hash(k) % SlotCount;
    }

    ui64 CalcSlotNo(const TValue& v) const
    {
        return CalcSlotNo(MakeKey(v));
    }

    bool IsEmpty(const char* slotData) const
    {
        return slotData[0] == 0
            && memcmp(slotData, slotData + 1, SlotSize - 1) == 0;
    }

    NProto::TError DoDelete(ui64 slotNo, TVector<TPageGroup>& pageGroups)
    {
        const ui64 nextSlotNo = (slotNo + 1) % SlotCount;

        TSlotIterator it(
            *PageStore,
            FirstPageNo,
            SlotSize,
            SlotCount,
            SlotsPerPage,
            nextSlotNo);

        //
        // Lookup next slot page - we need to check whether it's misplaced.
        //

        auto error = it.Init();
        if (HasError(error)) {
            return error;
        }

        const bool isNextSlotEmpty = IsEmpty(it.GetRaw());

        //
        // Moving back to the current slot.
        //

        error = it.ToPrevSlot();
        if (HasError(error)) {
            return error;
        }

        if (isNextSlotEmpty) {
            //
            // The next slot is empty - we can clear the current slot.
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
        } else {
            //
            // The next slot is not empty. This means that we can have a logical
            // slot chain which contains the current slot and which is necessary
            // to find some misplaced slots that go after this slot.
            //

            it.Write(reinterpret_cast<const char*>(&Tombstone));
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
