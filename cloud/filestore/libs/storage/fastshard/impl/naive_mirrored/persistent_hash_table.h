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
    static constexpr ui64 InvalidSlotNo = Max<ui64>();

    const ui64 FirstPageNo;
    const ui64 PageCount;
    const ui64 PageSize;
    const ui64 SlotSize;
    const ui64 SlotsPerPage;
    const ui64 SlotCount;
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
        const ui64 Lsn;
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
                ui64 lsn,
                ui64 firstPageNo,
                ui64 slotSize,
                ui64 slotCount,
                ui64 slotsPerPage,
                ui64 slotNo)
            : PageStore(pageStore)
            , Lsn(lsn)
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
            return PageStore.ReadPage(Lsn, pageNo, &Page);
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
            return PageStore.ReadPage(Lsn, newPageNo, &Page);
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
            char* dst = Page.begin() + offsetInPage;
            memcpy(dst, data, sizeof(TValue));
            dst += sizeof(TValue);
            const ui64 tail = SlotSize - sizeof(TValue);
            if (tail) {
                memset(dst, 0, tail);
            }
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
            ui64 slotSize,
            const TValue& tombstone,
            IPageStorePtr pageStore,
            TMakeKey makeKey,
            THash hash)
        : FirstPageNo(firstPageNo)
        , PageCount(pageCount)
        , PageSize(pageSize)
        , SlotSize(slotSize)
        , SlotsPerPage(PageSize / SlotSize)
        , SlotCount(PageCount * SlotsPerPage)
        , Tombstone(tombstone)
        , TombstoneKey(makeKey(tombstone))
        , PageStore(std::move(pageStore))
        , MakeKey(std::move(makeKey))
        , Hash(std::move(hash))
    {
        Y_ABORT_UNLESS(sizeof(TValue) <= SlotSize);
    }

public:
    ui64 GetSlotCount() const
    {
        return SlotCount;
    }

    NProto::TError Put(
        ui64 lsn,
        const TValue& v,
        TVector<TPageGroup>& pageGroups)
    {
        auto k = MakeKey(v);
        TValue existing{};
        ui64 slotNo = 0;
        auto error = AllocateSlot(lsn, k, &existing, &slotNo);
        if (HasError(error)) {
            return error;
        }

        return DoPut(lsn, v, slotNo, pageGroups);
    }

    NProto::TError Update(
        ui64 lsn,
        const TValue& v,
        const ui64 slotNo,
        TVector<TPageGroup>& pageGroups)
    {
        // TODO(#5895): verify that there's no key change
        return DoPut(lsn, v, slotNo, pageGroups);
    }

    NProto::TError Get(ui64 lsn, const TKey& k, TValue* v, ui64* slotNo) const
    {
        return FindSlot(lsn, k, v, slotNo);
    }

    NProto::TError Delete(
        ui64 lsn,
        const TKey& k,
        TValue* v,
        TVector<TPageGroup>& pageGroups)
    {
        ui64 slotNo = 0;
        auto error = FindSlot(lsn, k, v, &slotNo);
        if (HasError(error)) {
            return error;
        }

        return DoDelete(lsn, slotNo, pageGroups);
    }

    NProto::TError CollectStats(TPersistentHashTableStats* stats) const
    {
        TSlotIterator it(
            *PageStore,
            0 /* lsn */,
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
    NProto::TError WritePage(
        ui64 lsn,
        ui64 slotNo,
        TString page,
        TVector<TPageGroup>& pageGroups)
    {
        const ui64 pageNo = FirstPageNo + slotNo / SlotsPerPage;
        return PageStore->WritePage(lsn, pageNo, std::move(page), pageGroups);
    }

    NProto::TError ReadPage(ui64 lsn, ui64 slotNo, TString* page) const
    {
        const ui64 pageNo = FirstPageNo + slotNo / SlotsPerPage;
        return PageStore->ReadPage(lsn, pageNo, page);
    }

    bool LookupSlot(const char* slotData, TValue* v) const
    {
        if (IsEmpty(slotData)) {
            return false;
        }

        memcpy(v, slotData, sizeof(TValue));
        return true;
    }

    NProto::TError LookupSlot(ui64 lsn, ui64 slotNo, TValue* v) const
    {
        TString page;
        auto error = ReadPage(lsn, slotNo, &page);
        if (HasError(error)) {
            return error;
        }

        const ui32 relSlotNo = slotNo % SlotsPerPage;
        const char* ptr = page.data() + relSlotNo * SlotSize;
        return LookupSlot(ptr, v) ? MakeError(S_OK) : MakeError(S_FALSE);
    }

    NProto::TError AllocateSlot(
        ui64 lsn,
        const TKey& k,
        TValue* v,
        ui64* slotNo) const
    {
        const ui64 h = Hash(k);
        const ui64 firstSlotNo = h % SlotCount;

        TSlotIterator it(
            *PageStore,
            lsn,
            FirstPageNo,
            SlotSize,
            SlotCount,
            SlotsPerPage,
            firstSlotNo);

        auto error = it.Init();
        if (HasError(error)) {
            return error;
        }

        ui64 candidateSlotNo = InvalidSlotNo;
        while (true) {
            bool success = LookupSlot(it.GetRaw(), v);
            if (!success) {
                //
                // We found an empty slot - now we can stop.
                //

                if (candidateSlotNo == InvalidSlotNo) {
                    candidateSlotNo = it.SlotNo;
                }

                break;
            }

            if (TombstoneKey == MakeKey(*v)) {
                //
                // This is a candidate slot. But we might have the same key
                // somewhere after this tombstone. So we can't stop - we can
                // just memorize this slot.
                //

                if (candidateSlotNo == InvalidSlotNo) {
                    candidateSlotNo = it.SlotNo;
                }
            } else if (k == MakeKey(*v)) {
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

        *slotNo = candidateSlotNo;
        return {};
    }

    NProto::TError FindSlot(
        ui64 lsn,
        const TKey& k,
        TValue* v,
        ui64* slotNo) const
    {
        const ui64 h = Hash(k);
        const ui64 firstSlotNo = h % SlotCount;

        TSlotIterator it(
            *PageStore,
            lsn,
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
        ui64 lsn,
        const TValue& v,
        ui64 slotNo,
        TVector<TPageGroup>& pageGroups)
    {
        TString page;
        auto error = ReadPage(lsn, slotNo, &page);
        if (HasError(error)) {
            return error;
        }

        const ui32 relSlotNo = slotNo % SlotsPerPage;
        char* ptr = page.begin() + relSlotNo * SlotSize;
        memcpy(ptr, &v, sizeof(TValue));

        SILK_DEBUG(
            "pht DoPut: slotNo=%lu, logRecordPGs=%lu",
            slotNo,
            pageGroups.size());

        return WritePage(lsn, slotNo, std::move(page), pageGroups);
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

    NProto::TError DoDelete(
        ui64 lsn,
        ui64 slotNo,
        TVector<TPageGroup>& pageGroups)
    {
        const ui64 nextSlotNo = (slotNo + 1) % SlotCount;

        TSlotIterator it(
            *PageStore,
            lsn,
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
            // TODO(#5895): implement backward-shift deletion.
            //

            it.Write(reinterpret_cast<const char*>(&Tombstone));
        }

        SILK_DEBUG(
            "pht DoDelete: slotNo=%lu, logRecordPGs=%lu",
            slotNo,
            pageGroups.size());

        //
        // Writing the changes.
        //

        auto toWrite = it.Finish();
        for (auto& dp: toWrite) {
            auto error = PageStore->WritePage(
                lsn,
                dp.PageNo,
                std::move(dp.Content),
                pageGroups);

            if (HasError(error)) {
                //
                // The caller is responsible for rolling back the changes.
                //

                return error;
            }
        }

        return {};
    }
};

}   // namespace NCloud::NFileStore::NStorage::NFastShard
