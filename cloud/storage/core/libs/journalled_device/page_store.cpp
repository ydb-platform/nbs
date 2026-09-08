#include "page_store.h"

#include "device.h"

#include <cloud/storage/core/libs/common/future_helper.h>

#include <util/generic/map.h>
#include <util/generic/utility.h>
#include <util/string/builder.h>
#include <util/system/spinlock.h>
#include <util/system/yassert.h>

#include <optional>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

enum class EPageState
{
    Free,
    Allocated,
};

////////////////////////////////////////////////////////////////////////////////

class TPageStore final
    : public IPageStore
    , public std::enable_shared_from_this<TPageStore>
{
private:
    const IDevicePtr Device;
    const ui64 PageCount;
    const ui32 PageSize;

    TAdaptiveLock Lock;

    TMap<ui64, ui64> FreeRanges;
    ui64 FreePageCount = 0;

public:
    TPageStore(IDevicePtr device, ui64 pageCount, ui32 pageSize)
        : Device(std::move(device))
        , PageCount(pageCount)
        , PageSize(pageSize)
        , FreePageCount(pageCount)
    {
        if (PageCount) {
            FreeRanges[0] = PageCount;
        }
    }

    TVector<TPageGroupRef> Allocate(ui64 pageCount) override
    {
        with_lock (Lock) {
            return AllocateImpl(pageCount);
        }
    }

    NCloud::NProto::TError AllocateAt(
        const TVector<TPageGroupRef>& pageGroupRefs) override
    {
        with_lock (Lock) {
            auto error = ValidatePages(pageGroupRefs, EPageState::Free);
            if (HasError(error)) {
                return error;
            }

            for (const auto& ref: pageGroupRefs) {
                AllocateAtImpl(ref);
            }

            return MakeError(S_OK);
        }
    }

    NCloud::NProto::TError Free(
        const TVector<TPageGroupRef>& pageGroupRefs) override
    {
        with_lock (Lock) {
            auto error = ValidatePages(pageGroupRefs, EPageState::Allocated);
            if (HasError(error)) {
                return error;
            }

            for (const auto& ref: pageGroupRefs) {
                FreeImpl(ref);
            }

            return MakeError(S_OK);
        }
    }

    auto Write(
        const TVector<TPageGroupRef>& pageGroupRefs,
        TVector<TString> pages) -> TFuture<NCloud::NProto::TError> override
    {
        ui64 pageCount = 0;
        for (const auto& ref: pageGroupRefs) {
            pageCount += ref.PageCount;
        }

        if (pageCount != pages.size()) {
            return MakeFuture(MakeError(E_ARGUMENT, TStringBuilder()
                << "the page group refs hold " << pageCount << " pages, "
                << pages.size() << " given"));
        }

        if (!pageCount) {
            return MakeFuture(MakeError(S_OK));
        }

        for (const auto& page: pages) {
            if (page.size() != PageSize) {
                return MakeFuture(MakeError(E_ARGUMENT, TStringBuilder()
                    << "a page of " << page.size() << " bytes, the page size is "
                    << PageSize));
            }
        }

        with_lock (Lock) {
            auto error = ValidatePages(pageGroupRefs, EPageState::Allocated);
            if (HasError(error)) {
                return MakeFuture(error);
            }
        }

        NCloud::NProto::TWriteLogRecordRequest deviceRequest;

        ui64 pageIndex = 0;
        for (const auto& ref: pageGroupRefs) {
            auto& deviceGroup = *deviceRequest.AddPageGroups();
            deviceGroup.SetFirstPageNo(ref.FirstPageNo);

            for (ui64 i = 0; i < ref.PageCount; ++i) {
                *deviceGroup.AddContent() = std::move(pages[pageIndex++]);
            }
        }

        return Device->WritePages(std::move(deviceRequest)).Apply(
            [] (const auto& future) -> NCloud::NProto::TError
            {
                return future.GetValue().GetError();
            });
    }

    auto Read(const TVector<TPageGroupRef>& pageGroupRefs)
        -> TFuture<TResultOrError<TVector<TString>>> override
    {
        using TResult = TResultOrError<TVector<TString>>;

        NCloud::NProto::TReadPagesRequest deviceRequest;
        ui64 pageCount = 0;

        with_lock (Lock) {
            auto error = ValidatePages(pageGroupRefs, EPageState::Allocated);
            if (HasError(error)) {
                return MakeFuture<TResult>(error);
            }
        }

        for (const auto& ref: pageGroupRefs) {
            pageCount += ref.PageCount;

            auto& deviceRef = *deviceRequest.AddPageGroupRefs();
            deviceRef.SetFirstPageNo(ref.FirstPageNo);
            deviceRef.SetPageCount(ref.PageCount);
            deviceRef.SetPageSize(PageSize);
        }

        if (!pageCount) {
            return MakeFuture<TResult>(TVector<TString>());
        }

        return Device->ReadPages(std::move(deviceRequest)).Apply(
            [pageCount] (const auto& future) -> TResult
            {
                auto response = UnsafeExtractValue(future);
                if (HasError(response)) {
                    return response.GetError();
                }

                TVector<TString> pages;
                pages.reserve(pageCount);

                for (auto& group: *response.MutablePageGroups()) {
                    for (auto& content: *group.MutableContent()) {
                        pages.push_back(std::move(content));
                    }
                }

                if (pages.size() != pageCount) {
                    return MakeError(E_INVALID_STATE, TStringBuilder()
                        << "the device returned " << pages.size()
                        << " pages, expected " << pageCount);
                }

                return std::move(pages);
            });
    }

private:
    TVector<TPageGroupRef> AllocateImpl(ui64 pageCount)
    {
        if (!pageCount || pageCount > FreePageCount) {
            return {};
        }

        TVector<TPageGroupRef> refs;
        ui64 left = pageCount;

        while (left) {
            auto it = FreeRanges.begin();
            const ui64 firstPageNo = it->first;
            const ui64 rangeSize = it->second;
            const ui64 taken = Min(left, rangeSize);

            refs.push_back({
                .FirstPageNo = firstPageNo,
                .PageCount = taken});

            FreeRanges.erase(it);
            if (taken < rangeSize) {
                FreeRanges[firstPageNo + taken] = rangeSize - taken;
            }

            left -= taken;
        }

        FreePageCount -= pageCount;

        return refs;
    }

    void FreeImpl(const TPageGroupRef& ref)
    {
        if (!ref.PageCount) {
            return;
        }

        FreePageCount += ref.PageCount;

        ui64 begin = ref.FirstPageNo;
        ui64 end = ref.FirstPageNo + ref.PageCount;

        // merge with the range on the left, if they touch
        auto it = FreeRanges.upper_bound(begin);
        if (it != FreeRanges.begin()) {
            auto prev = std::prev(it);
            if (prev->first + prev->second >= begin) {
                begin = prev->first;
                end = Max(end, prev->first + prev->second);
                FreeRanges.erase(prev);
            }
        }

        // and with every range on the right that touches the result
        while (true) {
            auto next = FreeRanges.lower_bound(begin);
            if (next == FreeRanges.end() || next->first > end) {
                break;
            }

            end = Max(end, next->first + next->second);
            FreeRanges.erase(next);
        }

        FreeRanges[begin] = end - begin;
    }

    void AllocateAtImpl(const TPageGroupRef& ref)
    {
        if (!ref.PageCount) {
            return;
        }

        const ui64 endPageNo = ref.FirstPageNo + ref.PageCount;

        auto it = FreeRanges.upper_bound(ref.FirstPageNo);
        if (it != FreeRanges.begin()) {
            --it;
        }

        while (it != FreeRanges.end() && it->first < endPageNo) {
            const ui64 rangeBegin = it->first;
            const ui64 rangeEnd = rangeBegin + it->second;

            if (rangeEnd <= ref.FirstPageNo) {
                ++it;
                continue;
            }

            const ui64 busyBegin = Max(rangeBegin, ref.FirstPageNo);
            const ui64 busyEnd = Min(rangeEnd, endPageNo);
            FreePageCount -= busyEnd - busyBegin;

            auto next = std::next(it);
            FreeRanges.erase(it);

            if (rangeBegin < busyBegin) {
                FreeRanges[rangeBegin] = busyBegin - rangeBegin;
            }
            if (busyEnd < rangeEnd) {
                FreeRanges[busyEnd] = rangeEnd - busyEnd;
            }

            it = next;
        }
    }

    std::optional<ui64> FindBusyPage(const TPageGroupRef& ref) const
    {
        auto it = FreeRanges.upper_bound(ref.FirstPageNo);
        if (it == FreeRanges.begin()) {
            return ref.FirstPageNo;
        }

        --it;
        const ui64 freeEnd = it->first + it->second;
        if (freeEnd <= ref.FirstPageNo) {
            return ref.FirstPageNo;
        }

        const ui64 endPageNo = ref.FirstPageNo + ref.PageCount;
        if (freeEnd < endPageNo) {
            return freeEnd;
        }

        return std::nullopt;
    }

    std::optional<ui64> FindFreePage(const TPageGroupRef& ref) const
    {
        auto it = FreeRanges.upper_bound(ref.FirstPageNo);
        if (it != FreeRanges.begin()) {
            auto prev = std::prev(it);
            if (prev->first + prev->second > ref.FirstPageNo) {
                return ref.FirstPageNo;
            }
        }

        const ui64 endPageNo = ref.FirstPageNo + ref.PageCount;
        if (it != FreeRanges.end() && it->first < endPageNo) {
            return it->first;
        }

        return std::nullopt;
    }

    static bool HasIntersections(const TVector<TPageGroupRef>& refs)
    {
        TMap<ui64, ui64> ranges;

        for (const auto& ref: refs) {
            if (!ref.PageCount) {
                continue;
            }

            auto [it, inserted] =
                ranges.emplace(ref.FirstPageNo, ref.PageCount);
            if (!inserted) {
                return true;
            }

            if (it != ranges.begin()) {
                auto prev = std::prev(it);
                if (prev->first + prev->second > ref.FirstPageNo) {
                    return true;
                }
            }

            auto next = std::next(it);
            if (next != ranges.end() &&
                next->first < ref.FirstPageNo + ref.PageCount)
            {
                return true;
            }
        }

        return false;
    }

    NCloud::NProto::TError ValidatePages(
        const TVector<TPageGroupRef>& refs,
        EPageState expected) const
    {
        Y_DEBUG_ABORT_UNLESS(
            !HasIntersections(refs),
            "the page group refs of a single request intersect");

        const bool free = expected == EPageState::Free;

        for (const auto& ref: refs) {
            if (!ref.PageCount) {
                continue;
            }

            if (ref.FirstPageNo >= PageCount ||
                ref.PageCount > PageCount - ref.FirstPageNo)
            {
                return MakeError(E_ARGUMENT,
                    TStringBuilder()
                        << "page group ref " << ref.FirstPageNo << "x"
                        << ref.PageCount << " is beyond the device: "
                        << PageCount << " pages");
            }

            const auto pageNo = free ? FindBusyPage(ref) : FindFreePage(ref);
            if (pageNo) {
                return MakeError(E_INVALID_STATE,
                    TStringBuilder() << "page " << *pageNo
                        << (free ? " is busy already" : " is not busy"));
            }
        }

        return MakeError(S_OK);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IPageStorePtr CreatePageStore(
    IDevicePtr device,
    ui64 pageCount,
    ui32 pageSize)
{
    return std::make_shared<TPageStore>(
        std::move(device),
        pageCount,
        pageSize);
}

}   // namespace NCloud::NJournalled
