#include "device_page_store.h"

#include "device.h"

#include <cloud/storage/core/libs/common/disjoint_interval_map.h>
#include <cloud/storage/core/libs/common/future_helper.h>

#include <util/generic/utility.h>
#include <util/generic/ylimits.h>
#include <util/string/builder.h>
#include <util/system/spinlock.h>
#include <util/system/yassert.h>

#include <optional>
#include <variant>

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

// A set of disjoint page ranges - the map carries no value, its interval sum
// is the total number of pages it holds.
using TPageRanges = TDisjointIntervalMapWithStats<ui64, std::monostate>;

////////////////////////////////////////////////////////////////////////////////

class TDevicePageStore final
    : public IDevicePageStore
    , public std::enable_shared_from_this<TDevicePageStore>
{
private:
    const IDevicePtr Device;
    const ui64 PageCount;
    const ui32 PageSize;
    const EDevicePageStoreMode Mode;

    TAdaptiveLock Lock;

    TPageRanges FreeRanges;

public:
    TDevicePageStore(
            IDevicePtr device,
            ui64 pageCount,
            ui32 pageSize,
            EDevicePageStoreMode mode)
        : Device(std::move(device))
        , PageCount(pageCount)
        , PageSize(pageSize)
        , Mode(mode)
    {
        if (PageCount) {
            FreeRanges.Add(0, PageCount, {});
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
            if (Mode == EDevicePageStoreMode::Checked) {
                auto error =
                    ValidatePages(pageGroupRefs, EPageState::Allocated);
                if (HasError(error)) {
                    return error;
                }
            }

            for (const auto& ref: pageGroupRefs) {
                FreeImpl(ref);
            }

            return MakeError(S_OK);
        }
    }

    auto Write(
        const TVector<TPageGroupRef>& pageGroupRefs,
        const TVector<TBuffer>& pages)
        -> TFuture<NCloud::NProto::TError> override
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
            if (page.Size() != PageSize) {
                return MakeFuture(MakeError(E_ARGUMENT, TStringBuilder()
                    << "a page of " << page.Size()
                    << " bytes, the page size is " << PageSize));
            }
        }

        if (Mode == EDevicePageStoreMode::Checked) {
            with_lock (Lock) {
                auto error =
                    ValidatePages(pageGroupRefs, EPageState::Allocated);
                if (HasError(error)) {
                    return MakeFuture(error);
                }
            }
        }

        NCloud::NProto::TWriteLogRecordRequest deviceRequest;

        ui64 pageIndex = 0;
        for (const auto& ref: pageGroupRefs) {
            auto& deviceGroup = *deviceRequest.AddPageGroups();
            deviceGroup.SetFirstPageNo(ref.FirstPageNo);

            for (ui64 i = 0; i < ref.PageCount; ++i) {
                const auto& page = pages[pageIndex++];
                deviceGroup.AddContent()->assign(page.Data(), page.Size());
            }
        }

        return Device->WritePages(std::move(deviceRequest)).Apply(
            [] (const auto& future) -> NCloud::NProto::TError
            {
                return future.GetValue().GetError();
            });
    }

    auto Read(const TVector<TPageGroupRef>& pageGroupRefs)
        -> TFuture<TResultOrError<TVector<TBuffer>>> override
    {
        using TResult = TResultOrError<TVector<TBuffer>>;

        NCloud::NProto::TReadPagesRequest deviceRequest;
        ui64 pageCount = 0;

        if (Mode == EDevicePageStoreMode::Checked) {
            with_lock (Lock) {
                auto error =
                    ValidatePages(pageGroupRefs, EPageState::Allocated);
                if (HasError(error)) {
                    return MakeFuture<TResult>(error);
                }
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
            return MakeFuture<TResult>(TVector<TBuffer>());
        }

        return Device->ReadPages(std::move(deviceRequest)).Apply(
            [pageCount] (const auto& future) -> TResult
            {
                auto response = UnsafeExtractValue(future);
                if (HasError(response)) {
                    return response.GetError();
                }

                TVector<TBuffer> pages;
                pages.reserve(pageCount);

                for (const auto& group: response.GetPageGroups()) {
                    for (const auto& content: group.GetContent()) {
                        pages.emplace_back(content.data(), content.size());
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
        if (!pageCount || pageCount > FreeRanges.GetIntervalSum()) {
            return {};
        }

        TVector<TPageGroupRef> refs;
        ui64 left = pageCount;

        while (left) {
            auto it = FreeRanges.begin();
            const ui64 rangeBegin = it->second.Begin;
            const ui64 rangeEnd = it->second.End;
            const ui64 taken = Min(left, rangeEnd - rangeBegin);

            refs.push_back({
                .FirstPageNo = rangeBegin,
                .PageCount = taken});

            FreeRanges.Remove(it);
            if (rangeBegin + taken < rangeEnd) {
                FreeRanges.Add(rangeBegin + taken, rangeEnd, {});
            }

            left -= taken;
        }

        return refs;
    }

    void FreeImpl(const TPageGroupRef& ref)
    {
        if (!ref.PageCount) {
            return;
        }

        ui64 begin = ref.FirstPageNo;
        ui64 end = ref.FirstPageNo + ref.PageCount;

        FreeRanges.VisitOverlapping(
            begin ? begin - 1 : begin,
            end < Max<ui64>() ? end + 1 : end,
            [&] (auto it)
            {
                begin = Min(begin, it->second.Begin);
                end = Max(end, it->second.End);
                FreeRanges.Remove(it);
            });

        FreeRanges.Add(begin, end, {});
    }

    void AllocateAtImpl(const TPageGroupRef& ref)
    {
        if (!ref.PageCount) {
            return;
        }

        const ui64 begin = ref.FirstPageNo;
        const ui64 end = ref.FirstPageNo + ref.PageCount;

        FreeRanges.VisitOverlapping(
            begin,
            end,
            [&] (auto it)
            {
                const ui64 rangeBegin = it->second.Begin;
                const ui64 rangeEnd = it->second.End;

                FreeRanges.Remove(it);

                if (rangeBegin < begin) {
                    FreeRanges.Add(rangeBegin, begin, {});
                }
                if (end < rangeEnd) {
                    FreeRanges.Add(end, rangeEnd, {});
                }
            });
    }

    std::optional<ui64> FindBusyPage(const TPageGroupRef& ref) const
    {
        const ui64 endPageNo = ref.FirstPageNo + ref.PageCount;

        ui64 pageNo = ref.FirstPageNo;

        FreeRanges.VisitOverlapping(
            ref.FirstPageNo,
            endPageNo,
            [&] (auto it)
            {
                if (it->second.Begin <= pageNo) {
                    pageNo = Max(pageNo, it->second.End);
                }
            });

        if (pageNo < endPageNo) {
            return pageNo;
        }

        return std::nullopt;
    }

    std::optional<ui64> FindFreePage(const TPageGroupRef& ref) const
    {
        std::optional<ui64> pageNo;
        FreeRanges.VisitOverlapping(
            ref.FirstPageNo,
            ref.FirstPageNo + ref.PageCount,
            [&] (auto it)
            {
                if (!pageNo) {
                    pageNo = Max(it->second.Begin, ref.FirstPageNo);
                }
            });

        return pageNo;
    }

    static bool HasIntersections(const TVector<TPageGroupRef>& refs)
    {
        TPageRanges ranges;

        for (const auto& ref: refs) {
            if (!ref.PageCount) {
                continue;
            }

            const ui64 begin = ref.FirstPageNo;
            const ui64 end = ref.FirstPageNo + ref.PageCount;

            bool intersects = false;
            ranges.VisitOverlapping(begin, end, [&] (auto) {
                intersects = true;
            });

            if (intersects) {
                return true;
            }

            ranges.Add(begin, end, {});
        }

        return false;
    }

    NCloud::NProto::TError ValidatePages(
        const TVector<TPageGroupRef>& refs,
        EPageState expected) const
    {
        if (HasIntersections(refs)) {
            return MakeError(E_ARGUMENT,
                "the page group refs of a single request intersect");
        }


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

IDevicePageStorePtr CreateDevicePageStore(
    IDevicePtr device,
    ui64 pageCount,
    ui32 pageSize,
    EDevicePageStoreMode mode)
{
    return std::make_shared<TDevicePageStore>(
        std::move(device),
        pageCount,
        pageSize,
        mode);
}

}   // namespace NCloud::NJournalled
