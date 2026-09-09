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

    TVector<TPageRange> Allocate(ui64 pageCount) override
    {
        with_lock (Lock) {
            return AllocateImpl(pageCount);
        }
    }

    NCloud::NProto::TError AllocateAt(
        const TVector<TPageRange>& pageRanges) override
    {
        with_lock (Lock) {
            auto error = ValidatePages(pageRanges, EPageState::Free);
            if (HasError(error)) {
                return error;
            }

            for (const auto& pageRange: pageRanges) {
                AllocateAtImpl(pageRange);
            }

            return MakeError(S_OK);
        }
    }

    NCloud::NProto::TError Free(const TVector<TPageRange>& pageRanges) override
    {
        with_lock (Lock) {
            if (Mode == EDevicePageStoreMode::Checked) {
                auto error = ValidatePages(pageRanges, EPageState::Allocated);
                if (HasError(error)) {
                    return error;
                }
            }

            for (const auto& pageRange: pageRanges) {
                FreeImpl(pageRange);
            }

            return MakeError(S_OK);
        }
    }

    auto Write(
        const TVector<TPageRange>& pageRanges,
        const TVector<TBuffer>& pages)
        -> TFuture<NCloud::NProto::TError> override
    {
        ui64 pageCount = 0;
        for (const auto& pageRange: pageRanges) {
            pageCount += pageRange.PageCount;
        }

        if (pageCount != pages.size()) {
            return MakeFuture(MakeError(
                E_ARGUMENT,
                TStringBuilder() << "the page ranges hold " << pageCount
                                 << " pages, " << pages.size() << " given"));
        }

        if (!pageCount) {
            return MakeFuture(MakeError(S_OK));
        }

        for (const auto& page: pages) {
            if (page.Size() != PageSize) {
                return MakeFuture(MakeError(
                    E_ARGUMENT,
                    TStringBuilder()
                        << "a page of " << page.Size()
                        << " bytes, the page size is " << PageSize));
            }
        }

        if (Mode == EDevicePageStoreMode::Checked) {
            with_lock (Lock) {
                auto error = ValidatePages(pageRanges, EPageState::Allocated);
                if (HasError(error)) {
                    return MakeFuture(error);
                }
            }
        }

        NCloud::NProto::TWriteLogRecordRequest deviceRequest;

        ui64 pageIndex = 0;
        for (const auto& pageRange: pageRanges) {
            auto& deviceGroup = *deviceRequest.AddPageGroups();
            deviceGroup.SetFirstPageNo(pageRange.FirstPageNo);

            for (ui64 i = 0; i < pageRange.PageCount; ++i) {
                const auto& page = pages[pageIndex++];
                deviceGroup.AddContent()->assign(page.Data(), page.Size());
            }
        }

        return Device->WritePages(std::move(deviceRequest))
            .Apply(
                [](const auto& future) -> NCloud::NProto::TError
                { return future.GetValue().GetError(); });
    }

    auto Read(const TVector<TPageRange>& pageRanges)
        -> TFuture<TResultOrError<TVector<TBuffer>>> override
    {
        using TResult = TResultOrError<TVector<TBuffer>>;

        NCloud::NProto::TReadPagesRequest deviceRequest;
        ui64 pageCount = 0;

        if (Mode == EDevicePageStoreMode::Checked) {
            with_lock (Lock) {
                auto error = ValidatePages(pageRanges, EPageState::Allocated);
                if (HasError(error)) {
                    return MakeFuture<TResult>(error);
                }
            }
        }

        for (const auto& pageRange: pageRanges) {
            pageCount += pageRange.PageCount;

            auto& deviceRef = *deviceRequest.AddPageGroupRefs();
            deviceRef.SetFirstPageNo(pageRange.FirstPageNo);
            deviceRef.SetPageCount(pageRange.PageCount);
            deviceRef.SetPageSize(PageSize);
        }

        if (!pageCount) {
            return MakeFuture<TResult>(TVector<TBuffer>());
        }

        return Device->ReadPages(std::move(deviceRequest))
            .Apply(
                [pageCount](const auto& future) -> TResult
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
                        return MakeError(
                            E_INVALID_STATE,
                            TStringBuilder()
                                << "the device returned " << pages.size()
                                << " pages, expected " << pageCount);
                    }

                    return std::move(pages);
                });
    }

private:
    TVector<TPageRange> AllocateImpl(ui64 pageCount)
    {
        if (!pageCount || pageCount > FreeRanges.GetIntervalSum()) {
            return {};
        }

        TVector<TPageRange> ranges;
        ui64 left = pageCount;

        while (left) {
            auto it = FreeRanges.begin();
            const ui64 rangeBegin = it->second.Begin;
            const ui64 rangeEnd = it->second.End;
            const ui64 taken = Min(left, rangeEnd - rangeBegin);

            ranges.push_back({.FirstPageNo = rangeBegin, .PageCount = taken});

            FreeRanges.Remove(it);
            if (rangeBegin + taken < rangeEnd) {
                FreeRanges.Add(rangeBegin + taken, rangeEnd, {});
            }

            left -= taken;
        }

        return ranges;
    }

    void FreeImpl(const TPageRange& pageRange)
    {
        if (!pageRange.PageCount) {
            return;
        }

        ui64 begin = pageRange.FirstPageNo;
        ui64 end = pageRange.FirstPageNo + pageRange.PageCount;

        FreeRanges.VisitOverlapping(
            begin ? begin - 1 : begin,
            end < Max<ui64>() ? end + 1 : end,
            [&](auto it)
            {
                begin = Min(begin, it->second.Begin);
                end = Max(end, it->second.End);
                FreeRanges.Remove(it);
            });

        FreeRanges.Add(begin, end, {});
    }

    void AllocateAtImpl(const TPageRange& pageRange)
    {
        if (!pageRange.PageCount) {
            return;
        }

        const ui64 begin = pageRange.FirstPageNo;
        const ui64 end = pageRange.FirstPageNo + pageRange.PageCount;

        FreeRanges.VisitOverlapping(
            begin,
            end,
            [&](auto it)
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

    std::optional<ui64> FindAllocatedPage(const TPageRange& pageRange) const
    {
        const ui64 endPageNo = pageRange.FirstPageNo + pageRange.PageCount;

        ui64 pageNo = pageRange.FirstPageNo;

        FreeRanges.VisitOverlapping(
            pageRange.FirstPageNo,
            endPageNo,
            [&](auto it)
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

    std::optional<ui64> FindFreePage(const TPageRange& pageRange) const
    {
        std::optional<ui64> pageNo;
        FreeRanges.VisitOverlapping(
            pageRange.FirstPageNo,
            pageRange.FirstPageNo + pageRange.PageCount,
            [&](auto it)
            {
                if (!pageNo) {
                    pageNo = Max(it->second.Begin, pageRange.FirstPageNo);
                }
            });

        return pageNo;
    }

    static bool HasIntersections(const TVector<TPageRange>& ranges)
    {
        TPageRanges seen;

        for (const auto& pageRange: ranges) {
            if (!pageRange.PageCount) {
                continue;
            }

            const ui64 begin = pageRange.FirstPageNo;
            const ui64 end = pageRange.FirstPageNo + pageRange.PageCount;

            bool intersects = false;
            seen.VisitOverlapping(begin, end, [&](auto) { intersects = true; });

            if (intersects) {
                return true;
            }

            seen.Add(begin, end, {});
        }

        return false;
    }

    NCloud::NProto::TError ValidatePages(
        const TVector<TPageRange>& ranges,
        EPageState expected) const
    {
        if (HasIntersections(ranges)) {
            return MakeError(
                E_ARGUMENT,
                "the page ranges of a single request intersect");
        }

        const bool free = expected == EPageState::Free;

        for (const auto& pageRange: ranges) {
            if (!pageRange.PageCount) {
                continue;
            }

            if (pageRange.FirstPageNo >= PageCount ||
                pageRange.PageCount > PageCount - pageRange.FirstPageNo)
            {
                return MakeError(
                    E_ARGUMENT,
                    TStringBuilder()
                        << "page range " << pageRange.FirstPageNo << "x"
                        << pageRange.PageCount
                        << " is beyond the device: " << PageCount << " pages");
            }

            const auto pageNo =
                free ? FindAllocatedPage(pageRange) : FindFreePage(pageRange);
            if (pageNo) {
                return MakeError(
                    E_INVALID_STATE,
                    TStringBuilder()
                        << "page " << *pageNo
                        << (free ? " is already allocated" : " is not busy"));
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
