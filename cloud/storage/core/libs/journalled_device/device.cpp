#include "device.h"

#include <util/generic/hash.h>

#include <mutex>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TInMemoryDevice final: public IDevice
{
private:
    const ui32 PageSize;

    std::mutex Lock;
    THashMap<ui64 /*pageNo*/, TBuffer> Pages;

public:
    explicit TInMemoryDevice(ui32 pageSize)
        : PageSize(pageSize)
    {}

    // IStartable

    void Start() override
    {}

    void Stop() override
    {}

    // IDevice

    TFuture<TResultOrError<TVector<TBuffer>>> ReadPages(
        TVector<TPageRangeRef> rangeRefs) override
    {
        TVector<TBuffer> pages;

        {
            std::lock_guard lock(Lock);

            for (const auto& ref: rangeRefs) {
                for (ui64 i = 0; i < ref.PageCount; ++i) {
                    auto it = Pages.find(ref.FirstPageNo + i);
                    if (it != Pages.end()) {
                        pages.push_back(it->second);
                        continue;
                    }

                    // a page that has never been written reads as a zeroed one
                    pages.emplace_back().Fill('\0', PageSize);
                }
            }
        }

        return MakeFuture<TResultOrError<TVector<TBuffer>>>(std::move(pages));
    }

    TFuture<NCloud::NProto::TError> WritePages(
        TVector<TPageRange> ranges) override
    {
        {
            std::lock_guard lock(Lock);

            for (auto& range: ranges) {
                ui64 pageNo = range.FirstPageNo;
                for (auto& page: range.Pages) {
                    Pages[pageNo++] = std::move(page);
                }
            }
        }

        return MakeFuture<NCloud::NProto::TError>();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDevicePtr CreateInMemoryDevice(ui32 pageSize)
{
    return std::make_shared<TInMemoryDevice>(pageSize);
}

}   // namespace NCloud::NJournalled
