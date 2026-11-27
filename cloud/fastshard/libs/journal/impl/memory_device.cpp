#include "memory_device.h"

#include <cloud/fastshard/libs/journal/iface/device.h>

#include <util/generic/hash.h>
#include <util/generic/utility.h>
#include <util/string/builder.h>

#include <cstring>
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

    TFuture<NProto::TError> ReadPages(
        TVector<TPageRangeView> rangeViews) override
    {
        for (const auto& view: rangeViews) {
            for (const auto& page: view.Pages) {
                if (page.size() != PageSize) {
                    return MakeFuture(MakeError(
                        E_ARGUMENT,
                        TStringBuilder()
                            << "page size mismatch: expected " << PageSize
                            << ", got " << page.size()));
                }
            }
        }

        {
            std::lock_guard lock(Lock);

            for (const auto& view: rangeViews) {
                ui64 pageNo = view.FirstPageNo;
                for (const auto& page: view.Pages) {
                    auto it = Pages.find(pageNo++);

                    // a page that has never been written reads as a zeroed
                    // one, and so does a stored page past its size
                    const size_t size =
                        it != Pages.end() ? Min(it->second.Size(), page.size())
                                          : 0;

                    if (size) {
                        std::memcpy(page.data(), it->second.Data(), size);
                    }
                    std::memset(page.data() + size, 0, page.size() - size);
                }
            }
        }

        return MakeFuture<NProto::TError>();
    }

    TFuture<NProto::TError> WritePages(TVector<TPageRange> ranges) override
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

        return MakeFuture<NProto::TError>();
    }

    TFuture<NProto::TError> ZeroPages(TVector<TPageRangeRef> ranges) override
    {
        {
            std::lock_guard lock(Lock);

            // a page that is not stored reads as a zeroed one
            for (const auto& range: ranges) {
                for (ui64 i = 0; i < range.PageCount; ++i) {
                    Pages.erase(range.FirstPageNo + i);
                }
            }
        }

        return MakeFuture<NProto::TError>();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDevicePtr CreateInMemoryDevice(ui32 pageSize)
{
    return std::make_shared<TInMemoryDevice>(pageSize);
}

}   // namespace NCloud::NJournalled
