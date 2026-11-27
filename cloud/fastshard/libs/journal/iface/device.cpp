#include "device.h"

#include <util/string/builder.h>

#include <cstring>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDeviceStub final: public IDevice
{
public:
    TFuture<NProto::TError> ReadPages(
        TVector<TPageRangeView> rangeViews) override
    {
        // the stub holds no data, every page reads as a zeroed one
        for (const auto& view: rangeViews) {
            for (const auto& page: view.Pages) {
                std::memset(page.data(), 0, page.size());
            }
        }

        return MakeFuture<NProto::TError>();
    }

    TFuture<NProto::TError> WritePages(TVector<TPageRange> ranges) override
    {
        Y_UNUSED(ranges);
        return MakeFuture<NProto::TError>();
    }

    TFuture<NProto::TError> ZeroPages(TVector<TPageRangeRef> ranges) override
    {
        Y_UNUSED(ranges);
        return MakeFuture<NProto::TError>();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRegionDevice final: public IDevice
{
private:
    const IDevicePtr Device;
    const TPageRangeRef Region;

public:
    TRegionDevice(IDevicePtr device, TPageRangeRef region)
        : Device(std::move(device))
        , Region(region)
    {}

    // IDevice

    [[nodiscard]] TFuture<NProto::TError> ReadPages(
        TVector<TPageRangeView> rangeViews) final
    {
        for (auto& view: rangeViews) {
            if (auto error = Translate(view.FirstPageNo, view.Pages.size());
                HasError(error))
            {
                return MakeFuture(std::move(error));
            }
        }
        return Device->ReadPages(std::move(rangeViews));
    }

    [[nodiscard]] TFuture<NProto::TError> WritePages(
        TVector<TPageRange> ranges) final
    {
        for (auto& range: ranges) {
            if (auto error = Translate(range.FirstPageNo, range.Pages.size());
                HasError(error))
            {
                return MakeFuture(std::move(error));
            }
        }
        return Device->WritePages(std::move(ranges));
    }

    [[nodiscard]] TFuture<NProto::TError> ZeroPages(
        TVector<TPageRangeRef> rangeRefs) final
    {
        for (auto& ref: rangeRefs) {
            if (auto error = Translate(ref.FirstPageNo, ref.PageCount);
                HasError(error))
            {
                return MakeFuture(std::move(error));
            }
        }
        return Device->ZeroPages(std::move(rangeRefs));
    }

private:
    // Checks that the range fits the region and shifts it to the device.
    NProto::TError Translate(ui64& firstPageNo, ui64 pageCount) const
    {
        if (firstPageNo > Region.PageCount ||
            pageCount > Region.PageCount - firstPageNo)
        {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "pages [" << firstPageNo << ", "
                    << firstPageNo + pageCount << ") are outside the region "
                    << "of " << Region.PageCount << " pages");
        }

        firstPageNo += Region.FirstPageNo;
        return {};
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDevicePtr CreateDeviceStub()
{
    return std::make_shared<TDeviceStub>();
}

IDevicePtr CreateRegionDevice(IDevicePtr device, TPageRangeRef region)
{
    return std::make_shared<TRegionDevice>(std::move(device), region);
}

}   // namespace NCloud::NJournalled
