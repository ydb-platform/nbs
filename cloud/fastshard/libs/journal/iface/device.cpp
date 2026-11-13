#include "device.h"

#include <util/string/builder.h>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDeviceStub final: public IDevice
{
public:
    TFuture<TResultOrError<TVector<TBuffer>>> ReadPages(
        TVector<TPageRangeRef> rangeRefs) override
    {
        Y_UNUSED(rangeRefs);
        return MakeFuture(TResultOrError(TVector<TBuffer>()));
    }

    TFuture<NCloud::NProto::TError> WritePages(
        TVector<TPageRange> ranges) override
    {
        Y_UNUSED(ranges);
        return MakeFuture<NCloud::NProto::TError>();
    }

    TFuture<NCloud::NProto::TError> ZeroPages(
        TVector<TPageRangeRef> ranges) override
    {
        Y_UNUSED(ranges);
        return MakeFuture<NCloud::NProto::TError>();
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

    [[nodiscard]] TFuture<TResultOrError<TVector<TBuffer>>> ReadPages(
        TVector<TPageRangeRef> rangeRefs) final
    {
        for (auto& ref: rangeRefs) {
            if (auto error = Translate(ref.FirstPageNo, ref.PageCount);
                HasError(error))
            {
                return MakeFuture<TResultOrError<TVector<TBuffer>>>(
                    std::move(error));
            }
        }
        return Device->ReadPages(std::move(rangeRefs));
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
