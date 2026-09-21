#include "device.h"

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
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDevicePtr CreateDeviceStub()
{
    return std::make_shared<TDeviceStub>();
}

}   // namespace NCloud::NJournalled
