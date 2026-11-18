#pragma once

#include "public.h"

#include "types.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/buffer.h>
#include <util/generic/vector.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

struct IDevice
{
    virtual ~IDevice() = default;

    [[nodiscard]] virtual auto ReadPages(TVector<TPageRangeRef> rangeRefs)
        -> NThreading::TFuture<TResultOrError<TVector<TBuffer>>> = 0;

    // The caller must keep the pages alive until the future is set.
    [[nodiscard]] virtual NThreading::TFuture<NProto::TError> WritePages(
        TVector<TPageRangeView> ranges) = 0;

    [[nodiscard]] virtual NThreading::TFuture<NProto::TError> ZeroPages(
        TVector<TPageRangeRef> ranges) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IDevicePtr CreateDeviceStub();

/**
 * Creates a device over a region of |device|, addressed from the region's
 * first page. The devices created for the regions of the same device share
 * its pages. A request that reaches outside the region fails with
 * E_ARGUMENT.
 */
IDevicePtr CreateRegionDevice(IDevicePtr device, TPageRangeRef region);

}   // namespace NCloud::NJournalled
