#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/buffer.h>
#include <util/generic/vector.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

struct TPageRangeRef
{
    ui64 FirstPageNo = 0;
    ui64 PageCount = 0;
};

struct TPageRange
{
    ui64 FirstPageNo = 0;
    TVector<TBuffer> Pages;
};

////////////////////////////////////////////////////////////////////////////////

struct IDevice
{
    virtual ~IDevice() = default;

    [[nodiscard]] virtual auto ReadPages(TVector<TPageRangeRef> rangeRefs)
        -> NThreading::TFuture<TResultOrError<TVector<TBuffer>>> = 0;

    [[nodiscard]] virtual auto WritePages(TVector<TPageRange> ranges)
        -> NThreading::TFuture<NCloud::NProto::TError> = 0;

    [[nodiscard]] virtual auto ZeroPages(TVector<TPageRangeRef> ranges)
        -> NThreading::TFuture<NCloud::NProto::TError> = 0;
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
