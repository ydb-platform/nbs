#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>

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

struct IDevice: public IStartable
{
    virtual ~IDevice() = default;

    [[nodiscard]] virtual auto ReadPages(TVector<TPageRangeRef> rangeRefs)
        -> NThreading::TFuture<TResultOrError<TVector<TBuffer>>> = 0;

    [[nodiscard]] virtual auto WritePages(TVector<TPageRange> ranges)
        -> NThreading::TFuture<NCloud::NProto::TError> = 0;
};

////////////////////////////////////////////////////////////////////////////////

IDevicePtr CreateInMemoryDevice(ui32 pageSize);

}   // namespace NCloud::NJournalled
