#pragma once

#include "public.h"

#include "log_record.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/buffer.h>
#include <util/generic/vector.h>

#include <memory>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

enum class EDevicePageStoreMode
{
    // Every method checks that the page ranges lie within the device and do
    // not intersect. AllocateAt and Free also check the allocation state.
    Checked,

    // Only AllocateAt validates the page ranges, the caller is trusted.
    Trusted,
};

////////////////////////////////////////////////////////////////////////////////

struct IDevicePageStore
{
    virtual ~IDevicePageStore() = default;

    // Returns nothing if there are not enough free pages.
    [[nodiscard]] virtual TVector<TPageRangeRef> Allocate(ui64 pageCount) = 0;

    [[nodiscard]] virtual NCloud::NProto::TError AllocateAt(
        const TVector<TPageRangeRef>& pageRanges) = 0;

    [[nodiscard]] virtual NCloud::NProto::TError Free(
        const TVector<TPageRangeRef>& pageRanges) = 0;

    [[nodiscard]] virtual auto Write(
        const TVector<TPageRangeRef>& pageRanges,
        const TVector<TBuffer>& pages)
        -> NThreading::TFuture<NCloud::NProto::TError> = 0;

    [[nodiscard]] virtual auto Read(const TVector<TPageRangeRef>& pageRanges)
        -> NThreading::TFuture<TResultOrError<TVector<TBuffer>>> = 0;
};

////////////////////////////////////////////////////////////////////////////////

// The store owns the page allocation and guards it with a lock, so Allocate,
// AllocateAt and Free can be called from any thread.
//
// Read and Write work with any pages of the device, allocated or not, and do
// not take the lock. The caller must not call them concurrently for the same
// pages or free the pages in flight.
//
// The page ranges of a single request must not intersect.
IDevicePageStorePtr CreateDevicePageStore(
    IDevicePtr device,
    ui64 pageCount,
    ui32 pageSize,
    EDevicePageStoreMode mode);

}   // namespace NCloud::NJournalled
