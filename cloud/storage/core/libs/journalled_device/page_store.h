#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

struct TPageGroupRef
{
    ui64 FirstPageNo = 0;
    ui64 PageCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IPageStore
{
    virtual ~IPageStore() = default;

    // Returns nothing if there are not enough free pages.
    [[nodiscard]] virtual TVector<TPageGroupRef> Allocate(ui64 pageCount) = 0;

    [[nodiscard]] virtual NCloud::NProto::TError AllocateAt(
        const TVector<TPageGroupRef>& pageGroupRefs) = 0;

    [[nodiscard]] virtual NCloud::NProto::TError Free(
        const TVector<TPageGroupRef>& pageGroupRefs) = 0;

    [[nodiscard]] virtual auto Write(
        const TVector<TPageGroupRef>& pageGroupRefs,
        TVector<TString> pages)
        -> NThreading::TFuture<NCloud::NProto::TError> = 0;

    [[nodiscard]] virtual auto Read(
        const TVector<TPageGroupRef>& pageGroupRefs)
        -> NThreading::TFuture<TResultOrError<TVector<TString>>> = 0;
};

////////////////////////////////////////////////////////////////////////////////

// The store owns the page allocation and guards it with a lock, so Allocate,
// AllocateAt and Free can be called from any thread.
//
// The data methods - Read and Write - only validate the page group refs they
// are given against the allocation state under that lock, the device request
// itself is issued outside of it. The caller is expected to call them for the
// pages it has allocated beforehand and not to call them concurrently with
// each other or with Free for the same pages.
//
// The page group refs of a single request must not intersect with each other -
// the store does not check this and does not account for such refs properly.
IPageStorePtr CreatePageStore(
    IDevicePtr device,
    ui64 pageCount,
    ui32 pageSize);

}   // namespace NCloud::NJournalled
