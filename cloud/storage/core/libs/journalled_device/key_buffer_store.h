#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/buffer.h>
#include <util/generic/vector.h>

#include <memory>
#include <utility>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

struct IKeyBufferStore
{
    using TRestoreResult = TResultOrError<TVector<std::pair<ui64, TBuffer>>>;

    virtual ~IKeyBufferStore() = default;

    [[nodiscard]] virtual NThreading::TFuture<TRestoreResult> Restore() = 0;

    [[nodiscard]] virtual auto Write(ui64 key, TBuffer buffer)
        -> NThreading::TFuture<NCloud::NProto::TError> = 0;

    [[nodiscard]] virtual auto EraseBelow(ui64 key)
        -> NThreading::TFuture<NCloud::NProto::TError> = 0;
};

////////////////////////////////////////////////////////////////////////////////

IKeyBufferStorePtr CreateInMemoryKeyBufferStore();

IKeyBufferStorePtr CreateDeviceKeyBufferStore(
    IDevicePtr device,
    ui64 pageCount,
    ui32 pageSize);

}   // namespace NCloud::NJournalled
