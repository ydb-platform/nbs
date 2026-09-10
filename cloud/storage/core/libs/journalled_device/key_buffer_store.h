#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/buffer.h>
#include <util/generic/map.h>

#include <memory>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

struct IKeyBufferStore
{
    virtual ~IKeyBufferStore() = default;

    [[nodiscard]] virtual auto Restore()
        -> NThreading::TFuture<TResultOrError<TMap<ui64, TBuffer>>> = 0;

    [[nodiscard]] virtual auto Write(ui64 key, TBuffer buffer)
        -> NThreading::TFuture<NCloud::NProto::TError> = 0;

    [[nodiscard]] virtual auto EraseUpTo(ui64 lastKey)
        -> NThreading::TFuture<NCloud::NProto::TError> = 0;
};

////////////////////////////////////////////////////////////////////////////////

IKeyBufferStorePtr CreateInMemoryKeyBufferStore();

IKeyBufferStorePtr CreateDeviceKeyBufferStore(
    IDevicePtr device,
    ui64 pageCount,
    ui32 pageSize);

}   // namespace NCloud::NJournalled
