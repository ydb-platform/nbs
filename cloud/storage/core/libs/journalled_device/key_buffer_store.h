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

using TKeyBuffers = TVector<std::pair<ui64, TBuffer>>;

////////////////////////////////////////////////////////////////////////////////

struct IKeyBufferStore
{
    virtual ~IKeyBufferStore() = default;

    [[nodiscard]] virtual auto Restore()
        -> NThreading::TFuture<TResultOrError<TKeyBuffers>> = 0;

    [[nodiscard]] virtual auto Write(ui64 key, TBuffer buffer)
        -> NThreading::TFuture<NCloud::NProto::TError> = 0;

    [[nodiscard]] virtual auto EraseBelow(ui64 key)
        -> NThreading::TFuture<NCloud::NProto::TError> = 0;
};

////////////////////////////////////////////////////////////////////////////////

IKeyBufferStorePtr CreateInMemoryKeyBufferStore();

}   // namespace NCloud::NJournalled
