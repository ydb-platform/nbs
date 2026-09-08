#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/set.h>
#include <util/generic/string.h>

#include <memory>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

struct IKeyBufferStore
{
    virtual ~IKeyBufferStore() = default;

    [[nodiscard]] virtual auto Write(ui64 key, TString buffer)
        -> NThreading::TFuture<NCloud::NProto::TError> = 0;

    [[nodiscard]] virtual auto Read(ui64 key) const
        -> NThreading::TFuture<TResultOrError<TString>> = 0;

    [[nodiscard]] virtual auto EraseUpTo(ui64 lastKey)
        -> NThreading::TFuture<NCloud::NProto::TError> = 0;

    [[nodiscard]] virtual TSet<ui64> GetKeys() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

IKeyBufferStorePtr CreateInMemoryKeyBufferStore();

}   // namespace NCloud::NJournalled
