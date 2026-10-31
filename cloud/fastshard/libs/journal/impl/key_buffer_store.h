#pragma once

#include "public.h"

#include <cloud/fastshard/libs/journal/iface/public.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/buffer.h>
#include <util/generic/vector.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

struct TKeyBuffer
{
    ui64 Key = 0;
    TBuffer Buffer;
};

// What a store holds: the single metadata buffer and the key buffers.
struct TRestoredBuffers
{
    TBuffer MetaBuffer;
    TVector<TKeyBuffer> KeyBuffers;
};

////////////////////////////////////////////////////////////////////////////////

struct IKeyBufferStore
{
    using TRestoreResult = TResultOrError<TRestoredBuffers>;

    virtual ~IKeyBufferStore() = default;

    [[nodiscard]] virtual NThreading::TFuture<TRestoreResult> Restore() = 0;

    [[nodiscard]] virtual auto Write(ui64 key, TBuffer buffer)
        -> NThreading::TFuture<NCloud::NProto::TError> = 0;

    [[nodiscard]] virtual auto WriteMeta(TBuffer buffer)
        -> NThreading::TFuture<NCloud::NProto::TError> = 0;

    [[nodiscard]] virtual auto EraseBelow(ui64 key)
        -> NThreading::TFuture<NCloud::NProto::TError> = 0;
};

////////////////////////////////////////////////////////////////////////////////

IKeyBufferStorePtr CreateInMemoryKeyBufferStore();

IKeyBufferStorePtr CreateDeviceKeyBufferStore(
    ILoggingServicePtr logging,
    IDevicePtr device,
    ui64 pageCount,
    ui32 pageSize);

}   // namespace NCloud::NJournalled
