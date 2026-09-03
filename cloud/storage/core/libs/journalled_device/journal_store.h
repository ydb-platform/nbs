#pragma once

#include "public.h"

#include "log_record.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/device.pb.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/buffer.h>
#include <util/generic/set.h>

#include <memory>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

struct IKeyBufferStore
{
    virtual ~IKeyBufferStore() = default;

    [[nodiscard]] virtual auto Write(ui64 key, TBuffer buffer)
        -> NThreading::TFuture<NCloud::NProto::TError> = 0;
    [[nodiscard]] virtual auto Read(ui64 key) const
        -> TFutureResultOrError<TBuffer> = 0;
    [[nodiscard]] virtual auto EraseTo(ui64 key)
        -> NThreading::TFuture<NCloud::NProto::TError> = 0;
    [[nodiscard]] virtual TSet<ui64> GetKeys() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IPageStore
{
    virtual ~IPageStore() = default;

    [[nodiscard]] virtual auto WritePageGroups(
        const NCloud::NProto::TWriteLogRecordRequest& request)
        -> TFutureResultOrError<TVector<TPageGroupRef>> = 0;
    [[nodiscard]] virtual auto ReadPageGroups(
        const TVector<TPageGroupRef>& pageGroupRefs)
        -> TFutureResultOrError<TVector<TBuffer>> = 0;

    [[nodiscard]] virtual NCloud::NProto::TError Free(
        const TVector<TPageGroupRef>& pageGroupRefs) = 0;
    [[nodiscard]] virtual NCloud::NProto::TError MarkAsWritten(
        const TVector<TPageGroupRef>& pageGroupRefs) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IKeyBufferStorePtr CreateInMemoryKeyBufferStore();

IPageStorePtr CreateInMemoryPageStore();

}   // namespace NCloud::NJournalled
