#pragma once

#include "public.h"

#include "context.h"
#include "storage.h"

#include <library/cpp/threading/future/future.h>

#include <functional>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                   \
    using T##name##Handler = std::function<                                    \
        NThreading::TFuture<NProto::T##name##Response>(                        \
            TCallContextPtr callContext,                                       \
            std::shared_ptr<NProto::T##name##Request>)                         \
        >;                                                                     \
                                                                               \
    T##name##Handler name##Handler;                                            \
                                                                               \
    NThreading::TFuture<NProto::T##name##Response> name(                       \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        return name##Handler(std::move(callContext), std::move(request));      \
    }                                                                          \
// BLOCKSTORE_DECLARE_METHOD

////////////////////////////////////////////////////////////////////////////////

struct TTestStorage final
    : public IStorage
{
    BLOCKSTORE_DECLARE_METHOD(ZeroBlocks)
    BLOCKSTORE_DECLARE_METHOD(ReadBlocksLocal)
    BLOCKSTORE_DECLARE_METHOD(WriteBlocksLocal)

    NThreading::TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) override
    {
        Y_UNUSED(method);
        return NThreading::MakeFuture(NProto::TError());
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }
};

#undef BLOCKSTORE_DECLARE_METHOD

}   // namespace NCloud::NBlockStore
