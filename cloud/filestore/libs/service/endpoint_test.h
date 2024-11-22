#pragma once

#include "public.h"

#include "endpoint.h"

#include <functional>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

struct TEndpointManagerTest
    : public IEndpointManager
{
#define FILESTORE_IMPLEMENT_METHOD(name, ...)                                  \
    using T##name##Handler = std::function<                                    \
        NThreading::TFuture<NProto::T##name##Response>(                        \
            std::shared_ptr<NProto::T##name##Request>)                         \
        >;                                                                     \
                                                                               \
    T##name##Handler name##Handler;                                            \
                                                                               \
    NThreading::TFuture<NProto::T##name##Response> name(                       \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        Y_UNUSED(callContext);                                                 \
        return name##Handler(std::move(request));                              \
    }                                                                          \
// FILESTORE_IMPLEMENT_METHOD

    FILESTORE_ENDPOINT_SERVICE(FILESTORE_IMPLEMENT_METHOD)

#undef FILESTORE_IMPLEMENT_METHOD

    void Start() override
    {}

    void Stop() override
    {}

    void Drain() override
    {}

    NThreading::TFuture<void> RestoreEndpoints() override
    {
        return NThreading::MakeFuture();
    }
};

}   // namespace NCloud::NFileStore
