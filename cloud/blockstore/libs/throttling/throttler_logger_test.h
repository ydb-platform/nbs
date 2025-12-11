#pragma once

#include "throttler_logger.h"

#include <cloud/blockstore/libs/service/context.h>

#include <functional>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TTestThrottlerLogger final: public IThrottlerLogger
{
#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                  \
    using T##name##LogPostponedRequestHandler = std::function<void(           \
        ui64,                                                                 \
        TCallContext&,                                                        \
        IVolumeInfo*,                                                         \
        const NProto::T##name##Request&,                                      \
        TDuration)>;                                                          \
    using T##name##LogAdvancedRequestHandler = std::function<void(            \
        ui64,                                                                 \
        TCallContext&,                                                        \
        IVolumeInfo*,                                                         \
        const NProto::T##name##Request&)>;                                    \
    using T##name##LogErrorHandler =                                          \
        std::function<void(const NProto::T##name##Request&, const TString&)>; \
                                                                              \
    T##name##LogPostponedRequestHandler name##LogPostponedRequestHandler;     \
    T##name##LogAdvancedRequestHandler name##LogAdvancedRequestHandler;       \
    T##name##LogErrorHandler name##LogErrorHandler;                           \
                                                                              \
    void LogPostponedRequest(                                                 \
        ui64 nowCycles,                                                       \
        TCallContext& callContext,                                            \
        IVolumeInfo* volumeInfo,                                              \
        const NProto::T##name##Request& request,                              \
        TDuration postponeDelay) override                                     \
    {                                                                         \
        name##LogPostponedRequestHandler(                                     \
            nowCycles,                                                        \
            callContext,                                                      \
            volumeInfo,                                                       \
            request,                                                          \
            postponeDelay);                                                   \
    }                                                                         \
                                                                              \
    void LogAdvancedRequest(                                                  \
        ui64 nowCycles,                                                       \
        TCallContext& callContext,                                            \
        IVolumeInfo* volumeInfo,                                              \
        const NProto::T##name##Request& request) override                     \
    {                                                                         \
        name##LogAdvancedRequestHandler(                                      \
            nowCycles,                                                        \
            callContext,                                                      \
            volumeInfo,                                                       \
            request);                                                         \
    }                                                                         \
                                                                              \
    void LogError(                                                            \
        const NProto::T##name##Request& request,                              \
        const TString& errorMessage) override                                 \
    {                                                                         \
        name##LogErrorHandler(request, errorMessage);                         \
    }                                                                         \
    // BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD
};

}   // namespace NCloud::NBlockStore
