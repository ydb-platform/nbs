#pragma once

#include "throttler_tracker.h"

#include <cloud/blockstore/libs/service/context.h>

#include <functional>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TTestThrottlerTracker final: public IThrottlerTracker
{
#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                  \
    using T##name##TrackReceivedRequestHandler = std::function<void(          \
        TCallContext&,                                                        \
        IVolumeInfo*,                                                         \
        const NProto::T##name##Request& request)>;                            \
    using T##name##TrackPostponedRequestHandler =                             \
        std::function<void(TCallContext&, const NProto::T##name##Request&)>;  \
    using T##name##TrackAdvancedRequestHandler =                              \
        std::function<void(TCallContext&, const NProto::T##name##Request&)>;  \
                                                                              \
    T##name##TrackReceivedRequestHandler name##TrackReceivedRequestHandler;   \
    T##name##TrackPostponedRequestHandler name##TrackPostponedRequestHandler; \
    T##name##TrackAdvancedRequestHandler name##TrackAdvancedRequestHandler;   \
                                                                              \
    void TrackReceivedRequest(                                                \
        TCallContext& callContext,                                            \
        IVolumeInfo* volumeInfo,                                              \
        const NProto::T##name##Request& request) override                     \
    {                                                                         \
        name##TrackReceivedRequestHandler(callContext, volumeInfo, request);  \
    }                                                                         \
                                                                              \
    void TrackPostponedRequest(                                               \
        TCallContext& callContext,                                            \
        const NProto::T##name##Request& request) override                     \
    {                                                                         \
        name##TrackPostponedRequestHandler(callContext, request);             \
    }                                                                         \
                                                                              \
    void TrackAdvancedRequest(                                                \
        TCallContext& callContext,                                            \
        const NProto::T##name##Request& request) override                     \
    {                                                                         \
        name##TrackAdvancedRequestHandler(callContext, request);              \
    }                                                                         \
    // BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD
};

}   // namespace NCloud::NBlockStore
