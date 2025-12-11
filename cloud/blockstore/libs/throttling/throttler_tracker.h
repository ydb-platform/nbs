#pragma once

#include "public.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IThrottlerTracker
{
    virtual ~IThrottlerTracker() = default;

#define BLOCKSTORE_DECLARE_METHOD(name, ...)          \
    virtual void TrackReceivedRequest(                \
        TCallContext& callContext,                    \
        IVolumeInfo* volumeInfo,                      \
        const NProto::T##name##Request& request) = 0; \
                                                      \
    virtual void TrackPostponedRequest(               \
        TCallContext& callContext,                    \
        const NProto::T##name##Request& request) = 0; \
                                                      \
    virtual void TrackAdvancedRequest(                \
        TCallContext& callContext,                    \
        const NProto::T##name##Request& request) = 0; \
    // BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD
};

////////////////////////////////////////////////////////////////////////////////

IThrottlerTrackerPtr CreateThrottlerTrackerStub();

}   // namespace NCloud::NBlockStore
