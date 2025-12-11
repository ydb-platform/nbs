#include "throttler_tracker.h"

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TThrottlerTrackerStub final: public IThrottlerTracker
{
public:
#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)            \
    void TrackReceivedRequest(                            \
        TCallContext& callContext,                        \
        IVolumeInfo* volumeInfo,                          \
        const NProto::T##name##Request& request) override \
    {                                                     \
        Y_UNUSED(callContext);                            \
        Y_UNUSED(volumeInfo);                             \
        Y_UNUSED(request);                                \
    }                                                     \
                                                          \
    void TrackPostponedRequest(                           \
        TCallContext& callContext,                        \
        const NProto::T##name##Request& request) override \
    {                                                     \
        Y_UNUSED(callContext);                            \
        Y_UNUSED(request);                                \
    }                                                     \
                                                          \
    void TrackAdvancedRequest(                            \
        TCallContext& callContext,                        \
        const NProto::T##name##Request& request) override \
    {                                                     \
        Y_UNUSED(callContext);                            \
        Y_UNUSED(request);                                \
    }                                                     \
    // BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IThrottlerTrackerPtr CreateThrottlerTrackerStub()
{
    return std::make_shared<TThrottlerTrackerStub>();
}

}   // namespace NCloud::NBlockStore
