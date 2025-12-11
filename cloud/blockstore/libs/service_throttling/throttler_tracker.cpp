#include "throttler_tracker.h"

#include <cloud/blockstore/libs/diagnostics/probes.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/throttling/throttler_tracker.h>

namespace NCloud::NBlockStore {

LWTRACE_USING(BLOCKSTORE_SERVER_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class TThrottlerTracker final: public IThrottlerTracker
{
public:
#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                      \
    void TrackReceivedRequest(                                      \
        TCallContext& callContext,                                  \
        IVolumeInfo* volumeInfo,                                    \
        const NProto::T##name##Request& request) override           \
    {                                                               \
        if (volumeInfo) {                                           \
            LWTRACK(                                                \
                RequestReceived_ThrottlingService,                  \
                callContext.LWOrbit,                                \
                GetBlockStoreRequestName(EBlockStoreRequest::name), \
                static_cast<ui32>(                                  \
                    volumeInfo->GetInfo().GetStorageMediaKind()),   \
                GetRequestId(request),                              \
                GetDiskId(request));                                \
        }                                                           \
    }                                                               \
                                                                    \
    void TrackPostponedRequest(                                     \
        TCallContext& callContext,                                  \
        const NProto::T##name##Request& request) override           \
    {                                                               \
        LWTRACK(                                                    \
            RequestPostponed_ThrottlingService,                     \
            callContext.LWOrbit,                                    \
            GetBlockStoreRequestName(EBlockStoreRequest::name),     \
            GetRequestId(request),                                  \
            GetDiskId(request));                                    \
    }                                                               \
                                                                    \
    void TrackAdvancedRequest(                                      \
        TCallContext& callContext,                                  \
        const NProto::T##name##Request& request) override           \
    {                                                               \
        LWTRACK(                                                    \
            RequestAdvanced_ThrottlingService,                      \
            callContext.LWOrbit,                                    \
            GetBlockStoreRequestName(EBlockStoreRequest::name),     \
            GetRequestId(request),                                  \
            GetDiskId(request));                                    \
    }                                                               \
    // BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IThrottlerTrackerPtr CreateServiceThrottlerTracker()
{
    return std::make_shared<TThrottlerTracker>();
}

}   // namespace NCloud::NBlockStore
