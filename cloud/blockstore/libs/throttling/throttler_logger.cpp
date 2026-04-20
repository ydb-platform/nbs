#include "throttler_logger.h"

#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/throttling/throttler_logger.h>

#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TThrottlerLoggerStub final
    : public IThrottlerLogger
{
public:

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    void LogPostponedRequest(                                                  \
        IVolumeInfo* volumeInfo,                                               \
        const NProto::T##name##Request& request,                               \
        TDuration postponeDelay) override                                      \
    {                                                                          \
        Y_UNUSED(volumeInfo, request, postponeDelay);                          \
    }                                                                          \
                                                                               \
    void LogAdvancedRequest(                                                   \
        IVolumeInfo* volumeInfo,                                               \
        const NProto::T##name##Request& request) override                      \
    {                                                                          \
        Y_UNUSED(volumeInfo, request);                                         \
    }                                                                          \
                                                                               \
    void LogError(                                                             \
        const NProto::T##name##Request& request,                               \
        const TString& errorMessage) override                                  \
    {                                                                          \
        Y_UNUSED(request, errorMessage);                                       \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IThrottlerLoggerPtr CreateThrottlerLoggerStub()
{
    return std::make_shared<TThrottlerLoggerStub>();
}

}   // namespace NCloud::NBlockStore
