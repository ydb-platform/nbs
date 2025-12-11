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

class TThrottlerLoggerDefault final: public IThrottlerLogger
{
private:
    const IRequestStatsPtr RequestStats;

    TLog Log;

public:
    TThrottlerLoggerDefault(
        IRequestStatsPtr requestStats,
        ILoggingServicePtr logging,
        const TString& loggerName)
        : RequestStats(std::move(requestStats))
        , Log(logging->CreateLog(loggerName))
    {}

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                        \
    void LogPostponedRequest(                                         \
        ui64 nowCycles,                                               \
        TCallContext& callContext,                                    \
        IVolumeInfo* volumeInfo,                                      \
        const NProto::T##name##Request& request,                      \
        TDuration postponeDelay) override                             \
    {                                                                 \
        static constinit auto requestType = EBlockStoreRequest::name; \
        RequestStats->RequestPostponedServer(requestType);            \
        if (volumeInfo) {                                             \
            volumeInfo->RequestPostponedServer(requestType);          \
        }                                                             \
                                                                      \
        STORAGE_DEBUG(                                                \
            TRequestInfo(                                             \
                requestType,                                          \
                GetRequestId(request),                                \
                GetDiskId(request),                                   \
                GetClientId(request))                                 \
            << GetRequestDetails(request) << " request postponed"     \
            << " (delay: " << FormatDuration(postponeDelay) << ")");  \
                                                                      \
        callContext.Postpone(nowCycles);                              \
    }                                                                 \
                                                                      \
    void LogAdvancedRequest(                                          \
        ui64 nowCycles,                                               \
        TCallContext& callContext,                                    \
        IVolumeInfo* volumeInfo,                                      \
        const NProto::T##name##Request& request) override             \
    {                                                                 \
        static constinit auto requestType = EBlockStoreRequest::name; \
        RequestStats->RequestAdvancedServer(requestType);             \
        if (volumeInfo) {                                             \
            volumeInfo->RequestAdvancedServer(requestType);           \
        }                                                             \
                                                                      \
        STORAGE_DEBUG(                                                \
            TRequestInfo(                                             \
                requestType,                                          \
                GetRequestId(request),                                \
                GetDiskId(request),                                   \
                GetClientId(request))                                 \
            << GetRequestDetails(request) << " request advanced");    \
                                                                      \
        callContext.Advance(nowCycles);                               \
    }                                                                 \
                                                                      \
    void LogError(                                                    \
        const NProto::T##name##Request& request,                      \
        const TString& errorMessage) override                         \
    {                                                                 \
        static constinit auto requestType = EBlockStoreRequest::name; \
        STORAGE_ERROR(                                                \
            TRequestInfo(                                             \
                requestType,                                          \
                GetRequestId(request),                                \
                GetDiskId(request),                                   \
                GetClientId(request))                                 \
            << GetRequestDetails(request)                             \
            << " exception in callback: " << errorMessage);           \
    }                                                                 \
    // BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD
};

////////////////////////////////////////////////////////////////////////////////

class TThrottlerLoggerStub final: public IThrottlerLogger
{
public:
#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                \
    void LogPostponedRequest(                                                 \
        ui64 nowCycles,                                                       \
        TCallContext& callContext,                                            \
        IVolumeInfo* volumeInfo,                                              \
        const NProto::T##name##Request& request,                              \
        TDuration postponeDelay) override                                     \
    {                                                                         \
        Y_UNUSED(nowCycles, callContext, volumeInfo, request, postponeDelay); \
    }                                                                         \
                                                                              \
    void LogAdvancedRequest(                                                  \
        ui64 nowCycles,                                                       \
        TCallContext& callContext,                                            \
        IVolumeInfo* volumeInfo,                                              \
        const NProto::T##name##Request& request) override                     \
    {                                                                         \
        Y_UNUSED(nowCycles, callContext, volumeInfo, request);                \
    }                                                                         \
                                                                              \
    void LogError(                                                            \
        const NProto::T##name##Request& request,                              \
        const TString& errorMessage) override                                 \
    {                                                                         \
        Y_UNUSED(request, errorMessage);                                      \
    }                                                                         \
    // BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IThrottlerLoggerPtr CreateServiceThrottlerLogger(
    IRequestStatsPtr requestStats,
    ILoggingServicePtr logging)
{
    return std::make_shared<TThrottlerLoggerDefault>(
        std::move(requestStats),
        std::move(logging),
        "BLOCKSTORE_SERVER");
}

}   // namespace NCloud::NBlockStore
