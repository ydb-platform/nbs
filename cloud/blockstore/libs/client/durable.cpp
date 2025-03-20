#include "durable.h"

#include "config.h"

#include <cloud/blockstore/libs/diagnostics/probes.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/media.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>

#include <util/stream/format.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NClient {

using namespace NThreading;

LWTRACE_USING(BLOCKSTORE_SERVER_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

ELogPriority GetDetailsLogPriority(const NProto::TError& error)
{
    const auto kind = GetDiagnosticsErrorKind(error);

    switch (kind) {
        case EDiagnosticsErrorKind::ErrorAborted:
        case EDiagnosticsErrorKind::ErrorFatal:
        case EDiagnosticsErrorKind::ErrorRetriable:
        case EDiagnosticsErrorKind::ErrorSession:
        case EDiagnosticsErrorKind::ErrorSilent:
            return TLOG_INFO;
        default:
            return TLOG_DEBUG;
    }
}

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                   \
    struct T##name##Method                                                     \
    {                                                                          \
        static constexpr EBlockStoreRequest Request = EBlockStoreRequest::name;\
                                                                               \
        using TRequest = NProto::T##name##Request;                             \
        using TResponse = NProto::T##name##Response;                           \
                                                                               \
        template <typename T, typename ...TArgs>                               \
        static TFuture<TResponse> Execute(T& client, TArgs&& ...args)          \
        {                                                                      \
            return client.name(std::forward<TArgs>(args)...);                  \
        }                                                                      \
    };                                                                         \
// BLOCKSTORE_DECLARE_METHOD

BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD

////////////////////////////////////////////////////////////////////////////////

template <typename T>
struct TRequestStateBase
    : public TRetryState
{
    TCallContextPtr CallContext;
    std::shared_ptr<typename T::TRequest> Request;
    TPromise<typename T::TResponse> Response;
    ELogPriority DetailsLogPriority = TLOG_DEBUG;

    TRequestStateBase(
            TCallContextPtr callContext,
            std::shared_ptr<typename T::TRequest> request)
        : CallContext(std::move(callContext))
        , Request(std::move(request))
        , Response(NewPromise<typename T::TResponse>())
    {}
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
struct TRequestState
    : public TRequestStateBase<T>
    , public TAtomicRefCount<TRequestState<T>>
{
    using TRequestStateBase<T>::TRequestStateBase;
};

template <>
struct TRequestState<TReadBlocksLocalMethod>
    : public TRequestStateBase<TReadBlocksLocalMethod>
    , public TAtomicRefCount<TRequestState<TReadBlocksLocalMethod>>
{
    TGuardedSgList SentSgList;

    using TRequestStateBase<TReadBlocksLocalMethod>::TRequestStateBase;
};

template <typename T>
using TRequestStatePtr = TIntrusivePtr<TRequestState<T>>;

////////////////////////////////////////////////////////////////////////////////

class TDurableClient final
    : public IBlockStore
    , public std::enable_shared_from_this<TDurableClient>
{
protected:
    const TClientAppConfigPtr Config;
    const IBlockStorePtr Client;
    const IRetryPolicyPtr RetryPolicy;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const IRequestStatsPtr RequestStats;
    const IVolumeStatsPtr VolumeStats;

    TLog Log;

public:
    TDurableClient(
            TClientAppConfigPtr config,
            IBlockStorePtr client,
            IRetryPolicyPtr retryPolicy,
            ILoggingServicePtr logging,
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            IRequestStatsPtr requestStats,
            IVolumeStatsPtr volumeStats)
        : Config(std::move(config))
        , Client(std::move(client))
        , RetryPolicy(std::move(retryPolicy))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , RequestStats(std::move(requestStats))
        , VolumeStats(std::move(volumeStats))
        , Log(logging->CreateLog("BLOCKSTORE_CLIENT"))
    {}

    void Start() override
    {
        Client->Start();
    }

    void Stop() override
    {
        Client->Stop();
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Client->AllocateBuffer(bytesCount);
    }

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        return HandleRequest<T##name##Method>(                                 \
            std::move(callContext), std::move(request));                       \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD

private:
    template <typename T>
    TFuture<typename T::TResponse> HandleRequest(
        TCallContextPtr callContext,
        std::shared_ptr<typename T::TRequest> request)
    {
        auto state = MakeIntrusive<TRequestState<T>>(
            std::move(callContext),
            std::move(request));

        ExecuteRequest(state);
        return state->Response;
    }

    template <typename T>
    void ExecuteRequest(TRequestStatePtr<T> state)
    {
        auto request = CreateRequestToSend(*state);
        if (!request) {
            Y_DEBUG_ABORT_UNLESS(state->Response.HasValue());
            return;
        }

        auto weak_ptr = this->weak_from_this();

        T::Execute(*Client, state->CallContext, std::move(request)).Subscribe(
            [state_ = std::move(state), weak_ptr = std::move(weak_ptr)] (const auto& future) mutable {
                if (auto p = weak_ptr.lock()) {
                    p->HandleResponse(std::move(state_), future);
                } else {
                    state_->Response.SetValue(TErrorResponse(
                        E_REJECTED,
                        "Durable client is destroyed"));
                }
            });
    }

    template <typename T>
    void IncrementRequestTimeout(T& request)
    {
        auto& headers = *request.MutableHeaders();
        const auto timeout = TDuration::MilliSeconds(headers.GetRequestTimeout());
        const auto newTimeout = Min<TDuration>(
            timeout + Config->GetRequestTimeoutIncrementOnRetry(),
            Config->GetRequestTimeoutMax());
        headers.SetRequestTimeout(newTimeout.MilliSeconds());
    }

    template <typename T>
    void HandleResponse(
        TRequestStatePtr<T> state,
        TFuture<typename T::TResponse> future)
    {
        auto response = ExtractResponse(future);

        const ui64 requestId = GetRequestId(*state->Request);
        const auto& diskId = GetDiskId(*state->Request);
        const auto& clientId = GetClientId(*state->Request);

        if (HasError(response)) {
            const auto& error = response.GetError();
            const auto retrySpec = RetryPolicy->ShouldRetry(*state, error);
            state->DetailsLogPriority =
                Min(GetDetailsLogPriority(error), state->DetailsLogPriority);
            if (retrySpec.ShouldRetry) {
                // retry request
                ++state->Retries;

                LWTRACK(
                    RequestRetry,
                    state->CallContext->LWOrbit,
                    GetBlockStoreRequestName(T::Request),
                    requestId,
                    diskId,
                    state->Retries,
                    retrySpec.Backoff.MilliSeconds(),
                    error.GetCode());

                const auto errorKind = GetDiagnosticsErrorKind(error);
                const auto errorFlags = error.GetFlags();
                const bool throttling =
                    (errorKind == EDiagnosticsErrorKind::ErrorThrottling);

                bool doLogging = true;
                switch (errorKind) {
                    case EDiagnosticsErrorKind::ErrorThrottling: {
                        doLogging = false;
                        state->CallContext->SetHasUncountableRejects();
                        break;
                    }
                    case EDiagnosticsErrorKind::ErrorWriteRejectedByCheckpoint: {
                        // Do not flood in the log. One message in the log is
                        // enough.
                        doLogging = state->Retries == 1;
                        state->CallContext->SetHasUncountableRejects();
                        break;
                    }
                    default:
                        break;
                }
                if (error.GetCode() == E_TIMEOUT) {
                    IncrementRequestTimeout(*state->Request);
                }

                if (doLogging) {
                    STORAGE_WARN(
                        TRequestInfo(
                            T::Request,
                            requestId,
                            diskId,
                            clientId,
                            Config->GetInstanceId())
                        << GetRequestDetails(*state->Request)
                        << " retry request"
                        << " (retries: " << state->Retries
                        << ", timeout: " << FormatDuration(retrySpec.Backoff)
                        << ", error: " << FormatError(error)
                        << ")");
                }

                auto volumeInfo = VolumeStats->GetVolumeInfo(diskId, clientId);
                if (volumeInfo) {
                    volumeInfo->AddRetryStats(
                        T::Request,
                        errorKind,
                        errorFlags);
                }

                RequestStats->AddRetryStats(
                    VolumeStats->GetStorageMediaKind(diskId),
                    T::Request,
                    errorKind,
                    errorFlags);

                auto postponeCycles = GetCycleCount();
                if (throttling) {
                    state->CallContext->Postpone(postponeCycles);
                }

                auto weak_ptr = this->weak_from_this();

                Scheduler->Schedule(
                    Timer->Now() + retrySpec.Backoff,
                    [=, weak_ptr = std::move(weak_ptr)] {
                        auto nowCycles = GetCycleCount();
                        if (throttling) {
                            state->CallContext->Advance(nowCycles);
                        } else {
                            state->CallContext->AddTime(
                                EProcessingStage::Backoff,
                                CyclesToDurationSafe(nowCycles - postponeCycles));
                        }

                        if (auto p = weak_ptr.lock()) {
                            p->ExecuteRequest(state);
                        } else {
                            state->Response.SetValue(TErrorResponse(
                                E_REJECTED,
                                "Durable client is destroyed"));
                        }
                    });
                return;
            } else if (retrySpec.IsRetriableError) {
                auto& error = *response.MutableError();
                auto errorStr = FormatError(error);
                error.SetCode(E_RETRY_TIMEOUT);
                error.SetMessage(TStringBuilder() << "Retry timeout: " << errorStr);
            }

            auto duration = TInstant::Now() - state->Started;
            STORAGE_ERROR(
                TRequestInfo(
                    T::Request,
                    requestId,
                    diskId,
                    clientId,
                    Config->GetInstanceId())
                << GetRequestDetails(*state->Request)
                << " will not retry error: " << FormatError(response.GetError())
                << " (retries: " << state->Retries
                << ", duration: " << FormatDuration(duration)
                << ")");
        } else {
            // log successful request
            if (state->Retries) {
                auto duration = TInstant::Now() - state->Started;
                STORAGE_LOG(
                    state->DetailsLogPriority,
                    TRequestInfo(
                        T::Request,
                        requestId,
                        diskId,
                        clientId,
                        Config->GetInstanceId())
                    << GetRequestDetails(*state->Request)
                    << " request completed"
                    << " (retries: " << state->Retries
                    << ", duration: " << FormatDuration(duration)
                    << ")");
            }
        }

        try {
            state->Response.SetValue(std::move(response));
        } catch (...) {
            STORAGE_ERROR(
                TRequestInfo(
                    T::Request,
                    requestId,
                    diskId,
                    clientId,
                    Config->GetInstanceId())
                << GetRequestDetails(*state->Request)
                << " exception in callback: " << CurrentExceptionMessage());
        }
    }

    template <typename T>
    std::shared_ptr<typename T::TRequest> CreateRequestToSend(
        TRequestState<T>& state)
    {
        // send the same request for non local requests.
        return state.Request;
    }

    template <>
    std::shared_ptr<NProto::TReadBlocksLocalRequest> CreateRequestToSend(
        TRequestState<TReadBlocksLocalMethod>& state)
    {
        const auto& request = state.Request;

        if (!request->Sglist.Acquire()) {
            state.Response.SetValue(TErrorResponse(
                E_CANCELLED,
                "failed to acquire sglist in DurableClient"));
            return nullptr;
        }

        auto copy = std::make_shared<NProto::TReadBlocksLocalRequest>(*request);
        copy->Sglist = request->Sglist.CreateDepender();

        state.SentSgList.Close();
        state.SentSgList = copy->Sglist;

        return copy;
    }

    template <>
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> CreateRequestToSend(
        TRequestState<TWriteBlocksLocalMethod>& state)
    {
        const auto& request = state.Request;

        if (!request->Sglist.Acquire()) {
            state.Response.SetValue(TErrorResponse(
                E_CANCELLED,
                "failed to acquire sglist in DurableClient"));
            return nullptr;
        }

        // copy request without data (only TSgList).
        return std::make_shared<NProto::TWriteBlocksLocalRequest>(*request);
    }

    template <>
    std::shared_ptr<NProto::TZeroBlocksRequest> CreateRequestToSend(
        TRequestState<TZeroBlocksMethod>& state)
    {
        return std::make_shared<NProto::TZeroBlocksRequest>(*state.Request);
    }

    template <>
    std::shared_ptr<NProto::TMountVolumeRequest> CreateRequestToSend(
        TRequestState<TMountVolumeMethod>& state)
    {
        return std::make_shared<NProto::TMountVolumeRequest>(*state.Request);
    }

    template <>
    std::shared_ptr<NProto::TUnmountVolumeRequest> CreateRequestToSend(
        TRequestState<TUnmountVolumeMethod>& state)
    {
        return std::make_shared<NProto::TUnmountVolumeRequest>(*state.Request);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRetryPolicy final: public IRetryPolicy
{
private:
    TClientAppConfigPtr Config;
    TDuration InitialRetryTimeout;

public:
    TRetryPolicy(TClientAppConfigPtr config, TDuration initialRetryTimeout)
        : Config(std::move(config))
        , InitialRetryTimeout(initialRetryTimeout)
    {}

    TRetrySpec ShouldRetry(
        TRetryState& state,
        const NProto::TError& error) override
    {
        TRetrySpec spec;
        spec.IsRetriableError =
            GetErrorKind(error) == EErrorKind::ErrorRetriable;

        if (!spec.IsRetriableError ||
            TInstant::Now() - state.Started >= Config->GetRetryTimeout())
        {
            return spec;
        }

        const auto newRetryTimeout =
            state.Retries > 0
                ? (state.RetryTimeout + Config->GetRetryTimeoutIncrement())
                : InitialRetryTimeout;
        spec.ShouldRetry = true;

        if (HasProtoFlag(error.GetFlags(), NProto::EF_INSTANT_RETRIABLE) &&
            !state.DoneInstantRetry)
        {
            spec.Backoff = TDuration::Zero();
            state.DoneInstantRetry = true;
        } else {
            spec.Backoff = newRetryTimeout;
        }

        if (IsConnectionError(error) &&
            spec.Backoff > Config->GetConnectionErrorMaxRetryTimeout())
        {
            spec.Backoff = Config->GetConnectionErrorMaxRetryTimeout();
            return spec;
        }

        state.RetryTimeout = newRetryTimeout;
        return spec;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IRetryPolicyPtr CreateRetryPolicy(
    TClientAppConfigPtr config,
    std::optional<NProto::EStorageMediaKind> mediaKind)
{
    TDuration initialRetryTimeout = config->GetRetryTimeoutIncrement();
    if (mediaKind.has_value()) {
        if (IsDiskRegistryMediaKind(*mediaKind)) {
            initialRetryTimeout =
                config->GetDiskRegistryBasedDiskInitialRetryTimeout();
        } else {
            initialRetryTimeout = config->GetYDBBasedDiskInitialRetryTimeout();
        }
    }

    return std::make_shared<TRetryPolicy>(
        std::move(config),
        initialRetryTimeout);
}

IBlockStorePtr CreateDurableClient(
    TClientAppConfigPtr config,
    IBlockStorePtr client,
    IRetryPolicyPtr retryPolicy,
    ILoggingServicePtr logging,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    IRequestStatsPtr requestStats,
    IVolumeStatsPtr volumeStats)
{
    return std::make_shared<TDurableClient>(
        std::move(config),
        std::move(client),
        std::move(retryPolicy),
        std::move(logging),
        std::move(timer),
        std::move(scheduler),
        std::move(requestStats),
        std::move(volumeStats));
}

}   // namespace NCloud::NBlockStore::NClient
