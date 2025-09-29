#pragma once

#include <cloud/blockstore/libs/client/public.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/helpers.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>

#include <util/datetime/base.h>
#include <util/generic/map.h>
#include <util/system/types.h>

#include <functional>

namespace NCloud::NBlockStore::NClient::NRetryContext {

using TShouldRetryPredicate = std::function<bool(const NProto::TError&)>;

struct TRetryState
{
    const TInstant Started = TInstant::Now();

    TDuration RequestTimeout;
    TDuration BackoffIncrement = TDuration::MilliSeconds(500);

    ui32 Retries = 0;
    bool DoneInstantRetry = false;
    TShouldRetryPredicate ShouldRetry;
    bool Throttling = false;
    ui64 PostponeCycles = 0;

    TDuration Backoff = TDuration::Zero();

    TRetryState(
        TShouldRetryPredicate shouldRetry,
        const TClientAppConfigPtr& config)
        : RequestTimeout(config->GetRetryTimeout())
        , BackoffIncrement(config->GetRetryTimeoutIncrement())
        , ShouldRetry(std::move(shouldRetry))
    {}

    explicit TRetryState(
        TShouldRetryPredicate shouldRetry,
        const TDuration timeout,
        const TDuration backoffIncrement)
        : RequestTimeout(timeout)
        , BackoffIncrement(backoffIncrement)
        , ShouldRetry(std::move(shouldRetry))
    {}
};

using TRetryStatePtr = std::shared_ptr<TRetryState>;

template <typename TRequest, typename TResponse>
struct TRetryContext;

template <typename TRequest, typename TResponse>
using TRetryContextPtr = std::shared_ptr<TRetryContext<TRequest, TResponse>>;

template <typename TRequest, typename TResponse>
struct TRetryContext
    : std::enable_shared_from_this<TRetryContext<TRequest, TResponse>>
{
    TRetryContext(
        TRetryState state,
        TCallContextPtr callContext,
        std::shared_ptr<TRequest> request,
        NThreading::TPromise<TResponse> response)
        : State(std::move(state))
        , CallContext(std::move(callContext))
        , Request(std::move(request))
        , Response(std::move(response))
    {}
    TRetryState State;
    TCallContextPtr CallContext;
    std::shared_ptr<TRequest> Request;
    NThreading::TPromise<TResponse> Response;

    NProto::TError SetUpForRetry(const NProto::TError& error);
    NProto::TError Retry(
        const NProto::TError& error,
        ISchedulerPtr scheduler,
        ITimerPtr timer,
        std::function<void(TRetryContextPtr<TRequest, TResponse>)> callback);
};

template <typename TRequest, typename TResponse>
using TRetryContextPtr = std::shared_ptr<TRetryContext<TRequest, TResponse>>;

template <typename TRequest, typename TResponse>
TRetryContextPtr<TRequest, TResponse> CreateRetryContext(
    const bool enableRetries,
    TCallContextPtr callContext,
    std::shared_ptr<TRequest> request,
    NThreading::TPromise<TResponse> response,
    TShouldRetryPredicate shouldRetryPredicate,
    TClientAppConfigPtr config)
{
    if (!enableRetries) {
        return {nullptr};
    }
    return std::make_shared<TRetryContext<TRequest, TResponse>>(
        TRetryState{std::move(shouldRetryPredicate), std::move(config)},
        std::move(callContext),
        std::move(request),
        std::move(response));
}

template <typename TRequest, typename TResponse>
TRetryContextPtr<TRequest, TResponse> CreateRetryContext(
    const bool enableRetries,
    TCallContextPtr callContext,
    std::shared_ptr<TRequest> request,
    NThreading::TPromise<TResponse> response,
    TShouldRetryPredicate shouldRetryPredicate,
    const TDuration timeout,
    const TDuration backoffIncrement)
{
    if (!enableRetries) {
        return {nullptr};
    }
    return std::make_shared<TRetryContext<TRequest, TResponse>>(
        TRetryState{std::move(shouldRetryPredicate), timeout, backoffIncrement},
        std::move(callContext),
        std::move(request),
        std::move(response));
}

template <typename TRequest>
auto IncrementRequestTimeout(
    std::shared_ptr<TRequest> request,
    TDuration increment)
{
    auto& headers = *request->MutableHeaders();
    const auto timeout = TDuration::MilliSeconds(headers.GetRequestTimeout());
    const auto newTimeout = timeout + increment;
    headers.SetRequestTimeout(newTimeout.MilliSeconds());
}

template <typename TRequest>
auto IncrementRetryNumberInRequest(std::shared_ptr<TRequest> request)
{
    auto& headers = *request->MutableHeaders();
    headers.SetRetryNumber(headers.GetRetryNumber() + 1);
}

template <typename TRequest, typename TResponse>
NProto::TError TRetryContext<TRequest, TResponse>::SetUpForRetry(
    const NProto::TError& error)
{
    if (State.ShouldRetry(error)) {
        // Check for timeout
        if (TInstant::Now() - State.Started >= State.RequestTimeout) {
            return MakeError(
                E_RETRY_TIMEOUT,
                TStringBuilder() << "Retry timeout: " << FormatError(error));
        }
        // Calculate backoff time
        if (HasProtoFlag(error.GetFlags(), NProto::EF_INSTANT_RETRIABLE) &&
            !State.DoneInstantRetry)
        {
            State.DoneInstantRetry = true;
        } else {
            State.Backoff += State.BackoffIncrement;
        }

        // Prepare for retry
        ++State.Retries;
        const auto errorKind = GetDiagnosticsErrorKind(error);
        State.Throttling = errorKind == EDiagnosticsErrorKind::ErrorThrottling;

        switch (errorKind) {
            case EDiagnosticsErrorKind::ErrorThrottling:
            case EDiagnosticsErrorKind::ErrorWriteRejectedByCheckpoint: {
                CallContext->SetHasUncountableRejects();
                break;
            }
            default:
                break;
        }

        IncrementRetryNumberInRequest(Request);
        if (error.GetCode() == E_TIMEOUT) {
            IncrementRequestTimeout(Request, State.BackoffIncrement);
        }

        State.PostponeCycles = GetCycleCount();
        if (State.Throttling) {
            CallContext->Postpone(State.PostponeCycles);
        }

        return MakeError(S_OK);
    }

    return error;
}

template <typename TRequest, typename TResponse>
NProto::TError TRetryContext<TRequest, TResponse>::Retry(
    const NProto::TError& error,
    ISchedulerPtr scheduler,
    ITimerPtr timer,
    std::function<void(TRetryContextPtr<TRequest, TResponse>)> callback)
{
    const auto retry_err = SetUpForRetry(error);

    if (!HasError(retry_err)) {
        scheduler->Schedule(
            timer->Now() + State.Backoff,
            [retryCtx = this->shared_from_this(),
             callback = std::move(callback)]
            {
                const auto nowCycles = GetCycleCount();
                if (retryCtx->State.Throttling) {
                    retryCtx->CallContext->Advance(nowCycles);
                } else {
                    retryCtx->CallContext->AddTime(
                        EProcessingStage::Backoff,
                        CyclesToDurationSafe(
                            nowCycles - retryCtx->State.PostponeCycles));
                }
                callback(std::move(retryCtx));
            });
        return MakeError(S_OK);
    }
    return retry_err;
}

}   // namespace NCloud::NBlockStore::NClient::NRetryContext
