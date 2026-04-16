#include "durable.h"

#include "config.h"

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/endpoint.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/helpers.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/ptr.h>
#include <util/string/builder.h>

namespace NCloud::NFileStore::NClient {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

bool IsRetriableError(const NProto::TError& error)
{
    EErrorKind errorKind = GetErrorKind(error);
    return errorKind == EErrorKind::ErrorRetriable;
}

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_DECLARE_METHOD(name, proto, ...)                             \
    struct T##name##Method                                                     \
    {                                                                          \
        using TRequest = NProto::T##proto##Request;                            \
        using TResponse = NProto::T##proto##Response;                          \
                                                                               \
        template <typename T, typename ...TArgs>                               \
        static TFuture<TResponse> Execute(T& client, TArgs&& ...args)          \
        {                                                                      \
            return client.proto(std::forward<TArgs>(args)...);                 \
        }                                                                      \
    };                                                                         \
// FILESTORE_DECLARE_METHOD

#define FILESTORE_DECLARE_METHOD_FS(name, ...) \
    FILESTORE_DECLARE_METHOD(name##Fs, name, __VA_ARGS__)

#define FILESTORE_DECLARE_METHOD_VHOST(name, ...) \
    FILESTORE_DECLARE_METHOD(name##Vhost, name, __VA_ARGS__)

FILESTORE_SERVICE(FILESTORE_DECLARE_METHOD_FS)
FILESTORE_DECLARE_METHOD_FS(ReadDataLocal)
FILESTORE_DECLARE_METHOD_FS(WriteDataLocal)
FILESTORE_ENDPOINT_SERVICE(FILESTORE_DECLARE_METHOD_VHOST)

#undef FILESTORE_DECLARE_METHOD
#undef FILESTORE_DECLARE_METHOD_FS
#undef FILESTORE_DECLARE_METHOD_VHOST

////////////////////////////////////////////////////////////////////////////////

template <typename T>
struct TRequestState
    : public TRetryState
    , public TAtomicRefCount<TRequestState<T>>
{
    TCallContextPtr CallContext;
    std::shared_ptr<typename T::TRequest> Request;
    TPromise<typename T::TResponse> Response;

    TRequestState(
            TCallContextPtr callContext,
            std::shared_ptr<typename T::TRequest> request)
        : CallContext(std::move(callContext))
        , Request(std::move(request))
        , Response(NewPromise<typename T::TResponse>())
    {}
};

template <typename T>
using TRequestStatePtr = TIntrusivePtr<TRequestState<T>>;

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void ExtractResponse(TFuture<T>& future, T& response)
{
    try {
        response = future.ExtractValue();
    } catch (const TServiceError& e) {
        auto& error = *response.MutableError();
        error.SetCode(e.GetCode());
        error.SetMessage(e.what());
    } catch (...) {
        auto& error = *response.MutableError();
        error.SetCode(E_FAIL);
        error.SetMessage(CurrentExceptionMessage());
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename TClient>
class TDurableClientBase
    : public TClient
    , public std::enable_shared_from_this<TDurableClientBase<TClient>>
{
protected:
    const ILoggingServicePtr Logging;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const IRetryPolicyPtr RetryPolicy;
    const std::shared_ptr<TClient> Client;

    TLog Log;

public:
    TDurableClientBase(
            ILoggingServicePtr logging,
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            IRetryPolicyPtr retryPolicy,
            std::shared_ptr<TClient> client)
        : Logging(std::move(logging))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , RetryPolicy(std::move(retryPolicy))
        , Client(std::move(client))
    {
        Log = Logging->CreateLog("NFS_CLIENT");
    }

    void Start() override
    {
        Client->Start();
    }

    void Stop() override
    {
        Client->Stop();
    }

    template <typename T>
    void ExecuteRequest(TRequestStatePtr<T> state)
    {
        T::Execute(*Client, state->CallContext, state->Request).Subscribe(
            [state = std::move(state),
             weakPtr = this->weak_from_this()] (
                const auto& future) mutable
            {
                if (auto self = weakPtr.lock()) {
                    self->HandleResponse(std::move(state), future);
                } else {
                    state->Response.SetValue(
                        TErrorResponse(E_INVALID_STATE, "request was cancelled"));
                }
            });
    }

private:
    template <typename T>
    void HandleResponse(
        TRequestStatePtr<T> state,
        TFuture<typename T::TResponse> future)
    {
        typename T::TResponse response;
        ExtractResponse(future, response);

        if (HasError(response)) {
            const auto& error = response.GetError();
            if (RetryPolicy->ShouldRetry(*state, error)) {
                // retry request
                ++state->Retries;

                STORAGE_WARN(GetRequestInfo(*state->Request)
                    << " #" << state->CallContext->RequestId
                    << " retry request"
                    << " (retries: " << state->Retries
                    << ", timeout: " << FormatDuration(state->Backoff)
                    << ", error: " << FormatError(error)
                    << ")");

                Scheduler->Schedule(
                    Timer->Now() + state->Backoff,
                    [state, weakPtr = this->weak_from_this()] {
                        if (auto self = weakPtr.lock()) {
                            self->ExecuteRequest(state);
                        } else {
                            state->Response.SetValue(
                                TErrorResponse(E_INVALID_STATE, "request was cancelled"));
                        }
                    });
                return;
            } else if (IsRetriableError(error)) {
                auto& error = *response.MutableError();
                error.SetCode(E_RETRY_TIMEOUT);
                error.SetMessage(TStringBuilder()
                    << "Retry timeout "
                    << "(" << error.GetCode() << "). "
                    << error.GetMessage());
            }
        } else {
            // log successful request
            if (state->Retries) {
                auto duration = TInstant::Now() - state->Started;
                STORAGE_INFO(GetRequestInfo(*state->Request)
                    << " #" << state->CallContext->RequestId
                    << " request completed"
                    << " (retries: " << state->Retries
                    << ", duration: " << FormatDuration(duration)
                    << ")");
            }
        }

        try {
            state->Response.SetValue(std::move(response));
        } catch (...) {
            STORAGE_ERROR(GetRequestInfo(*state->Request)
                << " #" << state->CallContext->RequestId
                << " exception in callback: " << CurrentExceptionMessage());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDurableFileStoreClient final
    : public TDurableClientBase<IFileStoreService>
{
    using TDurableClientBase::TDurableClientBase;

#define FILESTORE_IMPLEMENT_METHOD(name, ...)                                  \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request)  override           \
    {                                                                          \
        auto state = MakeIntrusive<TRequestState<T##name##Fs##Method>>(        \
            std::move(callContext),                                            \
            std::move(request));                                               \
                                                                               \
        ExecuteRequest(state);                                                 \
        return state->Response;                                                \
    }                                                                          \
//  FILESTORE_IMPLEMENT_METHOD

    FILESTORE_SERVICE(FILESTORE_IMPLEMENT_METHOD);
    FILESTORE_IMPLEMENT_METHOD(ReadDataLocal);
    FILESTORE_IMPLEMENT_METHOD(WriteDataLocal);

#undef FILESTORE_IMPLEMENT_METHOD

    void GetSessionEventsStream(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TGetSessionEventsRequest> request,
        IResponseHandlerPtr<NProto::TGetSessionEventsResponse> responseHandler) override
    {
        Client->GetSessionEventsStream(
            std::move(callContext),
            std::move(request),
            std::move(responseHandler));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDurableEndpointManagerClient final
    : public TDurableClientBase<IEndpointManager>
{
    using TDurableClientBase::TDurableClientBase;

    void Drain() override
    {
        Client->Drain();
    }

    NThreading::TFuture<void> RestoreEndpoints() override
    {
        return Client->RestoreEndpoints();
    }

#define FILESTORE_IMPLEMENT_METHOD(name, ...)                                  \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request)  override           \
    {                                                                          \
        auto state = MakeIntrusive<TRequestState<T##name##Vhost##Method>>(     \
            std::move(callContext),                                            \
            std::move(request));                                               \
                                                                               \
        ExecuteRequest(state);                                                 \
        return state->Response;                                                \
    }                                                                          \
//  FILESTORE_IMPLEMENT_METHOD

    FILESTORE_ENDPOINT_SERVICE(FILESTORE_IMPLEMENT_METHOD);

#undef FILESTORE_IMPLEMENT_METHOD
};

////////////////////////////////////////////////////////////////////////////////

struct TRetryPolicy final
    : public IRetryPolicy
{
    TClientConfigPtr Config;

    explicit TRetryPolicy(TClientConfigPtr config)
        : Config(std::move(config))
    {}

    bool ShouldRetry(TRetryState& state, const NProto::TError& error) override
    {
        bool isRetriable = IsRetriableError(error);
        if (!isRetriable ||
            TInstant::Now() - state.Started >= Config->GetRetryTimeout())
        {
            return false;
        }

        if (HasProtoFlag(error.GetFlags(), NProto::EF_INSTANT_RETRIABLE) &&
            !state.DoneInstantRetry)
        {
            state.Backoff = TDuration::Zero();
            state.DoneInstantRetry = true;
            return true;
        }

        const auto newRetryTimeout =
            state.RetryTimeout + Config->GetRetryTimeoutIncrement();
        state.Backoff = newRetryTimeout;
        if (IsConnectionError(error) &&
            state.Backoff > Config->GetConnectionErrorMaxRetryTimeout())
        {
            state.Backoff = Config->GetConnectionErrorMaxRetryTimeout();
            return true;
        }

        state.RetryTimeout = newRetryTimeout;
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IRetryPolicyPtr CreateRetryPolicy(TClientConfigPtr config)
{
    return std::make_shared<TRetryPolicy>(std::move(config));
}

IFileStoreServicePtr CreateDurableClient(
    ILoggingServicePtr logging,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    IRetryPolicyPtr retryPolicy,
    IFileStoreServicePtr client)
{
    return std::make_shared<TDurableFileStoreClient>(
        std::move(logging),
        std::move(timer),
        std::move(scheduler),
        std::move(retryPolicy),
        std::move(client));
}

IEndpointManagerPtr CreateDurableClient(
    ILoggingServicePtr logging,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    IRetryPolicyPtr retryPolicy,
    IEndpointManagerPtr client)
{
    return std::make_shared<TDurableEndpointManagerClient>(
        std::move(logging),
        std::move(timer),
        std::move(scheduler),
        std::move(retryPolicy),
        std::move(client));
}

}   // namespace NCloud::NFileStore::NClient
