#include "service_auth.h"

#include <cloud/filestore/libs/service/auth_provider.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/endpoint.h>
#include <cloud/filestore/libs/service/filestore.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/hash.h>

namespace NCloud::NFileStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TAuthService final
    : public IEndpointManager
{
private:
    const IEndpointManagerPtr Service;
    const IAuthProviderPtr AuthProvider;

public:
    TAuthService(
            IEndpointManagerPtr service,
            IAuthProviderPtr authProvider)
        : Service(std::move(service))
        , AuthProvider(std::move(authProvider))
    {}

    void Start() override
    {
        Service->Start();
    }

    void Stop() override
    {
        Service->Stop();
    }

    void Drain() override
    {
        Service->Drain();
    }

    NThreading::TFuture<void> RestoreEndpoints() override
    {
        return Service->RestoreEndpoints();
    }

#define FILESTORE_IMPLEMENT_METHOD(name, ...)                                  \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr ctx,                                                   \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        using TRequest = NProto::T##name##Request;                             \
        using TResponse = NProto::T##name##Response;                           \
        return ExecuteRequest<TRequest, TResponse>(                            \
            std::move(ctx),                                                    \
            std::move(request));                                               \
    }                                                                          \
// FILESTORE_IMPLEMENT_METHOD

    FILESTORE_ENDPOINT_SERVICE(FILESTORE_IMPLEMENT_METHOD)

#undef FILESTORE_IMPLEMENT_METHOD

private:
    template <typename TRequest, typename TResponse>
    TFuture<TResponse> ExecuteRequest(
        TCallContextPtr ctx,
        std::shared_ptr<TRequest> request)
    {
        const auto& headers = request->GetHeaders();
        const auto& internal = headers.GetInternal();
        auto permissions = GetRequestPermissions(*request, {});

        bool needAuth = AuthProvider->NeedAuth(
            internal.GetRequestSource(),
            permissions);

        if (!needAuth) {
            return ExecuteServiceRequest(std::move(ctx), std::move(request));
        }

        auto authResponse = AuthProvider->CheckRequest(
            ctx,
            std::move(permissions),
            internal.GetAuthToken(),
            TDuration::MilliSeconds(headers.GetRequestTimeout()));

        return HandleAuthResponse<TRequest, TResponse>(
            std::move(authResponse),
            std::move(ctx),
            std::move(request));
    }

    template <typename TRequest, typename TResponse>
    TFuture<TResponse> HandleAuthResponse(
        TFuture<NProto::TError> authResponse,
        TCallContextPtr ctx,
        std::shared_ptr<TRequest> request)
    {
        auto promise = NewPromise<TResponse>();

        authResponse.Subscribe(
            [=, this, request = std::move(request), ctx = std::move(ctx)] (const auto& future) mutable {
                const auto& error = future.GetValue();

                if (HasError(error)) {
                    promise.SetValue(TErrorResponse(error));
                    return;
                }

                ExecuteServiceRequest(
                    std::move(ctx),
                    std::move(request)
                ).Subscribe(
                    [=] (const auto& future) mutable {
                        promise.SetValue(future.GetValue());
                    });
            });

        return promise.GetFuture();
    }

#define FILESTORE_IMPLEMENT_METHOD(name, ...)                                  \
    TFuture<NProto::T##name##Response> ExecuteServiceRequest(                  \
        TCallContextPtr ctx,                                                   \
        std::shared_ptr<NProto::T##name##Request> request)                     \
    {                                                                          \
        return Service->name(std::move(ctx), std::move(request));              \
    }                                                                          \
// FILESTORE_IMPLEMENT_METHOD

    FILESTORE_ENDPOINT_SERVICE(FILESTORE_IMPLEMENT_METHOD)

#undef FILESTORE_IMPLEMENT_METHOD
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IEndpointManagerPtr CreateAuthService(
    IEndpointManagerPtr service,
    IAuthProviderPtr authProvider)
{
    return std::make_shared<TAuthService>(
        std::move(service),
        std::move(authProvider));
}

}   // namespace NCloud::NFileStore
