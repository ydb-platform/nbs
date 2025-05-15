#include "service_auth.h"

#include "auth_provider.h"
#include "context.h"
#include "filestore.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/hash.h>

namespace NCloud::NFileStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TAuthService final
    : public IFileStoreService
{
private:
    const IFileStoreServicePtr Service;
    const IAuthProviderPtr AuthProvider;
    const TVector<TString> ActionsNoAuth;

public:
    TAuthService(
            IFileStoreServicePtr service,
            IAuthProviderPtr authProvider,
            TVector<TString> actionsNoAuth)
        : Service(std::move(service))
        , AuthProvider(std::move(authProvider))
        , ActionsNoAuth(std::move(actionsNoAuth))
    {}

    void Start() override
    {
        Service->Start();
    }

    void Stop() override
    {
        Service->Stop();
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

    FILESTORE_SERVICE(FILESTORE_IMPLEMENT_METHOD)
    FILESTORE_IMPLEMENT_METHOD(ReadDataLocal)
    FILESTORE_IMPLEMENT_METHOD(WriteDataLocal)

#undef FILESTORE_IMPLEMENT_METHOD

    void GetSessionEventsStream(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TGetSessionEventsRequest> request,
        IResponseHandlerPtr<NProto::TGetSessionEventsResponse> responseHandler) override
    {
        // TODO: support auth for this method
        Service->GetSessionEventsStream(
            std::move(callContext),
            std::move(request),
            std::move(responseHandler));
    }

private:
    template <typename TRequest, typename TResponse>
    TFuture<TResponse> ExecuteRequest(
        TCallContextPtr ctx,
        std::shared_ptr<TRequest> request)
    {
        const auto& headers = request->GetHeaders();
        const auto& internal = headers.GetInternal();
        auto permissions = GetRequestPermissions(*request, ActionsNoAuth);

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

    FILESTORE_SERVICE(FILESTORE_IMPLEMENT_METHOD)
    FILESTORE_IMPLEMENT_METHOD(ReadDataLocal)
    FILESTORE_IMPLEMENT_METHOD(WriteDataLocal)

#undef FILESTORE_IMPLEMENT_METHOD
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IFileStoreServicePtr CreateAuthService(
    IFileStoreServicePtr service,
    IAuthProviderPtr authProvider,
    const TVector<TString>& actionsNoAuth)
{
    return std::make_shared<TAuthService>(
        std::move(service),
        std::move(authProvider),
        actionsNoAuth);
}

}   // namespace NCloud::NFileStore
