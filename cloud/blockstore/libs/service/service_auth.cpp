#include "service_auth.h"

#include "auth_provider.h"
#include "context.h"
#include "service.h"

#include <cloud/storage/core/libs/common/error.h>

#include <cloud/blockstore/config/server.pb.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TAuthService final
    : public IBlockStore
{
private:
    const IBlockStorePtr Service;
    const IAuthProviderPtr AuthProvider;

public:
    TAuthService(
            IBlockStorePtr service,
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

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Service->AllocateBuffer(bytesCount);
    }

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
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
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD

private:
    template <typename TRequest, typename TResponse>
    TFuture<TResponse> ExecuteRequest(
        TCallContextPtr ctx,
        std::shared_ptr<TRequest> request)
    {
        const auto& headers = request->GetHeaders();
        const auto& internal = headers.GetInternal();
        auto permissions = GetRequestPermissions(*request);

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
            GetBlockStoreRequest<TRequest>(),
            TDuration::MilliSeconds(headers.GetRequestTimeout()),
            GetDiskId(*request));

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

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> ExecuteServiceRequest(                  \
        TCallContextPtr ctx,                                                   \
        std::shared_ptr<NProto::T##name##Request> request)                     \
    {                                                                          \
        return Service->name(std::move(ctx), std::move(request));              \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateAuthService(
    IBlockStorePtr service,
    IAuthProviderPtr authProvider)
{
    return std::make_shared<TAuthService>(
        std::move(service),
        std::move(authProvider));
}

}   // namespace NCloud::NBlockStore
