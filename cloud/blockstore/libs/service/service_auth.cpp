#include "service_auth.h"

#include "auth_provider.h"
#include "context.h"
#include "service.h"

#include <cloud/blockstore/config/server.pb.h>
#include <cloud/blockstore/libs/service/service_method.h>

#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TAuthService final: public TBlockStoreImpl<TAuthService, IBlockStore>
{
private:
    const IBlockStorePtr Service;
    const IAuthProviderPtr AuthProvider;

public:
    TAuthService(IBlockStorePtr service, IAuthProviderPtr authProvider)
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

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr ctx,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        const auto& headers = request->GetHeaders();
        const auto& internal = headers.GetInternal();
        auto permissions = GetRequestPermissions(*request);

        bool needAuth =
            AuthProvider->NeedAuth(internal.GetRequestSource(), permissions);

        if (!needAuth) {
            return TMethod::Execute(
                Service.get(),
                std::move(ctx),
                std::move(request));
        }

        auto authResponse = AuthProvider->CheckRequest(
            ctx,
            std::move(permissions),
            internal.GetAuthToken(),
            TMethod::BlockStoreRequest,
            TDuration::MilliSeconds(headers.GetRequestTimeout()),
            GetDiskId(*request));

        return HandleAuthResponse<TMethod>(
            std::move(authResponse),
            std::move(ctx),
            std::move(request));
    }

private:
    template <typename TMethod>
    TFuture<typename TMethod::TResponse> HandleAuthResponse(
        TFuture<NProto::TError> authResponse,
        TCallContextPtr ctx,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        auto promise = NewPromise<typename TMethod::TResponse>();
        auto result = promise.GetFuture();

        authResponse.Subscribe(
            [service = Service,
             promise = std::move(promise),
             request = std::move(request),
             ctx = std::move(ctx)]   //
            (const TFuture<NProto::TError>& future) mutable
            {
                const auto& error = future.GetValue();

                if (HasError(error)) {
                    promise.SetValue(TErrorResponse(error));
                    return;
                }

                TMethod::Execute(
                    service.get(),
                    std::move(ctx),
                    std::move(request))
                    .Subscribe(
                        [promise = std::move(promise)]   //
                        (const TFuture<typename TMethod::TResponse>&
                             future) mutable
                        {
                            promise.SetValue(future.GetValue());   //
                        });
            });

        return result;
    }
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
