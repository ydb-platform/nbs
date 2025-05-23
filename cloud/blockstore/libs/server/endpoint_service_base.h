#pragma once

#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

namespace NCloud::NBlockStore::NServer {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

class TEndpointServiceBase: public IBlockStore
{
public:
#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
        {                                                                      \
            return CreateUnsupportedResponse<NProto::T##name##Response>(       \
                std::move(callContext),                                        \
                std::move(request));                                           \
        }                                                                      \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD

    // virtual void CancelInFlightRequests() = 0;

private:
    template <typename TResponse, typename TRequest>
    TFuture<TResponse> CreateUnsupportedResponse(
        TCallContextPtr callContext,
        std::shared_ptr<TRequest> request)
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        auto requestType = GetBlockStoreRequest<TRequest>();
        const auto& requestName = GetBlockStoreRequestName(requestType);

        return MakeFuture<TResponse>(TErrorResponse(
            E_FAIL,
            TStringBuilder()
                << "Unsupported endpoint request: " << requestName.Quote()));
    }
};

}   // namespace NCloud::NBlockStore::NServer
