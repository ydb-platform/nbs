#include "service_filtered.h"

#include "context.h"
#include "service.h"

#include <cloud/blockstore/libs/service/service_method.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TFilteredService final
    : public TBlockStoreImpl<TFilteredService, IBlockStore>
{
private:
    const IBlockStorePtr Service;
    const TSet<EBlockStoreRequest> AllowedRequests;
    const TErrorResponse ErrorResponse;

public:
    TFilteredService(
            IBlockStorePtr service,
            TSet<EBlockStoreRequest> allowedRequests)
        : Service(std::move(service))
        , AllowedRequests(std::move(allowedRequests))
        , ErrorResponse(E_UNAUTHORIZED, "forbidden request")
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
        if (!IsRequestAllowed(TMethod::BlockStoreRequest)) {
            return MakeFuture<typename TMethod::TResponse>(ErrorResponse);
        }
        return TMethod::Execute(
            Service.get(),
            std::move(ctx),
            std::move(request));
    }

private:
    bool IsRequestAllowed(EBlockStoreRequest request) const
    {
        return AllowedRequests.find(request) != AllowedRequests.end();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateFilteredService(
    IBlockStorePtr service,
    TSet<EBlockStoreRequest> allowedRequests)
{
    return std::make_shared<TFilteredService>(
        std::move(service),
        std::move(allowedRequests));
}

}   // namespace NCloud::NBlockStore
