#include "service_filtered.h"

#include "context.h"
#include "service.h"

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TFilteredService final
    : public IBlockStore
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

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr ctx,                                                   \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        if (!IsRequestAllowed(EBlockStoreRequest::name)) {                     \
            return MakeFuture<NProto::T##name##Response>(ErrorResponse);       \
        }                                                                      \
        return Service->name(std::move(ctx), std::move(request));              \
    }
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD

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
