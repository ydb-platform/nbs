#include "service_error_transform.h"

#include "context.h"
#include "service.h"

#include <util/string/builder.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TErrorTransformService final
    : public IBlockStore
{
    using TErrorTransformMap = TMap<EErrorKind, EWellKnownResultCodes>;

private:
    const IBlockStorePtr Service;
    const std::shared_ptr<TErrorTransformMap> ErrorTransformMap;

public:
    TErrorTransformService(
            IBlockStorePtr service,
            TMap<EErrorKind, EWellKnownResultCodes> errorTransformMap)
        : Service(std::move(service))
        , ErrorTransformMap(std::make_shared<TErrorTransformMap>(
            std::move(errorTransformMap)))
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
        auto result = Service->name(std::move(ctx), std::move(request));       \
        return result.Apply([map = ErrorTransformMap] (const auto& future) {   \
            return HandleResponse(future, *map);                               \
        });                                                                    \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD

private:
    template <typename TResponse>
    static TFuture<TResponse> HandleResponse(
        const TFuture<TResponse>& future,
        const TErrorTransformMap& errorTransformMap)
    {
        const auto& response = future.GetValue();

        auto it = errorTransformMap.find(GetErrorKind(response.GetError()));
        if (it == errorTransformMap.end()) {
            return future;
        }

        auto transformedResponse = response;
        auto& error = *transformedResponse.MutableError();
        auto errorStr = FormatError(error);

        error.SetCode(it->second);
        error.SetMessage(
            TStringBuilder() << "Error was transformed from: " << errorStr);

        return MakeFuture<TResponse>(std::move(transformedResponse));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateErrorTransformService(
    IBlockStorePtr service,
    TMap<EErrorKind, EWellKnownResultCodes> errorTransformMap)
{
    return std::make_shared<TErrorTransformService>(
        std::move(service),
        std::move(errorTransformMap));
}

}   // namespace NCloud::NBlockStore
