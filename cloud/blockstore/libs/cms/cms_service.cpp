#include "cms_service.h"

#include <cloud/blockstore/libs/service/service_method.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCmsService final: public TBlockStoreImpl<TCmsService, IBlockStore>
{
private:
    const IBlockStorePtr Service;

public:
    explicit TCmsService(IBlockStorePtr service)
        : Service(std::move(service))
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
        return TMethod::Execute(
            Service.get(), std::move(ctx), std::move(request));
    }

    TFuture<NProto::TCmsActionResponse> CmsAction(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TCmsActionRequest> request) override
    {
        return Service->CmsAction(std::move(ctx), std::move(request));
    }

    TFuture<NProto::TExecuteActionResponse> ExecuteAction(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TExecuteActionRequest> request) override
    {
        TString action = request->GetAction();
        action.to_lower();
        if (action != "cms") {
            return Service->ExecuteAction(std::move(ctx), std::move(request));
        }

        auto cmsRequest = std::make_shared<NProto::TCmsActionRequest>();
        if (!google::protobuf::util::JsonStringToMessage(
                 request->GetInput(), cmsRequest.get())
                 .ok())
        {
            NProto::TExecuteActionResponse response;
            *response.MutableError() =
                MakeError(E_ARGUMENT, "Failed to parse input");
            return MakeFuture(std::move(response));
        }

        return CmsAction(std::move(ctx), std::move(cmsRequest))
            .Apply(
                [](const auto& future)
                {
                    const auto& cmsResponse = future.GetValue();
                    NProto::TExecuteActionResponse response;
                    *response.MutableError() = cmsResponse.GetError();
                    google::protobuf::util::MessageToJsonString(
                        cmsResponse, response.MutableOutput());
                    return response;
                });
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateCmsService(IBlockStorePtr service)
{
    return std::make_shared<TCmsService>(std::move(service));
}

}   // namespace NCloud::NBlockStore
