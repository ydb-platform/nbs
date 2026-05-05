#include "service_proxy.h"

#include <cloud/blockstore/libs/local_nvme/service.h>
#include <cloud/blockstore/libs/service/service_method.h>
#include <cloud/blockstore/public/api/protos/local_nvme.pb.h>

namespace NCloud::NBlockStore {

namespace {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

class TLocalNVMeServiceProxy
    : public TBlockStoreImpl<TLocalNVMeServiceProxy, IBlockStore>
{
private:
    const IBlockStorePtr BlockStoreService;
    const ILocalNVMeServicePtr LocalNVMeService;

public:
    TLocalNVMeServiceProxy(
        IBlockStorePtr blockStoreService,
        ILocalNVMeServicePtr localNVMeService)
        : BlockStoreService(std::move(blockStoreService))
        , LocalNVMeService(std::move(localNVMeService))
    {}

    void Start() override
    {
        BlockStoreService->Start();
    }

    void Stop() override
    {
        BlockStoreService->Stop();
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return BlockStoreService->AllocateBuffer(bytesCount);
    }

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr ctx,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        return TMethod::Execute(
            BlockStoreService.get(),
            std::move(ctx),
            std::move(request));
    }

    TFuture<NProto::TListNVMeDevicesResponse> ListNVMeDevices(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TListNVMeDevicesRequest> request) override
    {
        Y_UNUSED(ctx);
        Y_UNUSED(request);

        return LocalNVMeService->ListNVMeDevices().Apply(
            [](const auto& future)
            {
                auto [devices, error] = future.GetValue();

                NProto::TListNVMeDevicesResponse response;
                response.MutableError()->CopyFrom(error);
                for (const auto& src: devices) {
                    auto* dst = response.AddDevices();
                    dst->SetSerialNumber(src.GetSerialNumber());
                    dst->SetPCIAddress(src.GetPCIAddress());
                    if (src.HasIOMMUGroup()) {
                        dst->SetIOMMUGroup(src.GetIOMMUGroup());
                    }
                    if (src.HasVfioDevName()) {
                        dst->SetVfioDevName(src.GetVfioDevName());
                    }
                    if (src.HasNumaNode()) {
                        dst->SetNumaNode(src.GetNumaNode());
                    }
                }

                return response;
            });
    }

    TFuture<NProto::TAcquireNVMeDeviceResponse> AcquireNVMeDevice(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TAcquireNVMeDeviceRequest> request) override
    {
        Y_UNUSED(ctx);

        return LocalNVMeService->AcquireNVMeDevice(request->GetSerialNumber())
            .Apply(
                [](const auto& future)
                {
                    NProto::TAcquireNVMeDeviceResponse response;
                    response.MutableError()->CopyFrom(future.GetValue());
                    return response;
                });
    }

    TFuture<NProto::TReleaseNVMeDeviceResponse> ReleaseNVMeDevice(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TReleaseNVMeDeviceRequest> request) override
    {
        Y_UNUSED(ctx);

        return LocalNVMeService->ReleaseNVMeDevice(request->GetSerialNumber())
            .Apply(
                [](const auto& future)
                {
                    NProto::TReleaseNVMeDeviceResponse response;
                    response.MutableError()->CopyFrom(future.GetValue());
                    return response;
                });
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateLocalNVMeServiceProxy(
    IBlockStorePtr blockStoreService,
    ILocalNVMeServicePtr localNVMeService)
{
    return std::make_shared<TLocalNVMeServiceProxy>(
        std::move(blockStoreService),
        std::move(localNVMeService));
}

}   // namespace NCloud::NBlockStore
