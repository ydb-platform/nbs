#include "test_grpc_device_provider.h"

#include "device_provider.h"
#include "generic_inventory_service_client.h"

#include <cloud/blockstore/tools/testing/infra-device-provider/protos/infra.grpc.pb.h>
#include <cloud/blockstore/tools/testing/infra-device-provider/protos/infra.pb.h>

namespace NCloud::NBlockStore {

namespace {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

struct TTestServiceTrait
{
    using TService = NTest::NProto::TInfraService;
    using TListDevicesRequest = NTest::NProto::TListDevicesRequest;
    using TListDevicesResponse = NTest::NProto::TListDevicesResponse;

    static auto AsyncListDevices(
        TService::Stub& service,
        grpc::ClientContext& clientContext,
        const TListDevicesRequest& request,
        grpc::CompletionQueue& cq)
        -> std::unique_ptr<
            grpc::ClientAsyncResponseReader<TListDevicesResponse>>
    {
        return service.AsyncListDevices(&clientContext, request, &cq);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestGrpcDeviceProvider: ILocalNVMeDeviceProvider
{
    TGenericInventoryServiceClient<TTestServiceTrait> Client;
    const TString OwnerId;

    TTestGrpcDeviceProvider(
        ILoggingServicePtr logging,
        TString socketPath,
        TString ownerId)
        : Client(std::move(logging), std::move(socketPath))
        , OwnerId(std::move(ownerId))
    {}

    void Start() final
    {
        Client.Start();
    }

    void Stop() final
    {
        Client.Stop();
    }

    [[nodiscard]] auto ListNVMeDevices()
        -> TFuture<TVector<NProto::TNVMeDevice>> final
    {
        NTest::NProto::TListDevicesRequest request;
        request.SetOwnerId(OwnerId);

        return Client.ListDevices(std::move(request))
            .Apply(
                [](const auto& future)
                {
                    const NTest::NProto::TListDevicesResponse& response =
                        future.GetValue();

                    TVector<NProto::TNVMeDevice> devices;
                    devices.reserve(response.DevicesSize());

                    for (const auto& src: response.GetDevices()) {
                        auto& dst = devices.emplace_back();
                        dst.SetPCIAddress(src.GetPCIeAddress());
                        dst.SetSerialNumber(src.GetSerialNumber());
                    }

                    return devices;
                });
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

auto CreateTestGrpcDeviceProvider(ILoggingServicePtr logging, TStringBuf uri)
    -> ILocalNVMeDeviceProviderPtr
{
    uri.SkipPrefix("unix://");

    TStringBuf ownerId;
    TStringBuf socketPath;

    uri.Split("@", ownerId, socketPath);

    return std::make_shared<TTestGrpcDeviceProvider>(
        std::move(logging),
        ToString(socketPath),
        ToString(ownerId));
}

}   // namespace NCloud::NBlockStore
