#include "test_grpc_device_provider.h"

#include "generic_grpc_device_provider.h"

#include <cloud/blockstore/tools/testing/infra-device-provider/protos/infra.grpc.pb.h>
#include <cloud/blockstore/tools/testing/infra-device-provider/protos/infra.pb.h>

namespace NCloud::NBlockStore {

namespace {

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

    static auto GetResult(TListDevicesResponse response)
        -> TVector<NProto::TNVMeDevice>
    {
        TVector<NProto::TNVMeDevice> devices;
        devices.reserve(response.DevicesSize());

        for (auto& src: *response.MutableDevices()) {
            auto& dst = devices.emplace_back();
            dst.SetPCIAddress(src.GetPCIeAddress());
            dst.SetSerialNumber(src.GetSerialNumber());
        }

        return devices;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestGrpcDeviceProvider
    : TGenericGrpcDeviceProvider<TTestGrpcDeviceProvider, TTestServiceTrait>
{
    const TString OwnerId;

    TTestGrpcDeviceProvider(
        ILoggingServicePtr logging,
        TString socketPath,
        TString ownerId)
        : TGenericGrpcDeviceProvider(std::move(logging), std::move(socketPath))
        , OwnerId(std::move(ownerId))
    {}

    void PrepareRequest(NTest::NProto::TListDevicesRequest& request)
    {
        request.SetOwnerId(OwnerId);
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
