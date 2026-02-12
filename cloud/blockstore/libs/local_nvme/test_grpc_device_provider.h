#pragma once

#include "generic_grpc_device_provider.h"

#include <cloud/blockstore/tools/testing/infra-device-provider/protos/infra.grpc.pb.h>
#include <cloud/blockstore/tools/testing/infra-device-provider/protos/infra.pb.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TInfraServiceTrait
{
    using TService = NTest::NProto::TInfraService;
    using TListDevicesRequest = NTest::NProto::TListDevicesRequest;
    using TListDevicesResponse = NTest::NProto::TListDevicesResponse;

    static auto AsyncListDevices(
        TService::Stub& service,
        grpc::ClientContext* clientContext,
        const TListDevicesRequest& request,
        grpc::CompletionQueue* cq)
        -> std::unique_ptr<
            grpc::ClientAsyncResponseReader<TListDevicesResponse>>
    {
        return service.AsyncListDevices(clientContext, request, cq);
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

using TTestGrpcDeviceProvider = TGenericGrpcDeviceProvider<TInfraServiceTrait>;

}   // namespace NCloud::NBlockStore
