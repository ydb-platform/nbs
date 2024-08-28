#include "client.h"
#include "device_factory.h"

#include <cloud/blockstore/libs/nbd/device.h>
#include <cloud/blockstore/public/api/protos/endpoints.pb.h>
#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestEndpointProxyClient: NClient::IEndpointProxyClient
{
    NProto::TStartProxyEndpointRequest StartRequest;
    NProto::TStopProxyEndpointRequest StopRequest;

    template <typename T>
    using TFuture = NThreading::TFuture<T>;
    template <typename T>
    using TPromise = NThreading::TPromise<T>;

    TPromise<NProto::TStartProxyEndpointResponse> StartResponse;
    TPromise<NProto::TStopProxyEndpointResponse> StopResponse;

    void CompleteStartProxyEndpoint(NProto::TError error = {})
    {
        NProto::TStartProxyEndpointResponse response;
        response.SetInternalUnixSocketPath(
            StartRequest.GetUnixSocketPath() + ".p");
        *response.MutableError() = std::move(error);
        StartResponse.SetValue(std::move(response));
    }

    void CompleteStopProxyEndpoint(NProto::TError error = {})
    {
        NProto::TStopProxyEndpointResponse response;
        response.SetInternalUnixSocketPath(
            StopRequest.GetUnixSocketPath() + ".p");
        *response.MutableError() = std::move(error);
        StopResponse.SetValue(std::move(response));
    }

    TFuture<NProto::TStartProxyEndpointResponse> StartProxyEndpoint(
        std::shared_ptr<NProto::TStartProxyEndpointRequest> request) override
    {
        StartRequest = *request;
        StartResponse =
            NThreading::NewPromise<NProto::TStartProxyEndpointResponse>();
        return StartResponse;
    }

    TFuture<NProto::TStopProxyEndpointResponse> StopProxyEndpoint(
        std::shared_ptr<NProto::TStopProxyEndpointRequest> request) override
    {
        StopRequest = *request;
        StopResponse =
            NThreading::NewPromise<NProto::TStopProxyEndpointResponse>();
        return StopResponse;
    }

    TFuture<NProto::TListProxyEndpointsResponse> ListProxyEndpoints(
        std::shared_ptr<NProto::TListProxyEndpointsRequest> request) override
    {
        Y_UNUSED(request);

        return NThreading::MakeFuture<NProto::TListProxyEndpointsResponse>(
            TErrorResponse(E_NOT_IMPLEMENTED));
    }

    TFuture<NProto::TResizeProxyDeviceResponse> ResizeProxyDevice(
        std::shared_ptr<NProto::TResizeProxyDeviceRequest> request) override
    {
        Y_UNUSED(request);

        return NThreading::MakeFuture<NProto::TResizeProxyDeviceResponse>(
            TErrorResponse(E_NOT_IMPLEMENTED));
    }

    void Start() override
    {}

    void Stop() override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TProxyDeviceFactoryTest)
{
    Y_UNIT_TEST(ShouldCreateDevice)
    {
        const TString socketPath = "/some/socket";
        const TString deviceName = "/dev/nbdX";
        const ui64 blockCount = 1024;
        const ui32 blockSize = 4096;
        const ui32 sectorSize = 512;

        auto client = std::make_shared<TTestEndpointProxyClient>();
        auto factory = CreateProxyDeviceFactory({sectorSize}, client);

        auto device = factory->Create(
            TNetworkAddress(TUnixSocketPath(socketPath)),
            deviceName,
            blockCount,
            blockSize);
        UNIT_ASSERT_VALUES_EQUAL("", client->StartRequest.GetUnixSocketPath());
        UNIT_ASSERT_VALUES_EQUAL("", client->StopRequest.GetUnixSocketPath());

        auto startFuture = device->Start();
        UNIT_ASSERT(!startFuture.HasValue());
        client->CompleteStartProxyEndpoint();
        UNIT_ASSERT(startFuture.HasValue());
        auto error = startFuture.GetValue();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            error.GetCode(),
            error.GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(
            socketPath,
            client->StartRequest.GetUnixSocketPath());
        UNIT_ASSERT_VALUES_EQUAL(
            deviceName,
            client->StartRequest.GetNbdDevice());
        UNIT_ASSERT_VALUES_EQUAL(
            blockCount * blockSize / sectorSize,
            client->StartRequest.GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            sectorSize,
            client->StartRequest.GetBlockSize());
        UNIT_ASSERT_VALUES_EQUAL("", client->StopRequest.GetUnixSocketPath());

        auto stopFuture = device->Stop(true /* delete device */);
        UNIT_ASSERT(!stopFuture.HasValue());
        client->CompleteStopProxyEndpoint();
        UNIT_ASSERT(stopFuture.HasValue());
        error = stopFuture.GetValue();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            error.GetCode(),
            error.GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(
            socketPath,
            client->StopRequest.GetUnixSocketPath());
    }

    Y_UNIT_TEST(ShouldHandleClientErrors)
    {
        const TString socketPath = "/some/socket";
        const TString deviceName = "/dev/nbdX";
        const ui64 blockCount = 1024;
        const ui32 blockSize = 4096;
        const ui32 sectorSize = 512;

        auto client = std::make_shared<TTestEndpointProxyClient>();
        auto factory = CreateProxyDeviceFactory({sectorSize}, client);

        auto device = factory->Create(
            TNetworkAddress(TUnixSocketPath(socketPath)),
            deviceName,
            blockCount,
            blockSize);
        UNIT_ASSERT_VALUES_EQUAL("", client->StartRequest.GetUnixSocketPath());
        UNIT_ASSERT_VALUES_EQUAL("", client->StopRequest.GetUnixSocketPath());

        auto startFuture = device->Start();
        UNIT_ASSERT(!startFuture.HasValue());
        client->CompleteStartProxyEndpoint(MakeError(E_FAIL));
        UNIT_ASSERT(startFuture.HasValue());
        auto error = startFuture.GetValue();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_FAIL,
            error.GetCode(),
            error.GetMessage());

        auto stopFuture = device->Stop(true /* delete device */);
        UNIT_ASSERT(!stopFuture.HasValue());
        client->CompleteStopProxyEndpoint(MakeError(E_FAIL));
        UNIT_ASSERT(stopFuture.HasValue());
        error = stopFuture.GetValue();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_FAIL,
            error.GetCode(),
            error.GetMessage());

        stopFuture = device->Stop(false /* delete device */);
        UNIT_ASSERT(stopFuture.HasValue());
        error = stopFuture.GetValue();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            error.GetCode(),
            error.GetMessage());
    }
}

}   // namespace NCloud::NBlockStore
