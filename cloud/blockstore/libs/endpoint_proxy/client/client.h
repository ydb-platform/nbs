#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/public/api/protos/endpoints.pb.h>

#include <cloud/storage/core/libs/common/startable.h>

#include <library/cpp/threading/future/future.h>

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

struct IEndpointProxyClient: IStartable
{
    template <typename T>
    using TFuture = NThreading::TFuture<T>;

    virtual TFuture<NProto::TStartProxyEndpointResponse> StartProxyEndpoint(
        std::shared_ptr<NProto::TStartProxyEndpointRequest> request) = 0;

    virtual TFuture<NProto::TStopProxyEndpointResponse> StopProxyEndpoint(
        std::shared_ptr<NProto::TStopProxyEndpointRequest> request) = 0;

    virtual TFuture<NProto::TListProxyEndpointsResponse> ListProxyEndpoints(
        std::shared_ptr<NProto::TListProxyEndpointsRequest> request) = 0;

    virtual TFuture<NProto::TResizeProxyDeviceResponse> ResizeProxyDevice(
        std::shared_ptr<NProto::TResizeProxyDeviceRequest> request) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TEndpointProxyClientConfig
{
    struct TRetryPolicy
    {
        TDuration Backoff = TDuration::Seconds(1);
        TDuration TotalTimeout = TDuration::Days(1);
        TDuration UnixSocketConnectTimeout = TDuration::Seconds(5);
    };

    TString Host;
    ui16 Port;
    ui16 SecurePort;
    TString RootCertsFile;
    TString UnixSocketPath;
    TRetryPolicy RetryPolicy;
};

IEndpointProxyClientPtr CreateClient(
    TEndpointProxyClientConfig config,
    ISchedulerPtr scheduler,
    ITimerPtr timer,
    ILoggingServicePtr logging);

}   // namespace NCloud::NBlockStore::NClient
