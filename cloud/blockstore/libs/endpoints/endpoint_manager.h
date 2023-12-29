#pragma once

#include "public.h"

#include <cloud/blockstore/public/api/protos/endpoints.pb.h>

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/storage/core/libs/coroutine/public.h>

#include <library/cpp/threading/future/future.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct IEndpointManager
{
    virtual ~IEndpointManager() = default;

    virtual NThreading::TFuture<NProto::TStartEndpointResponse> StartEndpoint(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TStartEndpointRequest> request) = 0;

    virtual NThreading::TFuture<NProto::TStopEndpointResponse> StopEndpoint(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TStopEndpointRequest> request) = 0;

    virtual NThreading::TFuture<NProto::TListEndpointsResponse> ListEndpoints(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TListEndpointsRequest> request) = 0;

    virtual NThreading::TFuture<NProto::TDescribeEndpointResponse> DescribeEndpoint(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TDescribeEndpointRequest> request) = 0;

    virtual NThreading::TFuture<NProto::TRefreshEndpointResponse> RefreshEndpoint(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TRefreshEndpointRequest> request) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IEndpointManagerPtr CreateEndpointManager(
    ILoggingServicePtr logging,
    IServerStatsPtr serverStats,
    TExecutorPtr executor,
    IEndpointEventProxyPtr eventProxy,
    ISessionManagerPtr sessionManager,
    THashMap<NProto::EClientIpcType, IEndpointListenerPtr> listeners,
    TString nbdSocketSuffix);

bool IsSameStartEndpointRequests(
    const NProto::TStartEndpointRequest& left,
    const NProto::TStartEndpointRequest& right);

}   // namespace NCloud::NBlockStore::NServer
