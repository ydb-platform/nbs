#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/endpoints/public.h>
#include <cloud/blockstore/libs/vhost/public.h>
#include <cloud/storage/core/libs/coroutine/public.h>

#include <cloud/storage/core/protos/error.pb.h>

#include <library/cpp/threading/future/fwd.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct IExternalEndpoint
{
    virtual ~IExternalEndpoint() = default;
    virtual void Start() = 0;
    virtual NThreading::TFuture<NProto::TError> Stop() = 0;
};

using IExternalEndpointPtr = std::shared_ptr<IExternalEndpoint>;
using TExternalEndpointFactory = std::function<IExternalEndpointPtr (
    const TString& clientId,
    const TString& diskId,
    TVector<TString> args,
    TVector<TString> cgroups
)>;

////////////////////////////////////////////////////////////////////////////////

IEndpointListenerPtr CreateExternalVhostEndpointListener(
    ILoggingServicePtr logging,
    IServerStatsPtr serverStats,
    TExecutorPtr executor,
    TString binaryPath,
    TString localAgentId,
    ui32 socketAccessMode,
    IEndpointListenerPtr fallbackListener);

IEndpointListenerPtr CreateExternalVhostEndpointListener(
    ILoggingServicePtr logging,
    IServerStatsPtr serverStats,
    TExecutorPtr executor,
    TString localAgentId,
    ui32 socketAccessMode,
    IEndpointListenerPtr fallbackListener,
    TExternalEndpointFactory factory);

}   // namespace NCloud::NBlockStore::NServer
