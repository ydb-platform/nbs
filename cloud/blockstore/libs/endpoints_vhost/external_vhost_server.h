#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/endpoints/public.h>
#include <cloud/blockstore/libs/server/public.h>
#include <cloud/blockstore/libs/vhost/public.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/coroutine/public.h>

#include <library/cpp/threading/future/fwd.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct IExternalEndpoint
{
    virtual ~IExternalEndpoint() = default;
    virtual void PrepareToStart() = 0;
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
    TServerAppConfigPtr serverConfig,
    ILoggingServicePtr logging,
    IServerStatsPtr serverStats,
    TExecutorPtr executor,
    TString localAgentId,
    bool isAlignedDataEnabled,
    IEndpointListenerPtr fallbackListener);

IEndpointListenerPtr CreateExternalVhostEndpointListener(
    TServerAppConfigPtr serverConfig,
    ILoggingServicePtr logging,
    IServerStatsPtr serverStats,
    TExecutorPtr executor,
    TString localAgentId,
    bool isAlignedDataEnabled,
    IEndpointListenerPtr fallbackListener,
    TExternalEndpointFactory factory);

}   // namespace NCloud::NBlockStore::NServer
