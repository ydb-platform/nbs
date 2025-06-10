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

TExternalEndpointFactory CreateDefaultExternalEndpointFactory(
    ILoggingServicePtr logging,
    IServerStatsPtr serverStats,
    TExecutorPtr executor,
    TString binaryPath);

struct TCreateExternalVhostEndpointListenerParams
{
    ILoggingServicePtr Logging;
    IServerStatsPtr ServerStats;
    TExecutorPtr Executor;
    TString LocalAgentId;
    ui32 SocketAccessMode = 0;
    TDuration VhostServerTimeoutAfterParentExit;
    bool IsAlignedDataEnabled = false;
    bool IsNonDefaultLocalSSDBlockSizeAlertEnabled = true;
    IEndpointListenerPtr FallbackListener;
    TExternalEndpointFactory Factory;
};

IEndpointListenerPtr CreateExternalVhostEndpointListener(
    TCreateExternalVhostEndpointListenerParams params);

}   // namespace NCloud::NBlockStore::NServer
