#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/endpoints/public.h>
#include <cloud/blockstore/libs/spdk/public.h>

#include <cloud/storage/core/libs/coroutine/public.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TNVMeEndpointConfig
{
    TString Nqn;
    TVector<TString> TransportIDs;
};

IEndpointListenerPtr CreateNVMeEndpointListener(
    NSpdk::ISpdkEnvPtr env,
    ILoggingServicePtr logging,
    IServerStatsPtr serverStats,
    TExecutorPtr executor,
    const TNVMeEndpointConfig& config);

////////////////////////////////////////////////////////////////////////////////

struct TSCSIEndpointConfig
{
    TString ListenAddress;
    ui32 ListenPort;

    TString InitiatorIqn;
    TString InitiatorMask;
};

IEndpointListenerPtr CreateSCSIEndpointListener(
    NSpdk::ISpdkEnvPtr env,
    ILoggingServicePtr logging,
    IServerStatsPtr serverStats,
    TExecutorPtr executor,
    const TSCSIEndpointConfig& config);

}   // namespace NCloud::NBlockStore::NServer
