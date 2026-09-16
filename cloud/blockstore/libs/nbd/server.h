#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/incomplete_requests.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/common/affinity.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/size_literals.h>
#include <util/network/address.h>

namespace NCloud::NBlockStore::NBD {

////////////////////////////////////////////////////////////////////////////////

struct IServer
    : public IStartable
    , public IIncompleteRequestProvider
{
    virtual NThreading::TFuture<NProto::TError> StartEndpoint(
        TNetworkAddress listenAddress,
        IServerHandlerFactoryPtr handlerFactory) = 0;

    virtual NThreading::TFuture<NProto::TError> StopEndpoint(
        TNetworkAddress listenAddress) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TServerConfig
{
    size_t ThreadsCount = 1;
    bool LimiterEnabled = false;
    size_t MaxInFlightBytesPerThread = 128_MB;
    ui32 SocketAccessMode = S_IRGRP | S_IWGRP | S_IRUSR | S_IWUSR;
    TAffinity Affinity;
};

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    ILoggingServicePtr logging,
    const TServerConfig& serverConfig);

}   // namespace NCloud::NBlockStore::NBD
