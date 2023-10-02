#pragma once

#include "private.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/nbd/public.h>
#include <cloud/blockstore/libs/service/service.h>

#include <util/network/address.h>

namespace NCloud::NBlockStore::NBD {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
private:
    const TOptionsPtr Options;
    const TNetworkAddress ListenAddress;

    ILoggingServicePtr Logging;
    IMonitoringServicePtr Monitoring;

    IServerHandlerFactoryPtr ServerHandlerFactory;
    IBlockStorePtr Service;
    IServerPtr Server;

    IClientHandlerPtr ClientHandler;
    IClientPtr Client;
    IBlockStorePtr ClientEndpoint;

public:
    TBootstrap(TOptionsPtr options);
    ~TBootstrap();

    void Init();

    void Start();
    void Stop();

    IBlockStorePtr GetClient() const
    {
        return ClientEndpoint;
    }
};

}   // namespace NCloud::NBlockStore::NBD
