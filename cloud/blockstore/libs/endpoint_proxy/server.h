#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>

#include <cloud/storage/core/libs/common/startable.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct IServer: public IStartable
{
};

////////////////////////////////////////////////////////////////////////////////

struct TServerConfig
{
    ui16 Port;
    ui16 SecurePort;
    TString RootCertsFile;
    TString KeyFile;
    TString CertFile;
    TString SocketsDir;

    TServerConfig(
            ui16 port,
            ui16 securePort,
            TString rootCertsFile,
            TString keyFile,
            TString certFile,
            TString socketsDir)
        : Port(port)
        , SecurePort(securePort)
        , RootCertsFile(std::move(rootCertsFile))
        , KeyFile(std::move(keyFile))
        , CertFile(std::move(certFile))
        , SocketsDir(std::move(socketsDir))
    {
    }
};

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(TServerConfig config, ILoggingServicePtr logging);

}   // namespace NCloud::NBlockStore::NServer
