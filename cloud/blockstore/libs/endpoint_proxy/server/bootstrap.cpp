#include "bootstrap.h"
#include "server.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

void TBootstrap::ParseOptions(int argc, char** argv)
{
    Options.Parse(argc, argv);
}

void TBootstrap::Init()
{
    TLogSettings logSettings;
    logSettings.UseLocalTimestamps = true;
Server = CreateServer({
        static_cast<ui16>(Options.ServerPort),
        static_cast<ui16>(Options.SecureServerPort),
        Options.RootCertsFile,
        Options.KeyFile,
        Options.CertFile,
        Options.SocketsDir
    }, CreateLoggingService("console", logSettings));
}

void TBootstrap::Start()
{
    Server->Start();
}

void TBootstrap::Stop()
{
    Server->Stop();
}

}   // namespace NCloud::NBlockStore::NServer
