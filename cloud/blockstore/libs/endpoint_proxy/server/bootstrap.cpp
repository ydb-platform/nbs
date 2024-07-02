#include "bootstrap.h"
#include "server.h"

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
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
    Scheduler = CreateScheduler();
    logSettings.UseLocalTimestamps = true;
    Server = CreateServer({
        static_cast<ui16>(Options.ServerPort),
        static_cast<ui16>(Options.SecureServerPort),
        Options.RootCertsFile,
        Options.KeyFile,
        Options.CertFile,
        Options.UnixSocketPath,
        Options.Netlink,
        Options.StoredEndpointsPath,
        Options.NbdRequestTimeout,
    },
    CreateWallClockTimer(),
    Scheduler,
    CreateLoggingService("console", logSettings));
}

void TBootstrap::Start()
{
    Scheduler->Start();
    Server->Start();
}

void TBootstrap::Stop()
{
    Server->Stop();
    Scheduler->Stop();
}

}   // namespace NCloud::NBlockStore::NServer
