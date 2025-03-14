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
    Scheduler = CreateScheduler();
    const auto logLevel =
        GetLogLevel(Options.VerboseLevel).GetOrElse(TLOG_INFO);
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
        Options.NbdReconnectDelay,
        Options.DebugRestartEventsCount,
    },
    CreateWallClockTimer(),
    Scheduler,
    CreateLoggingService("console", TLogSettings{logLevel}));
}

void TBootstrap::Start()
{
    Scheduler->Start();
    Server->Start();
}

void TBootstrap::Stop()
{
    if (Server) {
        Server->Stop();
    }
    if (Scheduler) {
        Scheduler->Stop();
    }
}

}   // namespace NCloud::NBlockStore::NServer
