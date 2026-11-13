#include "app.h"

#include "bootstrap.h"

#include <cloud/fastshard/apps/jdserver/lib/options.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <signal.h>

#include <cstdio>
#include <new>

namespace NCloud::NFastShard::NJdServer {

namespace {

////////////////////////////////////////////////////////////////////////////////

// The signals that stop the server. They are blocked in every thread and
// picked up by the main thread with sigwait, so no handler runs.
sigset_t MakeStopSignals()
{
    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals, SIGINT);
    sigaddset(&signals, SIGTERM);
    return signals;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void ConfigureSignals()
{
    std::set_new_handler(abort);

    // make sure that errors can be seen by everybody :)
    setvbuf(stdout, nullptr, _IONBF, 0);
    setvbuf(stderr, nullptr, _IONBF, 0);

    // a client closing the connection must surface as a send error, not
    // kill the process
    signal(SIGPIPE, SIG_IGN);

    // the threads started later inherit the mask, so the stop signals reach
    // the main thread only
    const auto stopSignals = MakeStopSignals();
    pthread_sigmask(SIG_BLOCK, &stopSignals, nullptr);
}

int AppMain(TBootstrap& bootstrap)
{
    auto Log = bootstrap.GetLogging()->CreateLog("JDSERVER");

    const auto options = bootstrap.GetOptions();

    STORAGE_INFO(
        "Serving the device "
        << options->DeviceUUID.Quote() << " on port " << options->Port
        << (options->NoJournal ? " without a journal" : "") << " with "
        << options->AioThreads << " AIO thread(s), press Ctrl+C to stop");

    //
    // Wait for a stop signal; a second one is left to the default handler
    // and kills the process
    //

    const auto stopSignals = MakeStopSignals();
    int signum = 0;
    sigwait(&stopSignals, &signum);
    pthread_sigmask(SIG_UNBLOCK, &stopSignals, nullptr);

    STORAGE_INFO("Stopping on signal " << signum << "...");

    return 0;
}

}   // namespace NCloud::NFastShard::NJdServer
