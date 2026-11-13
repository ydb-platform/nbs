#include "app.h"

#include "bootstrap.h"
#include "options.h"

#include <cloud/fastshard/apps/loadtest/lib/context.h>
#include <cloud/fastshard/apps/loadtest/lib/loadtest.h>

#include <library/cpp/protobuf/json/proto2json.h>

#include <silk/util/logger.h>

#include <util/generic/singleton.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/stream/output.h>

#include <signal.h>
#include <unistd.h>

#include <atomic>
#include <cstdio>
#include <new>

namespace NCloud::NFastShard::NLoadTest {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TApp
{
private:
    TAppContext Context;

public:
    static TApp* GetInstance()
    {
        return Singleton<TApp>();
    }

    int Run(TBootstrap& bootstrap)
    {
        try {
            const auto options = bootstrap.GetOptions();

            auto test = CreateLoadTest(
                Context,
                options->Test,
                bootstrap.GetClient(),
                bootstrap.GetClientMetrics());

            const auto stats = test->Run();

            const auto json =
                NProtobufJson::Proto2Json(stats, {.FormatOutput = true});
            if (options->ResultsFile) {
                TFileOutput out(options->ResultsFile);
                out << json << Endl;
            } else {
                Cout << json << Endl;
            }

            if (!stats.GetSuccess()) {
                return 1;
            }
        } catch (...) {
            SILK_ERROR("%s", CurrentExceptionMessage().c_str());
            return 1;
        }

        return Context.ExitCode.load();
    }

    void Stop(int exitCode)
    {
        Context.ExitCode.store(exitCode);
        Context.ShouldStop.store(true);
    }
};

////////////////////////////////////////////////////////////////////////////////

// Set by the first SIGINT / SIGTERM; a second one leaves the process
// without waiting for the requests in flight.
std::atomic<bool> StopRequested{false};

void ProcessSignal(int signum)
{
    Y_UNUSED(signum);

    if (StopRequested.exchange(true)) {
        ::_exit(1);
    }
    AppStop(0);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void ConfigureSignals()
{
    std::set_new_handler(abort);

    // make sure that errors can be seen by everybody :)
    setvbuf(stdout, nullptr, _IONBF, 0);
    setvbuf(stderr, nullptr, _IONBF, 0);

    // a storage node closing the connection must surface as a send error,
    // not kill the process
    signal(SIGPIPE, SIG_IGN);

    //
    // Create the app before the handlers are installed: a signal handler
    // must not be the first one to construct the singleton
    //

    Y_UNUSED(TApp::GetInstance());

    // AppStop only sets flags that the test polls, so nothing needs the
    // interrupted syscall to fail: without SA_RESTART a signal landing on
    // a silk scheduler thread mid-read of stdin surfaces as EINTR and the
    // command fails with an I/O error instead of "command was stopped".
    struct sigaction sa = {};
    sa.sa_handler = ProcessSignal;
    sa.sa_flags = SA_RESTART;

    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
}

int AppMain(TBootstrap& bootstrap)
{
    return TApp::GetInstance()->Run(bootstrap);
}

void AppStop(int exitCode)
{
    TApp::GetInstance()->Stop(exitCode);
}

}   // namespace NCloud::NFastShard::NLoadTest
