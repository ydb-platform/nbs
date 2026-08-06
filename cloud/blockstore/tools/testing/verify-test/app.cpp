#include "app.h"

#include "options.h"
#include "test.h"

#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/singleton.h>

#include <signal.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TApp
{
private:
    ILoggingServicePtr Logging;
    ITestPtr Test;

public:
    static TApp* GetInstance()
    {
        return Singleton<TApp>();
    }

    int Run(TOptionsPtr options)
    {
        Logging = NCloud::CreateLoggingService(
            "console",
            {.FiltrationLevel = NCloud::GetLogLevel(options->VerboseLevel)
                                    .GetOrElse(TLOG_INFO)});
        Logging->Start();

        Test = CreateTest(std::move(options), Logging);
        return Test->Run();
    }

    void Stop(int exitCode)
    {
        if (Test) {
            Test->Stop(exitCode);
        }

        if (Logging) {
            Logging->Stop();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

void ProcessSignal(int signum)
{
    if (signum == SIGINT || signum == SIGTERM) {
        AppStop(0);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void ConfigureSignals()
{
    std::set_new_handler(abort);

    // make sure that errors can be seen by everybody :)
    setvbuf(stdout, nullptr, _IONBF, 0);
    setvbuf(stderr, nullptr, _IONBF, 0);

    // mask signals
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, ProcessSignal);
    signal(SIGTERM, ProcessSignal);
}

int AppMain(TOptionsPtr options)
{
    return TApp::GetInstance()->Run(std::move(options));
}

void AppStop(int exitCode)
{
    TApp::GetInstance()->Stop(exitCode);
}

}   // namespace NCloud::NBlockStore
