#include "app.h"

#include "bootstrap.h"
#include "context.h"
#include "unsensitivifier.h"

#include <library/cpp/sighandler/async_signals_handler.h>

#include <util/system/mutex.h>

namespace NCloud::NFileStore::NUnsensitivifier {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TApp
{
private:
    TAppContext Context;

    TMutex Lock;

public:
    static TApp* GetInstance()
    {
        return Singleton<TApp>();
    }

    int Run(TBootstrap& bootstrap) const
    {
        TUnsensitivifier unsensitivifier{bootstrap};
        unsensitivifier.Unsensitivifie(
            bootstrap.GetOptions()->InFile,
            bootstrap.GetOptions()->OutFile);

        return AtomicGet(Context.ExitCode);
    }

    void Stop(int exitCode)
    {
        AtomicSet(Context.ExitCode, exitCode);
        AtomicSet(Context.ShouldStop, 1);
    }
};

////////////////////////////////////////////////////////////////////////////////

void ProcessSignal(int signum)
{
    if (signum == SIGINT || signum == SIGTERM) {
        AppStop(0);
    }
}

void ProcessAsyncSignal(int /*signum*/)
{}

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

    SetAsyncSignalHandler(SIGHUP, ProcessAsyncSignal);
}

int AppMain(TBootstrap& bootstrap)
{
    return TApp::GetInstance()->Run(bootstrap);
}

void AppStop(int exitCode)
{
    TApp::GetInstance()->Stop(exitCode);
}

}   // namespace NCloud::NFileStore::NUnsensitivifier
