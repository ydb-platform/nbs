#include "app.h"

#include "bootstrap.h"
#include "runnable.h"

#include <cloud/storage/core/libs/common/thread.h>

#include <util/generic/singleton.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TApp
{
private:
    IRunnablePtr Runnable;

public:
    static TApp* GetInstance()
    {
        return Singleton<TApp>();
    }

    int Run(TBootstrap& bootstrap)
    {
        SetCurrentThreadName("Main");

        Runnable = bootstrap.CreateTest();
        return Runnable->Run();
    }

    void Stop(int exitCode)
    {
        if (Runnable) {
            Runnable->Stop(exitCode);
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

    struct sigaction sa = {};
    sa.sa_handler = ProcessSignal;

    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
    sigaction(SIGUSR1, &sa, nullptr);
}

int AppMain(TBootstrap& bootstrap)
{
    return TApp::GetInstance()->Run(bootstrap);
}

void AppStop(int exitCode)
{
    TApp::GetInstance()->Stop(exitCode);
}

}   // namespace NCloud::NBlockStore
