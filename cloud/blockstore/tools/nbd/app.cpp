#include "app.h"

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/generic/singleton.h>
#include <util/system/condvar.h>
#include <util/system/mutex.h>

namespace NCloud::NBlockStore::NBD {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TApp
{
private:
    TAtomic ShouldStop = 0;
    TAtomic ExitCode = 0;

    TMutex WaitMutex;
    TCondVar WaitCondVar;

public:
    static TApp* GetInstance()
    {
        return Singleton<TApp>();
    }

    int Run(TBootstrap& bootstrap)
    {
        Y_UNUSED(bootstrap);

        with_lock (WaitMutex) {
            while (AtomicGet(ShouldStop) == 0) {
                WaitCondVar.WaitT(WaitMutex, TDuration::Seconds(5));
            }
        }

        return AtomicGet(ExitCode);
    }

    void Stop(int exitCode)
    {
        AtomicSet(ExitCode, exitCode);
        AtomicSet(ShouldStop, 1);

        WaitCondVar.Signal();
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

int AppMain(TBootstrap& bootstrap)
{
    return TApp::GetInstance()->Run(bootstrap);
}

void AppStop(int exitCode)
{
    TApp::GetInstance()->Stop(exitCode);
}

}   // namespace NCloud::NBlockStore::NBD
