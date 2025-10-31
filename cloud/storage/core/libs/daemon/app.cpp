#include "app.h"

#include <cloud/storage/core/libs/common/thread.h>

#include <ydb/library/actors/util/should_continue.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/logger/backend.h>
#include <library/cpp/sighandler/async_signals_handler.h>

#include <util/datetime/base.h>
#include <util/generic/singleton.h>
#include <util/system/condvar.h>
#include <util/system/mutex.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

const TDuration ShouldContinueSleepTime = TDuration::Seconds(5);

////////////////////////////////////////////////////////////////////////////////

class TMainThread
{
private:
    TMutex WaitMutex;
    TCondVar WaitCondVar;

    static const TAtomicBase Init = 0;
    static const TAtomicBase Started = 1;
    static const TAtomicBase Stopped = 2;

    TProgramShouldContinue* ShouldContinue = nullptr;
    TAtomic State = Init;

public:
    static TMainThread* GetInstance()
    {
        return Singleton<TMainThread>();
    }

    int Run(TProgramShouldContinue& shouldContinue)
    {
        ShouldContinue = &shouldContinue;

        if (AtomicCas(&State, Started, Init)) {
            SetCurrentThreadName("Main");

            with_lock (WaitMutex) {
                while (ShouldContinue->PollState() == TProgramShouldContinue::Continue) {
                    WaitCondVar.WaitT(WaitMutex, ShouldContinueSleepTime);
                }
            }
        }

        return ShouldContinue->GetReturnCode();
    }

    void Stop()
    {
        auto prevState = AtomicSwap(&State, Stopped);
        if (prevState == Started) {
            ShouldContinue->ShouldStop(0);
            WaitCondVar.Signal();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

void ProcessSignal(int signum)
{
    if (signum == SIGINT || signum == SIGTERM) {
        AppStop();
    }
}

void ProcessAsyncSignal(int signum)
{
    if (signum == SIGHUP) {
        TLogBackend::ReopenAllBackends();
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void AppCreate()
{
    TMainThread::GetInstance();
}

int AppMain(TProgramShouldContinue& shouldContinue)
{
    return TMainThread::GetInstance()->Run(shouldContinue);
}

void AppStop()
{
    TMainThread::GetInstance()->Stop();
}

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
    // see filestore libs/fuse
    signal(SIGUSR1, SIG_IGN);

    SetAsyncSignalHandler(SIGHUP, ProcessAsyncSignal);
}

}   // namespace NCloud
