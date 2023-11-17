#include "app.h"

#include "initiator.h"
#include "options.h"
#include "runnable.h"
#include "target.h"

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

    int Run(TOptionsPtr options)
    {
        SetCurrentThreadName("Main");

        Runnable = CreateTest(options);
        return Runnable->Run();
    }

    void Stop(int exitCode)
    {
        if (Runnable) {
            Runnable->Stop(exitCode);
        }
    }

private:
    static IRunnablePtr CreateTest(TOptionsPtr options)
    {
        switch (options->TestMode) {
            case ETestMode::Target:
                return CreateTestTarget(std::move(options));
            case ETestMode::Initiator:
                return CreateTestInitiator(std::move(options));
            default:
                ythrow yexception() << "invalid test mode";
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
