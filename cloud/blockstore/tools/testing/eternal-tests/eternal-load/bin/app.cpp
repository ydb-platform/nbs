#include "app.h"

#include "options.h"
#include "test.h"

#include <util/generic/singleton.h>

#include <signal.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TApp
{
private:
    ITestPtr Test;

public:
    static TApp* GetInstance()
    {
        return Singleton<TApp>();
    }

    int Run(TOptionsPtr options)
    {
        Test = CreateTest(options);
        return Test->Run();
    }

    void Stop()
    {
        if (Test) {
            Test->Stop();
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

void AppStop()
{
    TApp::GetInstance()->Stop();
}

}   // namespace NCloud::NBlockStore
