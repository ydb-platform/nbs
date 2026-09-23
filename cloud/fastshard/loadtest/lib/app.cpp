#include "app.h"

#include "options.h"

#include <cloud/fastshard/bootstrap/core.h>

#include <library/cpp/getopt/small/last_getopt.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <util/generic/scope.h>
#include <util/generic/singleton.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/system/progname.h>

#include <signal.h>
#include <unistd.h>

#include <atomic>
#include <cstdio>
#include <new>

namespace NCloud::NFastShard::NLoadTest {

namespace {

////////////////////////////////////////////////////////////////////////////////

// Set by the first SIGINT / SIGTERM; a second one leaves the process
// without waiting for the requests in flight.
std::atomic<bool> StopRequested{false};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TApp& TApp::Instance()
{
    return *Singleton<TApp>();
}

void TApp::Shutdown()
{
    if (LoadTest) {
        LoadTest->Stop();
    }
}

int TApp::Run(int argc, const char* argv[])
{
    TOptions options;
    try {
        options.Parse(argc, argv);
    } catch (const NLastGetopt::TException&) {
        Cerr << GetProgramName() << " failed: " << CurrentExceptionMessage()
             << Endl;
        return 1;
    }

    try {
        if (options.Verbose) {
            EnableDebugLogging();
        }

        LoadTest = CreateLoadTest(options);

        Init();
        Y_DEFER
        {
            Destroy();
        };

        const auto stats = LoadTest->Run();

        const auto json =
            NProtobufJson::Proto2Json(stats, {.FormatOutput = true});
        if (options.ResultsFile) {
            TFileOutput out(options.ResultsFile);
            out << json << Endl;
        } else {
            Cout << json << Endl;
        }

        return stats.GetSuccess() ? 0 : 1;
    } catch (...) {
        Cerr << GetProgramName() << " failed: " << CurrentExceptionMessage()
             << Endl;
        return 1;
    }
}

////////////////////////////////////////////////////////////////////////////////

void Shutdown(int signum)
{
    Y_UNUSED(signum);

    if (StopRequested.exchange(true)) {
        ::_exit(1);
    }
    TApp::Instance().Shutdown();
}

void ConfigureSignals()
{
    std::set_new_handler(abort);

    // make sure that errors can be seen by everybody :)
    setvbuf(stdout, nullptr, _IONBF, 0);
    setvbuf(stderr, nullptr, _IONBF, 0);

    // a storage node closing the connection must surface as a send error,
    // not kill the process
    signal(SIGPIPE, SIG_IGN);

    struct sigaction sa = {};
    sa.sa_handler = Shutdown;
    sa.sa_flags = SA_RESTART;

    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
}

}   // namespace NCloud::NFastShard::NLoadTest
