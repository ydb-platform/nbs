#include "app.h"

#include "bootstrap.h"
#include "options.h"

#include <cloud/filestore/tools/testing/loadtest/lib/context.h>
#include <cloud/filestore/tools/testing/loadtest/lib/executor.h>
#include <cloud/filestore/tools/testing/loadtest/lib/test.h>

#include <cloud/filestore/tools/testing/loadtest/protos/loadtest.pb.h>

#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <contrib/ydb/library/actors/util/should_continue.h>
#include <library/cpp/logger/backend.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/protobuf/util/pb_io.h>
#include <library/cpp/sighandler/async_signals_handler.h>

#include <util/generic/map.h>
#include <util/generic/singleton.h>
#include <util/system/mutex.h>

namespace NCloud::NFileStore::NLoadTest {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TApp
{
private:
    TLog Log;

    TAppContext Context;

    TMutex Lock;
    TMap<int, NProto::TTestStats> Tests;

public:
    static TApp* GetInstance()
    {
        return Singleton<TApp>();
    }

    int Run(TBootstrap& bootstrap)
    {
        NCloud::SetCurrentThreadName("main");
        Log = bootstrap.GetLogging()->CreateLog("MAIN");

        try {
            NProto::TTestGraph graph;

            auto options = bootstrap.GetOptions();
            if (options->TestsConfig) {
                ParseFromTextFormat(options->TestsConfig, graph);
            }

            TVector<std::function<void()>> tests;
            for (ui32 i = 0; i < graph.TestsSize(); ++i) {
                const auto& entry = graph.GetTests(i);

                switch (entry.GetTestCase()) {
                    case NProto::TTestGraph::TTest::kLoadTest: {
                        auto name = entry.GetLoadTest().GetName();

                        auto test = CreateLoadTest(
                            Context,
                            entry.GetLoadTest(),
                            bootstrap.GetTimer(),
                            bootstrap.GetScheduler(),
                            bootstrap.GetLogging(),
                            bootstrap.GetClientFactory());

                        tests.push_back(
                            [=, this] () {
                                RunLoadTest(i, name, test);
                            });

                        break;
                    }
                    default:
                        Y_ABORT("unexpected test case: %s", entry.ShortDebugString().c_str());
                }
            }

            TExecutor executor(Context, std::move(tests));
            executor.Run();

            for (const auto& pair: Tests) {
                bootstrap.GetOptions()->GetOutputStream()
                    << NProtobufJson::Proto2Json(pair.second, {.FormatOutput = true}) << Endl;
            }

        } catch(...) {
            STORAGE_ERROR("app has failed: " << CurrentExceptionMessage());
            return 1;
        }

        return AtomicGet(Context.ExitCode);
    }

    void RunLoadTest(ui32 i, const TString& name, const ITestPtr& test)
    {
        try {
            const auto& stats = test->Run().GetValueSync();
            if (!stats.GetSuccess()) {
                ythrow yexception() << "not successful";
            }

            with_lock (Lock) {
                Tests[i] = stats;
            }

            STORAGE_INFO("%s has finished",
                MakeTestTag(name).c_str());

        } catch (...) {
            STORAGE_INFO("%s test has failed: %s",
                MakeTestTag(name).c_str(),
                CurrentExceptionMessage().c_str());

            Stop(1);
        }
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

void ProcessAsyncSignal(int signum)
{
    if (signum == SIGHUP) {
        TLogBackend::ReopenAllBackends();
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

}   // namespace NCloud::NFileStore::NLoadTest
