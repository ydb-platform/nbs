#include "app.h"

#include "action_graph.h"
#include "aliased_volumes.h"
#include "app_context.h"
#include "bootstrap.h"
#include "compare_data_action_runner.h"
#include "control_plane_action_runner.h"
#include "countdown_latch.h"
#include "helpers.h"
#include "load_test_runner.h"
#include "options.h"
#include "request_generator.h"
#include "suite_runner.h"
#include "test_runner.h"
#include "volume_infos.h"
#include "wait_for_fresh_devices_action.h"

#include <cloud/blockstore/libs/storage/model/public.h>
#include <cloud/blockstore/tools/testing/loadtest/protos/loadtest.pb.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/common/timer.h>

#include <library/cpp/logger/backend.h>
#include <library/cpp/protobuf/util/pb_io.h>
#include <library/cpp/sighandler/async_signals_handler.h>

#include <util/datetime/base.h>
#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/singleton.h>
#include <util/generic/yexception.h>
#include <util/system/thread.h>

namespace NCloud::NBlockStore::NLoadTest {

using namespace NThreading;

using namespace NCloud::NBlockStore::NClient;

////////////////////////////////////////////////////////////////////////////////

class TApp
{
private:
    TLog Log;
    TAppContext AppContext;
    TAliasedVolumes AliasedVolumes;
    TVolumeInfos VolumeInfos;
    TDeque<TTestContext> TestContexts;

public:
    TApp()
        : AliasedVolumes(Log)
    {
    }

    static TApp* GetInstance()
    {
        return Singleton<TApp>();
    }

    int Run(TBootstrap& bootstrap);
    void Stop(int exitCode);

private:
    static TStringBuf GetName(const NProto::TActionGraph::TVertex& v);

    void RunLoadTest(
        TBootstrap& bootstrap,
        TTestContext& testContext,
        const NProto::TLoadTest& test,
        const TVector<TTestContext*>& dependencies);

    void RunControlPlaneAction(
        TTestContext& testContext,
        IBlockStorePtr client,
        const NProto::TActionGraph::TControlPlaneAction& action);

    void RunCompareDataAction(
        TBootstrap& bootstrap,
        TTestContext& testContext,
        const NProto::TActionGraph::TCompareDataAction& action);

    void RunWaitForFreshDevicesAction(
        TBootstrap& bootstrap,
        TTestContext& testContext,
        const NProto::TActionGraph::TWaitForFreshDevicesAction& action);

    void RunGraph(const NProto::TActionGraph& graph, TBootstrap& bootstrap);

    void InitTestConfig(TBootstrap& bootstrap, NProto::TActionGraph* testConfig);
};

////////////////////////////////////////////////////////////////////////////////

int TApp::Run(TBootstrap& bootstrap)
{
    SetCurrentThreadName("Main");

    Log = bootstrap.GetLogging()->CreateLog("MAIN");

    NProto::TActionGraph graph;

    try {
        InitTestConfig(bootstrap, &graph);
    } catch (...) {
        STORAGE_ERROR("Could not load tests configuration: "
            << CurrentExceptionMessage());

        return EC_FAILED_TO_LOAD_TESTS_CONFIGURATION;
    }

    auto options = bootstrap.GetOptions();

    if (options->Timeout != TDuration::Zero()) {
        bootstrap.GetScheduler()->Schedule(
            bootstrap.GetTimer()->Now() + options->Timeout,
            [=, this] {
                Stop(EC_TIMEOUT);
            });
    }

    RunGraph(graph, bootstrap);

    try {
        AliasedVolumes.DestroyAliasedVolumesUnsafe(bootstrap);
    } catch (...) {
        STORAGE_ERROR("Could not destroy aliased volumes: "
            << CurrentExceptionMessage());

        return EC_FAILED_TO_DESTROY_ALIASED_VOLUMES;
    }

    auto& out = options->GetOutputStream();
    for (const auto& testContext: TestContexts) {
        out << testContext.Result.Str() << Endl;
    }

    TestContexts.clear();

    if (AppContext.FailedTests) {
        STORAGE_WARN("Several tests ("
            << AppContext.FailedTests.load(std::memory_order_acquire)
            << ") failed.");
    }

    const auto exitCode = AppContext.ExitCode.load(std::memory_order_acquire);
    return exitCode ? exitCode : (AppContext.FailedTests ? 1 : 0);
}

void TApp::Stop(int exitCode)
{
    AppContext.ExitCode.store(exitCode, std::memory_order_release);
    AppContext.ShouldStop.store(true, std::memory_order_release);
    for (auto& testContext: TestContexts) {
        StopTest(testContext);
    }
}

TStringBuf TApp::GetName(const NProto::TActionGraph::TVertex& v)
{
    switch (v.GetActionCase()) {
        case NProto::TActionGraph::TVertex::kTest:
            return v.GetTest().GetName();
        case NProto::TActionGraph::TVertex::kControlPlaneAction:
            return v.GetControlPlaneAction().GetName();
        case NProto::TActionGraph::TVertex::kSleep:
            return v.GetSleep().GetName();
        case NProto::TActionGraph::TVertex::kCompareData:
            return v.GetCompareData().GetName();
        case NProto::TActionGraph::TVertex::kWaitForFreshDevices:
            return v.GetWaitForFreshDevices().GetName();
        default:
            Y_ABORT_UNLESS(0);
    }
}

void TApp::RunLoadTest(
    TBootstrap& bootstrap,
    TTestContext& testContext,
    const NProto::TLoadTest& test,
    const TVector<TTestContext*>& dependencies)
{
    TLoadTestRunner runner(
        Log,
        AppContext,
        AliasedVolumes,
        bootstrap.GetClientConfig(),
        bootstrap.GetTimer(),
        bootstrap.GetScheduler(),
        bootstrap.GetLogging(),
        bootstrap.GetRequestStats(),
        bootstrap.GetVolumeStats(),
        bootstrap,
        testContext
    );

    if (auto code = runner.Run(test, dependencies)) {
        Stop(code);
    }
}

void TApp::RunControlPlaneAction(
    TTestContext& testContext,
    IBlockStorePtr client,
    const NProto::TActionGraph::TControlPlaneAction& action)
{
    TControlPlaneActionRunner runner(
        Log,
        AppContext,
        AliasedVolumes,
        VolumeInfos,
        std::move(client),
        testContext
    );

    if (auto code = runner.Run(action)) {
        Stop(code);
    }
}

void TApp::RunCompareDataAction(
    TBootstrap& bootstrap,
    TTestContext& testContext,
    const NProto::TActionGraph::TCompareDataAction& action)
{
    TCompareDataActionRunner runner(
        Log,
        AppContext,
        AliasedVolumes,
        bootstrap.GetClientConfig(),
        bootstrap.GetTimer(),
        bootstrap.GetScheduler(),
        bootstrap.GetLogging(),
        bootstrap.GetRequestStats(),
        bootstrap.GetVolumeStats(),
        bootstrap,
        testContext
    );

    if (auto code = runner.Run(action)) {
        Stop(code);
    }
}

void TApp::RunWaitForFreshDevicesAction(
    TBootstrap& bootstrap,
    TTestContext& testContext,
    const NProto::TActionGraph::TWaitForFreshDevicesAction& action)
{
    TWaitForFreshDevicesActionRunner runner(
        Log,
        AliasedVolumes,
        bootstrap.GetLogging(),
        bootstrap,
        testContext);

    if (auto code = runner.Run(action)) {
        Stop(code);
    }
}

void TApp::RunGraph(const NProto::TActionGraph& graph, TBootstrap& bootstrap)
{
    using namespace NLoadTest;

    TGraph ag;
    ag.Vertices.resize(graph.VerticesSize());
    ag.Edges.resize(graph.VerticesSize());

    THashMap<TString, TVertexId> name2vidx;
    THashSet<TVertexId> dependencyVertices;
    THashMap<TVertexId, TVector<TTestContext*>> vidx2dependencies;

    // initializing contexts
    TestContexts.resize(graph.VerticesSize());

    // preparing dependencies
    for (TVertexId i = 0; i < graph.VerticesSize(); ++i) {
        name2vidx[GetName(graph.GetVertices(i))] = i;
    }

    for (TVertexId i = 0; i < graph.VerticesSize(); ++i) {
        auto vertex = graph.GetVertices(i);
        if (vertex.GetActionCase() == NProto::TActionGraph::TVertex::kTest) {
            const auto& test = vertex.GetTest();
            for (const auto& verifyBy: test.GetVerifyBy()) {
                auto it = name2vidx.find(verifyBy);
                if (it == name2vidx.end()) {
                    throw yexception() << "no such vertex: " << verifyBy;
                }

                vidx2dependencies[i].push_back(&TestContexts[it->second]);
                dependencyVertices.insert(it->second);
            }
        }
    }

    // initializing vertices
    for (TVertexId i = 0; i < graph.VerticesSize(); ++i) {
        auto vertex = graph.GetVertices(i);
        const auto& dependencies = vidx2dependencies[i];
        if (dependencyVertices.contains(i)) {
            TestContexts[i].DigestCalculator = bootstrap.CreateDigestCalculator();
            TestContexts[i].BlockChecksums.set_empty_key(
                NStorage::InvalidBlockIndex
            );
        }

        ag.Vertices[i] = [this, vertex, i, dependencies, &bootstrap] () {
            if (AppContext.ShouldStop.load(std::memory_order_acquire)) {
                return;
            }

            auto& testContext = TestContexts[i];
            switch (vertex.GetActionCase()) {
                case NProto::TActionGraph::TVertex::kTest: {
                    const auto& test = vertex.GetTest();
                    for (ui32 j = 0; j < Max(1u, test.GetRepetitions()); ++j) {
                        if (j) {
                            testContext.ShouldStop.store(false, std::memory_order_release);
                            testContext.Result << Endl;
                        }

                        RunLoadTest(bootstrap, testContext, test, dependencies);

                        if (AppContext.ShouldStop.load(std::memory_order_acquire)) {
                            return;
                        }
                    }
                    break;
                }

                case NProto::TActionGraph::TVertex::kControlPlaneAction: {
                    const auto& cpa = vertex.GetControlPlaneAction();
                    auto client = bootstrap.CreateClient({}, cpa.GetName());
                    for (ui32 j = 0; j < Max(1u, cpa.GetRepetitions()); ++j) {
                        if (j) {
                            testContext.Result << Endl;
                        }

                        RunControlPlaneAction(testContext, client, cpa);

                        if (AppContext.ShouldStop.load(std::memory_order_acquire)) {
                            return;
                        }
                    }
                    break;
                }

                case NProto::TActionGraph::TVertex::kSleep: {
                    const auto& sleep = vertex.GetSleep();
                    STORAGE_INFO("Started sleep: " << sleep.GetName());
                    Sleep(TDuration::Seconds(sleep.GetDuration()));
                    testContext.Result << "{}";
                    break;
                }

                case NProto::TActionGraph::TVertex::kCompareData: {
                    RunCompareDataAction(
                        bootstrap,
                        testContext,
                        vertex.GetCompareData()
                    );
                    break;
                }

                case NProto::TActionGraph::TVertex::kWaitForFreshDevices: {
                    RunWaitForFreshDevicesAction(
                        bootstrap,
                        testContext,
                        vertex.GetWaitForFreshDevices());
                    break;
                }

                default: Y_ABORT_UNLESS(0);
            }
        };
    }

    // initializing edges
    for (const auto& x : graph.GetDependencies()) {
        const auto vidx = name2vidx.at(x.first);
        for (const auto& dep : x.second.GetNames()) {
            ag.Edges[vidx].push_back(name2vidx.at(dep));
        }
    }

    // running graph
    TGraphExecutor(std::move(ag)).Run();
}

void TApp::InitTestConfig(TBootstrap& bootstrap, NProto::TActionGraph* testConfig)
{
    auto options = bootstrap.GetOptions();
    if (options->TestConfig) {
        ParseFromTextFormat(options->TestConfig, *testConfig);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ProcessSignal(int signum)
{
    if (signum == SIGINT || signum == SIGTERM) {
        Cerr << "please wait until the app stops" << Endl;
        AppStop(0);
    }
}

void ProcessAsyncSignal(int signum)
{
    if (signum == SIGHUP) {
        TLogBackend::ReopenAllBackends();
    }
}

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

int DoMain(int argc, char** argv,
    std::shared_ptr<TModuleFactories> moduleFactories)
{
    ConfigureSignals();

    auto options = std::make_shared<TOptions>();
    try {
        options->Parse(argc, argv);
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }

    TBootstrap bootstrap(std::move(options), std::move(moduleFactories));
    try {
        bootstrap.Init();
        bootstrap.Start();
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }

    int exitCode = AppMain(bootstrap);

    try {
        bootstrap.Stop();
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }

    return exitCode;
}

}   // namespace NCloud::NBlockStore::NLoadTest
