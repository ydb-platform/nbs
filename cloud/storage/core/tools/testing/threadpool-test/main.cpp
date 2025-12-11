#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/diagnostics/histogram.h>

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/datetime/base.h>
#include <util/datetime/cputimer.h>
#include <util/generic/deque.h>
#include <util/generic/singleton.h>
#include <util/generic/vector.h>
#include <util/generic/ymath.h>
#include <util/system/condvar.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>

namespace {

using namespace NCloud;
using namespace NLastGetopt;

////////////////////////////////////////////////////////////////////////////////

struct TOptions;
using TOptionsPtr = std::shared_ptr<TOptions>;

class TStats;
using TStatsPtr = std::shared_ptr<TStats>;

class TClientThread;
using TClientThreadPtr = std::unique_ptr<TClientThread>;

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration WaitTimeout = TDuration::Seconds(1);
constexpr TDuration ReportInterval = TDuration::Seconds(5);

////////////////////////////////////////////////////////////////////////////////

class TDummyThreadPool final: public ITaskQueue
{
    struct TWorkerThread: ISimpleThread
    {
        TDummyThreadPool& ThreadPool;
        TString Name;

        TWorkerThread(TDummyThreadPool& threadPool, TString name)
            : ThreadPool(threadPool)
            , Name(std::move(name))
        {}

        void* ThreadProc() noexcept override
        {
            ::NCloud::SetCurrentThreadName(Name);

            ThreadPool.Run();
            return nullptr;
        }
    };

private:
    TDeque<ITaskPtr> WorkQueue;
    TVector<std::unique_ptr<ISimpleThread>> Workers;

    TMutex Mutex;
    TCondVar CondVar;

    bool ShouldStop = false;

public:
    TDummyThreadPool(const TString& threadName, size_t numWorkers)
        : Workers(numWorkers)
    {
        int i = 1;
        for (auto& worker: Workers) {
            worker = std::make_unique<TWorkerThread>(
                *this,
                TStringBuilder() << threadName << i++);
        }
    }

    void Start() override
    {
        for (auto& worker: Workers) {
            worker->Start();
        }
    }

    void Stop() override
    {
        with_lock (Mutex) {
            ShouldStop = true;
            CondVar.BroadCast();
        }

        for (auto& worker: Workers) {
            worker->Join();
        }
    }

    void Enqueue(ITaskPtr task) override
    {
        with_lock (Mutex) {
            WorkQueue.emplace_back(std::move(task));
            CondVar.Signal();
        }
    }

private:
    void Run()
    {
        auto lock = Guard(Mutex);
        while (!ShouldStop) {
            if (WorkQueue) {
                ITaskPtr task = std::move(WorkQueue.front());
                WorkQueue.pop_front();

                auto unlock = Unguard(Mutex);
                task->Execute();
            } else {
                CondVar.Wait(Mutex);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

ITaskQueuePtr CreateDummyThreadPool(
    const TString& threadName,
    size_t numWorkers)
{
    return std::make_shared<TDummyThreadPool>(threadName, numWorkers);
}

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    size_t ClientThreads = 1;
    size_t ServerThreads = 1;
    size_t IoDepth = 1;
    size_t BusyWork = 0;
    bool DummyQueue = false;

    void Parse(int argc, char* argv[])
    {
        TOpts opts;
        opts.AddHelpOption();

        opts.AddLongOption("client")
            .RequiredArgument("NUM")
            .DefaultValue(ToString(ClientThreads))
            .StoreResult(&ClientThreads);

        opts.AddLongOption("server")
            .RequiredArgument("NUM")
            .DefaultValue(ToString(ServerThreads))
            .StoreResult(&ServerThreads);

        opts.AddLongOption("iodepth")
            .RequiredArgument("NUM")
            .DefaultValue(ToString(IoDepth))
            .StoreResult(&IoDepth);

        opts.AddLongOption("busy")
            .RequiredArgument("NUM")
            .DefaultValue(ToString(BusyWork))
            .StoreResult(&BusyWork);

        opts.AddLongOption("dummy").NoArgument().SetFlag(&DummyQueue);

        TOptsParseResultException res(&opts, argc, argv);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TStats
{
private:
    TAtomic RequestCount = 0;
    THistogramBase RequestTime{1, 10000000, 3};

public:
    void ReportStats(ui64 cyclesCount)
    {
        AtomicIncrement(RequestCount);
        RequestTime.RecordValue(cyclesCount);
    }

    void DumpStats(TDuration duration)
    {
        size_t count = AtomicGet(RequestCount);
        Cout << "Total  : " << count << Endl
             << "RPS    : " << round(count / duration.SecondsFloat()) << Endl
             << Endl;

        Cout << "=== Request Latency (ns) ===" << Endl;
        DumpHistogram(RequestTime);
        Cout << Endl;
    }

private:
    static void DumpHistogram(const THistogramBase& hist)
    {
        static const TVector<TPercentileDesc> Percentiles = {
            {0.01, "    1"},
            {0.10, "   10"},
            {0.25, "   25"},
            {0.50, "   50"},
            {0.75, "   75"},
            {0.90, "   90"},
            {0.95, "   95"},
            {0.98, "   98"},
            {0.99, "   99"},
            {0.995, " 99.5"},
            {0.999, " 99.9"},
            {0.9999, "99.99"},
            {1.0000, "100.0"},
        };

        auto cyclesPerNanoSecond = GetCyclesPerMillisecond() / 1000000.;
        for (const auto& [p, name]: Percentiles) {
            Cout << name << " : "
                 << round(
                        hist.GetValueAtPercentile(100 * p) /
                        cyclesPerNanoSecond)
                 << Endl;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TClientThread final: public ISimpleThread
{
    struct Y_CACHE_ALIGNED TRequest
    {
        TAtomic SendTs = 0;
        TAtomic RecvTs = 0;
    };

private:
    const TString Name;
    const ITaskQueuePtr ThreadPool;
    const TOptionsPtr Options;
    const TStatsPtr Stats;
    const ui64 SpinCycles;

    TVector<TRequest> Requests;

    Y_CACHE_ALIGNED TAtomic ShouldStop = 0;
    Y_CACHE_ALIGNED TAtomic RecvResponse = 0;

public:
    TClientThread(
        TString name,
        ITaskQueuePtr threadPool,
        TOptionsPtr options,
        TStatsPtr stats)
        : Name(std::move(name))
        , ThreadPool(std::move(threadPool))
        , Options(std::move(options))
        , Stats(std::move(stats))
        , SpinCycles(
              DurationToCyclesSafe(TDuration::MicroSeconds(Options->BusyWork)))
        , Requests(Options->IoDepth)
    {}

    void Stop()
    {
        AtomicSet(ShouldStop, 1);
        AtomicSet(RecvResponse, 1);

        Join();
    }

private:
    void* ThreadProc() override
    {
        ::NCloud::SetCurrentThreadName(Name);

        while (AtomicGet(ShouldStop) == 0) {
            for (auto& request: Requests) {
                if (HandleResponse(request)) {
                    SendRequest(request);
                }
            }

            // wait for response
            while (AtomicGet(RecvResponse) == 0) {
                SpinLockPause();
            }
            AtomicSet(RecvResponse, 0);
        }

        return nullptr;
    }

    void SendRequest(TRequest& request)
    {
        AtomicSet(request.RecvTs, 0);
        AtomicSet(request.SendTs, GetCycleCount());

        ThreadPool->ExecuteSimple([&] { HandleRequest(request); });
    }

    void HandleRequest(TRequest& request)
    {
        ui64 recvTs = GetCycleCount();

        if (SpinCycles) {
            ui64 deadLine =
                NDateTimeHelpers::SumWithSaturation(recvTs, SpinCycles);

            size_t spin = 0;
            for (;;) {
                if (++spin & 31) {
                    SpinLockPause();
                } else if (deadLine < GetCycleCount()) {
                    break;
                }
            }
        }

        AtomicSet(request.RecvTs, recvTs);
        AtomicSet(RecvResponse, 1);
    }

    bool HandleResponse(TRequest& request)
    {
        ui64 sendTs = AtomicGet(request.SendTs);
        if (sendTs) {
            ui64 recvTs = AtomicGet(request.RecvTs);
            if (!recvTs) {
                // not ready
                return false;
            }

            Stats->ReportStats(recvTs - sendTs);
        }

        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TClientThreads
{
private:
    TVector<TClientThreadPtr> Threads;

public:
    TClientThreads(
        const TString& threadName,
        ITaskQueuePtr threadPool,
        TOptionsPtr options,
        TStatsPtr stats)
        : Threads(options->ClientThreads)
    {
        int i = 1;
        for (auto& thread: Threads) {
            thread = std::make_unique<TClientThread>(
                TStringBuilder() << threadName << i++,
                threadPool,
                options,
                stats);
        }
    }

    void Start()
    {
        for (auto& thread: Threads) {
            thread->Start();
        }
    }

    void Stop()
    {
        for (auto& thread: Threads) {
            thread->Stop();
        }
    }
};

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

    int Run(TOptionsPtr options)
    {
        auto stats = std::make_shared<TStats>();

        auto threadPool =
            options->DummyQueue
                ? CreateDummyThreadPool("srv", options->ServerThreads)
                : CreateThreadPool("srv", options->ServerThreads);

        auto clientThreads = TClientThreads("cli", threadPool, options, stats);

        auto started = TInstant::Now();
        threadPool->Start();
        clientThreads.Start();

        auto lastReportTs = started;
        with_lock (WaitMutex) {
            while (AtomicGet(ShouldStop) == 0) {
                WaitCondVar.WaitT(WaitMutex, WaitTimeout);

                auto now = Now();
                if (now - lastReportTs > ReportInterval) {
                    stats->DumpStats(now - started);
                    lastReportTs = now;
                }
            }
        }

        auto completed = TInstant::Now();
        clientThreads.Stop();
        threadPool->Stop();

        stats->DumpStats(completed - started);

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

int AppMain(TOptionsPtr options)
{
    return TApp::GetInstance()->Run(std::move(options));
}

void AppStop(int exitCode)
{
    TApp::GetInstance()->Stop(exitCode);
}

void ProcessSignal(int signum)
{
    if (signum == SIGINT || signum == SIGTERM) {
        AppStop(0);
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

    struct sigaction sa = {};
    sa.sa_handler = ProcessSignal;

    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
    sigaction(SIGUSR1, &sa, nullptr);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char* argv[])
{
    ConfigureSignals();

    auto options = std::make_shared<TOptions>();
    try {
        options->Parse(argc, argv);
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }

    return AppMain(std::move(options));
}
