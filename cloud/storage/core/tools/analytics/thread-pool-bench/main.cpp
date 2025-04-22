#include "histogram.h"
#include "thread_pool.h"

#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread_pool.h>

#include <library/cpp/getopt/small/last_getopt.h>
#include <library/cpp/json/json_reader.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/list.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/string/cast.h>

#include <format>
#include <functional>
#include <latch>
#include <mutex>
#include <thread>
#include <utility>

namespace {

////////////////////////////////////////////////////////////////////////////////

using TTaskQueueFactory =
    std::function<NCloud::ITaskQueuePtr(const NJson::TJsonValue& config)>;

const THashMap<TString, TTaskQueueFactory> TaskQueueFactoryMap{
    {"TaskQueueStub",
     [](const auto&)
     {
         return NCloud::CreateTaskQueueStub();
     }},
    {"ThreadPool",
     [](const auto& config)
     {
         const ui32 threads = config["threads"].GetUIntegerSafe(1);
         return NCloud::CreateThreadPool("test", threads, "test");
     }},
    {"LongRunningTaskExecutor",
     [](const auto&)
     {
         return NCloud::CreateLongRunningTaskExecutor("test");
     }},
     {"NaiveThreadPool",
        [](const auto& config)
        {
            const ui32 threads = config["threads"].GetUIntegerSafe(1);
            return std::make_shared<NaiveThreadPool>(threads);
        }},
};

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    TString ConfigPath;

    void Parse(int argc, char** argv);
};

////////////////////////////////////////////////////////////////////////////////

void TOptions::Parse(int argc, char** argv)
{
    using namespace NLastGetopt;

    TOpts opts;
    opts.AddHelpOption();

    opts.AddLongOption("config", "path to a config file")
        .Required()
        .RequiredArgument("FILE")
        .StoreResult(&ConfigPath);

    TOptsParseResult res(&opts, argc, argv);
}

NJson::TJsonValue LoadConfig(const TString& path)
{
    TFileInput stream{path};
    return NJson::ReadJsonTree(&stream, true);
}

NCloud::ITaskQueuePtr CreateTaskQueue(const NJson::TJsonValue& config)
{
    const TString& name = config["name"].GetStringSafe();
    const auto& factory = TaskQueueFactoryMap.at(name);

    return factory(config);
}

////////////////////////////////////////////////////////////////////////////////

struct TProducerStats
{
    THistogram SubmitTime;
    ui64 Submitted = 0;
};

struct TExecutorStats
{
    THistogram InvokeTime;
    ui64 Completed = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TPerfTest
{
private:
    const TOptions Options;

    std::mutex PerThreadExecutorStatsMutex;
    TList<TExecutorStats> PerThreadExecutorStats;

    TVector<std::thread> ProducersThreads;
    TVector<TProducerStats> ProducersStats;

    NCloud::ITaskQueuePtr TaskQueue;

    std::atomic_flag ShouldStop = false;

public:
    explicit TPerfTest(TOptions options)
        : Options(std::move(options))
    {}

    void Run()
    {
        NJson::TJsonValue config = LoadConfig(Options.ConfigPath);

        TaskQueue = CreateTaskQueue(config["task_queue"]);
        Y_ABORT_UNLESS(TaskQueue);

        auto& producers = config["producers"].GetArraySafe();
        Y_ABORT_IF(producers.empty());

        ui32 producerThreadsCount = 0;
        for (auto& p: producers) {
            producerThreadsCount += p["threads"].GetUIntegerSafe(1);
        }
        ProducersStats.resize(producerThreadsCount);
        ProducersThreads.reserve(producerThreadsCount);

        std::latch latch{producerThreadsCount + 1};

        for (ui32 index = 0; auto& p: producers) {
            const auto workload =
                FromString<TDuration>(p["workload"].GetStringSafe("0s"));

            const ui32 threads = p["threads"].GetUIntegerSafe(1);
            for (ui32 i = 0; i != threads; ++i, ++index) {
                ProducersThreads.emplace_back([this, index, workload, &latch] {
                    latch.arrive_and_wait();
                    ProducerThread(index, workload);
                });
            }
        }

        const auto testDuration =
            FromString<TDuration>(config["duration"].GetStringSafe("60s"));
        const TInstant deadline = testDuration.ToDeadLine();

        Cout << "Start with " << config["task_queue"] << "\n";
        Cout.Flush();
        TaskQueue->Start();
        const auto startTime = TInstant::Now();
        latch.arrive_and_wait();

        for (;;) {
            const auto now = TInstant::Now();
            const auto sleep = Min(TDuration::Seconds(1), deadline - now);
            Sleep(sleep);
            Cout << "\r" << (testDuration - (deadline - Now())) << " / " << testDuration;
            Cout.Flush();
            if (!sleep) {
                break;
            }
        }

        ShouldStop.test_and_set();

        for (auto& t: ProducersThreads) {
            t.join();
        }

        const TDuration testExecuteDuration = Now() - startTime;

        Cout << "\nDone: " << testExecuteDuration << "\n";

        TaskQueue->Stop();

        // ... collect stats ...
        Cout << "Producers:\n";
        for (ui32 i = 0; const auto& stats: ProducersStats) {
            Cout << std::format("  {}: {}\n", ++i, stats.Submitted);
        }
        Cout << "Executor:\n";
        for (ui32 i = 0; const auto& stats: PerThreadExecutorStats) {
            Cout << std::format("  {}: {}\n", ++i, stats.Completed);
        }

        Cout << "-----------------------------------------------------------\n";
        ui64 submitted = 0;
        ui64 completed = 0;
        for (const auto& stats: ProducersStats) {
            submitted += stats.Submitted;
        }
        for (const auto& stats: PerThreadExecutorStats) {
            completed += stats.Completed;
        }
        const double averageRps =
            static_cast<double>(completed) / testExecuteDuration.SecondsFloat();

        Cout << std::format(
            "Submitted: {}\nCompleted: {}\nAvg. RPS: {:8.2f}\n",
            submitted,
            completed,
            averageRps);
    }

private:
    void Task(ui64 t0, TDuration workload)
    {
        if (workload) {
            Sleep(workload);
        }

        TExecutorStats& stats = GetThreadLocalExecutorStats();

        stats.InvokeTime.Increment(GetCycleCount() - t0);
        stats.Completed += 1;
    }

    void ProducerThread(ui32 index, TDuration workload)
    {
        TProducerStats& stats = ProducersStats[index];

        while (!ShouldStop.test()) {
            const ui64 t0 = GetCycleCount();

            TaskQueue->ExecuteSimple([this, t0, workload] {
                Task(t0, workload);
            });

            stats.SubmitTime.Increment(GetCycleCount() - t0);
            stats.Submitted += 1;
        }
    }

    TExecutorStats& GetThreadLocalExecutorStats()
    {
        static thread_local TExecutorStats* Stats = nullptr;
        if (!Stats) {
            std::unique_lock lock {PerThreadExecutorStatsMutex};
            Stats = &PerThreadExecutorStats.emplace_back();
        }

        return *Stats;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    TOptions options;
    options.Parse(argc, argv);

    try {
        TPerfTest test{options};
        test.Run();
    } catch (...) {
        Cerr << "ERROR: " << CurrentExceptionMessage() << Endl;
        return 1;
    }

    return 0;
}
