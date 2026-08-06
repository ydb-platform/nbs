#include <cloud/filestore/libs/vfs/fsync_queue.h>

#include <library/cpp/histogram/hdr/histogram.h>
#include <library/cpp/testing/benchmark/bench.h>

#include <util/datetime/cputimer.h>
#include <util/generic/singleton.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/format.h>
#include <util/stream/output.h>
#include <util/stream/str.h>
#include <util/system/datetime.h>
#include <util/system/mutex.h>
#include <util/system/rusage.h>
#include <util/system/yassert.h>

#include <atomic>
#include <cstddef>
#include <latch>
#include <memory>
#include <thread>
#include <utility>

namespace NCloud::NFileStore::NVFS {

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString FileSystemId = "fsync_queue_bench";
constexpr ui32 MaxPendingRequests = 64;
constexpr ui64 IdsPerThread = 1'000'000;
const TDuration MaxTrackedLatency = TDuration::Minutes(10);

enum class EScenario
{
    DataEnqDeq,
    MetaEnqDeq,
    DataMixedEnqDeqLocalWait,
    MetaMixedEnqDeqLocalWait,
    DataMixedEnqDeqGlobalWait,
    MetaMixedEnqDeqGlobalWait,
};

struct TBenchmarkRuntime
{
    ILoggingServicePtr Logging = CreateLoggingService("console", {TLOG_INFO});
};

ILoggingServicePtr GetLogging()
{
    return Singleton<TBenchmarkRuntime>()->Logging;
}

ui64 GetMaxTrackedNanoseconds()
{
    static const ui64 nanoseconds = MaxTrackedLatency.NanoSeconds();
    return nanoseconds;
}

long double GetCyclesPerNanosecond()
{
    static const long double cyclesPerNanosecond =
        static_cast<long double>(GetCyclesPerMillisecond()) / 1'000'000.0L;
    return cyclesPerNanosecond;
}

ui64 CyclesToNanoseconds(ui64 cycles)
{
    return static_cast<ui64>(
        // Add 0.5 before truncating to round to the nearest nanosecond.
        static_cast<long double>(cycles) / GetCyclesPerNanosecond() + 0.5L);
}

struct TLatencyStats
{
    ui64 Count = 0;
    NHdr::THistogram Nanoseconds =
        NHdr::THistogram(static_cast<i64>(GetMaxTrackedNanoseconds()), 3);

    void RecordCycles(ui64 cycles)
    {
        auto nanoseconds = CyclesToNanoseconds(cycles);
        if (nanoseconds == 0) {
            nanoseconds = 1;
        }
        if (nanoseconds > GetMaxTrackedNanoseconds()) {
            nanoseconds = GetMaxTrackedNanoseconds();
        }

        ++Count;
        Y_ABORT_UNLESS(Nanoseconds.RecordValue(static_cast<i64>(nanoseconds)));
    }

    void Add(const TLatencyStats& stats)
    {
        Count += stats.Count;
        Y_ABORT_UNLESS(Nanoseconds.Add(stats.Nanoseconds) == 0);
    }
};

struct TThreadStats
{
    TLatencyStats Enqueue;
    TLatencyStats Dequeue;
    TLatencyStats Wait;
};

struct TRunStats
{
    TLatencyStats Enqueue;
    TLatencyStats Dequeue;
    TLatencyStats Wait;

    void Add(const TThreadStats& stats)
    {
        Enqueue.Add(stats.Enqueue);
        Dequeue.Add(stats.Dequeue);
        Wait.Add(stats.Wait);
    }

    void Add(const TRunStats& stats)
    {
        Enqueue.Add(stats.Enqueue);
        Dequeue.Add(stats.Dequeue);
        Wait.Add(stats.Wait);
    }
};

void PrintShortDurationValue(IOutputStream& out, double value)
{
    int digits = 0;
    if (value < 10) {
        digits = 2;
    } else if (value < 100) {
        digits = 1;
    }

    out << Prec(value, PREC_POINT_DIGITS_STRIP_ZEROES, digits);
}

void PrintHumanNanoseconds(IOutputStream& out, ui64 nanoseconds)
{
    constexpr ui64 Microsecond = 1'000;
    constexpr ui64 Millisecond = 1'000 * Microsecond;
    constexpr ui64 Second = 1'000 * Millisecond;

    if (nanoseconds < Microsecond) {
        out << nanoseconds << "ns";
        return;
    }

    if (nanoseconds < Millisecond) {
        PrintShortDurationValue(
            out,
            static_cast<double>(nanoseconds) / Microsecond);
        out << "us";
        return;
    }

    if (nanoseconds < Second) {
        PrintShortDurationValue(
            out,
            static_cast<double>(nanoseconds) / Millisecond);
        out << "ms";
        return;
    }

    if (nanoseconds < 60 * Second) {
        PrintShortDurationValue(out, static_cast<double>(nanoseconds) / Second);
        out << 's';
        return;
    }

    out << HumanReadable(TDuration::MicroSeconds(nanoseconds / Microsecond));
}

void PrintLatencyStats(
    IOutputStream& out,
    const char* name,
    const TLatencyStats& stats)
{
    const auto printValue = [&](const char* suffix, i64 nanoseconds)
    {
        out << ' ' << suffix << '=';
        if (stats.Count) {
            PrintHumanNanoseconds(out, static_cast<ui64>(nanoseconds));
        } else {
            out << '-';
        }
    };

    out << "  " << name << "_latency:";
    printValue("p50", stats.Nanoseconds.GetValueAtPercentile(50));
    printValue("p95", stats.Nanoseconds.GetValueAtPercentile(95));
    printValue("p99", stats.Nanoseconds.GetValueAtPercentile(99));
    printValue("max", stats.Nanoseconds.GetMax());
    out << Endl;
}

struct TCollectedRun
{
    ui64 Iterations = 0;
    ui64 Samples = 0;
    bool WarmupSkipped = false;
    TRunStats Stats;
    TDuration UserCpu;
    TDuration SystemCpu;
    ui64 WallCycles = 0;

    void Add(
        ui64 iterations,
        const TRunStats& stats,
        const TRusage& startedUsage,
        const TRusage& finishedUsage,
        ui64 startedCycles,
        ui64 finishedCycles)
    {
        Iterations += iterations;
        ++Samples;
        Stats.Add(stats);
        UserCpu = UserCpu + (finishedUsage.Utime - startedUsage.Utime);
        SystemCpu = SystemCpu + (finishedUsage.Stime - startedUsage.Stime);
        WallCycles += finishedCycles - startedCycles;
    }
};

TString FormatRunStats(const char* name, const TCollectedRun& run)
{
    const auto totalCpu = run.UserCpu + run.SystemCpu;
    const auto wall = CyclesToDurationSafe(run.WallCycles);

    const auto wallUs = wall.MicroSeconds();
    const auto cpuUtil =
        wallUs ? static_cast<double>(totalCpu.MicroSeconds()) / wallUs : 0.0;

    TStringStream out;
    out << "FSyncQueueBench" << " name=" << name << Endl
        << "  iterations=" << run.Iterations << Endl
        << "  operations:" << " enqueues=" << run.Stats.Enqueue.Count
        << " dequeues=" << run.Stats.Dequeue.Count
        << " waits=" << run.Stats.Wait.Count << Endl;

    PrintLatencyStats(out, "enqueue", run.Stats.Enqueue);
    PrintLatencyStats(out, "dequeue", run.Stats.Dequeue);
    PrintLatencyStats(out, "wait", run.Stats.Wait);

    out << "  cpu_time:";
    out << " user=";
    PrintHumanNanoseconds(out, run.UserCpu.NanoSeconds());
    out << " system=";
    PrintHumanNanoseconds(out, run.SystemCpu.NanoSeconds());
    out << " total=";
    PrintHumanNanoseconds(out, totalCpu.NanoSeconds());
    out << Endl;

    out << "  wall_time=";
    PrintHumanNanoseconds(out, wall.NanoSeconds());
    out << " cpu_util=" << cpuUtil;

    return out.Str();
}

struct TStatsCollector
{
    TMutex Lock;
    TString CurrentName;
    std::unique_ptr<TCollectedRun> CurrentRun;

    ~TStatsCollector()
    {
        PrintCurrentRun();
    }

    void Add(
        TString name,
        ui64 iterations,
        const TRunStats& stats,
        const TRusage& startedUsage,
        const TRusage& finishedUsage,
        ui64 startedCycles,
        ui64 finishedCycles)
    {
        with_lock (Lock) {
            if (!CurrentRun || CurrentName != name) {
                PrintCurrentRun();
                CurrentName = std::move(name);
                CurrentRun = std::make_unique<TCollectedRun>();
            }

            if (!CurrentRun->WarmupSkipped) {
                CurrentRun->WarmupSkipped = true;
                return;
            }

            CurrentRun->Add(
                iterations,
                stats,
                startedUsage,
                finishedUsage,
                startedCycles,
                finishedCycles);
        }
    }

private:
    void PrintCurrentRun()
    {
        if (CurrentRun && CurrentRun->Samples) {
            Cerr << FormatRunStats(CurrentName.data(), *CurrentRun) << Endl;
        }
        CurrentRun.reset();
    }
};

void CollectRunStats(
    const char* name,
    ui64 iterations,
    const TRunStats& stats,
    const TRusage& startedUsage,
    const TRusage& finishedUsage,
    ui64 startedCycles,
    ui64 finishedCycles)
{
    Singleton<TStatsCollector>()->Add(
        name,
        iterations,
        stats,
        startedUsage,
        finishedUsage,
        startedCycles,
        finishedCycles);
}

TNodeId GetNodeId(ui32 threadIndex, ui64 offset = 0)
{
    return TNodeId{1 + threadIndex * IdsPerThread + offset};
}

THandle GetHandle(ui32 threadIndex, ui64 offset = 0)
{
    return THandle{1 + threadIndex * IdsPerThread + offset};
}

ui64 GetThreadIterations(ui64 iterations, ui32 threadIndex, ui32 threadCount)
{
    // Split the benchmark framework iteration count evenly across workers while
    // preserving the exact total number of iterations.
    const ui64 base = iterations / threadCount;
    const ui64 extra = threadIndex < iterations % threadCount ? 1 : 0;
    return base + extra;
}

void WaitFuture(
    NThreading::TFuture<NProto::TError> future,
    ui64 startedCycles,
    TThreadStats& stats)
{
    const auto error = future.GetValueSync();
    Y_ABORT_UNLESS(!HasError(error));
    stats.Wait.RecordCycles(GetCycleCount() - startedCycles);
}

struct TPendingRequest
{
    IFSyncQueue::TRequestId ReqId = 0;
    TNodeId NodeId;
    THandle Handle;
};

TPendingRequest EnqueueDataRequest(
    TFSyncQueue& queue,
    std::atomic<IFSyncQueue::TRequestId>& requestId,
    TThreadStats& stats,
    TNodeId nodeId,
    THandle handle)
{
    const auto reqId = requestId.fetch_add(1, std::memory_order_relaxed);

    const auto startedCycles = GetCycleCount();
    queue.Enqueue(reqId, nodeId, handle);
    stats.Enqueue.RecordCycles(GetCycleCount() - startedCycles);

    return {
        .ReqId = reqId,
        .NodeId = nodeId,
        .Handle = handle,
    };
}

TPendingRequest EnqueueMetaRequest(
    TFSyncQueue& queue,
    std::atomic<IFSyncQueue::TRequestId>& requestId,
    TThreadStats& stats,
    TNodeId nodeId)
{
    const auto reqId = requestId.fetch_add(1, std::memory_order_relaxed);

    const auto startedCycles = GetCycleCount();
    queue.Enqueue(reqId, nodeId);
    stats.Enqueue.RecordCycles(GetCycleCount() - startedCycles);

    return {
        .ReqId = reqId,
        .NodeId = nodeId,
    };
}

void DequeueRequest(
    TFSyncQueue& queue,
    const TPendingRequest& request,
    TThreadStats& stats)
{
    const auto startedCycles = GetCycleCount();
    queue.Dequeue(request.ReqId, {}, request.NodeId, request.Handle);
    stats.Dequeue.RecordCycles(GetCycleCount() - startedCycles);
}

void CompleteLastRequest(
    TFSyncQueue& queue,
    TVector<TPendingRequest>& pending,
    TThreadStats& stats)
{
    auto request = pending.back();
    pending.pop_back();

    DequeueRequest(queue, request, stats);
}

void CompletePendingRequests(
    TFSyncQueue& queue,
    TVector<TPendingRequest>& pending,
    TThreadStats& stats)
{
    while (!pending.empty()) {
        CompleteLastRequest(queue, pending, stats);
    }
}

void RunDataEnqDeq(
    TFSyncQueue& queue,
    std::atomic<IFSyncQueue::TRequestId>& requestId,
    TThreadStats& stats,
    ui32 threadIndex,
    ui64 iterations)
{
    const auto nodeId = GetNodeId(threadIndex);
    const auto handle = GetHandle(threadIndex);

    for (ui64 i = 0; i < iterations; ++i) {
        auto request =
            EnqueueDataRequest(queue, requestId, stats, nodeId, handle);
        DequeueRequest(queue, request, stats);
    }
}

void RunMetaEnqDeq(
    TFSyncQueue& queue,
    std::atomic<IFSyncQueue::TRequestId>& requestId,
    TThreadStats& stats,
    ui32 threadIndex,
    ui64 iterations)
{
    const auto nodeId = GetNodeId(threadIndex);

    for (ui64 i = 0; i < iterations; ++i) {
        auto request = EnqueueMetaRequest(queue, requestId, stats, nodeId);
        DequeueRequest(queue, request, stats);
    }
}

void RunDataMixedEnqDeqLocalWait(
    TFSyncQueue& queue,
    std::atomic<IFSyncQueue::TRequestId>& requestId,
    TThreadStats& stats,
    ui32 threadIndex,
    ui64 iterations,
    ui32 iterationsBeforeWait)
{
    Y_ABORT_UNLESS(iterationsBeforeWait);

    const auto nodeId = GetNodeId(threadIndex);
    const auto handle = GetHandle(threadIndex);

    TVector<TPendingRequest> pending;
    pending.reserve(MaxPendingRequests);

    for (ui64 i = 0; i < iterations; ++i) {
        const bool shouldWait = (i + 1) % iterationsBeforeWait == 0;

        if (shouldWait) {
            const auto reqId =
                requestId.fetch_add(1, std::memory_order_relaxed);
            const auto startedCycles = GetCycleCount();
            auto future = queue.WaitForDataRequests(reqId, nodeId, handle);

            CompletePendingRequests(queue, pending, stats);
            WaitFuture(std::move(future), startedCycles, stats);

            continue;
        }

        pending.push_back(
            EnqueueDataRequest(queue, requestId, stats, nodeId, handle));

        if (pending.size() == MaxPendingRequests) {
            CompleteLastRequest(queue, pending, stats);
        }
    }

    CompletePendingRequests(queue, pending, stats);
}

void RunMetaMixedEnqDeqLocalWait(
    TFSyncQueue& queue,
    std::atomic<IFSyncQueue::TRequestId>& requestId,
    TThreadStats& stats,
    ui32 threadIndex,
    ui64 iterations,
    ui32 iterationsBeforeWait)
{
    Y_ABORT_UNLESS(iterationsBeforeWait);

    const auto nodeId = GetNodeId(threadIndex);

    TVector<TPendingRequest> pending;
    pending.reserve(MaxPendingRequests);

    for (ui64 i = 0; i < iterations; ++i) {
        const bool shouldWait = (i + 1) % iterationsBeforeWait == 0;

        if (shouldWait) {
            const auto reqId =
                requestId.fetch_add(1, std::memory_order_relaxed);
            const auto startedCycles = GetCycleCount();
            auto future = queue.WaitForRequests(reqId, nodeId);

            CompletePendingRequests(queue, pending, stats);
            WaitFuture(std::move(future), startedCycles, stats);

            continue;
        }

        pending.push_back(EnqueueMetaRequest(queue, requestId, stats, nodeId));

        if (pending.size() == MaxPendingRequests) {
            CompleteLastRequest(queue, pending, stats);
        }
    }

    CompletePendingRequests(queue, pending, stats);
}

void RunDataMixedEnqDeqGlobalWait(
    TFSyncQueue& queue,
    std::atomic<IFSyncQueue::TRequestId>& requestId,
    TThreadStats& stats,
    ui32 threadIndex,
    ui64 iterations,
    ui32 iterationsBeforeWait)
{
    Y_ABORT_UNLESS(iterationsBeforeWait);

    TVector<TPendingRequest> pending;
    pending.reserve(MaxPendingRequests);

    for (ui64 i = 0; i < iterations; ++i) {
        const bool shouldWait = (i + 1) % iterationsBeforeWait == 0;

        if (shouldWait) {
            const auto reqId =
                requestId.fetch_add(1, std::memory_order_relaxed);
            const auto startedCycles = GetCycleCount();
            auto future = queue.WaitForDataRequests(reqId);

            CompletePendingRequests(queue, pending, stats);
            WaitFuture(std::move(future), startedCycles, stats);

            continue;
        }

        const ui64 idOffset = 1 + i % MaxPendingRequests;
        pending.push_back(EnqueueDataRequest(
            queue,
            requestId,
            stats,
            GetNodeId(threadIndex, idOffset),
            GetHandle(threadIndex, idOffset)));

        if (pending.size() == MaxPendingRequests) {
            CompleteLastRequest(queue, pending, stats);
        }
    }

    CompletePendingRequests(queue, pending, stats);
}

void RunMetaMixedEnqDeqGlobalWait(
    TFSyncQueue& queue,
    std::atomic<IFSyncQueue::TRequestId>& requestId,
    TThreadStats& stats,
    ui32 threadIndex,
    ui64 iterations,
    ui32 iterationsBeforeWait)
{
    Y_ABORT_UNLESS(iterationsBeforeWait);

    TVector<TPendingRequest> pending;
    pending.reserve(MaxPendingRequests);

    for (ui64 i = 0; i < iterations; ++i) {
        const bool shouldWait = (i + 1) % iterationsBeforeWait == 0;

        if (shouldWait) {
            const auto reqId =
                requestId.fetch_add(1, std::memory_order_relaxed);
            const auto startedCycles = GetCycleCount();
            auto future = queue.WaitForRequests(reqId);

            CompletePendingRequests(queue, pending, stats);
            WaitFuture(std::move(future), startedCycles, stats);

            continue;
        }

        const ui64 idOffset = 1 + i % MaxPendingRequests;
        pending.push_back(EnqueueMetaRequest(
            queue,
            requestId,
            stats,
            GetNodeId(threadIndex, idOffset)));

        if (pending.size() == MaxPendingRequests) {
            CompleteLastRequest(queue, pending, stats);
        }
    }

    CompletePendingRequests(queue, pending, stats);
}

void RunWorker(
    EScenario scenario,
    TFSyncQueue& queue,
    std::atomic<IFSyncQueue::TRequestId>& requestId,
    TThreadStats& stats,
    ui32 threadIndex,
    ui64 iterations,
    ui32 iterationsBeforeWait)
{
    switch (scenario) {
        case EScenario::DataEnqDeq:
            RunDataEnqDeq(queue, requestId, stats, threadIndex, iterations);
            break;

        case EScenario::MetaEnqDeq:
            RunMetaEnqDeq(queue, requestId, stats, threadIndex, iterations);
            break;

        case EScenario::DataMixedEnqDeqLocalWait:
            RunDataMixedEnqDeqLocalWait(
                queue,
                requestId,
                stats,
                threadIndex,
                iterations,
                iterationsBeforeWait);
            break;

        case EScenario::MetaMixedEnqDeqLocalWait:
            RunMetaMixedEnqDeqLocalWait(
                queue,
                requestId,
                stats,
                threadIndex,
                iterations,
                iterationsBeforeWait);
            break;

        case EScenario::DataMixedEnqDeqGlobalWait:
            RunDataMixedEnqDeqGlobalWait(
                queue,
                requestId,
                stats,
                threadIndex,
                iterations,
                iterationsBeforeWait);
            break;

        case EScenario::MetaMixedEnqDeqGlobalWait:
            RunMetaMixedEnqDeqGlobalWait(
                queue,
                requestId,
                stats,
                threadIndex,
                iterations,
                iterationsBeforeWait);
            break;
    }
}

void RunFSyncQueueBench(
    const char* name,
    ui64 iterations,
    ui32 threadCount,
    EScenario scenario,
    ui32 iterationsBeforeWait = 0)
{
    if (iterations == 0) {
        return;
    }

    TFSyncQueue queue(FileSystemId, GetLogging());
    std::atomic<IFSyncQueue::TRequestId> requestId = 1;

    std::latch start{static_cast<std::ptrdiff_t>(threadCount + 1)};
    TVector<std::thread> threads;
    threads.reserve(threadCount);
    TVector<std::unique_ptr<TThreadStats>> threadStats;
    threadStats.reserve(threadCount);

    for (ui32 threadIndex = 0; threadIndex < threadCount; ++threadIndex) {
        threadStats.emplace_back(std::make_unique<TThreadStats>());
        auto* stats = threadStats.back().get();

        threads.emplace_back(
            [&queue,
             &requestId,
             &start,
             stats,
             scenario,
             threadIndex,
             iterationsBeforeWait,
             threadIterations =
                 GetThreadIterations(iterations, threadIndex, threadCount)]
            {
                start.arrive_and_wait();

                RunWorker(
                    scenario,
                    queue,
                    requestId,
                    *stats,
                    threadIndex,
                    threadIterations,
                    iterationsBeforeWait);
            });
    }

    GetMaxTrackedNanoseconds();
    GetCyclesPerNanosecond();
    const auto startedUsage = TRusage::Get();
    const auto startedCycles = GetCycleCount();

    start.arrive_and_wait();

    for (auto& thread: threads) {
        thread.join();
    }

    const auto finishedCycles = GetCycleCount();
    const auto finishedUsage = TRusage::Get();

    TRunStats stats;
    for (const auto& threadStat: threadStats) {
        stats.Add(*threadStat);
    }

    CollectRunStats(
        name,
        iterations,
        stats,
        startedUsage,
        finishedUsage,
        startedCycles,
        finishedCycles);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

#define FSYNC_QUEUE_BENCHMARK(name, scenario, threadCount)     \
    Y_CPU_BENCHMARK(TFSyncQueue_##name##_##threadCount, iface) \
    {                                                          \
        RunFSyncQueueBench(                                    \
            "TFSyncQueue_" #name "_" #threadCount,             \
            iface.Iterations(),                                \
            threadCount,                                       \
            EScenario::scenario);                              \
    }                                                          \
    // FSYNC_QUEUE_BENCHMARK

#define FSYNC_QUEUE_MIXED_BENCHMARK(                                               \
    name,                                                                          \
    scenario,                                                                      \
    iterationsBeforeWait,                                                          \
    threadCount)                                                                   \
    Y_CPU_BENCHMARK(                                                               \
        TFSyncQueue_##name##After##iterationsBeforeWait##Iterations_##threadCount, \
        iface)                                                                     \
    {                                                                              \
        RunFSyncQueueBench(                                                        \
            "TFSyncQueue_" #name "After" #iterationsBeforeWait                     \
            "Iterations_" #threadCount,                                            \
            iface.Iterations(),                                                    \
            threadCount,                                                           \
            EScenario::scenario,                                                   \
            iterationsBeforeWait);                                                 \
    }                                                                              \
    // FSYNC_QUEUE_MIXED_BENCHMARK

#define FSYNC_QUEUE_BENCHMARK_SET(name, scenario) \
    FSYNC_QUEUE_BENCHMARK(name, scenario, 1)      \
    FSYNC_QUEUE_BENCHMARK(name, scenario, 2)      \
    FSYNC_QUEUE_BENCHMARK(name, scenario, 4)      \
    FSYNC_QUEUE_BENCHMARK(name, scenario, 8)      \
    FSYNC_QUEUE_BENCHMARK(name, scenario, 16)     \
    // FSYNC_QUEUE_BENCHMARK_SET

#define FSYNC_QUEUE_MIXED_BENCHMARK_SET(name, scenario, iterationsBeforeWait) \
    FSYNC_QUEUE_MIXED_BENCHMARK(name, scenario, iterationsBeforeWait, 1)      \
    FSYNC_QUEUE_MIXED_BENCHMARK(name, scenario, iterationsBeforeWait, 2)      \
    FSYNC_QUEUE_MIXED_BENCHMARK(name, scenario, iterationsBeforeWait, 4)      \
    FSYNC_QUEUE_MIXED_BENCHMARK(name, scenario, iterationsBeforeWait, 8)      \
    FSYNC_QUEUE_MIXED_BENCHMARK(name, scenario, iterationsBeforeWait, 16)     \
    // FSYNC_QUEUE_MIXED_BENCHMARK_SET

FSYNC_QUEUE_BENCHMARK_SET(DataEnqDeq, DataEnqDeq)
FSYNC_QUEUE_BENCHMARK_SET(MetaEnqDeq, MetaEnqDeq)

FSYNC_QUEUE_MIXED_BENCHMARK_SET(
    DataMixedEnqDeqLocalWait,
    DataMixedEnqDeqLocalWait,
    64)

FSYNC_QUEUE_MIXED_BENCHMARK_SET(
    MetaMixedEnqDeqLocalWait,
    MetaMixedEnqDeqLocalWait,
    64)

FSYNC_QUEUE_MIXED_BENCHMARK_SET(
    DataMixedEnqDeqGlobalWait,
    DataMixedEnqDeqGlobalWait,
    64)

FSYNC_QUEUE_MIXED_BENCHMARK_SET(
    MetaMixedEnqDeqGlobalWait,
    MetaMixedEnqDeqGlobalWait,
    64)

#undef FSYNC_QUEUE_MIXED_BENCHMARK_SET
#undef FSYNC_QUEUE_BENCHMARK_SET
#undef FSYNC_QUEUE_MIXED_BENCHMARK
#undef FSYNC_QUEUE_BENCHMARK

}   // namespace NCloud::NFileStore::NVFS
