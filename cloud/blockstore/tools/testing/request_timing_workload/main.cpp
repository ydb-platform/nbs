// The same source is built against the previous and current product libraries.
// This measures the in-process service path, not transport or device performance.
#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/dumpable.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service_method.h>
#include <cloud/blockstore/libs/service/split_request_service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/datetime/cputimer.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <exception>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>
#include <sys/resource.h>

namespace NCloud::NBlockStore {
namespace {
using namespace NThreading;
using TClock = std::chrono::steady_clock;
constexpr ui64 BlockSize = 4096;
constexpr ui64 FlushBatchSize = 64;

struct TOptions
{
    TString ProfilePath;
    bool Read;
    ui64 Bytes;
    ui32 Parts;
    ui32 Concurrency;
    ui64 Iterations;
    i64 WaitUs;
    i64 ErrorPart;
};

void Require(bool condition, const char* message)
{
    if (!condition) {
        throw std::runtime_error(message);
    }
}

i64 Number(const char* argument)
{
    size_t consumed = 0;
    const std::string value(argument);
    const i64 result = std::stoll(value, &consumed);
    Require(consumed == value.size(), "Invalid integer argument");
    return result;
}

TOptions ParseOptions(int argc, char** argv)
{
    Require(argc == 9,
        "Usage: nbs-request-timing-workload PROFILE read|write "
        "BYTES PARTS CONCURRENCY ITERATIONS WAIT_US ERROR_PART");
    const std::string operation(argv[2]);
    Require(operation == "read" || operation == "write", "Invalid operation");
    const auto bytes = Number(argv[3]);
    const auto parts = Number(argv[4]);
    const auto concurrency = Number(argv[5]);
    const auto iterations = Number(argv[6]);
    const auto wait = Number(argv[7]);
    const auto error = Number(argv[8]);
    Require(bytes >= 4096 && bytes <= 4194304 &&
            parts >= 1 && parts <= 16 &&
            bytes % (parts * BlockSize) == 0 &&
            concurrency >= 1 && concurrency <= 64 &&
            iterations >= 1 && iterations <= 1000000 / concurrency &&
            wait >= -1 && wait <= 1000000 &&
            error >= -1 && error < parts,
        "Arguments outside workload limits");
    return {argv[1], operation == "read", static_cast<ui64>(bytes),
            static_cast<ui32>(parts), static_cast<ui32>(concurrency),
            static_cast<ui64>(iterations), wait, error};
}

double Seconds(TClock::duration duration)
{
    return std::chrono::duration<double>(duration).count();
}

rusage Usage()
{
    rusage result{};
    Require(getrusage(RUSAGE_SELF, &result) == 0, "getrusage failed");
    return result;
}

double CpuSeconds(const timeval& value)
{
    return value.tv_sec + value.tv_usec / 1000000.0;
}

struct TDumpable final: IDumpable
{
    void Dump(IOutputStream&) const override {}
    void DumpHtml(IOutputStream&) const override {}
};

class TBackend final: public TBlockStoreImpl<TBackend, IBlockStore>
{
    struct TPending
    {
        TCallContextPtr Context;
        std::function<void(bool)> Complete;
    };
    const TOptions Options;
    std::vector<char> Data;
    std::vector<TPending> Pending;

public:
    ui64 Calls = 0;

    explicit TBackend(const TOptions& options)
        : Options(options)
        , Data(options.Bytes, options.Read ? 'x' : '\0')
    {}

    TStorageBuffer AllocateBuffer(size_t) override { return nullptr; }
    void Start() override {}
    void Stop() override {}

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr,
        std::shared_ptr<typename TMethod::TRequest>)
    {
        return MakeFuture<typename TMethod::TResponse>();
    }

    TFuture<NProto::TMountVolumeResponse> MountVolume(
        TCallContextPtr,
        std::shared_ptr<NProto::TMountVolumeRequest> request) override
    {
        NProto::TMountVolumeResponse response;
        auto& volume = *response.MutableVolume();
        volume.SetDiskId(request->GetDiskId());
        volume.SetBlockSize(BlockSize);
        volume.SetBlocksCount(Options.Bytes / BlockSize);
        volume.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
        volume.MutableDevices()->Add()->SetBlockCount(
            Options.Bytes / Options.Parts / BlockSize);
        return MakeFuture(std::move(response));
    }

    TFuture<NProto::TReadBlocksResponse> ReadBlocks(
        TCallContextPtr context,
        std::shared_ptr<NProto::TReadBlocksRequest> request) override
    {
        ++Calls;
        auto promise = NewPromise<NProto::TReadBlocksResponse>();
        auto future = promise.GetFuture();
        Pending.push_back({
            std::move(context),
            [this, request = std::move(request), promise](bool inject) mutable {
                NProto::TReadBlocksResponse response;
                const ui64 first = request->GetStartIndex();
                const ui64 count = request->GetBlocksCount();
                const ui64 capacity = Data.size() / BlockSize;
                Require(first <= capacity && count <= capacity - first,
                        "Read exceeds backend");
                if (inject) {
                    *response.MutableError() = MakeError(E_REJECTED, "injected");
                } else {
                    for (ui64 i = 0; i < count; ++i) {
                        response.MutableBlocks()->AddBuffers()->assign(
                            Data.data() + (first + i) * BlockSize, BlockSize);
                    }
                }
                promise.SetValue(std::move(response));
            }});
        return future;
    }

    TFuture<NProto::TWriteBlocksResponse> WriteBlocks(
        TCallContextPtr context,
        std::shared_ptr<NProto::TWriteBlocksRequest> request) override
    {
        ++Calls;
        auto promise = NewPromise<NProto::TWriteBlocksResponse>();
        auto future = promise.GetFuture();
        Pending.push_back({
            std::move(context),
            [this, request = std::move(request), promise](bool inject) mutable {
                NProto::TWriteBlocksResponse response;
                const ui64 first = request->GetStartIndex();
                const auto& buffers = request->GetBlocks().GetBuffers();
                const ui64 count = static_cast<ui64>(buffers.size());
                const ui64 capacity = Data.size() / BlockSize;
                Require(first <= capacity && count <= capacity - first,
                        "Write exceeds backend");
                if (inject) {
                    *response.MutableError() = MakeError(E_REJECTED, "injected");
                } else {
                    ui64 index = first;
                    for (const auto& buffer: buffers) {
                        Require(buffer.size() == BlockSize &&
                                std::all_of(buffer.begin(), buffer.end(),
                                            [](char c) { return c == 'x'; }),
                                "Invalid write payload");
                        std::copy(buffer.begin(), buffer.end(),
                                  Data.begin() + index * BlockSize);
                        ++index;
                    }
                }
                promise.SetValue(std::move(response));
            }});
        return future;
    }

    void Drain()
    {
        Require(Pending.size() == Options.Parts, "Incorrect split count");
        auto pending = std::move(Pending);
        Pending.clear();
        if (Options.WaitUs > 0) {
            for (auto& item: pending) {
                item.Context->Postpone(GetCycleCount());
            }
            // All parts enter their waits before this single scheduled delay.
            std::this_thread::sleep_for(
                std::chrono::microseconds(Options.WaitUs));
        } else if (Options.WaitUs == -1) {
            for (auto& item: pending) {
                item.Context->AddTime(
                    EProcessingStage::Shaping, TDuration::MicroSeconds(1));
            }
        }
        for (size_t i = 0; i < pending.size(); ++i) {
            if (Options.WaitUs > 0) {
                pending[i].Context->Advance(GetCycleCount());
            }
            pending[i].Complete(static_cast<i64>(i) == Options.ErrorPart);
        }
        if (!Options.Read && Options.ErrorPart < 0) {
            Require(std::all_of(Data.begin(), Data.end(),
                                [](char c) { return c == 'x'; }),
                    "Backend data differs after successful write");
        }
        Require(Pending.empty(), "Unexpected backend work after drain");
    }
};

class TWorker
{
    const TOptions Options;
    const ui64 IdPrefix;
    ui64 Sequence = 0;
    std::shared_ptr<TBackend> Backend;
    IBlockStorePtr Split;
    IServerStatsPtr Stats;

    template <typename TResponse>
    void Finish(const TFuture<TResponse>& future, TMetricRequest& metric,
                const TCallContextPtr& context, TLog& log,
                TClock::time_point started, bool measured)
    {
        bool done = false;
        std::exception_ptr failure;
        future.Subscribe([&](const TFuture<TResponse>& completed) {
            try {
                const auto& response = completed.GetValue();
                Stats->RequestCompleted(log, metric, *context, response.GetError());
                const double latency = Seconds(TClock::now() - started) * 1e6;
                const bool error = HasError(response.GetError());
                Require(error == (Options.ErrorPart >= 0),
                        "Unexpected root response outcome");
                if constexpr (std::is_same_v<TResponse, NProto::TReadBlocksResponse>) {
                    if (!error) {
                        ui64 bytes = 0;
                        for (const auto& buffer: response.GetBlocks().GetBuffers()) {
                            bytes += buffer.size();
                            Require(std::all_of(buffer.begin(), buffer.end(),
                                                [](char c) { return c == 'x'; }),
                                    "Invalid read payload");
                        }
                        Require(bytes == Options.Bytes, "Incorrect read size");
                    }
                }
                if (measured) {
                    Latencies.push_back(latency);
                    Errors += error;
                }
            } catch (...) {
                failure = std::current_exception();
            }
            done = true;
        });
        // Backend promises and callbacks execute on this worker only. The
        // callback observes an early error before the remaining children drain.
        Backend->Drain();
        Require(done, "Root future did not finish after drain");
        if (failure) {
            std::rethrow_exception(failure);
        }
    }

public:
    std::vector<double> Latencies;
    ui64 Errors = 0;

    TWorker(const TOptions& options, ui32 index, const IProfileLogPtr& profile)
        : Options(options)
        , IdPrefix((static_cast<ui64>(index) + 1) << 48)
        , Backend(std::make_shared<TBackend>(options))
        , Split(CreateSplitRequestService(Backend))
    {
        Latencies.reserve(options.Iterations);
        auto monitoring = CreateMonitoringServiceStub();
        auto timer = CreateWallClockTimer();
        auto group = monitoring->GetCounters()->GetSubgroup("counters", "blockstore");
        Stats = CreateServerStats(
            std::make_shared<TDumpable>(), std::make_shared<TDiagnosticsConfig>(),
            monitoring, profile,
            CreateServerRequestStats(
                group, timer, EHistogramCounterOption::ReportMultipleCounters, {}),
            CreateVolumeStats(monitoring, {}, EVolumeStatsType::EServerStats, timer));
        Split->Start();
        auto request = std::make_shared<NProto::TMountVolumeRequest>();
        request->SetDiskId("volume");
        const auto response = Split->MountVolume(
            CreateCallContext(IdPrefix), std::move(request)).GetValue();
        Require(!HasError(response.GetError()), "Mount failed");
    }

    ~TWorker() { Split->Stop(); }
    ui64 Calls() const { return Backend->Calls; }

    void Run(ui64 count, bool measured)
    {
        for (ui64 i = 0; i < count; ++i) {
            auto context = CreateCallContext(IdPrefix | ++Sequence);
            TMetricRequest metric(Options.Read ? EBlockStoreRequest::ReadBlocks
                                              : EBlockStoreRequest::WriteBlocks);
            TLog log;
            Stats->PrepareMetricRequest(
                metric, "client", "volume", 0, Options.Bytes, false);
            if (Options.Read) {
                auto request = std::make_shared<NProto::TReadBlocksRequest>();
                request->SetDiskId("volume");
                request->SetStartIndex(0);
                request->SetBlocksCount(Options.Bytes / BlockSize);
                const auto started = TClock::now();
                Stats->RequestStarted(log, metric, *context, "timing-workload");
                Finish(Split->ReadBlocks(context, std::move(request)),
                       metric, context, log, started, measured);
            } else {
                auto request = std::make_shared<NProto::TWriteBlocksRequest>();
                request->SetDiskId("volume");
                request->SetStartIndex(0);
                for (ui64 block = 0; block < Options.Bytes / BlockSize; ++block) {
                    request->MutableBlocks()->AddBuffers()->assign(BlockSize, 'x');
                }
                const auto started = TClock::now();
                Stats->RequestStarted(log, metric, *context, "timing-workload");
                Finish(Split->WriteBlocks(context, std::move(request)),
                       metric, context, log, started, measured);
            }
        }
    }
};

class TPool
{
    std::vector<std::thread> Threads;
    std::mutex Mutex;
    std::condition_variable Start;
    std::condition_variable Done;
    ui64 Generation = 0;
    ui64 Count = 0;
    size_t Completed = 0;
    bool Measured = false;
    bool Stopping = false;
    std::exception_ptr Failure;

    void ThreadMain(size_t index)
    {
        ui64 seen = 0;
        for (;;) {
            std::unique_lock lock(Mutex);
            Start.wait(lock, [&] { return Stopping || Generation != seen; });
            if (Stopping) {
                return;
            }
            seen = Generation;
            const auto count = Count;
            const bool measured = Measured;
            lock.unlock();
            try {
                Workers[index]->Run(count, measured);
            } catch (...) {
                lock.lock();
                if (!Failure) {
                    Failure = std::current_exception();
                }
                lock.unlock();
            }
            lock.lock();
            if (++Completed == Workers.size()) {
                Done.notify_one();
            }
        }
    }

    void Stop()
    {
        {
            std::lock_guard lock(Mutex);
            Stopping = true;
        }
        Start.notify_all();
        for (auto& thread: Threads) {
            thread.join();
        }
    }

public:
    std::vector<std::unique_ptr<TWorker>> Workers;

    TPool(const TOptions& options, const IProfileLogPtr& profile)
    {
        for (ui32 i = 0; i < options.Concurrency; ++i) {
            Workers.push_back(std::make_unique<TWorker>(options, i, profile));
        }
        try {
            for (size_t i = 0; i < Workers.size(); ++i) {
                Threads.emplace_back([this, i] { ThreadMain(i); });
            }
        } catch (...) {
            Stop();
            throw;
        }
    }

    ~TPool() { Stop(); }

    void Run(ui64 count, bool measured)
    {
        std::unique_lock lock(Mutex);
        Count = count;
        Measured = measured;
        Completed = 0;
        ++Generation;
        Start.notify_all();
        Done.wait(lock, [&] { return Completed == Workers.size(); });
        if (Failure) {
            std::rethrow_exception(Failure);
        }
    }

    ui64 Calls() const
    {
        ui64 total = 0;
        for (const auto& worker: Workers) {
            total += worker->Calls();
        }
        return total;
    }
};

double Percentile(const std::vector<double>& sorted, double fraction)
{
    return sorted[static_cast<size_t>(std::ceil(fraction * sorted.size())) - 1];
}

int Run(const TOptions& options)
{
    auto profile = CreateProfileLog(
        TProfileLogSettings{options.ProfilePath, TDuration::Seconds(1)},
        CreateWallClockTimer(), CreateSchedulerStub());
    profile->Start();
    try {
        TPool pool(options, profile);
        const ui64 warmup = std::min<ui64>(options.Iterations, 20);
        pool.Run(warmup, false);
        Require(profile->Flush(), "Warmup profile flush failed");
        const ui64 callsBefore = pool.Calls();
        const ui64 warmupOperations = warmup * options.Concurrency;
        Require(callsBefore == warmupOperations * options.Parts,
                "Incorrect warmup split count");
        const auto usageBefore = Usage();
        const auto started = TClock::now();
        double drain = 0;
        for (ui64 remaining = options.Iterations; remaining;) {
            const ui64 count = std::min(remaining, FlushBatchSize);
            pool.Run(count, true);
            const auto flushStart = TClock::now();
            Require(profile->Flush(), "Measured profile flush failed");
            drain += Seconds(TClock::now() - flushStart);
            remaining -= count;
        }
        const double wall = Seconds(TClock::now() - started);
        const auto usageAfter = Usage();
        const ui64 operations = options.Iterations * options.Concurrency;
        const ui64 calls = pool.Calls() - callsBefore;
        std::vector<double> latencies;
        ui64 errors = 0;
        for (const auto& worker: pool.Workers) {
            errors += worker->Errors;
            latencies.insert(latencies.end(),
                             worker->Latencies.begin(), worker->Latencies.end());
        }
        Require(latencies.size() == operations && calls == operations * options.Parts &&
                errors == (options.ErrorPart >= 0 ? operations : 0) && wall > 0,
                "Measured results disagree");
        std::sort(latencies.begin(), latencies.end());
        profile->Stop();
        std::cout << std::fixed << std::setprecision(9)
            << "{\"iterations\":" << options.Iterations
            << ",\"operations\":" << operations
            << ",\"warmup_operations\":" << warmupOperations
            << ",\"backend_calls\":" << calls
            << ",\"profile_records_total\":" << operations + warmupOperations
            << ",\"expected_errors\":" << (options.ErrorPart >= 0 ? operations : 0)
            << ",\"error_count\":" << errors
            << ",\"p50_us\":" << Percentile(latencies, 0.50)
            << ",\"p95_us\":" << Percentile(latencies, 0.95)
            << ",\"p99_us\":" << Percentile(latencies, 0.99)
            << ",\"wall_seconds\":" << wall
            << ",\"operations_per_second\":" << operations / wall
            << ",\"mib_per_second\":"
            << (static_cast<double>(operations) * options.Bytes / 1048576.0) / wall
            << ",\"user_cpu_seconds\":"
            << CpuSeconds(usageAfter.ru_utime) - CpuSeconds(usageBefore.ru_utime)
            << ",\"system_cpu_seconds\":"
            << CpuSeconds(usageAfter.ru_stime) - CpuSeconds(usageBefore.ru_stime)
            << ",\"peak_rss_kib\":" << usageAfter.ru_maxrss
            << ",\"drain_seconds\":" << drain << "}\n";
        return 0;
    } catch (...) {
        profile->Stop();
        throw;
    }
}
} // namespace

int RunRequestTimingWorkload(int argc, char** argv)
{
    return Run(ParseOptions(argc, argv));
}
} // namespace NCloud::NBlockStore

int main(int argc, char** argv)
{
    try {
        return NCloud::NBlockStore::RunRequestTimingWorkload(argc, argv);
    } catch (const std::exception& error) {
        std::cerr << "request_timing_workload: " << error.what() << '\n';
        return 1;
    }
}
