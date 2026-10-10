#include <cloud/filestore/libs/storage/fastshard/storage_group/storage_group.h>
#include <cloud/filestore/libs/storage/fastshard/storage_group/storage_group_quorum.h>

#include <cloud/fastshard/bootstrap/core.h>
#include <cloud/fastshard/sn/iface/storage_node.h>
#include <cloud/fastshard/testlib/delay_policy.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/generic/buffer.h>
#include <util/generic/size_literals.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/join.h>
#include <util/string/split.h>
#include <util/system/hp_timer.h>

#include <benchmark/benchmark.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>
#include <silk/util/logger.h>

#include <memory>

#include <sched.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

using namespace NCloud::NFastShard;

using silk::FiberFuture;
using silk::FiberScheduler;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    ui32 Threads = 8;
    ui32 PageSize = 4_KB;
    ui32 PageCount = 2;
    ui32 DeviceCount = 3;
    ui64 RequestsPerFiber = 1lu << 16;
    TDuration DelayMean = DefaultStorageDelayMean;
    TDuration DelayStddev = DefaultStorageDelayStdDev;
    TVector<ui32> IoDepths = {1, 8, 32, 64, 128};
};

NLastGetopt::TOpts MakeOpts(TOptions& options)
{
    auto opts = NLastGetopt::TOpts::Default();
    opts.AddLongOption("threads", "silk scheduler threads")
        .RequiredArgument("N")
        .DefaultValue(options.Threads)
        .StoreResult(&options.Threads);
    opts.AddLongOption("page-size", "bytes per page")
        .RequiredArgument("N")
        .DefaultValue(options.PageSize)
        .StoreResult(&options.PageSize);
    opts.AddLongOption("page-count", "pages per request")
        .RequiredArgument("N")
        .DefaultValue(options.PageCount)
        .StoreResult(&options.PageCount);
    opts.AddLongOption("devices", "devices in the group")
        .RequiredArgument("N")
        .DefaultValue(options.DeviceCount)
        .StoreResult(&options.DeviceCount);
    opts.AddLongOption("requests", "requests per fiber, iodepth times per run")
        .RequiredArgument("N")
        .DefaultValue(options.RequestsPerFiber)
        .StoreResult(&options.RequestsPerFiber);
    opts.AddLongOption("delay-mean", "lognormal device response delay")
        .RequiredArgument("DURATION")
        .DefaultValue(options.DelayMean)
        .StoreResult(&options.DelayMean);
    opts.AddLongOption("delay-stddev", "of the device response delay")
        .RequiredArgument("DURATION")
        .DefaultValue(options.DelayStddev)
        .StoreResult(&options.DelayStddev);
    opts.AddLongOption("iodepths", "requests in flight, one run each")
        .RequiredArgument("N,N,...")
        .DefaultValue(JoinSeq(",", options.IoDepths))
        // SplitHandler would append to the preset list
        .Handler1T<TString>([&options](const TString& depths) {
            options.IoDepths.clear();
            StringSplitter(depths).Split(',').ParseInto(&options.IoDepths);
        });
    opts.SetFreeArgsNum(0);
    return opts;
}

////////////////////////////////////////////////////////////////////////////////

struct TNullStorageNode final: IStorageNode
{
    const TString Page;
    IDelayPolicyPtr Delay = CreateZeroDelayPolicy();

     TNullStorageNode(const TOptions& options)
        : Page(options.PageSize, '\0')
     {
         if (options.DelayMean != TDuration::Zero()) {
             Delay = CreateLognormalDelayPolicy(
                options.DelayMean,
                options.DelayStddev);

         }
     }

    NProto::TReadPagesResponse ReadPages(
        NProto::TReadPagesRequest request) override
    {
        Sleep();

        NProto::TReadPagesResponse response;
        for (const auto& ref: request.GetPageGroupRefs()) {
            auto* pg = response.AddPageGroups();
            pg->SetFirstPageNo(ref.GetFirstPageNo());
            for (ui64 i = 0; i < ref.GetPageCount(); ++i) {
                // refcount trick
                pg->AddContent(Page);
            }
        }

        return response;
    }

    NProto::TWriteLogRecordResponse WriteLogRecord(
        NProto::TWriteLogRecordRequest request) override
    {
        Y_UNUSED(request);
        Sleep();

        return {};
    }

    NProto::TAcquireDevicesResponse AcquireDevices(
        NProto::TAcquireDevicesRequest) override
    {
        return {};
    }

    NProto::TFormatDeviceResponse FormatDevice(
        NProto::TFormatDeviceRequest) override
    {
        return {};
    }

    NProto::TReleaseDevicesResponse ReleaseDevices(
        NProto::TReleaseDevicesRequest) override
    {
        return {};
    }

    NProto::TReadJournalTailResponse ReadJournalTail(
        NProto::TReadJournalTailRequest) override
    {
        return {};
    }

    NProto::TAdvanceLsnLowWatermarkResponse AdvanceLsnLowWatermark(
        NProto::TAdvanceLsnLowWatermarkRequest) override
    {
        return {};
    }

    void Sleep()
    {
        if (const auto delay = Delay->NextDelay()) {
            FiberScheduler::sleep(delay.NanoSeconds());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TBench
{
    const TOptions* Options = nullptr;
    IStorageGroup* Group = nullptr;
    benchmark::State* State = nullptr;
    ui32 IoDepth = 0;
    ui64 RequestsPerFiber = 0;
    bool Write = false;
};

int ReadFiberMain(TBench* bench) noexcept
{
    const TVector<TPageGroupRef> ref{
        {
            .FirstPageNo = 0,
            .PageCount = bench->Options->PageCount
        }};

    TVector<TPageGroup> pages;
    for (ui64 i = 0; i < bench->RequestsPerFiber; ++i) {
        const auto error = bench->Group->ReadPages({}, ref, &pages);
        Y_ABORT_UNLESS(!HasError(error), "%s", FormatError(error).c_str());
    }

    return 0;
}

int WriteFiberMain(TBench* bench) noexcept
{
    const auto& options = *bench->Options;

    TBuffer page;
    page.Fill('x', options.PageSize);
    TVector<TPageGroup> record(1);
    record[0].Content.assign(options.PageCount, page);

    ui64 lsn = 1;
    for (ui64 i = 0; i < bench->RequestsPerFiber; ++i) {
        ++lsn;
        const auto error = bench->Group->WriteLogRecord(
            {},
            record,
            {
                .Lsn = lsn,
                .PrevLsn = lsn - 1
            });
        Y_ABORT_UNLESS(!HasError(error), "%s", FormatError(error).c_str());
    }

    return 0;
}

int BenchFiberMain(TBench* bench) noexcept
{
    auto init = bench->Group->Init();
    Y_ABORT_UNLESS(
        !HasError(init.GetError()),
        "%s",
        FormatError(init.GetError()).c_str());

    TVector<FiberFuture> futures(bench->IoDepth);

    THPTimer timer;
    for (ui32 i = 0; i < bench->IoDepth; ++i) {
        const int r = FiberScheduler::run(
            bench->Write ? WriteFiberMain : ReadFiberMain,
            TBench(*bench),
            &futures[i]);
        Y_ABORT_UNLESS(r == 0, "failed to spawn fiber: %s", ::strerror(r));
    }

    for (auto& future: futures) {
        Y_ABORT_UNLESS(0 == future.wait());
    }

    bench->State->SetIterationTime(timer.Passed());
    bench->Group->TearDown();

    return 0;
}

void Bench(benchmark::State& state, const TOptions& options, bool write)
{
    const ui32 ioDepth = state.range(1);

    for (auto _: state) {
        auto node = std::make_shared<TNullStorageNode>(options);

        TVector<TStorageDevice> devices;
        for (ui32 i = 0; i < options.DeviceCount; ++i) {
            devices.push_back(
                {
                    .Node = node,
                    .DeviceUUID = TStringBuilder() << "dev-" << i
                });
        }

        TStorageGroupConfig config;
        config.PageSize = options.PageSize;
        config.LowWatermarkPeriod = TDuration::MilliSeconds(1);

        auto group = CreateQuorumMirroredStorageGroup(
            config,
            devices,
            CreateFiberTimer());

        Y_ABORT_UNLESS(0 == FiberScheduler::run(
            BenchFiberMain,
            TBench{
                .Options = &options,
                .Group = group.get(),
                .State = &state,
                .IoDepth = ioDepth,
                .RequestsPerFiber = options.RequestsPerFiber,
                .Write = write,
            }));
    }

    using benchmark::Counter;
    const double perFiber = state.iterations() * options.RequestsPerFiber;
    state.counters["requests"] =
        Counter(perFiber * ioDepth, Counter::kIsRate);
    state.counters["time_avg"] =
        Counter(perFiber, Counter::kIsRate | Counter::kInvert);
}

}   // namespace
}   // namespace NCloud::NFileStore::NStorage::NFastShard

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    using namespace NCloud::NFileStore::NStorage::NFastShard;

    // benchmark requires capturless lambda
    static TOptions Options;
    static NLastGetopt::TOpts Opts = MakeOpts(Options);
    auto printHelp = [] {
        benchmark::PrintDefaultHelp();
        Opts.PrintUsage("bench");
    };

    benchmark::Initialize(&argc, argv, printHelp);
    NLastGetopt::TOptsParseResult parsed(&Opts, argc, argv);

    auto cpuMask = FiberScheduler::defaultCpuMask();
    if (Options.Threads) {
        // Silk intersects the mask with the affinity mask.
        CPU_ZERO(&cpuMask);
        for (ui32 cpu = 0; cpu < Options.Threads; ++cpu) {
            CPU_SET(cpu, &cpuMask);
        }
    }

    NCloud::NFastShard::Init(cpuMask);
    silk::Logger::setLevel(silk::LogLevel::WARN);

    // What silk actually got, for the benchmark name.
    cpu_set_t affinity;
    Y_ABORT_UNLESS(0 == ::sched_getaffinity(0, sizeof(affinity), &affinity));
    CPU_AND(&cpuMask, &cpuMask, &affinity);
    const int threads = CPU_COUNT(&cpuMask);

    for (const bool write: {false, true}) {
        auto* bench = benchmark::RegisterBenchmark(
            write ? "QuorumMirroredGroup/write" : "QuorumMirroredGroup/read",
            [write](benchmark::State& state) {
                Bench(state, Options, write);
            });

        bench->ArgNames({"threads", "iodepth"})
            ->UseManualTime()
            ->Unit(benchmark::kMillisecond);

        for (const auto depth: Options.IoDepths) {
            bench->Args({threads, depth});
        }
    }

    benchmark::RunSpecifiedBenchmarks();

    NCloud::NFastShard::Destroy();

    return 0;
}
