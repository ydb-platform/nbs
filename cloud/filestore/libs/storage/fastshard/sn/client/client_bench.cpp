#include <cloud/filestore/libs/storage/fastshard/sn/client/client.h>
#include <cloud/filestore/libs/storage/fastshard/sn/iface/storage_node.h>
#include <cloud/filestore/libs/storage/fastshard/sn/server/server.h>
#include <cloud/filestore/libs/storage/fastshard/testlib/fake_storage_node.h>

#include <cloud/storage/core/protos/device.pb.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>
#include <silk/util/init.h>

#include <library/cpp/testing/common/network.h>

#include <util/generic/size_literals.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>

#include <benchmark/benchmark.h>

#include <atomic>
#include <memory>

using namespace NCloud::NFileStore::NStorage::NFastShard;
using silk::FiberFuture;
using silk::FiberScheduler;

namespace {

////////////////////////////////////////////////////////////////////////////////
// The google benchmark module owns main(), so the silk runtime is
// brought up lazily on first use and left to die with the process.

void EnsureSilk()
{
    static const bool initialized = [] {
        silk::initialize();
        FiberScheduler::initialize();
        return true;
    }();
    Y_UNUSED(initialized);
}

////////////////////////////////////////////////////////////////////////////////
// Fake storage node behind the real sn TCP server, with the pooled
// client pointed at it over loopback.

struct TBenchFixture
{
    std::shared_ptr<TFakeStorageNode> Storage;
    NTesting::TPortHolder Port;
    IServerPtr Server;
    IStorageNodePtr Client;

    TBenchFixture()
        : Storage(std::make_shared<TFakeStorageNode>())
        , Port(NTesting::GetFreePort())
        , Server(CreateServer(Port, Storage))
        , Client(CreateStorageNodeClient("localhost", Port))
    {
        Server->Start();
    }

    ~TBenchFixture()
    {
        Server->Stop();
    }
};

////////////////////////////////////////////////////////////////////////////////
// One benchmark iteration: `Concurrency` fibers, each doing
// `CallsPerFiber` request round trips.

struct TDriverParams
{
    TBenchFixture* Fixture;
    ui32 Concurrency;
    ui32 CallsPerFiber;
    ui32 WritePageCount;
    std::atomic<ui32>* ErrorCount;
};

static_assert(sizeof(TDriverParams) <= silk::FIBER_PARAMETERS_SIZE);

int CallFiberMain(TDriverParams* params) noexcept
{
    for (ui32 i = 0; i < params->CallsPerFiber; ++i) {
        if (params->WritePageCount) {
            NCloud::NProto::TWriteLogRecordRequest req;
            req.SetDeviceUUID("bench-dev");
            req.SetLogSequenceNumber(i);
            auto* pg = req.AddPageGroups();
            pg->SetFirstPageNo(0);
            for (ui32 p = 0; p < params->WritePageCount; ++p) {
                pg->AddContent(TString(4_KB, 'x'));
            }
            auto resp = params->Fixture->Client->WriteLogRecord(
                std::move(req));
            if (resp.GetError().GetCode()) {
                params->ErrorCount->fetch_add(1);
            }
        } else {
            NCloud::NProto::TAcquireDevicesRequest req;
            req.AddDeviceUUIDs("bench-dev");
            auto resp = params->Fixture->Client->AcquireDevices(
                std::move(req));
            if (resp.GetError().GetCode()) {
                params->ErrorCount->fetch_add(1);
            }
        }
    }
    return 0;
}

int DriverFiberMain(TDriverParams* params) noexcept
{
    constexpr ui32 MaxConcurrency = 256U;
    if (params->Concurrency > MaxConcurrency) {
        return EINVAL;
    }

    FiberFuture futures[MaxConcurrency];
    for (ui32 i = 0; i < params->Concurrency; ++i) {
        const int r = FiberScheduler::run(
            CallFiberMain,
            TDriverParams(*params),
            &futures[i]);
        if (r) {
            return r;
        }
    }
    for (ui32 i = 0; i < params->Concurrency; ++i) {
        futures[i].wait();
    }
    return 0;
}

////////////////////////////////////////////////////////////////////////////////

void RunBench(benchmark::State& state, ui32 writePageCount)
{
    EnsureSilk();

    TBenchFixture fixture;

    const ui32 concurrency = static_cast<ui32>(state.range(0));
    constexpr ui32 CallsPerFiber = 16U;

    std::atomic<ui32> errorCount{0};

    for (auto _: state) {
        FiberFuture done;
        const int r = FiberScheduler::run(
            DriverFiberMain,
            TDriverParams{
                .Fixture = &fixture,
                .Concurrency = concurrency,
                .CallsPerFiber = CallsPerFiber,
                .WritePageCount = writePageCount,
                .ErrorCount = &errorCount,
            },
            &done);
        Y_ENSURE(r == 0);
        Y_ENSURE(done.wait() == 0);
    }

    Y_ENSURE(errorCount.load() == 0);

    state.SetItemsProcessed(
        state.iterations() * concurrency * CallsPerFiber);
    if (writePageCount) {
        state.SetBytesProcessed(
            state.iterations() * concurrency * CallsPerFiber *
            writePageCount * 4_KB);
    }
}

void SnClientRoundTrip(benchmark::State& state)
{
    RunBench(state, 0 /* writePageCount */);
}

void SnClientWriteLogRecord4K(benchmark::State& state)
{
    RunBench(state, 1 /* writePageCount */);
}

void SnClientWriteLogRecord32K(benchmark::State& state)
{
    RunBench(state, 8 /* writePageCount */);
}

//
// The requests run on silk scheduler threads, not on the benchmark
// thread, so the meaningful clock is wall time.
//

BENCHMARK(SnClientRoundTrip)->Arg(1)->Arg(4)->Arg(16)->Arg(64)
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime();
BENCHMARK(SnClientWriteLogRecord4K)->Arg(1)->Arg(4)->Arg(16)->Arg(64)
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime();
BENCHMARK(SnClientWriteLogRecord32K)->Arg(1)->Arg(4)->Arg(16)->Arg(64)
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime();

}   // namespace
