#include "shard_bench.h"

#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/storage/fastshard/iface/fs.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/size_literals.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/string/cast.h>

#include <benchmark/benchmark.h>

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>

namespace NCloud::NFileStore::NStorage::NFastShard {

namespace {

using namespace NFileStore::NProto;

using TError = NCloud::NProto::TError;

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 CyclesPerIteration = 4U;
constexpr ui32 PrecreatedNodeCount = 64U;
constexpr ui32 PagesPerNode = 16U;
constexpr ui32 RequestSize = 4_KB;

////////////////////////////////////////////////////////////////////////////////

//
// Keeps a fixed number of requests in flight: every slot issues its next
// request as soon as its previous one completes. The shard rejects
// concurrent ops which touch the same pages with E_REJECTED and expects the
// caller to retry - a slot retries the rejected request, counting the
// rejects.
//

class TInflightDriver
{
public:
    // Issues the request number step of the slot and returns its error.
    using TIssue =
        std::function<NThreading::TFuture<TError>(ui32 slot, ui64 step)>;

private:
    const TIssue Issue;
    TVector<ui64> Steps;

    std::atomic<bool> Stopping = false;
    std::atomic<ui32> ActiveSlots = 0;
    std::atomic<ui64> Completed = 0;
    std::atomic<ui64> WaitTarget = 0;
    std::atomic<ui64> Rejects = 0;

    std::mutex Lock;
    std::condition_variable Cond;

public:
    TInflightDriver(ui32 parallelism, TIssue issue)
        : Issue(std::move(issue))
        , Steps(parallelism)
    {}

    void Start()
    {
        ActiveSlots = static_cast<ui32>(Steps.size());
        for (ui32 slot = 0; slot < Steps.size(); ++slot) {
            Pump(slot);
        }
    }

    // Waits until count requests have completed since Start.
    void WaitCompleted(ui64 count)
    {
        WaitTarget = count;
        std::unique_lock g(Lock);
        Cond.wait(g, [&] { return Completed >= count; });
    }

    // Stops issuing the requests and waits for the ones in flight.
    void Stop()
    {
        Stopping = true;
        std::unique_lock g(Lock);
        Cond.wait(g, [&] { return ActiveSlots == 0; });
    }

    ui64 GetRejectCount() const
    {
        return Rejects;
    }

private:
    void Pump(ui32 slot)
    {
        //
        // The requests which complete immediately are handled in this loop,
        // the others continue it from their completion callbacks.
        //

        while (!Stopping) {
            auto future = Issue(slot, Steps[slot]);
            if (!future.HasValue()) {
                future.Subscribe(
                    [this, slot](const auto& f)
                    {
                        Complete(slot, f.GetValue());
                        Pump(slot);
                    });
                return;
            }

            Complete(slot, future.GetValue());
        }

        std::lock_guard g(Lock);
        --ActiveSlots;
        Cond.notify_all();
    }

    void Complete(ui32 slot, const TError& error)
    {
        if (error.GetCode() == E_REJECTED) {
            ++Rejects;
            return;
        }

        Y_ABORT_UNLESS(!HasError(error), "%s", FormatError(error).c_str());
        ++Steps[slot];

        if (++Completed >= WaitTarget) {
            std::lock_guard g(Lock);
            Cond.notify_all();
        }
    }
};

//
// Converts a response to its error. A shard implementation may be a
// happy-path prototype: under page conflicts a rejected create/unlink may
// leave the name in either state, so the "already applied" code (E_FS_EXIST
// for creates, E_FS_NOENT for unlinks) is treated as success. The conflict
// rate stays visible through the rejects counter.
//

template <typename TResponse>
NThreading::TFuture<TError> ToError(
    NThreading::TFuture<TResponse> future,
    ui32 alreadyAppliedCode)
{
    return future.Apply(
        [alreadyAppliedCode](const auto& f)
        {
            const auto& error = f.GetValue().GetError();
            if (alreadyAppliedCode && error.GetCode() == alreadyAppliedCode) {
                return TError();
            }
            return error;
        });
}

//
// Runs the benchmark loop: every iteration waits for opsPerIteration more
// requests to complete while the driver keeps the requests in flight.
//

void RunInflight(
    benchmark::State& state,
    TInflightDriver& driver,
    ui64 opsPerIteration)
{
    ui64 target = 0;
    for (auto _: state) {
        if (!target) {
            driver.Start();
        }
        target += opsPerIteration;
        driver.WaitCompleted(target);
    }

    driver.Stop();
}

//
// The shard is leaked deliberately: the last request fiber of an
// implementation may still hold a reference to the shard internals
// when the benchmark returns, and letting the shard die on such a
// fiber races with process teardown.
//

IFileSystemShard& BuildShard(const TShardFactory& factory)
{
    auto* holder = new IFileSystemShardPtr(factory());
    auto& shard = **holder;

    const auto error = shard.Init().GetValueSync();
    Y_ABORT_UNLESS(
        !HasError(error),
        "shard init failed: %s",
        FormatError(error).c_str());

    return shard;
}

////////////////////////////////////////////////////////////////////////////////
// Scenario 1: every slot creates its node and unlinks it, over and over,
// `parallelism` requests in flight all the time.

void CreateUnlinkNodeBench(benchmark::State& state, const TShardFactory& factory)
{
    IFileSystemShard& shard = BuildShard(factory);
    const ui32 parallelism = static_cast<ui32>(state.range(0));

    TInflightDriver driver(
        parallelism,
        [&](ui32 slot, ui64 step)
        {
            const TString name = "tmp-" + ToString(slot);
            if (step % 2 == 0) {
                TCreateNodeRequest request;
                request.SetNodeId(RootNodeId);
                request.SetName(name);
                request.MutableFile()->SetMode(0644);
                return ToError(
                    shard.CreateNode(std::move(request)),
                    NCloud::E_FS_EXIST);
            }

            TUnlinkNodeRequest request;
            request.SetNodeId(RootNodeId);
            request.SetName(name);
            return ToError(
                shard.UnlinkNode(std::move(request)),
                NCloud::E_FS_NOENT);
        });

    const ui64 opsPerIteration = CyclesPerIteration * parallelism * 2;
    RunInflight(state, driver, opsPerIteration);

    state.SetItemsProcessed(state.iterations() * opsPerIteration);
    state.counters["rejects"] = benchmark::Counter(
        driver.GetRejectCount(),
        benchmark::Counter::kIsRate);
}

////////////////////////////////////////////////////////////////////////////////
// Scenario 2: 4K writes and reads over a set of precreated nodes, every slot
// alternates writes and reads, `parallelism` requests in flight all the time.

void WriteReadData4KBench(benchmark::State& state, const TShardFactory& factory)
{
    IFileSystemShard& shard = BuildShard(factory);
    const ui32 parallelism = static_cast<ui32>(state.range(0));

    //
    // Precreate the nodes and fill every page each node is going to
    // serve, so the benchmark loop reads real data instead of holes.
    //

    TVector<ui64> handles;
    handles.reserve(PrecreatedNodeCount);
    const ui32 createHandleFlags = ProtoFlag(TCreateHandleRequest::E_CREATE) |
                                   ProtoFlag(TCreateHandleRequest::E_READ) |
                                   ProtoFlag(TCreateHandleRequest::E_WRITE);
    for (ui32 i = 0; i < PrecreatedNodeCount; ++i) {
        TCreateHandleRequest request;
        request.SetNodeId(RootNodeId);
        request.SetName("node-" + ToString(i));
        request.SetMode(0644);
        request.SetFlags(createHandleFlags);
        auto response = shard.CreateHandle(std::move(request))
            .GetValueSync();
        Y_ENSURE(
            response.GetError().GetCode() == S_OK,
            FormatError(response.GetError()));
        handles.push_back(response.GetHandle());
    }

    for (const ui64 handle: handles) {
        TWriteDataRequest request;
        request.SetHandle(handle);
        request.SetOffset(0);
        *request.MutableBuffer() = TString(PagesPerNode * RequestSize, 'y');
        auto response = shard.WriteData(std::move(request))
            .GetValueSync();
        Y_ENSURE(
            response.GetError().GetCode() == S_OK,
            FormatError(response.GetError()));
    }

    const TString data(RequestSize, 'x');
    std::atomic<ui64> counter = 0;
    TInflightDriver driver(
        parallelism,
        [&](ui32 slot, ui64 step)
        {
            Y_UNUSED(slot);

            const ui64 target = counter++;
            const ui64 handle = handles[target % PrecreatedNodeCount];
            const ui64 offset =
                (target / PrecreatedNodeCount) % PagesPerNode * RequestSize;

            if (step % 2 == 0) {
                TWriteDataRequest request;
                request.SetHandle(handle);
                request.SetOffset(offset);
                *request.MutableBuffer() = data;
                return ToError(
                    shard.WriteData(std::move(request)),
                    0U /* alreadyAppliedCode */);
            }

            TReadDataRequest request;
            request.SetHandle(handle);
            request.SetOffset(offset);
            request.SetLength(RequestSize);
            return ToError(
                shard.ReadData(std::move(request)),
                0U /* alreadyAppliedCode */);
        });

    const ui64 opsPerIteration = CyclesPerIteration * parallelism * 2;
    RunInflight(state, driver, opsPerIteration);

    state.SetItemsProcessed(state.iterations() * opsPerIteration);
    state.SetBytesProcessed(
        state.iterations() * opsPerIteration * RequestSize);
    state.counters["rejects"] = benchmark::Counter(
        driver.GetRejectCount(),
        benchmark::Counter::kIsRate);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void RegisterShardBenchmarks(TString name, TShardFactory factory)
{
    //
    // The requests are executed on the shard implementation's own threads,
    // not on the benchmark thread: the time is measured as wall time and the
    // CPU time - as the CPU time of the whole process.
    //

    benchmark::RegisterBenchmark(
        (name + "CreateUnlinkNode").c_str(),
        [factory](benchmark::State& state) {
            CreateUnlinkNodeBench(state, factory);
        })
        ->Arg(1)->Arg(8)->Arg(32)->Arg(256)
        ->Unit(benchmark::kMicrosecond)
        ->MeasureProcessCPUTime()
        ->UseRealTime();

    benchmark::RegisterBenchmark(
        (name + "WriteReadData4K").c_str(),
        [factory](benchmark::State& state) {
            WriteReadData4KBench(state, factory);
        })
        ->Arg(1)->Arg(8)->Arg(32)->Arg(256)
        ->Unit(benchmark::kMicrosecond)
        ->MeasureProcessCPUTime()
        ->UseRealTime();
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
