#include "shard_bench.h"

#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/storage/fastshard/iface/fs.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/size_literals.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/string/cast.h>

#include <benchmark/benchmark.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

namespace {

using namespace NFileStore::NProto;

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 CyclesPerIteration = 4U;
constexpr ui32 PrecreatedNodeCount = 64U;
constexpr ui32 PagesPerNode = 16U;
constexpr ui32 RequestSize = 4_KB;

////////////////////////////////////////////////////////////////////////////////

//
// The shard rejects concurrent ops which touch the same pages with
// E_REJECTED and expects the caller to retry - so the driver retries,
// counting the rejects. A shard implementation may be a happy-path
// prototype: under page conflicts a rejected create/unlink may leave
// the name in either state, so the caller-provided "already applied"
// code (E_FS_EXIST for creates, E_FS_NOENT for unlinks) is treated as
// success. The conflict rate stays visible through the rejects counter.
//

template <typename TResponse, typename TRequest, typename TIssue>
void RunBatchWithRetries(
    TVector<TRequest> requests,
    TIssue issue,
    ui32 alreadyAppliedCode,
    ui64* rejectCount)
{
    while (!requests.empty()) {
        TVector<NThreading::TFuture<TResponse>> futures;
        futures.reserve(requests.size());
        for (const auto& request: requests) {
            futures.push_back(issue(request));
        }

        TVector<TRequest> rejected;
        for (ui32 i = 0; i < futures.size(); ++i) {
            const ui32 code = futures[i].GetValueSync().GetError().GetCode();
            if (code == E_REJECTED) {
                rejected.push_back(std::move(requests[i]));
                ++*rejectCount;
                continue;
            }

            if (alreadyAppliedCode && code == alreadyAppliedCode) {
                continue;
            }

            Y_ENSURE(
                code == S_OK,
                FormatError(futures[i].GetValueSync().GetError()));
        }

        requests = std::move(rejected);
    }
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
    return **holder;
}

////////////////////////////////////////////////////////////////////////////////
// Scenario 1: create/unlink node pairs, `parallelism` requests in
// flight at every step.

void CreateUnlinkNodeBench(benchmark::State& state, const TShardFactory& factory)
{
    IFileSystemShard& shard = BuildShard(factory);
    const ui32 parallelism = static_cast<ui32>(state.range(0));

    ui64 rejectCount = 0;
    for (auto _: state) {
        for (ui32 cycle = 0; cycle < CyclesPerIteration; ++cycle) {
            TVector<TCreateNodeRequest> creates(parallelism);
            for (ui32 slot = 0; slot < parallelism; ++slot) {
                creates[slot].SetNodeId(RootNodeId);
                creates[slot].SetName("tmp-" + ToString(slot));
                creates[slot].MutableFile()->SetMode(0644);
            }
            RunBatchWithRetries<TCreateNodeResponse>(
                std::move(creates),
                [&](const TCreateNodeRequest& request) {
                    return shard.CreateNode(request);
                },
                NCloud::E_FS_EXIST,
                &rejectCount);

            TVector<TUnlinkNodeRequest> unlinks(parallelism);
            for (ui32 slot = 0; slot < parallelism; ++slot) {
                unlinks[slot].SetNodeId(RootNodeId);
                unlinks[slot].SetName("tmp-" + ToString(slot));
            }
            RunBatchWithRetries<TUnlinkNodeResponse>(
                std::move(unlinks),
                [&](const TUnlinkNodeRequest& request) {
                    return shard.UnlinkNode(request);
                },
                NCloud::E_FS_NOENT,
                &rejectCount);
        }
    }

    state.SetItemsProcessed(
        state.iterations() * CyclesPerIteration * parallelism);
    state.counters["rejects"] = benchmark::Counter(
        rejectCount,
        benchmark::Counter::kIsRate);
}

////////////////////////////////////////////////////////////////////////////////
// Scenario 2: 4K writes and reads over a set of precreated nodes,
// `parallelism` requests in flight at every step.

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

    const TString data(RequestSize, 'x');
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

    ui64 counter = 0;
    auto pickTarget = [&](ui64* handle, ui64* offset) {
        *handle = handles[counter % PrecreatedNodeCount];
        *offset =
            (counter / PrecreatedNodeCount) % PagesPerNode * RequestSize;
        ++counter;
    };

    ui64 rejectCount = 0;
    for (auto _: state) {
        for (ui32 cycle = 0; cycle < CyclesPerIteration; ++cycle) {
            TVector<TWriteDataRequest> writes(parallelism);
            for (ui32 slot = 0; slot < parallelism; ++slot) {
                ui64 handle = 0;
                ui64 offset = 0;
                pickTarget(&handle, &offset);

                writes[slot].SetHandle(handle);
                writes[slot].SetOffset(offset);
                *writes[slot].MutableBuffer() = data;
            }
            RunBatchWithRetries<TWriteDataResponse>(
                std::move(writes),
                [&](const TWriteDataRequest& request) {
                    return shard.WriteData(request);
                },
                0U /* alreadyAppliedCode */,
                &rejectCount);

            TVector<TReadDataRequest> reads(parallelism);
            for (ui32 slot = 0; slot < parallelism; ++slot) {
                ui64 handle = 0;
                ui64 offset = 0;
                pickTarget(&handle, &offset);

                reads[slot].SetHandle(handle);
                reads[slot].SetOffset(offset);
                reads[slot].SetLength(RequestSize);
            }
            RunBatchWithRetries<TReadDataResponse>(
                std::move(reads),
                [&](const TReadDataRequest& request) {
                    return shard.ReadData(request);
                },
                0U /* alreadyAppliedCode */,
                &rejectCount);
        }
    }

    state.SetItemsProcessed(
        state.iterations() * CyclesPerIteration * parallelism * 2);
    state.SetBytesProcessed(
        state.iterations() * CyclesPerIteration * parallelism * 2 *
        RequestSize);
    state.counters["rejects"] = benchmark::Counter(
        rejectCount,
        benchmark::Counter::kIsRate);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void RegisterShardBenchmarks(TString name, TShardFactory factory)
{
    //
    // The requests complete on the shard implementation's own threads,
    // not on the benchmark thread, so the meaningful clock is wall
    // time.
    //

    benchmark::RegisterBenchmark(
        (name + "CreateUnlinkNode").c_str(),
        [factory](benchmark::State& state) {
            CreateUnlinkNodeBench(state, factory);
        })
        ->Arg(1)->Arg(8)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime();

    benchmark::RegisterBenchmark(
        (name + "WriteReadData4K").c_str(),
        [factory](benchmark::State& state) {
            WriteReadData4KBench(state, factory);
        })
        ->Arg(1)->Arg(8)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime();
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
