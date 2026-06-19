#include <cloud/filestore/libs/vfs_fuse/directory_entry_version_cache.h>
#include <cloud/filestore/libs/vfs_fuse/node_cache.h>

#include <library/cpp/testing/benchmark/bench.h>

#include <util/generic/vector.h>
#include <util/random/fast.h>
#include <util/string/cast.h>
#include <util/thread/factory.h>

#include <atomic>

////////////////////////////////////////////////////////////////////////////////

using namespace NCloud::NFileStore;
using namespace NFuse;

namespace {

////////////////////////////////////////////////////////////////////////////////

void RunUpdateNodeBench(ui64 iters, ui32 threads, ui32 shards)
{
    TNodeCache cache("fs", shards);
    std::atomic<ui64> version = 0;
    const ui64 nodeCount = 1'000'000;

    struct TContext
    {
        TManualEvent Ev;
        std::atomic<ui32> Todo;
    };

    auto context = std::make_shared<TContext>();
    context->Todo = threads;

    for (ui32 i = 0; i < threads; ++i) {
        SystemThreadFactory()->Run(
            [&cache, &version, iters, threads, context] ()
            {
                TReallyFastRng32 rng(777);
                for (size_t i = 0; i < iters / threads; ++i) {
                    NProto::TNodeAttr attrs;
                    attrs.SetId(rng.Uniform(nodeCount));
                    attrs.SetType(NProto::E_REGULAR_NODE);
                    cache.UpdateNode(
                        attrs,
                        version.fetch_add(1, std::memory_order_release));
                }

                context->Todo.fetch_sub(1, std::memory_order_release);
                context->Ev.Signal();
            });
    }

    while (context->Todo.load(std::memory_order_acquire)) {
        context->Ev.WaitI();
        context->Ev.Reset();
    }
}

void RunAdvanceEntryVersionBench(ui64 iters, ui32 threads, ui32 shards)
{
    TDirectoryEntryVersionCache cache(shards, nullptr);
    std::atomic<ui64> version = 0;
    const ui64 directoryCount = 4'096;

    TVector<TString> childNames;
    childNames.reserve(16);
    for (ui32 i = 0; i < 16; ++i) {
        childNames.push_back(ToString(i));
    }

    for (ui64 i = 0; i < directoryCount; ++i) {
        cache.RegisterHandle(i);
    }

    struct TContext
    {
        TManualEvent Ev;
        std::atomic<ui32> Todo;
    };

    auto context = std::make_shared<TContext>();
    context->Todo = threads;

    for (ui32 i = 0; i < threads; ++i) {
        SystemThreadFactory()->Run(
            [&cache, &version, &childNames, iters, threads, context] ()
            {
                TReallyFastRng32 rng(777);
                for (size_t i = 0; i < iters / threads; ++i) {
                    cache.AdvanceVersion(
                        rng.Uniform(directoryCount),
                        childNames[rng.Uniform(childNames.size())],
                        version.fetch_add(1, std::memory_order_release));
                }

                context->Todo.fetch_sub(1, std::memory_order_release);
                context->Ev.Signal();
            });
    }

    while (context->Todo.load(std::memory_order_acquire)) {
        context->Ev.WaitI();
        context->Ev.Reset();
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_CPU_BENCHMARK(UpdateNode1, iface)
{
    RunUpdateNodeBench(iface.Iterations(), 1, 1);
}

Y_CPU_BENCHMARK(UpdateNode2, iface)
{
    RunUpdateNodeBench(iface.Iterations(), 2, 1);
}

Y_CPU_BENCHMARK(UpdateNode4, iface)
{
    RunUpdateNodeBench(iface.Iterations(), 4, 1);
}

Y_CPU_BENCHMARK(UpdateNode8, iface)
{
    RunUpdateNodeBench(iface.Iterations(), 8, 1);
}

Y_CPU_BENCHMARK(UpdateNode16, iface)
{
    RunUpdateNodeBench(iface.Iterations(), 16, 1);
}

Y_CPU_BENCHMARK(UpdateNode1_16, iface)
{
    RunUpdateNodeBench(iface.Iterations(), 1, 16);
}

Y_CPU_BENCHMARK(UpdateNode2_16, iface)
{
    RunUpdateNodeBench(iface.Iterations(), 2, 16);
}

Y_CPU_BENCHMARK(UpdateNode4_16, iface)
{
    RunUpdateNodeBench(iface.Iterations(), 4, 16);
}

Y_CPU_BENCHMARK(UpdateNode8_16, iface)
{
    RunUpdateNodeBench(iface.Iterations(), 8, 16);
}

Y_CPU_BENCHMARK(UpdateNode16_16, iface)
{
    RunUpdateNodeBench(iface.Iterations(), 16, 16);
}

Y_CPU_BENCHMARK(AdvanceEntryVersion1, iface)
{
    RunAdvanceEntryVersionBench(iface.Iterations(), 1, 1);
}

Y_CPU_BENCHMARK(AdvanceEntryVersion2, iface)
{
    RunAdvanceEntryVersionBench(iface.Iterations(), 2, 1);
}

Y_CPU_BENCHMARK(AdvanceEntryVersion4, iface)
{
    RunAdvanceEntryVersionBench(iface.Iterations(), 4, 1);
}

Y_CPU_BENCHMARK(AdvanceEntryVersion8, iface)
{
    RunAdvanceEntryVersionBench(iface.Iterations(), 8, 1);
}

Y_CPU_BENCHMARK(AdvanceEntryVersion16, iface)
{
    RunAdvanceEntryVersionBench(iface.Iterations(), 16, 1);
}

Y_CPU_BENCHMARK(AdvanceEntryVersion1_16, iface)
{
    RunAdvanceEntryVersionBench(iface.Iterations(), 1, 16);
}

Y_CPU_BENCHMARK(AdvanceEntryVersion2_16, iface)
{
    RunAdvanceEntryVersionBench(iface.Iterations(), 2, 16);
}

Y_CPU_BENCHMARK(AdvanceEntryVersion4_16, iface)
{
    RunAdvanceEntryVersionBench(iface.Iterations(), 4, 16);
}

Y_CPU_BENCHMARK(AdvanceEntryVersion8_16, iface)
{
    RunAdvanceEntryVersionBench(iface.Iterations(), 8, 16);
}

Y_CPU_BENCHMARK(AdvanceEntryVersion16_16, iface)
{
    RunAdvanceEntryVersionBench(iface.Iterations(), 16, 16);
}
