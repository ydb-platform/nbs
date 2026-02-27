#include <cloud/filestore/libs/vfs_fuse/node_cache.h>

#include <library/cpp/testing/benchmark/bench.h>

#include <util/random/fast.h>
#include <util/thread/factory.h>

////////////////////////////////////////////////////////////////////////////////

using namespace NCloud::NFileStore;
using namespace NFuse;

namespace {

////////////////////////////////////////////////////////////////////////////////

void RunBench(ui64 iters, ui32 threads)
{
    TNodeCache cache("fs");
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_CPU_BENCHMARK(UpdateNode1, iface)
{
    RunBench(iface.Iterations(), 1);
}

Y_CPU_BENCHMARK(UpdateNode2, iface)
{
    RunBench(iface.Iterations(), 2);
}

Y_CPU_BENCHMARK(UpdateNode4, iface)
{
    RunBench(iface.Iterations(), 4);
}

Y_CPU_BENCHMARK(UpdateNode8, iface)
{
    RunBench(iface.Iterations(), 8);
}

Y_CPU_BENCHMARK(UpdateNode16, iface)
{
    RunBench(iface.Iterations(), 16);
}
