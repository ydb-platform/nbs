#include <cloud/storage/core/libs/common/context.h>
#include <cloud/storage/core/libs/common/request_timing_collector.h>

#include <library/cpp/testing/benchmark/bench.h>

// Synthetic diagnostic overhead only: identical observed timelines, excluding
// payload I/O. Disabled uses the current context with diagnostics disabled;
// it is not a previous-version baseline. Use the same service workload linked
// with old and new libraries separately for acceptance.
namespace {
using namespace NCloud;

template <ui32 Parts, bool Waits>
void Disabled(size_t iterations)
{
    for (size_t i = 0; i < iterations; ++i) {
        auto context = MakeIntrusive<TCallContextBase>(i);
        for (ui32 p = 0; p < Parts; ++p) {
            if constexpr (Waits) {
                context->AddTime(
                    EProcessingStage::Postponed, TDuration::MicroSeconds(80));
            }
        }
        Y_DO_NOT_OPTIMIZE_AWAY(context->Time(EProcessingStage::Postponed));
    }
}

template <ui32 Parts, bool Waits>
void Graph(size_t iterations)
{
    for (size_t i = 0; i < iterations; ++i) {
        TRequestTimingCollector timing(i);
        if constexpr (Parts == 1) {
            if constexpr (Waits) {
                timing.Wait(0, 1, 0, 80);
            }
        } else {
            const auto fork = timing.Fork(0, 0);
            TVector<ui32> children;
            for (ui32 p = 0; p < Parts; ++p) {
                const auto child = timing.Start(fork, 0);
                children.push_back(child);
                if constexpr (Waits) {
                    timing.Wait(child, 1, 0, 80);
                }
                timing.Finish(child, 90);
            }
            timing.Join(0, children, 90);
        }
        const auto result = timing.Complete(90, 7);
        Y_DO_NOT_OPTIMIZE_AWAY(result);
    }
}
}   // namespace

Y_CPU_BENCHMARK(DisabledOrdinary, iface) { Disabled<1, false>(iface.Iterations()); }
Y_CPU_BENCHMARK(GraphOrdinary, iface) { Graph<1, false>(iface.Iterations()); }
Y_CPU_BENCHMARK(DisabledTwoParts, iface) { Disabled<2, true>(iface.Iterations()); }
Y_CPU_BENCHMARK(GraphTwoParts, iface) { Graph<2, true>(iface.Iterations()); }
Y_CPU_BENCHMARK(DisabledSixteenParts, iface) { Disabled<16, true>(iface.Iterations()); }
Y_CPU_BENCHMARK(GraphSixteenParts, iface) { Graph<16, true>(iface.Iterations()); }
