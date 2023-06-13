#include "composite_id.h"
#include "recently_written_blocks.h"

#include <library/cpp/testing/gbenchmark/benchmark.h>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

static void BenchmarkSequence(benchmark::State& state)
{
    TString deviceUUID;
    TRecentlyWrittenBlocks recentlyWritten;

    TCompositeId id = TCompositeId::FromGeneration(1);

    // Sequence of incremented ids
    for (const auto _ : state) {
        TBlockRange64 range{0, 1023};
        auto result = recentlyWritten.CheckRange(id.Advance(), range);
        Y_VERIFY(result == EOverlapStatus::NotOverlapped);
        recentlyWritten.AddRange(id.Advance(), range, deviceUUID);
    }
}

static void BenchmarkSlightlyFromPastNotOverlapped(benchmark::State& state)
{
    Y_UNUSED(state);

    TString deviceUUID;
    TRecentlyWrittenBlocks recentlyWritten;
    // Id sequence:
    // 9- 8- 7- 6- 5- 4- 3- 2- 1- 0  ->
    // 19-18-17-16-15-14-13-12-11-10 ->
    // 29-28-27-26-25-24-23-22-21-20 -> etc
    const ui32 stepCount = 10;
    ui32 step = stepCount;
    ui32 sequenceIdStart = 0;
    for (const auto _ : state) {
        if (step == 0) {
            step = stepCount - 1;
            sequenceIdStart += stepCount;
        } else {
            --step;
        }
        TBlockRange64 range{step * 1024, (step + 1) * 1024 - 1};
        ui64 id = sequenceIdStart + step;
        auto result = recentlyWritten.CheckRange(id, range);
        Y_VERIFY(result == EOverlapStatus::NotOverlapped);
        recentlyWritten.AddRange(id, range, deviceUUID);
    }
}

BENCHMARK(BenchmarkSequence);
BENCHMARK(BenchmarkSlightlyFromPastNotOverlapped);

}   // namespace NCloud::NBlockStore::NStorage
