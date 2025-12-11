#include "recent_blocks_tracker.h"

#include <cloud/blockstore/libs/storage/model/composite_id.h>

#include <library/cpp/testing/gbenchmark/benchmark.h>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

static void BenchmarkSequence(benchmark::State& state)
{
    TRecentBlocksTracker tracker("device1");

    TCompositeId id = TCompositeId::FromGeneration(1);

    // Sequence of incremented ids
    for (const auto _: state) {
        auto range = TBlockRange64::WithLength(0, 1024);
        TString overlapReason;
        auto result =
            tracker.CheckRecorded(id.Advance(), range, &overlapReason);
        Y_ABORT_UNLESS(result == EOverlapStatus::NotOverlapped);
        tracker.AddRecorded(id.Advance(), range);
    }
}

static void BenchmarkSlightlyFromPastNotOverlapped(benchmark::State& state)
{
    Y_UNUSED(state);

    TRecentBlocksTracker tracker("device1");
    // Id sequence:
    // 9- 8- 7- 6- 5- 4- 3- 2- 1- 0  ->
    // 19-18-17-16-15-14-13-12-11-10 ->
    // 29-28-27-26-25-24-23-22-21-20 -> etc
    const ui32 stepCount = 10;
    ui32 step = stepCount;
    ui32 sequenceIdStart = 0;
    for (const auto _: state) {
        if (step == 0) {
            step = stepCount - 1;
            sequenceIdStart += stepCount;
        } else {
            --step;
        }
        auto range = TBlockRange64::WithLength(step * 1024, 1024);
        ui64 id = sequenceIdStart + step;
        TString overlapReason;
        auto result = tracker.CheckRecorded(id, range, &overlapReason);
        Y_ABORT_UNLESS(result == EOverlapStatus::NotOverlapped);
        tracker.AddRecorded(id, range);
    }
}

BENCHMARK(BenchmarkSequence);
BENCHMARK(BenchmarkSlightlyFromPastNotOverlapped);

}   // namespace NCloud::NBlockStore::NStorage
