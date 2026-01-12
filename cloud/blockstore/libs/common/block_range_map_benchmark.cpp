#include "block_range_map.h"

#include <library/cpp/testing/gbenchmark/benchmark.h>

#include <util/random/random.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

///////////////////////////////////////////////////////////////////////////////

void BlockRangeMapBenchmark(
    benchmark::State& state,
    size_t ioDeps,
    ui64 blockCount)
{
    const ui64 maxCount = 1024;
    TBlockRangeMap<ui64, TString> map;
    TVector<ui64> inflight;
    ui64 keyGenerator = 0;

    auto addRange = [&]() -> ui64
    {
        const ui64 key = keyGenerator++;
        map.AddRange(
            key,
            TBlockRange64::WithLength(
                RandomNumber<ui64>(blockCount),
                RandomNumber<ui64>(maxCount) + 1));
        return key;
    };

    for (size_t i = 0; i < ioDeps; ++i) {
        inflight.push_back(addRange());
    }

    for (const auto _: state) {
        const size_t indx = RandomNumber<size_t>(inflight.size());
        auto val = map.ExtractRange(inflight[indx]);
        Y_ABORT_UNLESS(val);
        inflight[indx] = addRange();
    }
}

}   // namespace

///////////////////////////////////////////////////////////////////////////////

#define DECLARE_BENCHMARK(ioDeps, blockCount)                           \
    void BlockRangeMap_##ioDeps##_##blockCount(benchmark::State& state) \
    {                                                                   \
        BlockRangeMapBenchmark(state, ioDeps, blockCount);              \
    }                                                                   \
    BENCHMARK(BlockRangeMap_##ioDeps##_##blockCount);

DECLARE_BENCHMARK(16, 100000);
DECLARE_BENCHMARK(32, 100000);
DECLARE_BENCHMARK(1024, 100000);
DECLARE_BENCHMARK(1024, 1000000);
DECLARE_BENCHMARK(1024, 10000000);

}   // namespace NCloud::NBlockStore::NStorage
