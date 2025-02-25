#include "compaction_map.h"

#include <library/cpp/testing/gbenchmark/benchmark.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

namespace {

void DoUpdate(ui64 diskSize, benchmark::State& state)
{
    constexpr size_t RangeSize = 1024;
    constexpr size_t BlockSize = 4096;
    const ui64 blockCount = diskSize / BlockSize;
    const ui64 rangeCount = blockCount / RangeSize;

    TCompactionMap compactionMap(
        RangeSize,
        BuildLoadOptimizationCompactionPolicy(
            {.MaxBlobSize = 4_MB,
             .BlockSize = 4_KB,
             .MaxReadIops = 400,
             .MaxReadBandwidth = 15_MB,
             .MaxWriteIops = 1000,
             .MaxWriteBandwidth = 15_MB,
             .MaxBlobsPerRange = 70}));

    TCompressedBitmap usedBlocks(rangeCount * RangeSize);
    usedBlocks.Set(0, rangeCount * RangeSize);

    TVector<TCompactionCounter> counters(Reserve(rangeCount));
    for (size_t i = 0; i < rangeCount; ++i) {
        auto rangeStat = TRangeStat(
            3,       // blobCount
            1000,    // blockCount
            0,       // usedBlockCount
            0,       // readRequestCount
            0,       // readRequestBlobCount
            0,       // readRequestBlockCount
            false,   // compacted
            0.1      // score
        );

        counters.emplace_back(i * RangeSize, rangeStat);
    }

    for (const auto _: state) {
        compactionMap.Update(counters, &usedBlocks);
    }
}

}   // namespace

#define DECLARE_BENCH(diskSize)                              \
    void Update_##diskSize(benchmark::State& state) \
    {                                                        \
        DoUpdate(diskSize, state);                  \
    }                                                        \
    BENCHMARK(Update_##diskSize);

DECLARE_BENCH(1_TB)
DECLARE_BENCH(5_TB)
DECLARE_BENCH(10_TB)
DECLARE_BENCH(50_TB)
DECLARE_BENCH(100_TB)
DECLARE_BENCH(500_TB)

}   // namespace NCloud::NBlockStore::NStorage
