#include "compaction_map.h"

#include <library/cpp/testing/gbenchmark/benchmark.h>

#include <util/generic/size_literals.h>
#include <util/random/random.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

namespace {

void DoUpdate(ui64 diskSize, benchmark::State& state)
{
    constexpr size_t RangeSize = 1024;
    constexpr size_t BlockSize = 4096;
    const ui64 blockCount = diskSize / BlockSize;
    const ui64 rangeCount = blockCount / RangeSize;

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

        compactionMap.Update(counters, &usedBlocks);
    }
}

ui16 RandomUsedBlockCount(ui16 blockCount)
{
    const ui32 bucket = RandomNumber<ui32>(4);
    if (bucket == 0) {
        return blockCount;
    }
    if (bucket == 1) {
        return 0;
    }
    return static_cast<ui16>(RandomNumber<ui32>(blockCount - 1) + 1);
}

void DoUpdateRandomized(ui64 diskSize, benchmark::State& state)
{
    constexpr size_t RangeSize = 1024;
    constexpr size_t BlockSize = 4096;
    const ui64 blockCount = diskSize / BlockSize;
    const ui64 rangeCount = blockCount / RangeSize;

    TCompressedBitmap usedBlocks(rangeCount * RangeSize);

    TVector<TCompactionCounter> counters(Reserve(rangeCount));
    for (size_t i = 0; i < rangeCount; ++i) {
        const ui16 blobCount =
            static_cast<ui16>(RandomNumber<ui32>(5) + 1);
        const ui16 statBlockCount =
            static_cast<ui16>(RandomNumber<ui32>(901) + 100);
        const ui16 usedBlockCount = RandomUsedBlockCount(statBlockCount);
        const float score = 0.1f + 0.1f * RandomNumber<float>();

        auto rangeStat = TRangeStat(
            blobCount,
            statBlockCount,
            usedBlockCount,
            0,
            0,
            0,
            false,
            score);

        counters.emplace_back(i * RangeSize, rangeStat);

        const ui64 rangeBase = i * RangeSize;
        if (usedBlockCount != 0) {
            usedBlocks.Set(rangeBase, rangeBase + usedBlockCount);
        }
    }

    for (const auto _: state) {
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

#define DECLARE_BENCH_RANDOMIZED(diskSize)                   \
    void UpdateRandomized_##diskSize(benchmark::State& state)  \
    {                                                        \
        DoUpdateRandomized(diskSize, state);                 \
    }                                                        \
    BENCHMARK(UpdateRandomized_##diskSize);

DECLARE_BENCH_RANDOMIZED(1_TB)
DECLARE_BENCH_RANDOMIZED(5_TB)
DECLARE_BENCH_RANDOMIZED(10_TB)
DECLARE_BENCH_RANDOMIZED(50_TB)
DECLARE_BENCH_RANDOMIZED(100_TB)
DECLARE_BENCH_RANDOMIZED(500_TB)

}   // namespace NCloud::NBlockStore::NStorage
