#include "compaction_map.h"

#include <library/cpp/testing/gbenchmark/benchmark.h>

#include <util/generic/size_literals.h>
#include <util/random/fast.h>
#include <util/random/random.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

namespace {

void DoUpdate(ui64 diskSize, benchmark::State& state)
{
    constexpr size_t BlocksInRange = 1024;
    constexpr size_t BlockSize = 4096;
    const ui64 blockCount = diskSize / BlockSize;
    const ui64 rangeCount = blockCount / BlocksInRange;

    TCompressedBitmap usedBlocks(rangeCount * BlocksInRange);
    usedBlocks.Set(0, rangeCount * BlocksInRange);

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

        counters.emplace_back(i * BlocksInRange, rangeStat);
    }

    for (const auto _: state) {
        TCompactionMap compactionMap(
            BlocksInRange,
            BuildLoadOptimizationCompactionPolicy(
                {.MaxBlobSize = 4_MB,
                 .BlockSize = 4_KB,
                 .MaxReadIops = 400,
                 .MaxReadBandwidth = 15_MB,
                 .MaxWriteIops = 1000,
                 .MaxWriteBandwidth = 15_MB,
                 .MaxBlobsPerRange = 70},
                0));

        compactionMap.Update(counters, &usedBlocks);
    }
}

ui16 RandomUsedBlockCount(ui16 blockCount)
{
    const ui32 bucket = RandomNumber<ui32>(4);
    if (bucket == 0) {
        // No garbage with probability 25%.
        return blockCount;
    }
    if (bucket == 1) {
        // Everything is garbage with probability 25%.
        return 0;
    }

    if (blockCount <= 1) {
        return blockCount;
    }
    return static_cast<ui16>(RandomNumber<ui32>(blockCount - 1) + 1);
}

void DoUpdateRandomized(ui64 diskSize, benchmark::State& state)
{
    constexpr size_t BlocksInRange = 1024;
    constexpr size_t BlockSize = 4096;
    const ui64 blockCount = diskSize / BlockSize;
    const ui64 rangeCount = blockCount / BlocksInRange;

    TCompressedBitmap usedBlocks(rangeCount * BlocksInRange);
    TVector<TCompactionCounter> counters(Reserve(rangeCount));

    for (size_t i = 0; i < rangeCount; ++i) {
        const ui16 blobCount =
            static_cast<ui16>(RandomNumber<ui32>(6)); // [0, 5]
        const ui16 statBlockCount =
            static_cast<ui16>(RandomNumber<ui32>(BlocksInRange + 1)); // [0, BlocksInRange]
        const ui16 usedBlockCount = RandomUsedBlockCount(statBlockCount);
        const ui16 mixedBlockCount = static_cast<ui16>(
            RandomNumber<ui32>(statBlockCount + 1)); // [0, statBlockCount]
        const float score = 0.1f * RandomNumber<float>(); // [0.0, 0.1]

        auto rangeStat = TRangeStat(
            blobCount,
            statBlockCount,
            usedBlockCount,
            0,
            0,
            0,
            false,
            score);
        rangeStat.MixedBlockCount = mixedBlockCount;

        counters.emplace_back(i * BlocksInRange, rangeStat);

        const ui64 rangeBlockOffset = i * BlocksInRange;
        if (usedBlockCount != 0) {
            usedBlocks.Set(rangeBlockOffset, rangeBlockOffset + usedBlockCount);
        }
    }

    for (const auto _: state) {
        TCompactionMap compactionMap(
            BlocksInRange,
            BuildLoadOptimizationCompactionPolicy(
                {.MaxBlobSize = 4_MB,
                 .BlockSize = 4_KB,
                 .MaxReadIops = 400,
                 .MaxReadBandwidth = 15_MB,
                 .MaxWriteIops = 1000,
                 .MaxWriteBandwidth = 15_MB,
                 .MaxBlobsPerRange = 70},
                0));

        compactionMap.Update(counters, &usedBlocks);
    }
}

enum class ERangeUpdateKind
{
    Unchanged,
    BlockCount,
    Compacted,
};

void DoUpdateRange(
    bool loadOptimization,
    ERangeUpdateKind updateKind,
    benchmark::State& state)
{
    constexpr ui32 BlocksInRange = 1024;
    constexpr ui32 GroupCount = 256;
    constexpr ui32 BlocksInGroup = BlocksInRange * TCompactionMap::GroupSize;

    ICompactionPolicyPtr policy;
    if (loadOptimization) {
        policy = BuildLoadOptimizationCompactionPolicy(
            {.MaxBlobSize = 4_MB,
             .BlockSize = 4_KB,
             .MaxReadIops = 1000,
             .MaxReadBandwidth = 15_MB,
             .MaxWriteIops = 1000,
             .MaxWriteBandwidth = 15_MB,
             .MaxBlobsPerRange = 70},
            /*usedBlocksThresholdForMixedBlocksCompaction=*/0,
            /*mixedBlocksCountCompactionEnabled=*/false);
    } else {
        policy = BuildDefaultCompactionPolicy(
            /*compactionThreshold=*/70,
            /*usedBlocksThresholdForMixedBlocksCompaction=*/0,
            /*mixedBlocksCountCompactionEnabled=*/false);
    }
    TCompactionMap compactionMap(BlocksInRange, std::move(policy));

    // Populate one range per group, leaving the other ranges empty. Updating
    // the populated range exercises maintenance of the group's maximum.
    for (ui32 group = 0; group < GroupCount; ++group) {
        compactionMap.Update(
            group * BlocksInGroup,
            8,
            BlocksInRange,
            BlocksInRange,
            0,
            0,
            false);
    }

    // Use the same sequence for both policies and across benchmark runs.
    TFastRng64 rng(12345);
    ui64 iteration = 0;
    for (const auto _: state) {
        const ui32 blockIndex = (rng.GenRand() % GroupCount) * BlocksInGroup;
        const bool alternate = iteration++ % 2;
        ui32 blobCount = 8;
        ui32 blockCount = BlocksInRange;
        bool compacted = false;

        switch (updateKind) {
            case ERangeUpdateKind::Unchanged:
                break;
            case ERangeUpdateKind::BlockCount:
                blockCount += alternate;
                break;
            case ERangeUpdateKind::Compacted:
                compacted = !alternate;
                blobCount = compacted ? 2 : 8;
                break;
        }

        compactionMap.Update(
            blockIndex,
            blobCount,
            blockCount,
            BlocksInRange,
            0,
            0,
            compacted);
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

#define DECLARE_RANGE_UPDATE_BENCH(kind)                                       \
    void UpdateRange##kind##_Default(benchmark::State& state)                  \
    {                                                                          \
        DoUpdateRange(false, ERangeUpdateKind::kind, state);                   \
    }                                                                          \
    BENCHMARK(UpdateRange##kind##_Default);                                    \
    void UpdateRange##kind##_Load(benchmark::State& state)                     \
    {                                                                          \
        DoUpdateRange(true, ERangeUpdateKind::kind, state);                    \
    }                                                                          \
    BENCHMARK(UpdateRange##kind##_Load);

DECLARE_RANGE_UPDATE_BENCH(Unchanged)
DECLARE_RANGE_UPDATE_BENCH(BlockCount)
DECLARE_RANGE_UPDATE_BENCH(Compacted)

}   // namespace NCloud::NBlockStore::NStorage
