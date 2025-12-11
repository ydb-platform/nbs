#include <cloud/blockstore/libs/storage/partition2/model/blob_index.h>
#include <cloud/blockstore/libs/storage/partition2/model/disjoint_range_map.h>

#include <library/cpp/testing/benchmark/bench.h>

#include <util/generic/algorithm.h>
#include <util/random/fast.h>

using namespace NCloud;
using namespace NCloud::NBlockStore;
using namespace NCloud::NBlockStore::NStorage;
using namespace NCloud::NBlockStore::NStorage::NPartition2;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TBlobBuilder
{
private:
    TVector<TAddBlob> Blobs;
    TAddBlob* Current = nullptr;
    ui32 Cookie = 0;

public:
    void AddBlock(const TBlock& block)
    {
        if (!Current) {
            Blobs.emplace_back();
            Current = &Blobs.back();
        }

        Current->Blocks.push_back(block);

        if (Current->Blocks.size() == MaxBlocksCount) {
            Finalize();
        }
    }

    TVector<TAddBlob> Finish()
    {
        if (Current) {
            Finalize();
        }
        return std::move(Blobs);
    }

private:
    void Finalize()
    {
        Current->BlobId = TPartialBlobId(
            12,     // gen
            3456,   // step
            3,      // channel
            4096 * Current->Blocks.size(),
            Cookie++,
            0);
        Current = nullptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

TFastRng<ui32> Random(12345);

TVector<TAddBlob> GetRandomBlobs(size_t blocksCount = 1000 * MaxBlocksCount)
{
    ui32 blockIndex = 123456;
    ui64 commitId = MakeCommitId(12, 345);

    TVector<TBlock> blocks(Reserve(blocksCount));
    for (size_t i = 0; i < blocksCount; ++i) {
        blocks.emplace_back(
            blockIndex + Random.Uniform(10000),
            commitId + Random.Uniform(10),
            InvalidCommitId,
            false);
    }
    Sort(blocks);

    TBlobBuilder builder;
    for (const auto& block: blocks) {
        builder.AddBlock(block);
    }
    return builder.Finish();
}

struct TMark
{
    TBlockRange32 Range;
    ui64 Mark = 0;
};

TVector<TMark> GetRandomMarks(size_t rangeSize, size_t blocksCount = 1 << 20)
{
    TVector<TMark> marks(Reserve(blocksCount));
    ui64 mark = 0;
    for (size_t i = 0; i < blocksCount; ++i) {
        TBlockRange32 range;
        range.Start = Random.Uniform(blocksCount - rangeSize + 1);
        range.End = range.Start + rangeSize - 1;
        marks.push_back({range, ++mark});
    }

    return marks;
}

////////////////////////////////////////////////////////////////////////////////

const ui32 ZoneCount = 32;
const ui32 ZoneBlockCount = 32 * MaxBlocksCount;
const ui32 BlockListCacheSize = 32 * 32;
const ui32 MaxBlocksInBlob = 1024;
const EOptimizationMode DeletedRangesMapMode =
    EOptimizationMode::OptimizeForShortRanges;

////////////////////////////////////////////////////////////////////////////////

void FindBlob(
    const TVector<TAddBlob>& blobs,
    const NBench::NCpu::TParams& iface)
{
    TBlobIndex blobIndex(
        ZoneCount,
        ZoneBlockCount,
        BlockListCacheSize,
        MaxBlocksInBlob,
        DeletedRangesMapMode);
    for (const auto& blob: blobs) {
        auto blobRange = TBlockRange32::MakeClosedInterval(
            blob.Blocks.front().BlockIndex,
            blob.Blocks.back().BlockIndex);

        ui32 z0 = blobRange.Start / ZoneBlockCount;
        ui32 z1 = blobRange.End / ZoneBlockCount;
        for (ui32 z = z0; z <= z1; ++z) {
            blobIndex.InitializeZone(z);
        }
        blobIndex.AddBlob(blob.BlobId, {blobRange}, blob.Blocks.size(), 0);
    }

    size_t index = 0;
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        const auto& blob = blobs[index];
        if (++index == blobs.size()) {
            index = 0;
        }

        Y_DO_NOT_OPTIMIZE_AWAY(
            blobIndex
                .FindBlob(
                    blob.Blocks.front().BlockIndex / ZoneBlockCount,
                    blob.BlobId)
                .Blob);
    }
}

void FindBlobs(
    const TVector<TAddBlob>& blobs,
    const NBench::NCpu::TParams& iface)
{
    TBlobIndex blobIndex(
        ZoneCount,
        ZoneBlockCount,
        BlockListCacheSize,
        MaxBlocksInBlob,
        DeletedRangesMapMode);
    for (const auto& blob: blobs) {
        auto blobRange = TBlockRange32::MakeClosedInterval(
            blob.Blocks.front().BlockIndex,
            blob.Blocks.back().BlockIndex);

        ui32 z0 = blobRange.Start / ZoneBlockCount;
        ui32 z1 = blobRange.End / ZoneBlockCount;
        for (ui32 z = z0; z <= z1; ++z) {
            blobIndex.InitializeZone(z);
        }
        blobIndex.AddBlob(blob.BlobId, {blobRange}, blob.Blocks.size(), 0);
    }

    size_t index = 0;
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        const auto& blob = blobs[index];
        if (++index == blobs.size()) {
            index = 0;
        }

        auto blobRange = TBlockRange32::MakeClosedInterval(
            blob.Blocks.front().BlockIndex,
            blob.Blocks.back().BlockIndex);

        ui32 rangeSize = 1 + Random.Uniform(blobRange.Size() / 2);
        ui32 startIndex =
            blobRange.Start + Random.Uniform(blobRange.Size() - rangeSize);

        Y_DO_NOT_OPTIMIZE_AWAY(blobIndex.FindBlobs(
            TBlockRange32::WithLength(startIndex, rangeSize)));
    }
}

void MarkAndFind(
    const TVector<TMark>& marks,
    const EOptimizationMode mode,
    const NBench::NCpu::TParams& iface)
{
    TDisjointRangeMap m(mode);
    TVector<TDeletedBlock> omarks;
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        for (const auto& mark: marks) {
            m.Mark(mark.Range, mark.Mark);
        }

        for (const auto& mark: marks) {
            omarks.clear();
            m.FindMarks(mark.Range, Max<ui64>(), &omarks);
        }

        m.Clear();
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

static const auto RandomBlobs = GetRandomBlobs();
static const auto RandomShortMarks = GetRandomMarks(1);
static const auto RandomLongMarks = GetRandomMarks(128);

Y_CPU_BENCHMARK(TBlobIndex_FindBlob, iface)
{
    FindBlob(RandomBlobs, iface);
}

Y_CPU_BENCHMARK(TBlobIndex_FindBlobs, iface)
{
    FindBlobs(RandomBlobs, iface);
}

Y_CPU_BENCHMARK(TDisjointRangeMap_OptimizeForLongRanges_LongMarks, iface)
{
    MarkAndFind(
        RandomLongMarks,
        EOptimizationMode::OptimizeForLongRanges,
        iface);
}

Y_CPU_BENCHMARK(TDisjointRangeMap_OptimizeForLongRanges_ShortMarks, iface)
{
    MarkAndFind(
        RandomShortMarks,
        EOptimizationMode::OptimizeForLongRanges,
        iface);
}

Y_CPU_BENCHMARK(TDisjointRangeMap_OptimizeForShortRanges_LongMarks, iface)
{
    MarkAndFind(
        RandomLongMarks,
        EOptimizationMode::OptimizeForShortRanges,
        iface);
}

Y_CPU_BENCHMARK(TDisjointRangeMap_OptimizeForShortRanges_ShortMarks, iface)
{
    MarkAndFind(
        RandomShortMarks,
        EOptimizationMode::OptimizeForShortRanges,
        iface);
}
