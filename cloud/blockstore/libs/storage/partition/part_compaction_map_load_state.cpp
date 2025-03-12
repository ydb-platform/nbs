#include "part_compaction_map_load_state.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

TCompactionMapLoadState::TCompactionMapLoadState(
    ui32 maxRangesPerTx,
    ui32 maxOutOfOrderChunksInflight)
    : MaxRangesPerTx(maxRangesPerTx)
    , MaxOutOfOrderChunksInflight(maxOutOfOrderChunksInflight)
{}

const TBlockRange32& TCompactionMapLoadState::LoadNextChunk()
{
    if (!OutOfOrderRanges.empty()) {
        LoadingRange = *OutOfOrderRanges.begin();
        OutOfOrderRanges.erase(OutOfOrderRanges.begin());
        if (LoadingRange.Start == NextRangeIndex) {
            NextRangeIndex += MaxRangesPerTx;
        }
    } else {
        LoadingRange =
            TBlockRange32::WithLength(NextRangeIndex, MaxRangesPerTx);

        NextRangeIndex += MaxRangesPerTx;
        for (; !LoadedOutOfOrderRanges.empty() &&
               NextRangeIndex == LoadedOutOfOrderRanges.begin()->Start;
               NextRangeIndex += MaxRangesPerTx)
        {
            LoadedOutOfOrderRanges.erase(LoadedOutOfOrderRanges.begin());
        }
    }
    return LoadingRange;
}

bool TCompactionMapLoadState::EnqueueOutOfOrderRanges(
    const THashSet<ui32>& rangeIndices)
{
    bool isNotLoaded = false;

    for (const ui32 rangeIndex: rangeIndices) {
        const TBlockRange32 range = TBlockRange32::WithLength(
            (rangeIndex / MaxRangesPerTx) * MaxRangesPerTx,
            MaxRangesPerTx);

        if (!IsRangeLoaded(range)) {
            isNotLoaded = true;
            EnqueueOutOfOrderRange(range);
        }
    }

    return isNotLoaded;
}

void TCompactionMapLoadState::RangeIsLoaded(const TBlockRange32& range)
{
    if (range.Start > NextRangeIndex) {
        LoadedOutOfOrderRanges.emplace(range);
    }
}

bool TCompactionMapLoadState::IsRangeLoaded(const TBlockRange32& range) const
{
    if (range.Start < NextRangeIndex && range != LoadingRange) {
        return true;
    }
    return LoadedOutOfOrderRanges.contains(range);
}

bool TCompactionMapLoadState::IsRangeInLoadingQueue(
    const TBlockRange32& range) const
{
    if (LoadingRange == range) {
        return true;
    }

    return OutOfOrderRanges.contains(range);
}

void TCompactionMapLoadState::EnqueueOutOfOrderRange(const TBlockRange32& range)
{
    if (OutOfOrderRanges.size() < MaxOutOfOrderChunksInflight &&
        !IsRangeInLoadingQueue(range))
    {
        OutOfOrderRanges.emplace(range);
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
