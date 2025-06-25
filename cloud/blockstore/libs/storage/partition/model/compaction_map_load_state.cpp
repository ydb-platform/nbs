#include "compaction_map_load_state.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

TCompactionMapLoadState::TCompactionMapLoadState(
        ui32 maxRangesPerChunk,
        ui32 maxOutOfOrderChunksInflight)
    : MaxRangesPerChunk(maxRangesPerChunk)
    , MaxOutOfOrderChunksInflight(maxOutOfOrderChunksInflight)
{}

TBlockRange32 TCompactionMapLoadState::LoadNextChunk()
{
    if (!OutOfOrderRanges.empty()) {
        LoadingRange = *OutOfOrderRanges.begin();
        OutOfOrderRanges.erase(OutOfOrderRanges.begin());
        if (LoadingRange.Start == NextRangeIndex) {
            NextRangeIndex += MaxRangesPerChunk;
        }
    } else {
        LoadingRange =
            TBlockRange32::WithLength(NextRangeIndex, MaxRangesPerChunk);

        NextRangeIndex += MaxRangesPerChunk;
        while (!LoadedOutOfOrderRanges.empty() &&
               NextRangeIndex == LoadedOutOfOrderRanges.begin()->Start)
        {
            NextRangeIndex += MaxRangesPerChunk;
            LoadedOutOfOrderRanges.erase(LoadedOutOfOrderRanges.begin());
        }
    }
    return LoadingRange;
}

TBlockRangeSet32 TCompactionMapLoadState::GetNotLoadedRanges(
    const THashSet<ui32>& rangeIndices) const
{
    TBlockRangeSet32 ranges;
    for (const ui32 rangeIndex: rangeIndices) {
        const TBlockRange32 range = TBlockRange32::WithLength(
            (rangeIndex / MaxRangesPerChunk) * MaxRangesPerChunk,
            MaxRangesPerChunk);

        if (!IsRangeLoaded(range)) {
            ranges.emplace(range);
        }
    }

    return ranges;
}

void TCompactionMapLoadState::EnqueueOutOfOrderRanges(
    const TBlockRangeSet32& ranges)
{
    for (const auto range: ranges) {
        EnqueueOutOfOrderRange(range);
    }
}

void TCompactionMapLoadState::OnRangeLoaded(TBlockRange32 range)
{
    if (range.Start > NextRangeIndex) {
        LoadedOutOfOrderRanges.emplace(range);
    }
}

bool TCompactionMapLoadState::IsRangeLoaded(TBlockRange32 range) const
{
    if (range.Start < NextRangeIndex && range != LoadingRange) {
        return true;
    }
    return LoadedOutOfOrderRanges.contains(range);
}

bool TCompactionMapLoadState::IsRangeLoading(TBlockRange32 range) const
{
    if (LoadingRange == range) {
        return true;
    }

    return OutOfOrderRanges.contains(range);
}

void TCompactionMapLoadState::EnqueueOutOfOrderRange(TBlockRange32 range)
{
    if (OutOfOrderRanges.size() < MaxOutOfOrderChunksInflight &&
        !IsRangeLoading(range))
    {
        OutOfOrderRanges.emplace(range);
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
