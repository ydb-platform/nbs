#include "rebase_logic.h"

#include "block.h"

#include <util/generic/algorithm.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

bool CheckSortedWithGaps(const TVector<TBlock>& blocks)
{
    TBlock prevBlock;

    for (const auto& block: blocks) {
        if (block.MinCommitId == block.MaxCommitId) {
            continue;
        }

        if (prevBlock.MinCommitId != prevBlock.MaxCommitId && block < prevBlock)
        {
            return false;
        }

        prevBlock = block;
    }

    return true;
}

template <typename It>
auto LinearSearchEqualRange(It f, It l, ui32 blockIndex)
{
    while (f != l && f->BlockIndex != blockIndex) {
        ++f;
    }

    auto begin = f;
    while (f != l && f->BlockIndex == blockIndex) {
        ++f;
    }

    return std::make_pair(begin, f);
}

template <class T>
bool IsSortedAndUnique(const TVector<T>& v)
{
    for (ui32 i = 1; i < v.size(); ++i) {
        if (v[i - 1] >= v[i]) {
            return false;
        }
    }

    return true;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TBlockCounts RebaseBlocks(
    const IPivotalCommitStorage& pivotalCommitStorage,
    const std::function<bool(ui32)>& isFrozenBlock,
    const ui64 lastCommitId,
    TVector<TBlock>& blocks,
    TSet<ui64>& pivotalCommitIds)
{
    TBlockCounts counts;

    for (ui32 i = 0; i < blocks.size(); ++i) {
        auto& block = blocks[i];

        ui64 minCommitId = block.MinCommitId;
        ui64 maxCommitId = block.MaxCommitId;
        bool visibleAtPivot = false;
        const bool markedDeleted = minCommitId == maxCommitId;

        if (!markedDeleted) {
            minCommitId = pivotalCommitStorage.RebaseCommitId(minCommitId);
            if (minCommitId == InvalidCommitId) {
                minCommitId = lastCommitId;
            }

            if (maxCommitId != InvalidCommitId && minCommitId < maxCommitId) {
                visibleAtPivot = true;
                pivotalCommitIds.insert(minCommitId);

                ++counts.CheckpointBlocks;
            }

            if (maxCommitId != InvalidCommitId) {
                maxCommitId = pivotalCommitStorage.RebaseCommitId(maxCommitId);
                if (maxCommitId == InvalidCommitId) {
                    maxCommitId = lastCommitId;
                }
            }
        }

        if (maxCommitId == InvalidCommitId || visibleAtPivot) {
            ++counts.LiveBlocks;

            // we should not change relative order of the block versions
            if (!isFrozenBlock(block.BlockIndex)) {
                block.MinCommitId = minCommitId;
                block.MaxCommitId = maxCommitId;
            }
        } else {
            ++counts.GarbageBlocks;

            // block is not visible anymore and could be safely deleted
            block.MinCommitId = 0;
            block.MaxCommitId = 0;
        }
    }

    return counts;
}

ui32 MarkOverwrittenBlocks(
    const TVector<TBlock>& overwritten,
    TVector<TBlock>& blocks)
{
    Y_DEBUG_ABORT_UNLESS(IsSorted(overwritten.begin(), overwritten.end()));
    Y_DEBUG_ABORT_UNLESS(CheckSortedWithGaps(blocks));
    Y_DEBUG_ABORT_UNLESS(overwritten.size() <= blocks.size());

    auto o = overwritten.begin();
    auto b = blocks.begin();

    ui32 c = 0;

    while (o != overwritten.end()) {
        const auto blockIndex = o->BlockIndex;

        while (b != blocks.end() && b->BlockIndex < blockIndex) {
            ++b;
        }

        while (b != blocks.end() && b->BlockIndex == blockIndex) {
            b->MaxCommitId = b->MinCommitId;
            ++c;
            ++b;
        }

        while (o != overwritten.end() && o->BlockIndex == blockIndex) {
            ++o;
        }
    }

    return c;
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
