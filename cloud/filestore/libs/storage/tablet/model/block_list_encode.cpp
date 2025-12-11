#include "block_list.h"

#include "binary_writer.h"
#include "block_list_spec.h"
#include "group_by.h"

#include <util/generic/algorithm.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

size_t GetBlockOffset(const TVector<TBlock>& blocks, const TBlock& block)
{
    size_t offset = &block - &blocks[0];
    Y_ABORT_UNLESS(&blocks[offset] == &block);
    return offset;
}

TByteVector
EncodeBlockEntries(const TBlock& block, ui32 blocksCount, IAllocator* alloc)
{
    TBinaryWriter writer(alloc);

    writer.Write<NBlockListSpec::TListHeader>(
        {NBlockListSpec::TListHeader::Blocks});

    writer.Write<NBlockListSpec::TMergedBlockGroup>({
        block.NodeId,
        block.MinCommitId,
        static_cast<ui16>(blocksCount),
        block.BlockIndex,
        0   // blobOffset
    });

    return writer.Finish();
}

TByteVector EncodeBlockEntries(const TVector<TBlock>& blocks, IAllocator* alloc)
{
    TBinaryWriter writer(alloc);

    writer.Write<NBlockListSpec::TListHeader>(
        {NBlockListSpec::TListHeader::Blocks});

    auto writeSingleEntry = [&](const TBlock& block)
    {
        writer.Write<NBlockListSpec::TSingleBlockEntry>(
            {block.NodeId,
             block.MinCommitId,
             block.BlockIndex,
             static_cast<ui16>(GetBlockOffset(blocks, block))});
    };

    auto writeMergedGroup = [&](TArrayRef<const TBlock*> group)
    {
        const auto& block = *group[0];
        writer.Write<NBlockListSpec::TMergedBlockGroup>(
            {block.NodeId,
             block.MinCommitId,
             static_cast<ui16>(group.size()),
             block.BlockIndex,
             static_cast<ui16>(GetBlockOffset(blocks, block))});
    };

    auto writeMixedGroup = [&](TArrayRef<const TBlock*> group)
    {
        const auto& block = *group[0];
        writer.Write<NBlockListSpec::TMixedBlockGroup>(
            {block.NodeId, block.MinCommitId, static_cast<ui16>(group.size())});

        for (const auto* block: group) {
            writer.Write<ui32>(block->BlockIndex);
        }
        for (const auto* block: group) {
            writer.Write<ui16>(GetBlockOffset(blocks, *block));
        }
        if (group.size() & 1) {
            writer.Write<ui16>(0);   // padding
        }
    };

    GroupBy(
        MakeArrayRef(blocks),
        [](const TBlock& l, const TBlock& r) { return l.NodeId == r.NodeId; },
        [&](TArrayRef<const TBlock> group)
        {
            if (group.size() == 1) {
                writeSingleEntry(group[0]);
                return;
            }

            TVector<const TBlock*> refs(Reserve(group.size()));
            for (const TBlock& block: group) {
                refs.push_back(&block);
            }

            // order by (MinCommitId DESC)
            StableSort(
                refs,
                [](const TBlock* l, const TBlock* r)
                { return l->MinCommitId > r->MinCommitId; });

            GroupBy(
                MakeArrayRef(refs),
                [](const TBlock* l, const TBlock* r)
                { return l->MinCommitId == r->MinCommitId; },
                [&](TArrayRef<const TBlock*> group)
                {
                    if (group.size() == 1) {
                        writeSingleEntry(*group[0]);
                        return;
                    }

                    TVector<const TBlock*> mixedGroup;
                    GroupBy(
                        group,
                        [](const TBlock* l, const TBlock* r) {
                            return r == l + 1 &&
                                   r->BlockIndex == l->BlockIndex + 1;
                        },
                        [&](TArrayRef<const TBlock*> mergedGroup)
                        {
                            if (mergedGroup.size() >=
                                NBlockListSpec::MergedGroupMinSize) {
                                writeMergedGroup(mergedGroup);
                            } else {
                                for (const TBlock* block: mergedGroup) {
                                    mixedGroup.push_back(block);
                                }
                            }
                        });

                    if (mixedGroup.size() == 1) {
                        writeSingleEntry(*mixedGroup[0]);
                    } else if (mixedGroup.size() > 1) {
                        writeMixedGroup(mixedGroup);
                    }
                });
        });

    return writer.Finish();
}

TByteVector
EncodeDeletionMarkers(const TBlock& block, ui32 blocksCount, IAllocator* alloc)
{
    TBinaryWriter writer(alloc);

    writer.Write<NBlockListSpec::TListHeader>(
        {NBlockListSpec::TListHeader::DeletionMarkers});

    if (block.MaxCommitId != InvalidCommitId) {
        writer.Write<NBlockListSpec::TMergedDeletionGroup>({
            block.MaxCommitId,
            static_cast<ui16>(blocksCount),
            0   // blobOffset
        });
    }

    return writer.Finish();
}

TByteVector EncodeDeletionMarkers(
    const TVector<TBlock>& blocks,
    IAllocator* alloc)
{
    TBinaryWriter writer(alloc);

    writer.Write<NBlockListSpec::TListHeader>(
        {NBlockListSpec::TListHeader::DeletionMarkers});

    auto writeSingleEntry = [&](const TBlock& block)
    {
        writer.Write<NBlockListSpec::TSingleDeletionMarker>(
            {block.MaxCommitId,
             static_cast<ui16>(GetBlockOffset(blocks, block))});
    };

    auto writeMergedGroup = [&](TArrayRef<const TBlock*> group)
    {
        const auto& block = *group[0];
        writer.Write<NBlockListSpec::TMergedDeletionGroup>(
            {block.MaxCommitId,
             static_cast<ui16>(group.size()),
             static_cast<ui16>(GetBlockOffset(blocks, block))});
    };

    auto writeMixedGroup = [&](TArrayRef<const TBlock*> group)
    {
        const auto& block = *group[0];
        writer.Write<NBlockListSpec::TMixedDeletionGroup>(
            {block.MaxCommitId, static_cast<ui16>(group.size())});

        for (const auto* block: group) {
            writer.Write<ui16>(GetBlockOffset(blocks, *block));
        }
        if (group.size() & 1) {
            writer.Write<ui16>(0);   // padding
        }
    };

    TVector<const TBlock*> refs(Reserve(blocks.size()));
    for (const TBlock& block: blocks) {
        if (block.MaxCommitId != InvalidCommitId) {
            refs.push_back(&block);
        }
    }

    // order by (MaxCommitId DESC)
    StableSort(
        refs,
        [](const TBlock* l, const TBlock* r)
        { return l->MaxCommitId > r->MaxCommitId; });

    GroupBy(
        MakeArrayRef(refs),
        [](const TBlock* l, const TBlock* r)
        { return l->MaxCommitId == r->MaxCommitId; },
        [&](TArrayRef<const TBlock*> group)
        {
            if (group.size() == 1) {
                writeSingleEntry(*group[0]);
                return;
            }

            TVector<const TBlock*> mixedGroup;
            GroupBy(
                group,
                [](const TBlock* l, const TBlock* r) { return r == l + 1; },
                [&](TArrayRef<const TBlock*> mergedGroup)
                {
                    if (mergedGroup.size() >=
                        NBlockListSpec::MergedGroupMinSize) {
                        writeMergedGroup(mergedGroup);
                    } else {
                        for (const TBlock* block: mergedGroup) {
                            mixedGroup.push_back(block);
                        }
                    }
                });

            if (mixedGroup.size() == 1) {
                writeSingleEntry(*mixedGroup[0]);
            } else if (mixedGroup.size() > 1) {
                writeMixedGroup(mixedGroup);
            }
        });

    return writer.Finish();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TBlockList TBlockList::EncodeBlocks(
    const TBlock& block,
    ui32 blocksCount,
    IAllocator* alloc)
{
    Y_ABORT_UNLESS(blocksCount <= MaxBlocksCount);

    return {
        EncodeBlockEntries(block, blocksCount, alloc),
        EncodeDeletionMarkers(block, blocksCount, alloc),
    };
}

TBlockList TBlockList::EncodeBlocks(
    const TVector<TBlock>& blocks,
    IAllocator* alloc)
{
    Y_ABORT_UNLESS(blocks.size() <= MaxBlocksCount);

    Y_IF_DEBUG({
        // sanity check
        bool sorted = IsSorted(blocks.begin(), blocks.end(), TBlockCompare());
        Y_ABORT_UNLESS(sorted);
    });

    return {
        EncodeBlockEntries(blocks, alloc),
        EncodeDeletionMarkers(blocks, alloc),
    };
}

}   // namespace NCloud::NFileStore::NStorage
