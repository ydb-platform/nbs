#include "block_list.h"

#include "binary_reader.h"
#include "block_list_spec.h"

#include <util/generic/algorithm.h>
#include <util/system/align.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
T Align2(T count)
{
    return AlignUp<T>(count, 2);
}

template <typename T>
size_t FindOffset(const T* begin, const T* end, T item)
{
    static constexpr size_t LinearSearchLimit = 10;

    if (begin + LinearSearchLimit > end) {
        const T* it = std::find(begin, end, item);
        if (it != end) {
            return it - begin;
        }
    } else {
        const T* it = std::lower_bound(begin, end, item);
        if (it != end && *it == item) {
            return it - begin;
        }
    }

    return NPOS;
}

size_t DecodeBlockEntries(const TByteVector& encodedBlocks, TVector<TBlock>& blocks)
{
    TBinaryReader reader(encodedBlocks);

    const auto& header = reader.Read<NBlockListSpec::TListHeader>();
    Y_ABORT_UNLESS(header.ListType == NBlockListSpec::TListHeader::Blocks);

    size_t blocksCount = 0;
    while (reader.Avail()) {
        const auto& group = reader.Read<NBlockListSpec::TGroupHeader>();
        if (!group.IsMulti) {
            const auto& entry = reader.Read<NBlockListSpec::TBlockEntry>();
            Y_ABORT_UNLESS(entry.BlobOffset < blocks.size());
            blocks[entry.BlobOffset] = TBlock(
                group.NodeId,
                entry.BlockIndex,
                group.CommitId,
                InvalidCommitId);
            ++blocksCount;
        } else {
            const auto& multi = reader.Read<NBlockListSpec::TMultiGroupHeader>();
            switch (multi.GroupType) {
                case NBlockListSpec::TMultiGroupHeader::MergedGroup: {
                    const auto& entry = reader.Read<NBlockListSpec::TBlockEntry>();
                    Y_ABORT_UNLESS(entry.BlobOffset + multi.Count <= blocks.size());
                    for (size_t i = 0; i < multi.Count; ++i) {
                        blocks[entry.BlobOffset + i] = TBlock(
                            group.NodeId,
                            entry.BlockIndex + i,
                            group.CommitId,
                            InvalidCommitId);
                    }
                    blocksCount += multi.Count;
                    break;
                }

                case NBlockListSpec::TMultiGroupHeader::MixedGroup: {
                    const auto* blockIndices = reader.Read<ui32>(multi.Count);
                    const auto* blobOffsets = reader.Read<ui16>(Align2(multi.Count));
                    for (size_t i = 0; i < multi.Count; ++i) {
                        Y_ABORT_UNLESS(blobOffsets[i] < blocks.size());
                        blocks[blobOffsets[i]] = TBlock(
                            group.NodeId,
                            blockIndices[i],
                            group.CommitId,
                            InvalidCommitId);
                    }
                    blocksCount += multi.Count;
                    break;
                }
            }
        }
    }

    return blocksCount;
}

void StatBlockEntries(const TByteVector& encodedBlocks, TBlockList::TStats& stats)
{
    TBinaryReader reader(encodedBlocks);

    const auto& header = reader.Read<NBlockListSpec::TListHeader>();
    Y_ABORT_UNLESS(header.ListType == NBlockListSpec::TListHeader::Blocks);

    while (reader.Avail()) {
        const auto& group = reader.Read<NBlockListSpec::TGroupHeader>();
        if (!group.IsMulti) {
            const auto& entry = reader.Read<NBlockListSpec::TBlockEntry>();
            Y_UNUSED(entry);

            ++stats.BlockEntries;
            ++stats.BlockGroups;
        } else {
            const auto& multi = reader.Read<NBlockListSpec::TMultiGroupHeader>();
            switch (multi.GroupType) {
                case NBlockListSpec::TMultiGroupHeader::MergedGroup: {
                    const auto& entry = reader.Read<NBlockListSpec::TBlockEntry>();
                    Y_UNUSED(entry);

                    stats.BlockEntries += multi.Count;
                    ++stats.BlockGroups;
                    break;
                }

                case NBlockListSpec::TMultiGroupHeader::MixedGroup: {
                    const auto* blockIndices = reader.Read<ui32>(multi.Count);
                    const auto* blobOffsets = reader.Read<ui16>(Align2(multi.Count));
                    Y_UNUSED(blockIndices);
                    Y_UNUSED(blobOffsets);

                    stats.BlockEntries += multi.Count;
                    ++stats.BlockGroups;
                    break;
                }
            }
        }
    }
}

void DecodeDeletionMarkers(const TByteVector& encodedDeletionMarkers, TVector<TBlock>& blocks)
{
    TBinaryReader reader(encodedDeletionMarkers);

    const auto& header = reader.Read<NBlockListSpec::TListHeader>();
    Y_ABORT_UNLESS(header.ListType == NBlockListSpec::TListHeader::DeletionMarkers);

    while (reader.Avail()) {
        const auto& group = reader.Read<NBlockListSpec::TGroupHeader>();
        if (!group.IsMulti) {
            const auto& entry = reader.Read<NBlockListSpec::TDeletionMarker>();
            Y_ABORT_UNLESS(entry.BlobOffset < blocks.size());
            blocks[entry.BlobOffset].MaxCommitId = group.CommitId;
        } else {
            const auto& multi = reader.Read<NBlockListSpec::TMultiGroupHeader>();
            switch (multi.GroupType) {
                case NBlockListSpec::TMultiGroupHeader::MergedGroup: {
                    const auto& entry = reader.Read<NBlockListSpec::TDeletionMarker>();
                    Y_ABORT_UNLESS(entry.BlobOffset + multi.Count <= blocks.size());
                    for (size_t i = 0; i < multi.Count; ++i) {
                        blocks[entry.BlobOffset + i].MaxCommitId = group.CommitId;
                    }
                    break;
                }

                case NBlockListSpec::TMultiGroupHeader::MixedGroup: {
                    const auto* blobOffsets = reader.Read<ui16>(Align2(multi.Count));
                    for (size_t i = 0; i < multi.Count; ++i) {
                        Y_ABORT_UNLESS(blobOffsets[i] < blocks.size());
                        blocks[blobOffsets[i]].MaxCommitId = group.CommitId;
                    }
                    break;
                }
            }
        }
    }
}

// Returns number of blocks with max commit id equal to |maxCommitId|
ui32 FindDeletionMarkers(
    const TByteVector& encodedDeletionMarkers,
    ui16 blobOffset,
    ui32 maxBlocksToFind,
    ui64* maxCommitId)
{
    Y_ABORT_UNLESS(maxCommitId);
    Y_ABORT_UNLESS(maxBlocksToFind);

    ui16 minSeenBlobOffset = Max<ui16>();

    TBinaryReader reader(encodedDeletionMarkers);

    const auto& header = reader.Read<NBlockListSpec::TListHeader>();
    Y_ABORT_UNLESS(header.ListType == NBlockListSpec::TListHeader::DeletionMarkers);

    while (reader.Avail()) {
        const auto& group = reader.Read<NBlockListSpec::TGroupHeader>();
        if (!group.IsMulti) {
            const auto& entry = reader.Read<NBlockListSpec::TDeletionMarker>();
            if (entry.BlobOffset == blobOffset) {
                *maxCommitId = group.CommitId;
                return 1;
            }

            if (blobOffset <= entry.BlobOffset &&
                entry.BlobOffset < blobOffset + maxBlocksToFind)
            {
                minSeenBlobOffset = Min<ui16>(minSeenBlobOffset, entry.BlobOffset);
            }
        } else {
            const auto& multi = reader.Read<NBlockListSpec::TMultiGroupHeader>();
            switch (multi.GroupType) {
                case NBlockListSpec::TMultiGroupHeader::MergedGroup: {
                    const auto& entry = reader.Read<NBlockListSpec::TDeletionMarker>();
                    const auto maxBlobOffset = Min<ui32>(
                        blobOffset + maxBlocksToFind,
                        entry.BlobOffset + multi.Count);
                    if (entry.BlobOffset <= blobOffset && blobOffset < maxBlobOffset) {
                        *maxCommitId = group.CommitId;
                        return maxBlobOffset - blobOffset;
                    }
                    break;
                }

                case NBlockListSpec::TMultiGroupHeader::MixedGroup: {
                    const auto* blobOffsets = reader.Read<ui16>(Align2(multi.Count));
                    size_t i = FindOffset(
                        blobOffsets,
                        blobOffsets + multi.Count,
                        blobOffset);
                    if (i != NPOS) {
                        *maxCommitId = group.CommitId;
                        return 1;
                    }

                    if (blobOffset <= blobOffsets[0] &&
                        blobOffsets[0] < blobOffset + maxBlocksToFind)
                    {
                        minSeenBlobOffset = Min<ui16>(minSeenBlobOffset, blobOffsets[0]);
                    }
                    break;
                }
            }
        }
    }

    *maxCommitId = InvalidCommitId;

    if (minSeenBlobOffset == Max<ui16>()) {
        return maxBlocksToFind;
    }

    if (minSeenBlobOffset <= blobOffset) {
        return 1;
    }

    return Min<ui16>(minSeenBlobOffset - blobOffset, maxBlocksToFind);
}

ui64 FindDeletionMarker(
    const TByteVector& encodedDeletionMarkers,
    ui16 blobOffset)
{
    ui64 maxCommitId = 0;
    FindDeletionMarkers(
        encodedDeletionMarkers,
        blobOffset,
        /*maxBlocksToFind=*/1,
        &maxCommitId);
    return maxCommitId;
}

void StatDeletionMarkers(const TByteVector& encodedDeletionMarkers, TBlockList::TStats& stats)
{
    TBinaryReader reader(encodedDeletionMarkers);

    const auto& header = reader.Read<NBlockListSpec::TListHeader>();
    Y_ABORT_UNLESS(header.ListType == NBlockListSpec::TListHeader::DeletionMarkers);

    while (reader.Avail()) {
        const auto& group = reader.Read<NBlockListSpec::TGroupHeader>();
        if (!group.IsMulti) {
            const auto& entry = reader.Read<NBlockListSpec::TDeletionMarker>();
            Y_UNUSED(entry);

            ++stats.DeletionMarkers;
            ++stats.DeletionGroups;
        } else {
            const auto& multi = reader.Read<NBlockListSpec::TMultiGroupHeader>();
            switch (multi.GroupType) {
                case NBlockListSpec::TMultiGroupHeader::MergedGroup: {
                    const auto& entry = reader.Read<NBlockListSpec::TDeletionMarker>();
                    Y_UNUSED(entry);

                    stats.DeletionMarkers += multi.Count;
                    ++stats.DeletionGroups;
                    break;
                }

                case NBlockListSpec::TMultiGroupHeader::MixedGroup: {
                    const auto* blobOffsets = reader.Read<ui16>(Align2(multi.Count));
                    Y_UNUSED(blobOffsets);

                    stats.DeletionMarkers += multi.Count;
                    ++stats.DeletionGroups;
                    break;
                }
            }
        }
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

bool TBlockIterator::TBlockFilter::CheckGroup(ui64 nodeId, ui64 minCommitId)
{
    return NodeId == nodeId && CommitId >= minCommitId;
}

bool TBlockIterator::TBlockFilter::CheckEntry(ui32 blockIndex, ui64 maxCommitId)
{
    return CommitId < maxCommitId
        && MinBlockIndex <= blockIndex
        && MaxBlockIndex > blockIndex;
}

////////////////////////////////////////////////////////////////////////////////

TBlockIterator::TBlockIterator(
        const TByteVector& encodedBlocks,
        const TByteVector& encodedDeletionMarkers,
        const TBlockFilter& filter)
    : Reader(encodedBlocks)
    , EncodedDeletionMarkers(encodedDeletionMarkers)
    , Filter(filter)
{
    const auto& header = Reader.Read<NBlockListSpec::TListHeader>();
    Y_ABORT_UNLESS(header.ListType == NBlockListSpec::TListHeader::Blocks);

    Zero(Group);
}

bool TBlockIterator::Next()
{
    for (;;) {
        if (NextBlock && (this->*NextBlock)()) {
            return true;
        }

        if (!Reader.Avail()) {
            return false;
        }

        const auto& group = Reader.Read<NBlockListSpec::TGroupHeader>();
        if (!group.IsMulti) {
            const auto& entry = Reader.Read<NBlockListSpec::TBlockEntry>();
            if (Filter.CheckGroup(group.NodeId, group.CommitId)) {
                const ui64 maxCommitId = FindDeletionMarker(
                    EncodedDeletionMarkers,
                    entry.BlobOffset);

                if (Filter.CheckEntry(entry.BlockIndex, maxCommitId)) {
                    Block.NodeId = group.NodeId;
                    Block.BlockIndex = entry.BlockIndex;
                    Block.MinCommitId = group.CommitId;
                    Block.MaxCommitId = maxCommitId;
                    BlobOffset = entry.BlobOffset;
                    BlocksInCurrentIteration = 1;
                    return true;
                }
            }
        } else {
            const auto& multi = Reader.Read<NBlockListSpec::TMultiGroupHeader>();
            switch (multi.GroupType) {
                case NBlockListSpec::TMultiGroupHeader::MergedGroup: {
                    const auto& entry = Reader.Read<NBlockListSpec::TBlockEntry>();
                    if (Filter.CheckGroup(group.NodeId, group.CommitId)) {
                        SetMerged(
                            group.NodeId,
                            group.CommitId,
                            multi.Count,
                            entry.BlockIndex,
                            entry.BlobOffset);
                    }
                    break;
                }

                case NBlockListSpec::TMultiGroupHeader::MixedGroup: {
                    const auto* blockIndices = Reader.Read<ui32>(multi.Count);
                    const auto* blobOffsets = Reader.Read<ui16>(Align2(multi.Count));
                    if (Filter.CheckGroup(group.NodeId, group.CommitId)) {
                        SetMixed(
                            group.NodeId,
                            group.CommitId,
                            multi.Count,
                            blockIndices,
                            blobOffsets);
                    }
                    break;
                }
            }
        }
    }
}

void TBlockIterator::SetMerged(
    ui64 nodeId,
    ui64 commitId,
    ui32 count,
    ui32 blockIndex,
    ui16 blobOffset)
{
    Block.NodeId = nodeId;
    Block.MinCommitId = commitId;

    Group.Index = 0;
    Group.Count = count;

    Group.Merged.BlockIndex = blockIndex;
    Group.Merged.BlobOffset = blobOffset;

    NextBlock = &TBlockIterator::NextMerged;
}

void TBlockIterator::SetMixed(
    ui64 nodeId,
    ui64 commitId,
    ui32 count,
    const ui32* blockIndices,
    const ui16* blobOffsets)
{
    Block.NodeId = nodeId;
    Block.MinCommitId = commitId;

    Group.Index = 0;
    Group.Count = count;

    Group.Mixed.BlockIndices = blockIndices;
    Group.Mixed.BlobOffsets = blobOffsets;

    NextBlock = &TBlockIterator::NextMixed;
}

bool TBlockIterator::NextMerged()
{
    while (Group.Index < Group.Count) {
        ui32 blockIndex = Group.Merged.BlockIndex + Group.Index;
        ui16 blobOffset = Group.Merged.BlobOffset + Group.Index;

        if (Filter.MinBlockIndex > blockIndex) {
            // Skip first part of merged group
            Group.Index += Filter.MinBlockIndex - blockIndex;
            continue;
        }

        ui64 maxCommitId = 0;
        const auto blocksFound = FindDeletionMarkers(
            EncodedDeletionMarkers,
            blobOffset,
            /*maxBlocksToFind=*/Group.Count - Group.Index,
            &maxCommitId);

        const auto maxBlockIndex = Min(
            blockIndex + blocksFound,
            Filter.MaxBlockIndex);
        if (maxBlockIndex <= blockIndex) {
            // All possible blocks were filtered by |Filter.MaxBlockIndex|
            break;
        }

        Group.Index += maxBlockIndex - blockIndex;

        if (Filter.CommitId < maxCommitId) {
            Block.BlockIndex = blockIndex;
            Block.MaxCommitId = maxCommitId;
            BlobOffset = blobOffset;
            BlocksInCurrentIteration = maxBlockIndex - blockIndex;
            return true;
        }
    }

    NextBlock = nullptr;
    return false;
}

bool TBlockIterator::NextMixed()
{
    while (Group.Index < Group.Count) {
        ui32 blockIndex = Group.Mixed.BlockIndices[Group.Index];
        ui16 blobOffset = Group.Mixed.BlobOffsets[Group.Index];
        ++Group.Index;

        const ui64 maxCommitId = FindDeletionMarker(
            EncodedDeletionMarkers,
            blobOffset);

        if (Filter.CheckEntry(blockIndex, maxCommitId)) {
            Block.BlockIndex = blockIndex;
            Block.MaxCommitId = maxCommitId;
            BlobOffset = blobOffset;
            BlocksInCurrentIteration = 1;
            return true;
        }
    }

    NextBlock = nullptr;
    return false;
}

////////////////////////////////////////////////////////////////////////////////

TBlockIterator TBlockList::FindBlocks(
    ui64 nodeId,
    ui64 commitId,
    ui32 blockIndex,
    ui32 blocksCount) const
{
    TBlockIterator::TBlockFilter filter{
        .NodeId = nodeId,
        .CommitId = commitId,
        .MinBlockIndex = blockIndex,
        .MaxBlockIndex = blockIndex + blocksCount
    };
    return TBlockIterator(
        EncodedBlocks,
        EncodedDeletionMarkers,
        std::move(filter));
}

TBlockList::TStats TBlockList::GetStats() const
{
    TStats stats {};

    StatBlockEntries(EncodedBlocks, stats);
    StatDeletionMarkers(EncodedDeletionMarkers, stats);

    return stats;
}

TVector<TBlock> TBlockList::DecodeBlocks() const
{
    TVector<TBlock> blocks;
    blocks.resize(MaxBlocksCount);

    blocks.resize(DecodeBlockEntries(EncodedBlocks, blocks));
    DecodeDeletionMarkers(EncodedDeletionMarkers, blocks);
    return blocks;
}

}   // namespace NCloud::NFileStore::NStorage
