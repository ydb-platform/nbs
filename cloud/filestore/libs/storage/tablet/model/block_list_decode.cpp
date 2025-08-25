#include "block_list.h"

#include "binary_reader.h"
#include "block_list_spec.h"

#include <library/cpp/containers/stack_vector/stack_vec.h>

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

////////////////////////////////////////////////////////////////////////////////

class TLazyDeletionMarkersDecoder
{
private:
    TBinaryReader Reader;
    TStackVec<ui64, MaxBlocksCount> MaxCommitIds;

public:
    explicit TLazyDeletionMarkersDecoder(
        const TByteVector& encodedDeletionMarkers);

    ui64 GetMaxCommitId(ui32 blobOffset);

private:
    bool IsMaxCommitIdSet(ui32 blobOffset) const;
    void SetMaxCommitId(ui32 blobOffset, ui32 blocksCount, ui64 maxCommitId);
};

TLazyDeletionMarkersDecoder::TLazyDeletionMarkersDecoder(
        const TByteVector& encodedDeletionMarkers)
    : Reader(encodedDeletionMarkers)
{
    const auto& header = Reader.Read<NBlockListSpec::TListHeader>();
    Y_ABORT_UNLESS(header.ListType == NBlockListSpec::TListHeader::DeletionMarkers);
}

ui64 TLazyDeletionMarkersDecoder::GetMaxCommitId(ui32 blobOffset)
{
    if (IsMaxCommitIdSet(blobOffset)) {
        return MaxCommitIds[blobOffset];
    }

    while (Reader.Avail()) {
        const auto& group = Reader.Read<NBlockListSpec::TGroupHeader>();
        if (!group.IsMulti) {
            const auto& entry = Reader.Read<NBlockListSpec::TDeletionMarker>();
            SetMaxCommitId(entry.BlobOffset, 1, group.CommitId);
        } else {
            const auto& multi = Reader.Read<NBlockListSpec::TMultiGroupHeader>();
            switch (multi.GroupType) {
                case NBlockListSpec::TMultiGroupHeader::MergedGroup: {
                    const auto& entry = Reader.Read<NBlockListSpec::TDeletionMarker>();
                    SetMaxCommitId(entry.BlobOffset, multi.Count, group.CommitId);
                    break;
                }

                case NBlockListSpec::TMultiGroupHeader::MixedGroup: {
                    const auto* blobOffsets = Reader.Read<ui16>(Align2(multi.Count));
                    for (size_t i = 0; i < multi.Count; ++i) {
                        SetMaxCommitId(blobOffsets[i], 1, group.CommitId);
                    }
                    break;
                }
            }
        }

        if (IsMaxCommitIdSet(blobOffset)) {
            return MaxCommitIds[blobOffset];
        }
    }

    return InvalidCommitId;
}

bool TLazyDeletionMarkersDecoder::IsMaxCommitIdSet(ui32 blobOffset) const
{
    return blobOffset < MaxCommitIds.size() &&
        MaxCommitIds[blobOffset] != InvalidCommitId;
}

void TLazyDeletionMarkersDecoder::SetMaxCommitId(
    ui32 blobOffset,
    ui32 blocksCount,
    ui64 maxCommitId)
{
    Y_ABORT_UNLESS(blobOffset + blocksCount <= MaxBlocksCount);
    Y_ABORT_UNLESS(maxCommitId != InvalidCommitId);

    // only MaxCommitIds growth is allowed
    if (blobOffset + blocksCount > MaxCommitIds.size()) {
        MaxCommitIds.resize(blobOffset + blocksCount, InvalidCommitId);
    }

    std::fill(
        MaxCommitIds.begin() + blobOffset,
        MaxCommitIds.begin() + blobOffset + blocksCount,
        maxCommitId);
}

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

struct TEmptyFilter
{
};

bool CheckGroup(const TEmptyFilter&, ui64, ui64)
{
    return true;
}

bool CheckEntry(const TEmptyFilter&, ui32, ui64)
{
    return true;
}

////////////////////////////////////////////////////////////////////////////////

struct TBlockFilter
{
    ui64 NodeId = 0;
    ui64 CommitId = 0;
    ui32 MinBlockIndex = 0;
    ui32 MaxBlockIndex = 0;
};

bool CheckGroup(const TBlockFilter& filter, ui64 nodeId, ui64 minCommitId)
{
    return filter.NodeId == nodeId
        && filter.CommitId >= minCommitId;
}

bool CheckEntry(const TBlockFilter& filter, ui32 blockIndex, ui64 maxCommitId)
{
    return filter.CommitId < maxCommitId
        && filter.MinBlockIndex <= blockIndex
        && filter.MaxBlockIndex > blockIndex;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TFilter>
class TBlockIterator final
    : public IBlockIterator
{
    using PNextBlockFunc = bool (TBlockIterator::*)(void);

private:
    TBinaryReader Reader;
    TLazyDeletionMarkersDecoder DeletionMarkers;
    TFilter Filter;

    PNextBlockFunc NextBlock = nullptr;

    struct {
        ui32 Index;
        ui32 Count;

        union {
            struct {
                ui32 BlockIndex;
                ui32 BlobOffset;
            } Merged;

            struct {
                const ui32* BlockIndices;
                const ui16* BlobOffsets;
            } Mixed;
        };
    } Group;

public:
    TBlockIterator(
            const TByteVector& encodedBlocks,
            const TByteVector& encodedDeletionMarkers,
            const TFilter& filter)
        : Reader(encodedBlocks)
        , DeletionMarkers(encodedDeletionMarkers)
        , Filter(filter)
    {
        const auto& header = Reader.Read<NBlockListSpec::TListHeader>();
        Y_ABORT_UNLESS(header.ListType == NBlockListSpec::TListHeader::Blocks);

        Zero(Group);
    }

    bool Next() override
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
                if (CheckGroup(Filter, group.NodeId, group.CommitId)) {
                    ui64 maxCommitId = DeletionMarkers.GetMaxCommitId(
                        entry.BlobOffset);

                    if (CheckEntry(Filter, entry.BlockIndex, maxCommitId)) {
                        Block.NodeId = group.NodeId;
                        Block.BlockIndex = entry.BlockIndex;
                        Block.MinCommitId = group.CommitId;
                        Block.MaxCommitId = maxCommitId;
                        BlobOffset = entry.BlobOffset;
                        return true;
                    }
                }
            } else {
                const auto& multi = Reader.Read<NBlockListSpec::TMultiGroupHeader>();
                switch (multi.GroupType) {
                    case NBlockListSpec::TMultiGroupHeader::MergedGroup: {
                        const auto& entry = Reader.Read<NBlockListSpec::TBlockEntry>();
                        if (CheckGroup(Filter, group.NodeId, group.CommitId)) {
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
                        if (CheckGroup(Filter, group.NodeId, group.CommitId)) {
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

private:
    void SetMerged(
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

    void SetMixed(
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

    bool NextMerged()
    {
        while (Group.Index < Group.Count) {
            ui32 blockIndex = Group.Merged.BlockIndex + Group.Index;
            ui16 blobOffset = Group.Merged.BlobOffset + Group.Index;
            ++Group.Index;

            ui64 maxCommitId = DeletionMarkers.GetMaxCommitId(blobOffset);

            if (CheckEntry(Filter, blockIndex, maxCommitId)) {
                Block.BlockIndex = blockIndex;
                Block.MaxCommitId = maxCommitId;
                BlobOffset = blobOffset;
                return true;
            }
        }

        NextBlock = nullptr;
        return false;
    }

    bool NextMixed()
    {
        while (Group.Index < Group.Count) {
            ui32 blockIndex = Group.Mixed.BlockIndices[Group.Index];
            ui16 blobOffset = Group.Mixed.BlobOffsets[Group.Index];
            ++Group.Index;

            ui64 maxCommitId = DeletionMarkers.GetMaxCommitId(blobOffset);

            if (CheckEntry(Filter, blockIndex, maxCommitId)) {
                Block.BlockIndex = blockIndex;
                Block.MaxCommitId = maxCommitId;
                BlobOffset = blobOffset;
                return true;
            }
        }

        NextBlock = nullptr;
        return false;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockIteratorPtr TBlockList::FindBlocks() const
{
    return std::make_shared<TBlockIterator<TEmptyFilter>>(
        EncodedBlocks,
        EncodedDeletionMarkers,
        TEmptyFilter {});
}

IBlockIteratorPtr TBlockList::FindBlocks(
    ui64 nodeId,
    ui64 commitId,
    ui32 blockIndex,
    ui32 blocksCount) const
{
    return std::make_shared<TBlockIterator<TBlockFilter>>(
        EncodedBlocks,
        EncodedDeletionMarkers,
        TBlockFilter {nodeId, commitId, blockIndex, blockIndex + blocksCount});
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
