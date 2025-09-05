#pragma once

#include "public.h"

#include "alloc.h"
#include "binary_reader.h"
#include "block.h"

#include <cloud/storage/core/libs/common/byte_vector.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TBlockIterator
{
    using PNextBlockFunc = bool (TBlockIterator::*)(void);

public:
    struct TBlockFilter
    {
        ui64 NodeId = 0;
        ui64 CommitId = 0;
        ui32 MinBlockIndex = 0;
        ui32 MaxBlockIndex = 0;

        bool CheckGroup(ui64 nodeId, ui64 minCommitId);
        bool CheckEntry(ui32 blockIndex, ui64 maxCommitId);
    };

    TBlock Block;
    ui32 BlobOffset = 0;

private:
    TBinaryReader Reader;
    const TByteVector& EncodedDeletionMarkers;
    TBlockFilter Filter;

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
        const TBlockFilter& filter);

    bool Next();

private:
    void SetMerged(
        ui64 nodeId,
        ui64 commitId,
        ui32 count,
        ui32 blockIndex,
        ui16 blobOffset);

    void SetMixed(
        ui64 nodeId,
        ui64 commitId,
        ui32 count,
        const ui32* blockIndices,
        const ui16* blobOffsets);

    bool NextMerged();

    bool NextMixed();
};

////////////////////////////////////////////////////////////////////////////////

class TBlockList
{
private:
    TByteVector EncodedBlocks;
    TByteVector EncodedDeletionMarkers;

public:
    TBlockList(TByteVector encodedBlocks, TByteVector encodedDeletionMarkers)
        : EncodedBlocks(std::move(encodedBlocks))
        , EncodedDeletionMarkers(std::move(encodedDeletionMarkers))
    {}

    const TByteVector& GetEncodedBlocks() const
    {
        return EncodedBlocks;
    }

    const TByteVector& GetEncodedDeletionMarkers() const
    {
        return EncodedDeletionMarkers;
    }

    // Performance of FindBlocks is slightly better if TBlockIterator is allocated
    // on stack (not on heap). See https://github.com/ydb-platform/nbs/pull/4244
    TBlockIterator FindBlocks(
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex,
        ui32 blocksCount) const;

    struct TStats
    {
        size_t BlockEntries;
        size_t BlockGroups;

        size_t DeletionMarkers;
        size_t DeletionGroups;
    };

    TStats GetStats() const;

    TVector<TBlock> DecodeBlocks() const;

    static TBlockList EncodeBlocks(
        const TBlock& block,
        ui32 blocksCount,
        IAllocator* alloc);

    static TBlockList EncodeBlocks(
        const TVector<TBlock>& blocks,
        IAllocator* alloc);
};

}   // namespace NCloud::NFileStore::NStorage
