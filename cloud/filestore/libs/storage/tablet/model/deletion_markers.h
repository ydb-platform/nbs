#pragma once

#include "public.h"

#include "block.h"

#include <util/generic/array_ref.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TDeletionMarker
{
    ui64 NodeId;
    ui64 CommitId;
    ui32 BlockIndex;
    ui32 BlockCount;

    TDeletionMarker(
            ui64 nodeId,
            ui64 commitId,
            ui32 blockIndex,
            ui32 blockCount)
        : NodeId(nodeId)
        , CommitId(commitId)
        , BlockIndex(blockIndex)
        , BlockCount(blockCount)
    {}

    bool operator==(const TDeletionMarker& other) const
    {
        return NodeId == other.NodeId
            && CommitId == other.CommitId
            && BlockIndex == other.BlockIndex
            && BlockCount == other.BlockCount;
    }

    bool IsValid() const
    {
        return CommitId != InvalidCommitId && BlockCount > 0;
    }
};

////////////////////////////////////////////////////////////////////////////////
// TODO(#1923): support checkpoints in TDeletionMarkers. Right now the
// implementation simply overwrites older commitIds with newer ones.

class TDeletionMarkers
{
private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    class TIterator
    {
    private:
        friend struct TImpl;
        const TImpl& Impl;

    public:
        ui64 MaxCommitId;
        ui32 BlocksCount = 0;

    private:
        TBlock Block;
        ui64 MaxBlockIndex;

        TIterator(
            const TImpl& impl,
            const TBlock& block,
            ui64 maxBlockIndex);

    public:
        bool Next();
    };

public:
    TDeletionMarkers(IAllocator* alloc);
    ~TDeletionMarkers();

    bool Empty() const;

    TIterator FindBlocks(const TBlock& block, ui32 maxBlocksToIterate);

    void Add(TDeletionMarker deletionMarker);
    ui32 Apply(TBlock& block) const;
    ui32 Apply(TArrayRef<TBlock> blocks) const;
    TVector<TDeletionMarker> Extract();
    void Swap(TDeletionMarkers& other);
};

}   // namespace NCloud::NFileStore::NStorage
