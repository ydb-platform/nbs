#pragma once

#include "public.h"

#include "block.h"

#include <cloud/blockstore/libs/common/block_range.h>

#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/generic/vector.h>
#include <util/memory/alloc.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

class TMixedIndex
{
private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    TMixedIndex();
    ~TMixedIndex();

    void SetOrUpdateBlock(const TBlockAndLocation& b);
    void SetOrUpdateBlock(ui32 blockIndex, const TBlockLocation& location);
    void ClearBlock(ui32 blockIndex);

    void OnCheckpoint(ui64 checkpointId);
    void OnCheckpointDeletion(ui64 checkpointId);

    bool FindBlock(ui32 blockIndex, TBlockAndLocation* result) const;
    bool
    FindBlock(ui64 commitId, ui32 blockIndex, TBlockAndLocation* result) const;

    TVector<TBlockAndLocation> FindAllBlocks(const TBlockRange32& range) const;

    TPartialBlobIdHashSet ExtractOverwrittenBlobIds();

    bool IsEmpty() const;

    friend class TMixedIndexBuilder;
};

////////////////////////////////////////////////////////////////////////////////

class TMixedIndexBuilder
{
private:
    const TBlockRange32 Range;
    TVector<TStackVec<TBlockAndLocation, 4>> Blocks;
    TPartialBlobIdHashSet OverwrittenBlobIds;

public:
    TMixedIndexBuilder(const TBlockRange32& range);

public:
    void AddBlock(const TBlockAndLocation& b);
    void AddOverwrittenBlob(const TPartialBlobId& blobId);

    std::unique_ptr<TMixedIndex> Build(const TVector<ui64>& checkpointIds);
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
