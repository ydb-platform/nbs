#pragma once

#include "public.h"

#include "block.h"

#include <cloud/blockstore/libs/common/block_range.h>

#include <cloud/storage/core/libs/common/block_buffer.h>
#include <cloud/storage/core/libs/common/guarded_sglist.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <ydb/library/actors/core/actorid.h>

#include <util/digest/multi.h>
#include <util/generic/hash.h>
#include <util/generic/list.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

using TBlockRanges = TStackVec<TBlockRange32, 1>;

struct TBlob
{
    TPartialBlobId BlobId;
    TBlockRanges BlockRanges;
    ui16 BlockCount = 0;
    ui16 CheckpointBlockCount = 0;

    TBlob() = default;

    TBlob(const TPartialBlobId& blobId,
          TBlockRanges blockRanges,
          ui16 blockCount,
          ui16 checkpointBlockCount)
        : BlobId(blobId)
        , BlockRanges(std::move(blockRanges))
        , BlockCount(blockCount)
        , CheckpointBlockCount(checkpointBlockCount)
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteBlob
{
    TPartialBlobId BlobId;
    TVector<TBlock> Blocks;
    TGuardedBuffer<TBlockBuffer> BlobContent;

    TWriteBlob() = default;

    TWriteBlob(const TPartialBlobId& blobId)
        : BlobId(blobId)
    {}

    TWriteBlob(
            const TPartialBlobId& blobId,
            TVector<TBlock> blocks,
            TBlockBuffer blobContent)
        : BlobId(blobId)
        , Blocks(std::move(blocks))
        , BlobContent(std::move(blobContent))
    {}
};

////////////////////////////////////////////////////////////////////////////////

enum EAddBlobMode
{
    ADD_WRITE_RESULT,
    ADD_ZERO_RESULT,
    ADD_FLUSH_RESULT,
    ADD_COMPACTION_RESULT,
};

////////////////////////////////////////////////////////////////////////////////

struct TAddBlob
{
    TPartialBlobId BlobId;
    TVector<TBlock> Blocks;

    TAddBlob() = default;

    TAddBlob(const TPartialBlobId& blobId)
        : BlobId(blobId)
    {}

    TAddBlob(const TPartialBlobId& blobId, TVector<TBlock> blocks)
        : BlobId(blobId)
        , Blocks(std::move(blocks))
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TBlockRef
{
    TBlock Block;
    TPartialBlobId BlobId;
    ui16 BlobOffset = 0;

    TBlockRef() = default;

    TBlockRef(
            const TBlock& block,
            const TPartialBlobId& blobId,
            ui16 blobOffset)
        : Block(block)
        , BlobId(blobId)
        , BlobOffset(blobOffset)
    {}

    bool operator <(const TBlockRef& other) const
    {
        return Block.BlockIndex < other.Block.BlockIndex;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TBlobRefs
{
    TPartialBlobId BlobId;
    NActors::TActorId Proxy;
    TVector<TBlock> Blocks;
    TVector<ui16> DataBlobOffsets;
    ui32 GroupId = 0;

    TBlobRefs() = default;

    TBlobRefs(const TPartialBlobId& blobId)
        : BlobId(blobId)
    {}

    TBlobRefs(
            const TPartialBlobId& blobId,
            const NActors::TActorId& proxy,
            TVector<TBlock> blocks,
            TVector<ui16> dataBlobOffsets,
            ui32 groupId)
        : BlobId(blobId)
        , Proxy(proxy)
        , Blocks(std::move(blocks))
        , DataBlobOffsets(std::move(dataBlobOffsets))
        , GroupId(groupId)
    {}

    bool operator <(const TBlobRefs& other) const
    {
        return BlobId < other.BlobId;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TBlobRefsList : TVector<TBlobRefs>
{
    void AddBlock(
        const TBlock& block,
        const TPartialBlobId& blobId,
        ui16 blobOffset)
    {
        Y_ABORT_UNLESS(blobOffset != InvalidBlobOffset);

        if (empty() || back().BlobId != blobId) {
            emplace_back(blobId);
        }

        auto& blob = back();
        blob.Blocks.push_back(block);
        if (blobOffset != ZeroBlobOffset) {
            blob.DataBlobOffsets.push_back(blobOffset);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TAffectedBlobInfo
{
    TPartialBlobId BlobId;
    TVector<TBlock> Blocks;

    TAffectedBlobInfo(TPartialBlobId blobId, TVector<TBlock> blocks)
        : BlobId(blobId)
        , Blocks(std::move(blocks))
    {
    }
};

using TAffectedBlobInfos = TVector<TAffectedBlobInfo>;

////////////////////////////////////////////////////////////////////////////////

struct TBlobUpdate
{
    TBlockRange32 BlockRange;
    ui64 CommitId;
    ui64 DeletionId;

    TBlobUpdate(TBlockRange32 blockRange, ui64 commitId, ui64 deletionId)
        : BlockRange(blockRange)
        , CommitId(commitId)
        , DeletionId(deletionId)
    {}

    bool operator==(const TBlobUpdate& other) const
    {
        return BlockRange == other.BlockRange
            && CommitId == other.CommitId
            && DeletionId == other.DeletionId;
    }
};

struct TBlobUpdateHash
{
    size_t operator()(const TBlobUpdate& update) const
    {
        return MultiHash(update.CommitId, update.BlockRange.Start);
    }
};

using TBlobUpdatesByFresh = THashSet<
    TBlobUpdate,
    TBlobUpdateHash
>;

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
