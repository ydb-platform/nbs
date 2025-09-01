#pragma once

#include "public.h"

#include <cloud/storage/core/libs/tablet/model/commit.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <util/digest/numeric.h>
#include <util/generic/strbuf.h>
#include <util/generic/variant.h>
#include <util/generic/vector.h>
#include <util/string/printf.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 MaxBlocksCount = 1024;

constexpr ui32 NodeGroupSize = 16;
constexpr ui32 BlockGroupSize = 64;

////////////////////////////////////////////////////////////////////////////////

struct TBlock
{
    ui64 NodeId = 0;
    ui32 BlockIndex = 0;
    ui64 MinCommitId = 0;
    ui64 MaxCommitId = 0;

    TBlock() = default;

    TBlock(ui64 nodeId, ui32 blockIndex, ui64 minCommitId, ui64 maxCommitId)
        : NodeId(nodeId)
        , BlockIndex(blockIndex)
        , MinCommitId(minCommitId)
        , MaxCommitId(maxCommitId)
    {}

    bool operator==(const TBlock& other) const
    {
        return NodeId == other.NodeId
            && BlockIndex == other.BlockIndex
            && MinCommitId == other.MinCommitId
            && MaxCommitId == other.MaxCommitId;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFreshBlock: TBlock
{
    TStringBuf BlockData;
};

////////////////////////////////////////////////////////////////////////////////

struct TOwningFreshBlock: TBlock
{
    TString BlockData;
};

////////////////////////////////////////////////////////////////////////////////

struct TBlockDataRef: TBlock
{
    TPartialBlobId BlobId;
    ui32 BlobOffset = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TBlockDeletion
{
    ui64 NodeId = 0;
    ui32 BlockIndex = 0;
    ui64 CommitId = 0;

    TBlockDeletion(ui64 nodeId, ui32 blockIndex, ui64 commitId)
        : NodeId(nodeId)
        , BlockIndex(blockIndex)
        , CommitId(commitId)
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TBytes
{
    ui64 NodeId = 0;
    ui64 Offset = 0;
    ui64 Length = 0;
    ui64 MinCommitId = 0;
    ui64 MaxCommitId = 0;

    TBytes() = default;

    TBytes(
            ui64 nodeId,
            ui64 offset,
            ui64 length,
            ui64 minCommitId,
            ui64 maxCommitId)
        : NodeId(nodeId)
        , Offset(offset)
        , Length(length)
        , MinCommitId(minCommitId)
        , MaxCommitId(maxCommitId)
    {}

    TString Describe() const
    {
        return Sprintf(
            "{%lu, %lu, %lu, %lu, %lu}",
            NodeId,
            Offset,
            Length,
            MinCommitId,
            MaxCommitId);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TBlockBytes
{
    struct TInterval
    {
        ui32 OffsetInBlock = 0;
        TString Data;
    };

    TVector<TInterval> Intervals;
};

////////////////////////////////////////////////////////////////////////////////

struct TBlockWithBytes
{
    struct TNoBlock {};

    ui64 NodeId = 0;
    ui32 BlockIndex = 0;
    ui64 BytesMinCommitId = 0;
    TBlockBytes BlockBytes;
    std::variant<TNoBlock, TBlockDataRef, TOwningFreshBlock> Block;
};

////////////////////////////////////////////////////////////////////////////////

struct TBlockBytesMeta
{
    ui64 NodeId = 0;
    ui32 BlockIndex = 0;
    ui32 OffsetInBlock = 0;
    TString Data;
};

////////////////////////////////////////////////////////////////////////////////

// XXX not the best place for this struct
struct TFlushBytesCleanupInfo
{
    ui64 ChunkId = 0;
    ui64 ClosingCommitId = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TBlockCompare
{
    bool operator ()(const TBlock& l, const TBlock& r) const
    {
        return (l.NodeId < r.NodeId || (l.NodeId == r.NodeId
            && (l.BlockIndex < r.BlockIndex || (l.BlockIndex == r.BlockIndex
            && (l.MinCommitId > r.MinCommitId)))));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TBlockWithBytesCompare
{
    bool operator ()(const TBlockWithBytes& l, const TBlockWithBytes& r) const
    {
        return TBlockCompare()(
            {l.NodeId, l.BlockIndex, l.BytesMinCommitId, InvalidCommitId},
            {r.NodeId, r.BlockIndex, r.BytesMinCommitId, InvalidCommitId}
        );
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TBlockLocation
{
    ui64 NodeId;
    ui32 BlockIndex;

    bool operator==(const TBlockLocation& other) const
    {
        return NodeId == other.NodeId && BlockIndex == other.BlockIndex;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TBlockLocationHash
{
    size_t operator ()(const TBlockLocation& bl) const
    {
        return CombineHashes(
            IntHash<ui64>(bl.NodeId),
            IntHash<ui64>(bl.BlockIndex));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TBlockLocationInBlob
{
    TPartialBlobId BlobId;
    ui32 BlobOffset = 0;

    bool operator==(const TBlockLocationInBlob& other) const
    {
        return BlobId == other.BlobId && BlobOffset == other.BlobOffset;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TBlockLocationInBlobHash
{
    size_t operator ()(const TBlockLocationInBlob& bl) const
    {
        return CombineHashes(
            TPartialBlobIdHash()(bl.BlobId),
            IntHash<ui64>(bl.BlobOffset));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct IFreshBlockVisitor
{
    virtual ~IFreshBlockVisitor() = default;

    virtual void Accept(const TBlock& block, TStringBuf blockData) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IMixedBlockVisitor
{
    virtual ~IMixedBlockVisitor() = default;

    virtual void Accept(
        const TBlock& block,
        const TPartialBlobId& blobId,
        ui32 blobOffset,
        ui32 blocksCount) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ILargeBlockVisitor
{
    virtual ~ILargeBlockVisitor() = default;

    virtual void Accept(
        const TBlockDeletion& marker) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IFreshBytesVisitor
{
    virtual ~IFreshBytesVisitor() = default;

    virtual void Accept(const TBytes& bytes, TStringBuf data) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IBlockLocation2RangeIndex
{
    virtual ~IBlockLocation2RangeIndex() = default;

    virtual ui32 Calc(ui64 nodeId, ui32 blockIndex) const = 0;
};

IBlockLocation2RangeIndexPtr CreateRangeIdHasher(ui32 type);

////////////////////////////////////////////////////////////////////////////////

inline ui32 GetMixedRangeIndex(
    const IBlockLocation2RangeIndex& hasher,
    ui64 nodeId,
    ui32 blockIndex)
{
    return hasher.Calc(nodeId, blockIndex);
}

inline ui32 GetMixedRangeIndex(
    const IBlockLocation2RangeIndex& hasher,
    ui64 nodeId,
    ui32 blockIndex,
    ui32 blocksCount)
{
    Y_ABORT_UNLESS(blocksCount && blocksCount <= MaxBlocksCount);

    ui32 rangeId = GetMixedRangeIndex(hasher, nodeId, blockIndex);

    Y_IF_DEBUG({
        // sanity check
        ui32 nextRangeId = GetMixedRangeIndex(
            hasher,
            nodeId,
            blockIndex + blocksCount - 1);

        Y_ABORT_UNLESS(nextRangeId == rangeId, "node %lu: rangeId %u vs nextRangeId %u at (index %u, count %u)",
            nodeId, rangeId, nextRangeId, blockIndex, blocksCount);
    });

    return rangeId;
}

inline ui32 GetMixedRangeIndex(
    const IBlockLocation2RangeIndex& hasher,
    const TVector<TBlock>& blocks)
{
    Y_ABORT_UNLESS(blocks && blocks.size() <= MaxBlocksCount);

    ui32 rangeId = GetMixedRangeIndex(
        hasher,
        blocks[0].NodeId,
        blocks[0].BlockIndex);

    Y_IF_DEBUG({
        // sanity check
        for (size_t i = 1; i < blocks.size(); ++i) {
            ui32 nextRangeId = GetMixedRangeIndex(
                hasher,
                blocks[i].NodeId,
                blocks[i].BlockIndex);
            Y_ABORT_UNLESS(nextRangeId == rangeId);
        }
    });

    return rangeId;
}

}   // namespace NCloud::NFileStore::NStorage
