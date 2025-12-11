#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/block_range.h>

#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/tablet/model/commit.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <compare>

class IOutputStream;

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

struct TBlock
{
    ui32 BlockIndex = 0;
    // this goes right after ui32 because we don't want TBlock size to be
    // increased
    bool Zeroed = false;
    ui64 MinCommitId = 0;
    ui64 MaxCommitId = 0;

    TBlock() = default;

    TBlock(ui32 blockIndex, ui64 minCommitId, ui64 maxCommitId, bool zeroed)
        : BlockIndex(blockIndex)
        , Zeroed(zeroed)
        , MinCommitId(minCommitId)
        , MaxCommitId(maxCommitId)
    {}

    bool operator==(const TBlock& other) const
    {
        return BlockIndex == other.BlockIndex &&
               MinCommitId == other.MinCommitId &&
               MaxCommitId == other.MaxCommitId && Zeroed == other.Zeroed;
    }

    bool operator<(const TBlock& other) const
    {
        // order by (BlockIndex ASC, MinCommitId DESC)
        return BlockIndex < other.BlockIndex ||
               (BlockIndex == other.BlockIndex &&
                MinCommitId > other.MinCommitId);
    }
};

static_assert(sizeof(TBlock) == 24);

IOutputStream& operator<<(IOutputStream& out, const TBlock& block);

////////////////////////////////////////////////////////////////////////////////

struct TBlockLocation
{
    TPartialBlobId BlobId;
    ui16 BlobOffset = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TBlockAndLocation
{
    TBlock Block;
    TBlockLocation Location;
};

////////////////////////////////////////////////////////////////////////////////

bool IsContinuousBlockRange(const TVector<TBlock>& blocks);
TVector<ui32> BuildBlocks(const TVector<TBlock>& blocks);

////////////////////////////////////////////////////////////////////////////////

struct TBlockKey
{
    ui32 BlockIndex = 0;
    ui64 MinCommitId = 0;

    TBlockKey() = default;

    TBlockKey(ui32 blockIndex, ui64 minCommitId)
        : BlockIndex(blockIndex)
        , MinCommitId(minCommitId)
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TBlockCompare
{
    using is_transparent = void;

    template <typename T1, typename T2>
    bool operator()(const T1& l, const T2& r) const
    {
        return Compare(l, r);
    }

    template <typename T1, typename T2>
    static bool Compare(const T1& l, const T2& r)
    {
        // order by (BlockIndex ASC, MinCommitId DESC)
        return l.BlockIndex < r.BlockIndex ||
               (l.BlockIndex == r.BlockIndex && l.MinCommitId > r.MinCommitId);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TDeletedBlock
{
    ui32 BlockIndex;
    ui64 CommitId;

    TDeletedBlock(ui32 blockIndex, ui64 commitId)
        : BlockIndex(blockIndex)
        , CommitId(commitId)
    {}

    auto operator<=>(const TDeletedBlock&) const = default;
};

////////////////////////////////////////////////////////////////////////////////

struct TOwningFreshBlock
{
    TBlock Meta;
    TString Content;

    TOwningFreshBlock(TBlock meta, TString content)
        : Meta(meta)
        , Content(std::move(content))
    {}
};

struct TFreshBlockRangeWithContent
{
    TBlockRange32 BlockRange;
    ui64 DeletionId;
    TSgList SgList;
    TString Data;

    TFreshBlockRangeWithContent(
        TBlockRange32 blockRange,
        ui64 deletionId,
        TSgList sgList,
        TString data)
        : BlockRange(blockRange)
        , DeletionId(deletionId)
        , SgList(std::move(sgList))
        , Data(std::move(data))
    {}
};

struct TFreshBlockUpdate
{
    ui64 CommitId;
    TBlockRange32 BlockRange;

    TFreshBlockUpdate(ui64 commitId, TBlockRange32 blockRange)
        : CommitId(commitId)
        , BlockRange(blockRange)
    {}

    auto operator<=>(const TFreshBlockUpdate&) const = default;
};

using TFreshBlockUpdates = TDeque<TFreshBlockUpdate>;

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
