#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/block_range.h>

#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

struct TBlock
{
    ui32 BlockIndex;
    ui64 CommitId;

    // fresh blocks only
    bool IsStoredInDb;

    TBlock(ui32 blockIndex, ui64 commitId, bool isStoredInDb)
        : BlockIndex(blockIndex)
        , CommitId(commitId)
        , IsStoredInDb(isStoredInDb)
    {}

    bool operator==(const TBlock& other) const
    {
        return BlockIndex == other.BlockIndex && CommitId == other.CommitId;
    }

    bool operator<(const TBlock& other) const
    {
        // order by BlockIndex ASC
        return BlockIndex < other.BlockIndex;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFreshBlock
{
    TBlock Meta;
    TStringBuf Content;

    TFreshBlock(TBlock meta, TStringBuf content)
        : Meta(meta)
        , Content(content)
    {}
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

////////////////////////////////////////////////////////////////////////////////

struct IBlocksIndexVisitor
{
    virtual ~IBlocksIndexVisitor() = default;

    virtual bool Visit(
        ui32 blockIndex,
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui16 blobOffset) = 0;
};

struct IExtendedBlocksIndexVisitor
{
    virtual ~IExtendedBlocksIndexVisitor() = default;

    virtual bool Visit(
        ui32 blockIndex,
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui16 blobOffset,
        ui32 checksum) = 0;
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
