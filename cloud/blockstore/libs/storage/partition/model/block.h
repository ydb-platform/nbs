#pragma once

#include "public.h"

#include "block_mask.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/partition_common/model/block.h>
#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <optional>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

struct IBlobsVisitor
{
    virtual ~IBlobsVisitor() = default;

    virtual bool NeedsCompressionMetadata() const
    {
        return true;
    }

    // compressionMetadata is empty for uncompressed blobs or when the visitor
    // opts out.
    virtual bool Visit(
        TBlockRange32 blockRange,
        const TPartialBlobId& blobId,
        const TBlockMask& skipMask,
        const std::optional<NProto::TBlobCompressionInfo>&
            compressionMetadata) = 0;
};

struct TIndexOnlyBlobsVisitor final: IBlobsVisitor
{
    bool NeedsCompressionMetadata() const override
    {
        return false;
    }

    bool Visit(
        TBlockRange32 /*blockRange*/,
        const TPartialBlobId& /*blobId*/,
        const TBlockMask& /*skipMask*/,
        const std::optional<
            NProto::TBlobCompressionInfo>& /*compressionMetadata*/) override
    {
        return true;
    }
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

struct IMixedBlocksIndexVisitor
{
    virtual ~IMixedBlocksIndexVisitor() = default;

    virtual bool VisitBlock(
        ui32 blockIndex,
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui16 blobOffset,
        ui8 compactionRangeCount) = 0;
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
