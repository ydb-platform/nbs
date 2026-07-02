#pragma once

#include "block.h"

namespace NCloud::NFileStore::NStorage {

/**
 * Block & byte index consists of 4 layers:
 * 1. fresh blocks - buffer for small block-aligned writes
 * 2. mixed blocks - stores most of the data, block-aligned
 * 3. large blocks - wide deletion markers, block-aligned
 * 4. fresh bytes - buffer for unaligned writes, each item lies entirely within
 *  one block
 *
 * Deletions (both overwrites and explicit deletions) are applied to:
 * 1. fresh blocks - always
 * 2. mixed blocks - if the deleted range length is below some threshold
 * 3. large blocks - if the deleted range length is above some threshold
 * 4. fresh bytes - always
 *
 * Each block has:
 * * MinCommitId - when it became visible
 * * MaxCommitId - when it got overwritten/deleted (if it's still visible, the
 *  value here is InvalidCommitId)
 *
 * Since writes to fresh bytes by definition do not cover whole blocks there's
 * no way to generate block-level deletion markers upon such writes so fresh
 * bytes layer should be visited last upon read and any visited bytes should be
 * assumed to overwrite anything visited before that. This is made possible
 * because fresh bytes layer is assumed to be organized in such a way that newer
 * byte ranges are visited after older byte ranges.
 *
 * Large blocks layer can hide some of the blocks in the mixed blocks layer so
 * it should be visited after mixed blocks layer.
 *
 * In general, the following lookup order is recommended:
 * 1. fresh blocks
 * 2. mixed blocks
 * 3. large blocks
 * 4. fresh bytes
 */

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

    // Accepts |blocksCount| consecutive |block|'s with offset |blobOffset|
    // from the beginning of the blob with |blobId|
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

    virtual void Accept(const TBlockDeletion& marker) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IFreshBytesVisitor
{
    virtual ~IFreshBytesVisitor() = default;

    virtual void Accept(const TBytes& bytes, TStringBuf data) = 0;
};

}   // namespace NCloud::NFileStore::NStorage
