#pragma once

#include "block.h"

namespace NCloud::NFileStore::NStorage {

/**
 * Block & byte index consists of 4 layers:
 * 1. fresh blocks (buffer for small aligned writes)
 * 2. mixed blocks (stores most of the data)
 * 3. large blocks (wide deletion markers)
 * 4. fresh bytes (buffer for unaligned writes)
 *
 * Deletions (both overwrites and explicit deletions) are applied to:
 * 1. fresh blocks - always
 * 2. mixed blocks - if the deleted range is below some threshold
 * 3. large blocks - if the deleted range is above some threshold
 * 4. fresh bytes - always
 *
 * Each block has:
 * * MinCommitId - when it became visible
 * * MaxCommitId - when it got overwritten/deleted (if it's still visible, the
 *  value here is InvalidCommitId)
 *
 * Fresh bytes layer doesn't store MaxCommitIds so no deletion markers can be
 * applied to it post factum. That's why fresh bytes layer must be visited last
 * upon read.
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
