#pragma once

#include "block.h"

#include <util/generic/vector.h>
#include <util/system/yassert.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Resolves the visible source for every block in a [begin, begin + blocksCount)
// range. For each block the source with the greatest CommitId wins.
//
// Stored as a flat per-block array, so updates are O(blocks touched) with no
// allocations. All block indices passed by the caller must lie within the
// range.
class TBlockRanges
{
public:
    enum class ESourceKind : ui8
    {
        Empty,
        Blob,
        Fresh,
        Deleted,
    };

    struct TSource
    {
        ESourceKind Kind = ESourceKind::Empty;

        // Commit which produced this visible source.
        // The source with the greatest CommitId wins.
        ui64 CommitId = 0;

        // Valid only for Kind == Blob.
        TPartialBlobId BlobId;

        // Valid only for Kind == Blob.
        // Offset in blob corresponding to this block.
        ui32 BlobOffset = 0;
    };

    struct TBlobRange
    {
        ui32 BlockIndex = 0;
        ui32 BlocksCount = 0;

        ui64 CommitId = 0;
        TPartialBlobId BlobId;
        ui32 BlobOffset = 0;
    };

private:
    // Not const: TReadData reassigns its TBlockRanges member in place.
    ui32 BeginBlockIndex = 0;

    // One entry per block, Blocks[i] describes block BeginBlockIndex + i.
    TVector<TSource> Blocks;

public:
    TBlockRanges(ui32 beginBlockIndex, ui32 blocksCount)
        : BeginBlockIndex(beginBlockIndex)
        , Blocks(blocksCount)
    {
        Y_ABORT_UNLESS(blocksCount);
    }

    bool HasBlobs() const
    {
        for (const auto& source: Blocks) {
            if (source.Kind == ESourceKind::Blob) {
                return true;
            }
        }

        return false;
    }

    // Commit id of the source which currently wins the block: visible data
    // (Blob/Fresh) or a deletion. Zero for a block nothing was recorded for.
    ui64 GetCommitIdByBlockOffset(ui32 blockOffset) const
    {
        Y_DEBUG_ABORT_UNLESS(blockOffset < Blocks.size());
        return Blocks[blockOffset].CommitId;
    }

    bool AddBlobRange(
        ui32 blockIndex,
        ui32 blocksCount,
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui32 blobOffset)
    {
        Y_ABORT_UNLESS(blobId);

        TSource source;
        source.Kind = ESourceKind::Blob;
        source.CommitId = commitId;
        source.BlobId = blobId;
        source.BlobOffset = blobOffset;

        return UpdateRange(blockIndex, blocksCount, source);
    }

    bool AddFreshRange(ui32 blockIndex, ui32 blocksCount, ui64 commitId)
    {
        TSource source;
        source.Kind = ESourceKind::Fresh;
        source.CommitId = commitId;

        return UpdateRange(blockIndex, blocksCount, source);
    }

    bool AddDeletionRange(ui32 blockIndex, ui32 blocksCount, ui64 commitId)
    {
        TSource source;
        source.Kind = ESourceKind::Deleted;
        source.CommitId = commitId;

        return UpdateRange(blockIndex, blocksCount, source);
    }

    // Visits every block in order, passing the block's offset within the
    // range and its source (which may be Empty).
    template <typename TVisitor>
    void VisitBlocks(TVisitor&& visitor) const
    {
        for (ui32 i = 0; i < Blocks.size(); ++i) {
            visitor(i, Blocks[i]);
        }
    }

    // Visits maximal blob runs (same blob, same commit, contiguous offset).
    template <typename TVisitor>
    void VisitBlobRanges(TVisitor&& visitor) const
    {
        ui32 i = 0;
        while (i < Blocks.size()) {
            if (Blocks[i].Kind != ESourceKind::Blob) {
                ++i;
                continue;
            }

            const ui32 runStart = i;
            while (++i < Blocks.size()
                    && SameBlobConsecutiveBlocks(Blocks[i - 1], Blocks[i]))
            {
            }

            const auto& source = Blocks[runStart];

            TBlobRange range;
            range.BlockIndex = BeginBlockIndex + runStart;
            range.BlocksCount = i - runStart;
            range.CommitId = source.CommitId;
            range.BlobId = source.BlobId;
            range.BlobOffset = source.BlobOffset;

            visitor(range);
        }
    }

private:
    // prev is known to be a Blob.
    static bool SameBlobConsecutiveBlocks(
        const TSource& prev,
        const TSource& next)
    {
        return next.Kind == ESourceKind::Blob
            && next.CommitId == prev.CommitId
            && next.BlobId == prev.BlobId
            && next.BlobOffset == prev.BlobOffset + 1;
    }

    bool UpdateRange(ui32 blockIndex, ui32 blocksCount, const TSource& source)
    {
        const ui32 first = blockIndex - BeginBlockIndex;
        Y_DEBUG_ABORT_UNLESS(blocksCount);
        Y_DEBUG_ABORT_UNLESS(first < Blocks.size());
        Y_DEBUG_ABORT_UNLESS(blocksCount <= Blocks.size() - first);

        bool changed = false;

        for (ui32 i = 0; i < blocksCount; ++i) {
            auto& prev = Blocks[first + i];

            if (prev.CommitId < source.CommitId) {
                prev = source;

                if (source.Kind == ESourceKind::Blob) {
                    // Blob offset advances together with the block index.
                    prev.BlobOffset = source.BlobOffset + i;
                }

                changed = true;
            }
        }

        return changed;
    }
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NFileStore::NStorage
