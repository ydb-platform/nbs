#pragma once

#include "block.h"

#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <limits>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Resolves the visible source for every block in a [begin, begin + blocksCount)
// range. For each block the source with the greatest CommitId wins.
//
// The public surface matches the range/segment oriented API used by the read
// path, but the implementation is a flat per-block array (as the original read
// path used). Updates are therefore O(blocks touched) with no allocations or
// tree rebalancing, which keeps the hot DescribeData/ReadData paths - where the
// same block may be visited once per overlapping blob - as cheap as before.
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

    struct TRange
    {
        ui32 BlockIndex = 0;
        ui32 BlocksCount = 0;
        TSource Source;
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
    ui32 Begin = 0;
    ui32 End = 0;

    // One entry per block in [Begin, End), indexed by (block - Begin).
    TVector<TSource> Blocks;

public:
    TBlockRanges(ui32 begin, ui32 blocksCount)
        : Begin(begin)
        , End(begin + blocksCount)
        , Blocks(blocksCount)
    {
        Y_ABORT_UNLESS(blocksCount);
    }

    bool Empty() const
    {
        return Begin == End;
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

    // CommitId of the visible data (Blob/Fresh) at the block, or 0 if the
    // block currently has no data (Empty or Deleted) or is out of range.
    ui64 GetVisibleDataCommitId(ui32 blockIndex) const
    {
        if (blockIndex < Begin || blockIndex >= End) {
            return 0;
        }

        const auto& source = Blocks[blockIndex - Begin];

        if (source.Kind == ESourceKind::Empty
                || source.Kind == ESourceKind::Deleted)
        {
            return 0;
        }

        return source.CommitId;
    }

    bool IsFreshBlock(ui32 blockIndex) const
    {
        return Blocks[blockIndex - Begin].Kind == ESourceKind::Fresh;
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

        // Blob offset advances together with the block index across the range.
        return UpdateRange(blockIndex, blocksCount, source, true);
    }

    bool AddFreshRange(ui32 blockIndex, ui32 blocksCount, ui64 commitId)
    {
        TSource source;
        source.Kind = ESourceKind::Fresh;
        source.CommitId = commitId;

        return UpdateRange(blockIndex, blocksCount, source, false);
    }

    bool AddDeletionRange(ui32 blockIndex, ui32 blocksCount, ui64 commitId)
    {
        TSource source;
        source.Kind = ESourceKind::Deleted;
        source.CommitId = commitId;

        return UpdateRange(blockIndex, blocksCount, source, false);
    }

    // Visits maximal runs of equal source, skipping blocks with no source.
    // Two adjacent blocks belong to the same run iff they share Kind and
    // CommitId and, for blobs, share BlobId with contiguous BlobOffset.
    template <typename TVisitor>
    void VisitRanges(TVisitor&& visitor) const
    {
        ui32 i = 0;
        while (i < Blocks.size()) {
            if (Blocks[i].Kind == ESourceKind::Empty) {
                ++i;
                continue;
            }

            const ui32 runStart = i;
            ++i;
            while (i < Blocks.size() && SameRun(Blocks[i - 1], Blocks[i])) {
                ++i;
            }

            TRange range;
            range.BlockIndex = Begin + runStart;
            range.BlocksCount = i - runStart;
            range.Source = Blocks[runStart];

            visitor(range);
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
            ++i;
            while (i < Blocks.size() && SameRun(Blocks[i - 1], Blocks[i])) {
                ++i;
            }

            const auto& source = Blocks[runStart];

            TBlobRange range;
            range.BlockIndex = Begin + runStart;
            range.BlocksCount = i - runStart;
            range.CommitId = source.CommitId;
            range.BlobId = source.BlobId;
            range.BlobOffset = source.BlobOffset;

            visitor(range);
        }
    }

private:
    static bool SameRun(const TSource& l, const TSource& r)
    {
        if (l.Kind != r.Kind || l.CommitId != r.CommitId) {
            return false;
        }

        if (l.Kind != ESourceKind::Blob) {
            return true;
        }

        return l.BlobId == r.BlobId && l.BlobOffset + 1 == r.BlobOffset;
    }

    bool UpdateRange(
        ui32 blockIndex,
        ui32 blocksCount,
        const TSource& source,
        bool shiftBlobOffset)
    {
        if (!blocksCount || Empty()) {
            return false;
        }

        const ui64 rawStart = blockIndex;
        const ui64 rawEnd = ui64(blockIndex) + blocksCount;

        const ui64 start = Max<ui64>(rawStart, Begin);
        const ui64 end = Min<ui64>(rawEnd, End);

        if (start >= end) {
            return false;
        }

        bool changed = false;

        for (ui64 b = start; b < end; ++b) {
            auto& prev = Blocks[b - Begin];

            if (prev.CommitId < source.CommitId) {
                prev = source;

                if (shiftBlobOffset) {
                    prev.BlobOffset = ToUi32(ui64(source.BlobOffset) + (b - rawStart));
                }

                changed = true;
            }
        }

        return changed;
    }

    static ui32 ToUi32(ui64 value)
    {
        Y_ABORT_UNLESS(value <= std::numeric_limits<ui32>::max());
        return static_cast<ui32>(value);
    }
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NFileStore::NStorage
