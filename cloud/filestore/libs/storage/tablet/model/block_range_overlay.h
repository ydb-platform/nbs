#pragma once

#include "block.h"

#include <util/generic/map.h>
#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <iterator>
#include <limits>
#include <utility>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TBlockRangeOverlay
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
        // Offset in blob corresponding to the segment start.
        ui32 BlobOffset = 0;
    };

    struct TSegment
    {
        // Exclusive logical block index.
        ui64 End = 0;

        TSource Source;
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
    ui64 Begin = 0;
    ui64 End = 0;

    // key = segment start, value.End = segment end.
    // Segments are always disjoint, sorted and cover [Begin, End).
    TMap<ui64, TSegment> Segments;

public:
    TBlockRangeOverlay(ui32 begin, ui32 blocksCount)
    {
        Y_ABORT_UNLESS(blocksCount);

        Begin = begin;
        End = begin + blocksCount;

        TSegment segment;
        segment.End = End;
        segment.Source = {};

        Segments.emplace(Begin, segment);
    }

    bool Empty() const
    {
        return Begin == End;
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

    bool AddFreshRange(
        ui32 blockIndex,
        ui32 blocksCount,
        ui64 commitId)
    {
        TSource source;
        source.Kind = ESourceKind::Fresh;
        source.CommitId = commitId;

        return UpdateRange(blockIndex, blocksCount, source);
    }

    bool AddDeletionRange(
        ui32 blockIndex,
        ui32 blocksCount,
        ui64 commitId)
    {
        TSource source;
        source.Kind = ESourceKind::Deleted;
        source.CommitId = commitId;

        return UpdateRange(blockIndex, blocksCount, source);
    }

    template <typename TVisitor>
    void VisitRanges(TVisitor&& visitor) const
    {
        for (const auto& x: Segments) {
            const ui64 start = x.first;
            const auto& segment = x.second;

            if (segment.Source.Kind == ESourceKind::Empty) {
                continue;
            }

            TRange range;
            range.BlockIndex = ToUi32(start);
            range.BlocksCount = ToUi32(segment.End - start);
            range.Source = segment.Source;

            visitor(range);
        }
    }

    template <typename TVisitor>
    void VisitBlobRanges(TVisitor&& visitor) const
    {
        bool hasLast = false;
        TBlobRange last;

        auto flush = [&] {
            if (hasLast) {
                visitor(last);
                hasLast = false;
                last = {};
            }
        };

        for (const auto& x: Segments) {
            const ui64 start = x.first;
            const auto& segment = x.second;
            const auto& source = segment.Source;

            if (source.Kind != ESourceKind::Blob) {
                flush();
                continue;
            }

            TBlobRange current;
            current.BlockIndex = ToUi32(start);
            current.BlocksCount = ToUi32(segment.End - start);
            current.CommitId = source.CommitId;
            current.BlobId = source.BlobId;
            current.BlobOffset = source.BlobOffset;

            if (hasLast && CanAppend(last, current)) {
                last.BlocksCount =
                    ToUi32(last.BlocksCount + current.BlocksCount);
                continue;
            }

            flush();

            last = current;
            hasLast = true;
        }

        flush();
    }

    ui64 GetVisibleDataCommitId(ui32 blockIndex) const
    {
        const ui64 idx = blockIndex;

        if (idx < Begin || idx >= End || Segments.empty()) {
            return 0;
        }

        auto it = Segments.upper_bound(idx);
        Y_ABORT_UNLESS(it != Segments.begin());
        --it;

        Y_ABORT_UNLESS(it->first <= idx);
        Y_ABORT_UNLESS(idx < it->second.End);

        const auto& source = it->second.Source;

        if (source.Kind == ESourceKind::Empty
                || source.Kind == ESourceKind::Deleted)
        {
            return 0;
        }

        return source.CommitId;
    }

private:
    static ui32 ToUi32(ui64 value)
    {
        Y_ABORT_UNLESS(value <= std::numeric_limits<ui32>::max());
        return static_cast<ui32>(value);
    }

    static TSource ShiftSource(const TSource& source, ui64 delta)
    {
        auto shifted = source;

        if (shifted.Kind == ESourceKind::Blob) {
            shifted.BlobOffset = ToUi32(shifted.BlobOffset + delta);
        }

        return shifted;
    }

    typename TMap<ui64, TSegment>::iterator Split(ui64 pos)
    {
        if (pos <= Begin) {
            return Segments.begin();
        }

        if (pos >= End) {
            return Segments.end();
        }

        auto it = Segments.upper_bound(pos);
        Y_ABORT_UNLESS(it != Segments.begin());

        --it;

        const ui64 start = it->first;

        if (start == pos) {
            return it;
        }

        Y_ABORT_UNLESS(start < pos);
        Y_ABORT_UNLESS(pos < it->second.End);

        auto right = it->second;
        right.Source = ShiftSource(right.Source, pos - start);

        it->second.End = pos;

        return Segments.emplace(pos, right).first;
    }

    static bool CanMerge(
        ui64 leftStart,
        const TSegment& left,
        ui64 rightStart,
        const TSegment& right)
    {
        if (left.End != rightStart) {
            return false;
        }

        const auto& l = left.Source;
        const auto& r = right.Source;

        if (l.Kind != r.Kind) {
            return false;
        }

        if (l.CommitId != r.CommitId) {
            return false;
        }

        if (l.Kind != ESourceKind::Blob) {
            return true;
        }

        if (l.BlobId != r.BlobId) {
            return false;
        }

        const ui64 leftLen = left.End - leftStart;

        return l.BlobOffset + leftLen == r.BlobOffset;
    }

    static bool CanAppend(const TBlobRange& left, const TBlobRange& right)
    {
        if (left.BlockIndex + left.BlocksCount != right.BlockIndex) {
            return false;
        }

        if (left.CommitId != right.CommitId) {
            return false;
        }

        if (left.BlobId != right.BlobId) {
            return false;
        }

        if (left.BlobOffset + left.BlocksCount != right.BlobOffset) {
            return false;
        }

        return true;
    }

    void MergeAround(ui64 start, ui64 end)
    {
        if (Segments.empty()) {
            return;
        }

        auto it = Segments.lower_bound(start);
        if (it == Segments.end()) {
            --it;
        } else if (it != Segments.begin()) {
            --it;
        }

        // Include one segment after the updated range, because the last
        // updated segment may now be mergeable with the right neighbor.
        auto stop = Segments.upper_bound(end);
        if (stop != Segments.end()) {
            ++stop;
        }

        while (it != Segments.end()) {
            auto next = std::next(it);

            if (next == Segments.end() || next == stop) {
                break;
            }

            if (CanMerge(it->first, it->second, next->first, next->second)) {
                it->second.End = next->second.End;
                Segments.erase(next);
                continue;
            }

            ++it;
        }
    }

    bool UpdateRange(
        ui32 blockIndex,
        ui32 blocksCount,
        const TSource& source)
    {
        if (!blocksCount || Empty()) {
            return false;
        }

        const ui64 rawStart = blockIndex;
        const ui64 rawEnd = blockIndex + blocksCount;

        const ui64 start = Max(rawStart, Begin);
        const ui64 end = Min(rawEnd, End);

        if (start >= end) {
            return false;
        }

        Split(start);
        Split(end);

        bool changed = false;

        auto it = Segments.lower_bound(start);

        while (it != Segments.end() && it->first < end) {
            auto& segment = it->second;

            if (segment.Source.CommitId < source.CommitId) {
                const auto newSource =
                    ShiftSource(source, it->first - blockIndex);

                segment.Source = newSource;
                changed = true;
            }

            ++it;
        }

        // Important even when changed == false:
        // Split(start) / Split(end) may have introduced artificial
        // boundaries into an already newer visible range.
        MergeAround(start, end);

        return changed;
    }
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NFileStore::NStorage
