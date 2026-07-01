#include "fresh_bytes.h"
#include "verify.h"

#include <cloud/storage/core/libs/tablet/model/commit.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TDeletedRangeMap::InsertRange(
    ui64 nodeId,
    ui64 offset,
    ui64 len,
    ui64 commitId)
{
    Ranges[{.NodeId = nodeId, .End = offset + len}] = {
        .Len = len,
        .Offset = offset,
        .CommitId = commitId,
    };
}

void TDeletedRangeMap::Advance(
    ui64& offset,
    ui64& len,
    TImpl::const_iterator& it) const
{
    TABLET_VERIFY_C(
        it->first.End > offset,
        "offset=" << offset << ", len=" << len
            << ", it->first.NodeId=" << it->first.NodeId
            << ", it->first.End=" << it->first.End
            << ", it->second.Len=" << it->second.Len
            << ", it->second.Offset=" << it->second.Offset
            << ", it->second.CommitId=" << it->second.CommitId);

    const ui64 shift = Min(it->first.End - offset, len);
    offset += shift;
    len -= shift;
    ++it;
}

void TDeletedRangeMap::AddRange(
    ui64 nodeId,
    ui64 offset,
    ui64 len,
    ui64 commitId)
{
    const ui64 end = offset + len;

    auto it = static_cast<const TImpl&>(Ranges)
        .upper_bound({.NodeId = nodeId, .End = offset});
    while (it != Ranges.end()
            && it->first.NodeId == nodeId
            && it->second.Offset < end)
    {
        //
        // We expect AddRange calls to be ordered by commitId.
        //

        TABLET_VERIFY_C(
            it->second.CommitId < commitId,
            "range.CommitId=" << it->second.CommitId
                << ", commitId=" << commitId);

        //
        // [___________________) <--- input range
        // ____[____) <--- range pointed to by `it`
        // [___) <--- inserting this segment with `CommitId := commitId`
        //

        if (offset < it->second.Offset) {
            InsertRange(nodeId, offset, it->second.Offset - offset, commitId);
        }

        //
        // [___________________) <--- input range
        // ____[____) <--- range pointed to by `it`
        // _________[__________) <--- updating the input range to point to this
        //

        Advance(offset, len, it);
    }

    //
    // If we still have a non-empty subrange after processing the overlapping
    // ranges we should insert it as well.
    //

    if (len) {
        InsertRange(nodeId, offset, len, commitId);
    }
}

TVector<TByteRange> TDeletedRangeMap::ApplyRanges(
    ui64 nodeId,
    TByteRange byteRange,
    ui64 commitId) const
{
    auto it = Ranges.upper_bound({.NodeId = nodeId, .End = byteRange.Offset});
    TVector<TByteRange> result;
    while (it != Ranges.end()
            && it->first.NodeId == nodeId
            && it->second.Offset < byteRange.End())
    {
        //
        // Skipping the ranges that are not visible at `commitId`.
        //

        if (it->second.CommitId > commitId) {
            ++it;
            continue;
        }

        //
        // [___________________) <--- input range
        // ____[____) <--- range pointed to by `it`
        // [___) <--- adding this segment to the result
        //

        if (byteRange.Offset < it->second.Offset) {
            result.push_back(TByteRange(
                byteRange.Offset,
                it->second.Offset - byteRange.Offset,
                byteRange.BlockSize));
        }

        //
        // [___________________) <--- input range
        // ____[____) <--- range pointed to by `it`
        // _________[__________) <--- updating the input range to point to this
        //

        Advance(byteRange.Offset, byteRange.Length, it);
    }

    //
    // If we still have a non-empty subrange after processing the overlapping
    // ranges we should insert it as well.
    //

    if (byteRange.Length) {
        result.push_back(byteRange);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TFreshBytes::TFreshBytes(IAllocator* allocator)
    : Allocator(allocator)
    , Chunks(allocator)
{
    Chunks.emplace_back(Allocator);
    Chunks.back().Id = ++LastChunkId;
}

TFreshBytes::~TFreshBytes()
{}

void TFreshBytes::DeleteBytes(
    TChunk& c,
    ui64 nodeId,
    ui64 offset,
    ui64 len,
    ui64 commitId)
{
    auto end = offset + len;

    auto lo = c.Refs.upper_bound({.NodeId = nodeId, .End = offset});
    auto hi = c.Refs.upper_bound({.NodeId = nodeId, .End = end});

    if (lo != c.Refs.end() && lo->first.NodeId == nodeId) {
        if (lo->second.Offset == offset && lo->first.End == end) {
            // special case
            c.Refs.erase(lo);
            return;
        }

        if (lo->second.Offset < offset) {
            // cutting lo from the right side
            TABLET_VERIFY_DEBUG(lo->second.CommitId < commitId);
            auto& loRef = c.Refs[{.NodeId = nodeId, .End = offset}];
            loRef = lo->second;
            loRef.Buf = loRef.Buf.substr(0, offset - loRef.Offset);

            if (lo->first.End <= end) {
                // blockRange is not contained strictly inside lo
                TABLET_VERIFY_DEBUG(lo != hi);
                c.Refs.erase(lo++);
            }
        }

        if (hi != c.Refs.end()
                && hi->first.NodeId == nodeId
                && hi->second.Offset < end)
        {
            // cutting hi from the left side
            // hi might be equal to lo - it's not a problem
            TABLET_VERIFY_DEBUG(hi->second.CommitId < commitId);
            const auto shift = end - hi->second.Offset;
            hi->second.Buf = hi->second.Buf.substr(
                shift,
                hi->second.Buf.size() - shift
            );
            hi->second.Offset = end;
        }

        // deleting ranges between lo and hi
        while (lo != c.Refs.end() && lo->first.NodeId == nodeId && lo != hi) {
            c.Refs.erase(lo++);
        }
    }
}

NProto::TError TFreshBytes::CheckBytes(
    ui64 nodeId,
    ui64 offset,
    TStringBuf data,
    ui64 commitId) const
{
    // might use these in the future
    Y_UNUSED(nodeId);
    Y_UNUSED(offset);
    Y_UNUSED(data);

    const auto& c = Chunks.back();
    if (c.FirstCommitId != InvalidCommitId && commitId < c.FirstCommitId) {
        return MakeError(
            E_REJECTED,
            TStringBuilder() << "bytes' commitId too old: " << commitId << " < "
                << c.FirstCommitId);
    }

    return {};
}

void TFreshBytes::AddBytes(
    ui64 nodeId,
    ui64 offset,
    TStringBuf data,
    ui64 commitId)
{
    //
    // Validate CommitId order.
    //

    auto& c = Chunks.back();
    if (c.FirstCommitId == InvalidCommitId) {
        c.FirstCommitId = commitId;
    } else {
        TABLET_VERIFY(commitId >= c.FirstCommitId);
    }

    //
    // Copy the data into a buffer and update stats.
    //

    TByteVector buffer(Reserve(data.size()), Allocator);
    buffer.assign(data.begin(), data.end());

    c.TotalBytes += buffer.size();
    TotalBytes += buffer.size();

    TBytes descriptor(nodeId, offset, buffer.size(), commitId, InvalidCommitId);
    c.Data.emplace_back(descriptor, std::move(buffer));

    TotalDataItemCount++;

    const auto& storage = c.Data.back().Data;
    TKey key{.NodeId = nodeId, .End = offset + storage.size()};
    TRef ref{
        .Buf = TStringBuf(storage.data(), storage.size()),
        .Offset = offset,
        .CommitId = commitId,
    };

    //
    // Delete any data that we might have in this chunk that is covered by this
    // range and then insert this range.
    //

    DeleteBytes(c, nodeId, offset, data.size(), commitId);

    c.Refs[key] = ref;
}

void TFreshBytes::AddDeletionMarker(
    ui64 nodeId,
    ui64 offset,
    ui64 len,
    ui64 commitId)
{
    //
    // Validate CommitId order.
    //

    auto& c = Chunks.back();
    if (c.FirstCommitId == InvalidCommitId) {
        c.FirstCommitId = commitId;
    } else {
        TABLET_VERIFY(commitId >= c.FirstCommitId);
    }

    c.TotalDeletedBytes += len;
    TotalDeletedBytes += len;

    c.DeletionMarkers.push_back({
        nodeId,
        offset,
        len,
        commitId,
        InvalidCommitId});

    //
    // We need to erase the data in this chunk that's covered by this marker.
    //

    DeleteBytes(c, nodeId, offset, len, commitId);

    //
    // We need to mark this range as deleted to make the corresponding range
    // invisible in the previous chunks if read at this commitId or higher.
    //

    c.DeletedRanges.AddRange(nodeId, offset, len, commitId);
}

void TFreshBytes::Barrier(ui64 commitId)
{
    TABLET_VERIFY(!Chunks.empty());
    auto& c = Chunks.back();

    if (!c.Data.empty() || !c.DeletionMarkers.empty()) {
        if (!c.Data.empty()) {
            TABLET_VERIFY(c.Data.back().Descriptor.MinCommitId <= commitId);
        }
        if (!c.DeletionMarkers.empty()) {
            TABLET_VERIFY(c.DeletionMarkers.back().MinCommitId <= commitId);
        }
        Chunks.back().ClosingCommitId = commitId;
        Chunks.emplace_back(Allocator);
        Chunks.back().Id = ++LastChunkId;
    }
}

void TFreshBytes::OnCheckpoint(ui64 commitId)
{
    //
    // We need to seal the current chunk because the last chunk is mutable and
    // we want to freeze the current state for this checkpoint.
    //

    Barrier(commitId);
}

TFlushBytesCleanupInfo TFreshBytes::StartCleanup(
    ui64 commitId,
    TVector<TBytes>* entries,
    TVector<TBytes>* deletionMarkers)
{
    if (Chunks.size() == 1) {
        //
        // We need to seal the front chunk only if it's active - i.e. if it's
        // not the only chunk that we have. The goal is to ensure that no
        // updates are made to this chunk after that - we want to safely process
        // it without worrying about any kind of races.
        //

        Barrier(commitId);
    }

    for (auto& e: Chunks.front().Data) {
        entries->push_back(e.Descriptor);
    }

    for (auto& descriptor: Chunks.front().DeletionMarkers) {
        deletionMarkers->push_back(descriptor);
    }

    return {
        .ChunkId = Chunks.front().Id,
        .ClosingCommitId = Chunks.front().ClosingCommitId
    };
}

void TFreshBytes::VisitTop(
    ui64 itemLimit,
    const TChunkVisitor& visitor)
{
    ui64 cnt = 0;
    for (const auto& e: Chunks.front().Data) {
        if (cnt++ == itemLimit) {
            return;
        }
        visitor(e.Descriptor, false);
    }

    for (const auto& descriptor: Chunks.front().DeletionMarkers) {
        if (cnt++ == itemLimit) {
            return;
        }
        visitor(descriptor, true);
    }
}

bool TFreshBytes::FinishCleanup(
    ui64 chunkId,
    ui64 dataItemCount,
    ui64 deletionMarkerCount)
{
    TABLET_VERIFY(Chunks.size() > 1);
    TABLET_VERIFY(Chunks.front().Id == chunkId);

    TotalDataItemCount -= dataItemCount;

    auto& chunk = Chunks.front();

    const auto dataSize = chunk.Data.size();
    const auto deletionSize = chunk.DeletionMarkers.size();
    if (dataItemCount == dataSize && deletionMarkerCount == deletionSize) {
        TotalBytes -= chunk.TotalBytes;
        TotalDeletedBytes -= chunk.TotalDeletedBytes;
        Chunks.pop_front();
        return true;
    }

    const auto check =
        dataItemCount <= dataSize && deletionMarkerCount <= deletionSize;
    TABLET_VERIFY(check);

    //
    // Paged cleanup is used. We should just delete the cleaned up ranges and
    // then the caller will call StartCleanup again for this chunk and the next
    // page will be processed.
    //

    chunk.Data.erase(
        chunk.Data.begin(),
        std::next(chunk.Data.begin(), dataItemCount));

    chunk.DeletionMarkers.erase(
        chunk.DeletionMarkers.begin(),
        std::next(chunk.DeletionMarkers.begin(), deletionMarkerCount));

    return false;
}

void TFreshBytes::FindBytes(
    IFreshBytesVisitor& visitor,
    ui64 nodeId,
    TByteRange byteRange,
    ui64 commitId) const
{
    for (ui64 i = 0; i < Chunks.size(); ++i) {
        if (Chunks[i].FirstCommitId > commitId) {
            //
            // Chunk CommitId ranges do not overlap and are ordered by
            // FirstCommitId so all the remaining ranges are not visible.
            //

            break;
        }

        FindBytes(Chunks, i, visitor, nodeId, byteRange, commitId);
    }
}

/* static */ TVector<TByteRange> TFreshBytes::ApplyDeletedRanges(
    const TChunks& chunks,
    ui64 firstChunkIndex,
    ui64 nodeId,
    TByteRange initialRange,
    ui64 commitId)
{
    TVector<TByteRange> result(1, initialRange);

    //
    // Most of the time there will be no other chunks.
    //

    for (ui64 i = firstChunkIndex; i < chunks.size(); ++i) {
        const auto& nextChunk = chunks[i];
        if (nextChunk.FirstCommitId > commitId) {
            break;
        }

        TVector<TByteRange> newResult;

        //
        // Most of the time we'll end up realizing that there're no
        // deleted ranges to apply and thus we'll end up having just
        // one range till the very end of this procedure.
        //

        for (const auto& r: result) {
            auto split =
                nextChunk.DeletedRanges.ApplyRanges(nodeId, r, commitId);
            newResult.reserve(newResult.size() + split.size());
            newResult.insert(newResult.end(), split.begin(), split.end());
        }

        result.swap(newResult);
    }

    return result;
}

void TFreshBytes::FindBytes(
    const TChunks& chunks,
    ui64 chunkIndex,
    IFreshBytesVisitor& visitor,
    ui64 nodeId,
    TByteRange byteRange,
    ui64 commitId) const
{
    const auto& chunk = chunks[chunkIndex];
    auto it = chunk.Refs.upper_bound({nodeId, byteRange.Offset});
    while (it != chunk.Refs.end()
            && it->first.NodeId == nodeId
            && it->second.Offset < byteRange.End())
    {
        TByteRange itRange(
            it->second.Offset,
            it->first.End - it->second.Offset,
            byteRange.BlockSize
        );

        const auto intersection = itRange.Intersect(byteRange);
        if (it->second.CommitId <= commitId && intersection.Length) {
            auto actualRanges = ApplyDeletedRanges(
                chunks,
                chunkIndex + 1,
                nodeId,
                intersection,
                commitId);

            for (const auto& actualRange: actualRanges) {
                TStringBuf data = it->second.Buf.substr(
                    actualRange.Offset - itRange.Offset,
                    actualRange.Length);

                visitor.Accept({
                    nodeId,
                    actualRange.Offset,
                    actualRange.Length,
                    it->second.CommitId,
                    InvalidCommitId
                }, data);
            }
        }

        ++it;
    }
}

bool TFreshBytes::Intersects(ui64 nodeId, TByteRange byteRange) const
{
    struct TVisitor: IFreshBytesVisitor
    {
        bool FoundBytes = false;

        void Accept(const TBytes& bytes, TStringBuf data) override
        {
            Y_UNUSED(bytes);
            Y_UNUSED(data);

            FoundBytes = true;
        }
    } visitor;

    FindBytes(visitor, nodeId, byteRange, InvalidCommitId);

    return visitor.FoundBytes;
}

}   // namespace NCloud::NFileStore::NStorage
