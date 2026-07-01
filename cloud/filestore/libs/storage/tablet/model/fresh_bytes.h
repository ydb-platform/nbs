#pragma once

#include "public.h"

#include "block.h"

#include <cloud/storage/core/libs/common/byte_range.h>
#include <cloud/storage/core/libs/common/byte_vector.h>
#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/map.h>
#include <util/generic/deque.h>
#include <util/generic/strbuf.h>
#include <util/memory/alloc.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TNodeByteRangeKey
{
    ui64 NodeId = 0;
    ui64 End = 0;

    bool operator<(const TNodeByteRangeKey& rhs) const
    {
        // (NodeId, End) ASC
        return NodeId < rhs.NodeId
            || NodeId == rhs.NodeId && End < rhs.End;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDeletedRangeMap
{
private:
    struct TDeletedRange
    {
        ui64 Len = 0;
        ui64 Offset = 0;
        ui64 CommitId = 0;
    };

private:
    using TKey = TNodeByteRangeKey;
    using TImpl = TMap<TKey, TDeletedRange, TLess<TKey>, TStlAllocator>;
    TImpl Ranges;
    TString LogTag;

public:
    explicit TDeletedRangeMap(IAllocator* allocator)
        : Ranges(allocator)
    {}

public:
    void AddRange(ui64 nodeId, ui64 offset, ui64 len, ui64 commitId);

    TVector<TByteRange> ApplyRanges(
        ui64 nodeId,
        TByteRange byteRange,
        ui64 commitId) const;

    void UpdateLogTag(TString logTag)
    {
        LogTag = std::move(logTag);
    }

private:
    void InsertRange(ui64 nodeId, ui64 offset, ui64 len, ui64 commitId);
    void Advance(
        ui64& offset,
        ui64& len,
        TImpl::const_iterator& it) const;
};

////////////////////////////////////////////////////////////////////////////////

using TChunkVisitor =
    std::function<void(const TBytes& bytes, bool isDeletionMarker)>;

////////////////////////////////////////////////////////////////////////////////

class TFreshBytes
{
private:
    using TKey = TNodeByteRangeKey;

    struct TElement
    {
        TBytes Descriptor;
        TByteVector Data;

        TElement(TBytes descriptor, TByteVector data)
            : Descriptor(std::move(descriptor))
            , Data(std::move(data))
        {}
    };

    struct TRef
    {
        TStringBuf Buf;
        ui64 Offset = 0;
        ui64 CommitId = 0;
    };

    struct TChunk
    {
        //
        // Bytes of this chunk. Newer writes overwrite overlapping parts of
        // older writes.
        //

        TMap<TKey, TRef, TLess<TKey>, TStlAllocator> Refs;
        TDeque<TElement, TStlAllocator> Data;

        //
        // A list of deletion markers - needed upon cleanup to be able to clean
        // them up in the tablet's local db.
        //

        TDeque<TBytes, TStlAllocator> DeletionMarkers;

        //
        // Ranges deleted in this chunk. Only the non-overlapping parts of newer
        // deletions are applied. Needed to apply this chunk's deletions upon
        // reading previous chunks.
        //

        TDeletedRangeMap DeletedRanges;

        //
        // CommitId range for this chunk.
        //

        ui64 FirstCommitId = InvalidCommitId;
        ui64 ClosingCommitId = 0;

        //
        // The Id of this chunk. Needed to verify that FinishCleanup is called
        // for the front chunk.
        //

        ui64 Id = 0;

        //
        // Counters.
        //

        ui64 TotalBytes = 0;
        ui64 TotalDeletedBytes = 0;

        explicit TChunk(IAllocator* allocator)
            : Refs(allocator)
            , Data(allocator)
            , DeletionMarkers(allocator)
            , DeletedRanges(allocator)
        {}
    };

private:
    //
    // Chunks do not overlap by CommitId ranges and are ordered by
    // FirstCommitId. FirstCommitId for an empty chunk == InvalidCommitId.
    // A chunk is sealed upon StartCleanup and OnCheckpoint ops - after that no
    // updates should happen to it, only FinishCleanup.
    //

    IAllocator* Allocator;
    using TChunks = TDeque<TChunk, TStlAllocator>;
    TChunks Chunks;
    ui64 LastChunkId = 0;
    TString LogTag;

    //
    // Total counters.
    //

    ui64 TotalBytes = 0;
    ui64 TotalDeletedBytes = 0;
    ui64 TotalDataItemCount = 0;

public:
    explicit TFreshBytes(IAllocator* allocator);
    ~TFreshBytes();

    size_t GetTotalBytes() const
    {
        return TotalBytes;
    }

    size_t GetTotalDeletedBytes() const
    {
        return TotalDeletedBytes;
    }

    size_t GetTotalDataItemCount() const
    {
        return TotalDataItemCount;
    }

    void UpdateLogTag(TString logTag)
    {
        LogTag = std::move(logTag);
        for (auto& c: Chunks) {
            c.DeletedRanges.UpdateLogTag(LogTag);
        }
    }

    /**
     * Validates whether the supplied bytes can be added. Can be called before
     * AddBytes.
     *
     * @param nodeId - NodeId.
     * @param offset - Offset within the file.
     * @param data - Bytes to add.
     * @param commitId - CommitId at which these bytes appeared.
     *
     * @return - E_REJECTED upon failure, S_OK - otherwise.
     */
    NProto::TError CheckBytes(
        ui64 nodeId,
        ui64 offset,
        TStringBuf data,
        ui64 commitId) const;

    /**
     * Adds a range of bytes into the last chunk. CommitIds should grow
     * monotonically.
     *
     * @param nodeId - NodeId.
     * @param offset - Offset within the file.
     * @param data - Bytes to add.
     * @param commitId - CommitId at which these bytes appeared.
     */
    void AddBytes(ui64 nodeId, ui64 offset, TStringBuf data, ui64 commitId);

    /**
     * Registers this range as deleted in the last chunk. Deletes overlapping
     * byte ranges in this chunk in-place and remembers this range to apply this
     * deletion when previous chunks are read.
     *
     * @param nodeId - NodeId.
     * @param offset - Offset within the file.
     * @param len - Length of the range.
     * @param commitId - CommitId at which this range was deleted.
     */
    void AddDeletionMarker(ui64 nodeId, ui64 offset, ui64 len, ui64 commitId);

    /**
     * Seals the last chunk and creates a new empty one. Needed to make a frozen
     * snapshot of the current state of this data structure. CommitId order is
     * again expected to be monotonic.
     *
     * @param commitId - CommitId of the checkpoint.
     */
    void OnCheckpoint(ui64 commitId);

    /**
     * If there's only one chunk it gets sealed and a new chunk is created.
     * This method is supposed to be used to copy the bytes in this chunk
     * elsewhere and then delete this chunk via FinishCleanup.
     *
     * @param commitId - CommitId of the moment when cleanup starts.
     * @param entries (out) - Returns the bytes in the last chunk.
     * @param deletionMarkers (out) - Returns the deletion markers in the last
     *  chunk.
     *
     * @return - Chunk meta of the sealed chunk.
     */
    TFlushBytesCleanupInfo StartCleanup(
        ui64 commitId,
        TVector<TBytes>* entries,
        TVector<TBytes>* deletionMarkers);

    /**
     * Applies the supplied visitor to the items in the first chunk.
     *
     * @param itemLimit - Limits the number of traversed items.
     * @param visitor - The visitor.
     */
    void VisitTop(ui64 itemLimit, const TChunkVisitor& visitor);

    /**
     * Completes the cleanup that was initiated via StartCleanup.
     *
     * @param chunkId - The id of the chunk - supposed to be obtained from the
     *  chunk meta returned by StartCleanup.
     * @param dataItemCount - Limits the number of cleaned up byte items.
     * @param deletionMarkerCount - Limits the number of cleaned up deletion
     *  marker items.
     *
     * @return - true if the chunk is now empty, false - otherwise.
     */
    bool FinishCleanup(
        ui64 chunkId,
        ui64 dataItemCount,
        ui64 deletionMarkerCount);

    /**
     * Applies the supplied visitor to the bytes stored in this structure that
     * are visible at the supplied commit-id.
     *
     * @param visitor - The visitor.
     * @param nodeId - NodeId filter.
     * @param byteRange - Range filter.
     * @param commitId - The commit-id at which we want to read, newer bytes and
     *  deletions will be skipped.
     */
    void FindBytes(
        IFreshBytesVisitor& visitor,
        ui64 nodeId,
        TByteRange byteRange,
        ui64 commitId) const;

    /**
     * Checks whether the supplied range overlaps with any of the stored ranges.
     *
     * @param nodeId - NodeId filter.
     * @param byteRange - Range filter.
     *
     * @return - true if overlapping ranges exist, false - otherwise.
     */
    bool Intersects(ui64 nodeId, TByteRange byteRange) const;

private:
    void DeleteBytes(
        TChunk& c,
        ui64 nodeId,
        ui64 offset,
        ui64 len,
        ui64 commitId);

    void Barrier(ui64 commitId);

    void FindBytes(
        const TChunks& chunks,
        ui64 chunkIndex,
        IFreshBytesVisitor& visitor,
        ui64 nodeId,
        TByteRange byteRange,
        ui64 commitId) const;

    static TVector<TByteRange> ApplyDeletedRanges(
        const TChunks& chunks,
        ui64 firstChunkIndex,
        ui64 nodeId,
        TByteRange initialRange,
        ui64 commitId);
};

}   // namespace NCloud::NFileStore::NStorage
