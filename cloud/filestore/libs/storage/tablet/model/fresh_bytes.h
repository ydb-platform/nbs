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
    TMap<TKey, TDeletedRange, TLess<TKey>, TStlAllocator> Ranges;
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
        // reading previous chunk.
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
    TDeque<TChunk, TStlAllocator> Chunks;
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

    NProto::TError CheckBytes(
        ui64 nodeId,
        ui64 offset,
        TStringBuf data,
        ui64 commitId) const;
    void AddBytes(ui64 nodeId, ui64 offset, TStringBuf data, ui64 commitId);
    void AddDeletionMarker(ui64 nodeId, ui64 offset, ui64 len, ui64 commitId);

    void OnCheckpoint(ui64 commitId);

    TFlushBytesCleanupInfo StartCleanup(
        ui64 commitId,
        TVector<TBytes>* entries,
        TVector<TBytes>* deletionMarkers);
    void VisitTop(ui64 itemLimit, const TChunkVisitor& visitor);
    bool FinishCleanup(
        ui64 chunkId,
        ui64 dataItemCount,
        ui64 deletionMarkerCount);

    void FindBytes(
        IFreshBytesVisitor& visitor,
        ui64 nodeId,
        TByteRange byteRange,
        ui64 commitId) const;

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
        const TChunk& chunk,
        const TChunk* nextChunk,
        IFreshBytesVisitor& visitor,
        ui64 nodeId,
        TByteRange byteRange,
        ui64 commitId) const;
};

}   // namespace NCloud::NFileStore::NStorage
