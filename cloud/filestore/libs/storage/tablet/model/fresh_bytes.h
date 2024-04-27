#pragma once

#include "public.h"

#include "block.h"

#include <cloud/filestore/libs/storage/model/range.h>
#include <cloud/storage/core/libs/common/byte_vector.h>

#include <util/generic/map.h>
#include <util/generic/deque.h>
#include <util/generic/strbuf.h>
#include <util/memory/alloc.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

using TChunkVisitor =
    std::function<void(const TBytes& bytes, bool isDeletionMarker)>;

////////////////////////////////////////////////////////////////////////////////

class TFreshBytes
{
public:
    struct TKey
    {
        ui64 NodeId = 0;
        ui64 End = 0;

        bool operator<(const TKey& rhs) const
        {
            // (NodeId, End) ASC
            return NodeId < rhs.NodeId
                || NodeId == rhs.NodeId && End < rhs.End;
        }
    };

private:
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
        TMap<TKey, TRef, TLess<TKey>, TStlAllocator> Refs;
        TDeque<TElement, TStlAllocator> Data;
        TDeque<TBytes, TStlAllocator> DeletionMarkers;
        ui64 FirstCommitId = InvalidCommitId;
        ui64 TotalBytes = 0;
        ui64 Id = 0;
        ui64 ClosingCommitId = 0;

        explicit TChunk(IAllocator* allocator)
            : Refs(allocator)
            , Data(allocator)
            , DeletionMarkers(allocator)
        {}
    };

private:
    IAllocator* Allocator;
    TDeque<TChunk, TStlAllocator> Chunks;
    ui64 LastChunkId = 0;

public:
    TFreshBytes(IAllocator* allocator);
    ~TFreshBytes();

    size_t GetTotalBytes() const
    {
        ui64 bytes = 0;
        for (const auto& c: Chunks) {
            bytes += c.TotalBytes;
        }
        return bytes;
    }

    void AddBytes(ui64 nodeId, ui64 offset, TStringBuf data, ui64 commitId);
    void AddDeletionMarker(ui64 nodeId, ui64 offset, ui64 len, ui64 commitId);

    void OnCheckpoint(ui64 commitId);

    TFlushBytesCleanupInfo StartCleanup(
        ui64 commitId,
        TVector<TBytes>* entries,
        TVector<TBytes>* deletionMarkers);
    void VisitTop(const TChunkVisitor& visitor);
    void FinishCleanup(ui64 chunkId);

    void FindBytes(
        IFreshBytesVisitor& visitor,
        ui64 nodeId,
        TByteRange byteRange,
        ui64 commitId) const;

    bool Intersects(ui64 nodeId, TByteRange byteRange) const;

private:
    void DeleteBytes(TChunk& c, ui64 nodeId, ui64 offset, ui64 len, ui64 commitId);

    void Barrier(ui64 commitId);

    void FindBytes(
        const TChunk& chunk,
        IFreshBytesVisitor& visitor,
        ui64 nodeId,
        TByteRange byteRange,
        ui64 commitId) const;
};

}   // namespace NCloud::NFileStore::NStorage
