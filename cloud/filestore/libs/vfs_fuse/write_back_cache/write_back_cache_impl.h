#pragma once

#include "write_back_cache.h"

#include <cloud/filestore/libs/service/filestore.h>

#include <util/generic/intrlist.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

enum class TWriteBackCache::EWriteDataEntryStatus
{
    // Restoration from the persisent buffer has failed
    Corrupted,

    // Write request is waiting for the avaible space in the persistent buffer
    Pending,

    // Write request has been stored in the persistent buffer and the caller
    // code observes the request as completed
    Cached,

    // Same as |Cached|, but Flush is also requested
    FlushRequested,

    // Write request has been written to the session and can be removed from
    // the persistent buffer
    Flushed
};

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCache::TWriteDataEntry
{
private:
    // Store request metadata and request buffer separately
    // The idea is to deduplicate memory and to reference request buffer
    // directly in the persistent buffer if the request is stored there.
    std::shared_ptr<NProto::TWriteDataRequest> Request;
    TString RequestBuffer;

    // Memory allocated in CachedEntriesPersistentQueue
    char* AllocationPtr = nullptr;

    // Reference to either RequestBuffer or a memory region
    // referenced by AllocationPtr in CachedEntriesPersistentQueue
    TStringBuf BufferRef;

    NThreading::TPromise<NProto::TWriteDataResponse> CachedPromise;
    NThreading::TPromise<void> FlushPromise;
    EWriteDataEntryStatus Status = EWriteDataEntryStatus::Corrupted;

public:
    explicit TWriteDataEntry(
        std::shared_ptr<NProto::TWriteDataRequest> request);

    TWriteDataEntry(ui32 checksum, TStringBuf serializedRequest);

    const NProto::TWriteDataRequest* GetRequest() const
    {
        return Request.get();
    }

    ui64 GetHandle() const
    {
        return Request->GetHandle();
    }

    TStringBuf GetBuffer() const
    {
        return BufferRef;
    }

    ui64 Offset() const
    {
        return Request->GetOffset();
    }

    ui64 End() const
    {
        return Request->GetOffset() + BufferRef.size();
    }

    bool IsCached() const
    {
        return Status == EWriteDataEntryStatus::Cached ||
               Status == EWriteDataEntryStatus::FlushRequested;
    }

    bool IsFinished() const
    {
        return Status == EWriteDataEntryStatus::Flushed ||
               Status == EWriteDataEntryStatus::Corrupted;
    }

    bool IsFlushRequested() const
    {
        return Status == EWriteDataEntryStatus::FlushRequested;
    }

    size_t GetSerializedSize() const;

    void SerializeAndMoveRequestBuffer(
        char* allocationPtr,
        TPendingOperations& pendingOperations);

    void Finish(TPendingOperations& pendingOperations);

    bool RequestFlush();

    NThreading::TFuture<NProto::TWriteDataResponse> GetCachedFuture();
    NThreading::TFuture<void> GetFlushFuture();
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteBackCache::TWriteDataEntryPart
{
    const TWriteDataEntry* Source = nullptr;
    ui64 OffsetInSource = 0;
    ui64 Offset = 0;
    ui64 Length = 0;

    ui64 End() const
    {
        return Offset + Length;
    }

    bool operator==(const TWriteDataEntryPart& p) const
    {
        return std::tie(Source, OffsetInSource, Offset, Length) ==
            std::tie(p.Source, p.OffsetInSource, p.Offset, p.Length);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCache::TUtil
{
public:
    /**
     * Finds cached data from the sequence of write operations for the specified
     *   interval.
     *
     * @param entries The sequence of write operations. Write operations may
     *   overlap and are applied on top of each other.
     * @param startingFromOffset The starting offset of the interval to read.
     * @param length The length of the interval to read.
     * @return A sorted vector of non-overlapping intervals representing cached
     *   data.
     */
    static TVector<TWriteDataEntryPart> CalculateDataPartsToRead(
        const TDeque<TWriteDataEntry*>& entries,
        ui64 startingFromOffset,
        ui64 length);

    /**
    * Calculate a set of intervals that is complimentary to the input set of
    * intervals for the specified range.
    *
    * @param sortedParts A sorted vector of non-overlapping intervals.
    * @param startingFromOffset The starting offset of the interval to invert
    *   within.
    * @param length The length of the interval to invert within.
    * @return A vector of intervals representing the inverted (missing) parts
    *   within the specified range. Only fields Offset and Length are set.
    *
    * Note: The interval [startingFromOffset, startingFromOffset + length) may
    *   be narrower than the minimal bounding interval for sortedParts.
    *
    * Example:
    *   sortedParts: {{.Offset = 1, .Length = 3}, {.Offset = 5, .Length = 2}}
    *   startingFromOffset: 0
    *   length: 6
    *   result: {{.Offset = 0, .Length = 1}, {.Offset = 3, .Length = 2}}
    */
    static TVector<TWriteDataEntryPart> InvertDataParts(
        const TVector<TWriteDataEntryPart>& sortedParts,
        ui64 startingFromOffset,
        ui64 length);

    /**
    * Count the number of entries that can be taken for flushing.
    *
    * @param entries The sequence of client write requests.
    * @param maxWriteRequestSize The maximum size of a single consolidated
    *   WriteData request.
    * @param maxWriteRequestsCount The maximum number of consolidated WriteData
    *   requests.
    * @param maxSumWriteRequestsSize The maximum total size of all consolidated
    *   WriteData requests.
    *
    * @return The number of entries that can be taken from the beginning of
    *   the entries sequence for flushing.
    */
    static size_t CalculateEntriesCountToFlush(
        const TDeque<TWriteDataEntry*>& entries,
        ui32 maxWriteRequestSize,
        ui32 maxWriteRequestsCount,
        ui32 maxSumWriteRequestsSize);

    /**
     * Finds cached data from the sequence of write operations.
     *
     * @param entries The sequence of write operations. Write operations may
     *   overlap and are applied on top of each other.
     * @param count Take the first 'count' entries from the sequence.
     * @return A sorted vector of non-overlapping intervals representing cached
     *   data.
     */
    static TVector<TWriteDataEntryPart> CalculateDataPartsToFlush(
        const TDeque<TWriteDataEntry*>& entries,
        size_t entryCount);

    static bool IsSorted(const TVector<TWriteDataEntryPart>& parts);
    static bool IsContiguousSequence(const TVector<TWriteDataEntryPart>& parts);
};

}   // namespace NCloud::NFileStore::NFuse
