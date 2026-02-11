#pragma once

#include "write_back_cache.h"
#include "write_back_cache_stats.h"

#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/storage/core/helpers.h>

#include <cloud/storage/core/libs/common/disjoint_interval_map.h>

#include <util/datetime/base.h>

#include <util/generic/intrlist.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>

#include <span>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

struct TWriteBackCache::TWriteDataEntryDeserializationStats
{
    ui64 EntryCount = 0;
    ui64 EntrySizeMismatchCount = 0;
    ui64 ProtobufDeserializationErrorCount = 0;

    bool HasFailed() const
    {
        return EntrySizeMismatchCount > 0 ||
               ProtobufDeserializationErrorCount > 0;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct Y_PACKED TWriteBackCache::TCachedWriteDataRequest
{
    ui64 NodeId = 0;
    ui64 Handle = 0;
    ui64 Offset = 0;

    // Data goes right after the header, |byteCount| bytes
    // The validity is ensured by code logic
    TStringBuf GetBuffer(ui64 byteCount) const
    {
        return {
            reinterpret_cast<const char*>(this) +
                sizeof(TCachedWriteDataRequest),
            byteCount};
    }
};

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCache::TWriteDataEntry
    : public TIntrusiveListItem<TWriteDataEntry>
    , public TIntrusiveListItem<TWriteDataEntry, TGlobalListTag>
    , public TIntrusiveListItem<TWriteDataEntry, TNodeListTag>
{
private:
    ui64 RequestId = 0;

    // Original write data request is stored until serialization into
    // persistent queue is performed.
    // The idea is to deduplicate memory and to reference request buffer
    // directly in the persistent buffer if the request is stored there.
    std::shared_ptr<NProto::TWriteDataRequest> PendingRequest;

    // WriteData request stored in CachedEntriesPersistentQueue
    const TCachedWriteDataRequest* CachedRequest = nullptr;

    // ByteCount is not serialized to the persistent queue as it is calculated
    // implicitly
    const ui64 ByteCount = 0;

    NThreading::TPromise<NProto::TWriteDataResponse> CachedPromise;

    NWriteBackCache::EWriteDataRequestStatus Status =
        NWriteBackCache::EWriteDataRequestStatus::Initial;

    TInstant StatusChangeTime = TInstant::Zero();

public:
    explicit TWriteDataEntry(
        std::shared_ptr<NProto::TWriteDataRequest> request);

    TWriteDataEntry(
        TStringBuf serializedRequest,
        TWriteDataEntryDeserializationStats& stats,
        TImpl* impl);

    ui64 GetRequestId() const
    {
        return RequestId;
    }

    void SetRequestId(ui64 requestId)
    {
        Y_ABORT_UNLESS(RequestId == 0);
        RequestId = requestId;
    }

    const void* GetAllocationPtr() const
    {
        return CachedRequest;
    }

    ui64 GetNodeId() const
    {
        if (CachedRequest) {
            return CachedRequest->NodeId;
        }
        if (PendingRequest) {
            return PendingRequest->GetNodeId();
        }
        Y_ABORT("The request is in the invalid state (GetNodeId)");
    }

    ui64 GetHandle() const
    {
        if (CachedRequest) {
            return CachedRequest->Handle;
        }
        if (PendingRequest) {
            return PendingRequest->GetHandle();
        }
        Y_ABORT("The request is in the invalid state (GetHandle)");
    }

    TStringBuf GetBuffer() const
    {
        Y_ABORT_UNLESS(
            CachedRequest != nullptr,
            "The buffer can be referenced only for cached requests");
        return CachedRequest->GetBuffer(ByteCount);
    }

    ui64 GetOffset() const
    {
        if (CachedRequest) {
            return CachedRequest->Offset;
        }
        if (PendingRequest) {
            return PendingRequest->GetOffset();
        }
        Y_ABORT("The request is in the invalid state (GetOffset)");
    }

    ui64 GetByteCount() const
    {
        return ByteCount;
    }

    ui64 GetEnd() const
    {
        return GetOffset() + ByteCount;
    }

    bool IsCorrupted() const
    {
        return Status == NWriteBackCache::EWriteDataRequestStatus::Corrupted;
    }

    bool IsFlushed() const
    {
        return Status == NWriteBackCache::EWriteDataRequestStatus::Flushed;
    }

    size_t GetSerializedSize() const;

    void SetPending(TImpl* impl);

    void Serialize(std::span<char> allocation) const;

    void MoveRequestBuffer(
        const void* allocationPtr,
        TQueuedOperations& pendingOperations,
        TImpl* impl);

    void SetFlushed(TImpl* impl);
    void Complete(TImpl* impl);

    NThreading::TFuture<NProto::TWriteDataResponse> GetCachedFuture();

private:
    void SetStatus(
        NWriteBackCache::EWriteDataRequestStatus status,
        TImpl* impl);
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteBackCache::TWriteDataEntryPart
{
    const TWriteDataEntry* Source = nullptr;
    ui64 OffsetInSource = 0;
    ui64 Offset = 0;
    ui64 Length = 0;

    ui64 GetEnd() const
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
     * @param map The interval map of cached write operations.
     * @param startingFromOffset The starting offset of the interval to read.
     * @param length The length of the interval to read.
     * @return A sorted vector of non-overlapping intervals representing cached
     *   data.
     */
    static TVector<TWriteDataEntryPart> CalculateDataPartsToRead(
        const TWriteDataEntryIntervalMap& map,
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

    static bool IsSorted(const TVector<TWriteDataEntryPart>& parts);
    static bool IsContiguousSequence(const TVector<TWriteDataEntryPart>& parts);

    static NProto::TError ValidateReadDataRequest(
        const NProto::TReadDataRequest& request,
        const TString& expectedFileSystemId);

    static NProto::TError ValidateWriteDataRequest(
        const NProto::TWriteDataRequest& request,
        const TString& expectedFileSystemId);
};

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCache::TWriteDataEntryIntervalMap
    : public TDisjointIntervalMap<ui64, TWriteDataEntry*>
{
private:
    using TBase = TDisjointIntervalMap<ui64, TWriteDataEntry*>;

public:
    void Add(TWriteDataEntry* entry);
    void Remove(TWriteDataEntry* entry);
};

}   // namespace NCloud::NFileStore::NFuse
