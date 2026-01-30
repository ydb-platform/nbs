#pragma once

#include "write_data_request.h"

#include <cloud/storage/core/libs/common/disjoint_interval_map.h>

#include <util/generic/deque.h>
#include <util/generic/function_ref.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct TCachedDataPart
{
    // Offset in ReadData response buffer - this is relative to the requested
    // |offset| in TNodeCache::GetCachedData
    ui64 RelativeOffset = 0;
    TStringBuf Data;
};

////////////////////////////////////////////////////////////////////////////////

struct TCachedData
{
    TVector<TCachedDataPart> Parts;
    // The expected size of ReadData response. If the ReadData response has less
    // length than this value, it should be extended up to this value.
    // This is needed to avoid truncation when there are unflushed data parts
    // beyond the requested range.
    ui64 ReadDataByteCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

// The class is not thread-safe
class TNodeCache
{
public:
    using TEntryVisitor =
        TFunctionRef<bool(const TCachedWriteDataRequest* entry)>;

private:
    TDeque<std::unique_ptr<TPendingWriteDataRequest>> PendingRequests;
    TDeque<std::unique_ptr<TCachedWriteDataRequest>> UnflushedRequests;
    TDeque<std::unique_ptr<TCachedWriteDataRequest>> FlushedRequests;
    TDisjointIntervalMap<ui64, TCachedWriteDataRequest*> CachedData;

public:
    void EnqueuePendingRequest(
        std::unique_ptr<TPendingWriteDataRequest> request);

    std::unique_ptr<TPendingWriteDataRequest> DequeuePendingRequest();

    void EnqueueUnflushedRequest(
        std::unique_ptr<TCachedWriteDataRequest> request);

    TCachedWriteDataRequest* MoveFrontUnflushedRequestToFlushed();

    std::unique_ptr<TCachedWriteDataRequest> DequeueFlushedRequest();

    bool Empty() const;

    bool HasPendingRequests() const;

    bool HasUnflushedRequests() const;
    ui64 GetMinUnflushedSequenceId() const;
    ui64 GetMaxUnflushedSequenceId() const;

    bool HasPendingOrUnflushedRequests() const;
    ui64 GetMinPendingOrUnflushedSequenceId(ui64 defValue) const;
    ui64 GetMaxPendingOrUnflushedSequenceId() const;

    bool HasFlushedRequests() const;
    ui64 GetMinFlushedSequenceId() const;

    void VisitUnflushedRequests(const TEntryVisitor& visitor) const;

    TCachedData GetCachedData(ui64 offset, ui64 byteCount) const;
    ui64 GetCachedDataEndOffset() const;
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
