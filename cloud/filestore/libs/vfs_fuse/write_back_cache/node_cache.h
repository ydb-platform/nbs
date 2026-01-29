#pragma once

#include "cached_write_data_request.h"

#include <cloud/storage/core/libs/common/disjoint_interval_map.h>

#include <util/generic/deque.h>
#include <util/generic/function_ref.h>
#include <util/generic/strbuf.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

class TNodeCache
{
public:
    using TEntryVisitor =
        TFunctionRef<bool(const TCachedWriteDataRequest* entry)>;

    using TCachedDataVisitor = TFunctionRef<bool(ui64 offset, TStringBuf data)>;

private:
    TDeque<std::unique_ptr<TCachedWriteDataRequest>> UnflushedRequests;
    TDeque<std::unique_ptr<TCachedWriteDataRequest>> FlushedRequests;
    TDisjointIntervalMap<ui64, TCachedWriteDataRequest*> IntervalMap;

public:
    void PushUnflushed(std::unique_ptr<TCachedWriteDataRequest> entry);
    TCachedWriteDataRequest* SetFrontFlushed();
    std::unique_ptr<TCachedWriteDataRequest> PopFlushed();

    bool Empty() const;

    bool HasUnflushedRequests() const;
    ui64 GetMinUnflushedSequenceId() const;

    bool HasFlushedRequests() const;
    ui64 GetMinFlushedSequenceId() const;

    void VisitUnflushedRequests(const TEntryVisitor& visitor) const;

    void VisitCachedData(
        ui64 offset,
        ui64 byteCount,
        const TCachedDataVisitor& visitor) const;
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
