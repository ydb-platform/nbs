#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/ring_buffer.h>

#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

constexpr size_t DEFAULT_TRACKING_DEPTH = 1024;

///////////////////////////////////////////////////////////////////////////////

enum class EOverlapStatus
{
    NotOverlapped,
    Partial,
    Complete,
    Unknown,
};

///////////////////////////////////////////////////////////////////////////////
// Translate EOverlapStatus to EWellKnownResultCodes.
// N.B. For multidevice requests any overlapping should be treated as
// E_REJECTED.
EWellKnownResultCodes OverlapStatusToResult(
    EOverlapStatus overlapStatus,
    bool isMultideviceRequest);

///////////////////////////////////////////////////////////////////////////////

// Tracks the overlapping of block ranges for the last executed requests. If
// the check shows that the request has a smaller identifier (from the past) and
// overwrites part of the data recorded by the request with a larger identifier,
// that request should be rejected.
class TRecentlyWrittenBlocks
{
    using TRequestRangeMap = TMap<ui64, TBlockRange64>;

private:
    const size_t TrackDepth;
    TRequestRangeMap OrderedById;
    TRingBuffer<TRequestRangeMap::iterator> OrderedByArrival;

public:
    explicit TRecentlyWrittenBlocks(size_t trackDepth = DEFAULT_TRACKING_DEPTH);

    [[nodiscard]] EOverlapStatus CheckRange(
        ui64 requestId,
        const TBlockRange64& range) const;

    void AddRange(
        ui64 requestId,
        const TBlockRange64& range,
        const TString& deviceUUID);
};

///////////////////////////////////////////////////////////////////////////////

class TInflightBlocks
{
private:
    TMap<ui64, TBlockRange64> OrderedById;

public:
    TInflightBlocks();

    [[nodiscard]] bool CheckOverlap(ui64 requestId, const TBlockRange64& range)
        const;

    void AddRange(ui64 requestId, const TBlockRange64& range);
    void Remove(ui64 requestId);
};

///////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
