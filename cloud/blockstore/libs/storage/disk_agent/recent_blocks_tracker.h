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

constexpr size_t DEFAULT_TRACKING_DEPTH = 3072;

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
EWellKnownResultCodes OverlapStatusToResult(EOverlapStatus overlapStatus);

///////////////////////////////////////////////////////////////////////////////

// Tracks the overlapping of block ranges for the last executed requests. If
// the check shows that the request has a smaller identifier (from the past) and
// overwrites part of the data recorded by the request with a larger identifier,
// that request should be rejected.
// Additionally tracks inflight requests.
class TRecentBlocksTracker
{
    using TRequestRangeMap = TMap<ui64, TBlockRange64>;

private:
    const TString DeviceUUID;
    const size_t TrackDepth;
    TRequestRangeMap OrderedById;
    TRingBuffer<TRequestRangeMap::iterator> OrderedByArrival;
    TMap<ui64, TBlockRange64> InflightBlocks;

public:
    explicit TRecentBlocksTracker(
        const TString& deviceUUID,
        size_t trackDepth = DEFAULT_TRACKING_DEPTH);

    // Checks overlapping among the recently written blocks.
    [[nodiscard]] EOverlapStatus CheckRecorded(
        ui64 requestId,
        const TBlockRange64& range,
        TString* overlapDetails) const;

    // Store the request block range to recently written blocks.
    void AddRecorded(ui64 requestId, const TBlockRange64& range);

    // Check overlapping among the inflight requests.
    [[nodiscard]] bool CheckInflight(ui64 requestId, const TBlockRange64& range)
        const;

    // Has inflight request tracked.
    [[nodiscard]] bool HasInflight() const;

    // Add inflight request to track.
    void AddInflight(ui64 requestId, const TBlockRange64& range);

    // Remove inflight request from tracking.
    void RemoveInflight(ui64 requestId);

    [[nodiscard]] const TString& GetDeviceUUID() const;

    // Clears the internal state and is ready to accept requests from any
    // generation.
    void Reset();

private:
    void ReportRepeatedRequestId(ui64 requestId, const TBlockRange64& range)
        const;
};

}   // namespace NCloud::NBlockStore::NStorage
