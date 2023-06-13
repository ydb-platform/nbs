#include "recently_written_blocks.h"

#include <cloud/storage/core/libs/common/verify.h>

#include <util/generic/bitmap.h>
#include <util/generic/list.h>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

EWellKnownResultCodes OverlapStatusToResult(
    EOverlapStatus overlapStatus,
    bool isMultideviceRequest)
{
    switch (overlapStatus) {
        case EOverlapStatus::NotOverlapped:
            return S_OK;
        case EOverlapStatus::Partial:
            return E_REJECTED;
        case EOverlapStatus::Complete:
            return isMultideviceRequest ? E_REJECTED : S_ALREADY;
        case EOverlapStatus::Unknown:
            return E_REJECTED;
            break;
    }
    Y_FAIL();
    return E_REJECTED;
}

///////////////////////////////////////////////////////////////////////////////

TRecentlyWrittenBlocks::TRecentlyWrittenBlocks(size_t trackDepth)
    : TrackDepth(trackDepth)
    , OrderedByArrival(TrackDepth, OrderedById.end())
{}

EOverlapStatus TRecentlyWrittenBlocks::CheckRange(
    ui64 requestId,
    const TBlockRange64& range) const
{
    if (requestId == 0) {
        // Some unit tests do not set the request ID.
        return EOverlapStatus::NotOverlapped;
    }

    if (OrderedById.size() == TrackDepth) {
        const ui64 oldestRequestId = OrderedById.begin()->first;
        if (requestId < oldestRequestId) {
            return EOverlapStatus::Unknown;
        }
    }

    auto it = OrderedById.lower_bound(requestId);
    if (it == OrderedById.end()) {
        return EOverlapStatus::NotOverlapped;
    }

    if (it->first == requestId) {
        // Got same requestId. Reject it.
        return EOverlapStatus::Unknown;
    }

    bool foundIntersections = false;
    TDynBitMap bitmap;
    const ui64 rangeSize = range.Size();
    bitmap.Reserve(rangeSize);
    bitmap.Set(0, rangeSize);
    for (; it != OrderedById.end(); ++it) {
        if (!range.Overlaps(it->second)) {
            continue;
        }
        foundIntersections = true;
        TBlockRange64 other = range.Intersect(it->second);
        bitmap.Reset(other.Start - range.Start, other.End - range.Start + 1);
    }
    if (bitmap.FirstNonZeroBit() >= rangeSize) {
        return EOverlapStatus::Complete;
    }

    if (foundIntersections) {
        return EOverlapStatus::Partial;
    }
    return EOverlapStatus::NotOverlapped;
}

void TRecentlyWrittenBlocks::AddRange(
    ui64 requestId,
    const TBlockRange64& range,
    const TString& deviceUUID)
{
    if (requestId == 0) {
        // Some unit tests do not set the request ID.
        return;
    }

    auto [it, inserted] = OrderedById.emplace(requestId, range);
    STORAGE_VERIFY(inserted, TWellKnownEntityTypes::DEVICE, deviceUUID);

    auto removedOldIterator = OrderedByArrival.PushBack(it);
    if (removedOldIterator) {
        OrderedById.erase(*removedOldIterator);
    }
}

///////////////////////////////////////////////////////////////////////////////

TInflightBlocks::TInflightBlocks() = default;

bool TInflightBlocks::CheckOverlap(ui64 requestId, const TBlockRange64& range)
    const
{
    if (requestId == 0) {
        // Some unit tests do not set the request ID.
        return false;
    }

    return std::any_of(
        OrderedById.lower_bound(requestId),
        OrderedById.end(),
        [&](const auto& p) {
            return p.first == requestId   // Reject requests with same id
                   || p.second.Overlaps(range);
        });
}

void TInflightBlocks::AddRange(ui64 requestId, const TBlockRange64& range)
{
    if (requestId == 0) {
        // Some unit tests do not set the request ID.
        return;
    }

    auto [it, inserted] = OrderedById.emplace(requestId, range);
    Y_VERIFY(inserted);
}

void TInflightBlocks::Remove(ui64 requestId)
{
    if (requestId == 0) {
        // Some unit tests do not set the request ID.
        return;
    }

    OrderedById.erase(requestId);
}

}   // namespace NCloud::NBlockStore::NStorage
