#include "recent_blocks_tracker.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/storage/model/composite_id.h>
#include <cloud/storage/core/libs/common/verify.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/log.h>

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
    Y_ABORT();
    return E_REJECTED;
}

///////////////////////////////////////////////////////////////////////////////

TRecentBlocksTracker::TRecentBlocksTracker(
        const TString& deviceUUID,
        size_t trackDepth)
    : DeviceUUID(deviceUUID)
    , TrackDepth(trackDepth)
    , OrderedByArrival(TrackDepth, OrderedById.end())
{}

EOverlapStatus TRecentBlocksTracker::CheckRecorded(
    ui64 requestId,
    const TBlockRange64& range,
    TString* overlapDetails) const
{
    if (requestId == 0) {
        // Some unit tests do not set the request ID.
        return EOverlapStatus::NotOverlapped;
    }

    if (OrderedById.size() == TrackDepth) {
        const ui64 oldestRequestId = OrderedById.begin()->first;
        if (requestId < oldestRequestId) {
            *overlapDetails =
                TStringBuilder()
                << "[" << DeviceUUID << "] "
                << "The request is too old. Oldest tracked id="
                << TCompositeId::FromRaw(oldestRequestId).Print()
                << ", requestId=" << TCompositeId::FromRaw(requestId).Print();
            LOG_WARN(
                *NActors::TActivationContext::ActorSystem(),
                TBlockStoreComponents::DISK_AGENT,
                *overlapDetails);
            return EOverlapStatus::Unknown;
        }
    }

    auto it = OrderedById.lower_bound(requestId);
    if (it == OrderedById.end()) {
        return EOverlapStatus::NotOverlapped;
    }

    if (it->first == requestId) {
        ReportRepeatedRequestId(requestId, range);
        // Got same requestId. Reject it.
        *overlapDetails = TStringBuilder()
                          << "[" << DeviceUUID << "] The request with id="
                          << TCompositeId::FromRaw(requestId).Print()
                          << " repeated";
        LOG_WARN(
            *NActors::TActivationContext::ActorSystem(),
            TBlockStoreComponents::DISK_AGENT,
            *overlapDetails);
        return EOverlapStatus::Unknown;
    }

    bool foundIntersections = false;
    TDynBitMap bitmap;
    const ui64 rangeSize = range.Size();
    bitmap.Reserve(rangeSize);
    bitmap.Set(0, rangeSize);
    auto lastOverlapped = OrderedById.end();
    for (; it != OrderedById.end(); ++it) {
        if (!range.Overlaps(it->second)) {
            continue;
        }
        foundIntersections = true;
        lastOverlapped = it;
        TBlockRange64 other = range.Intersect(it->second);
        bitmap.Reset(other.Start - range.Start, other.End - range.Start + 1);
    }
    if (bitmap.FirstNonZeroBit() >= rangeSize) {
        *overlapDetails = TStringBuilder()
                         << "[" << DeviceUUID << "] Complete overlapping "
                         << TCompositeId::FromRaw(requestId).Print() << " "
                         << DescribeRange(range) << " with "
                         << TCompositeId::FromRaw(lastOverlapped->first).Print()
                         << " " << DescribeRange(lastOverlapped->second);
        LOG_WARN(
            *NActors::TActivationContext::ActorSystem(),
            TBlockStoreComponents::DISK_AGENT,
            *overlapDetails);
        return EOverlapStatus::Complete;
    }

    if (foundIntersections) {
        *overlapDetails = TStringBuilder()
                          << "[" << DeviceUUID << "] Partial overlapping "
                          << TCompositeId::FromRaw(requestId).Print() << " "
                          << DescribeRange(range) << " with "
                          << TCompositeId::FromRaw(lastOverlapped->first).Print()
                          << " " << DescribeRange(lastOverlapped->second);

        LOG_WARN(
            *NActors::TActivationContext::ActorSystem(),
            TBlockStoreComponents::DISK_AGENT,
            *overlapDetails);

        return EOverlapStatus::Partial;
    }
    return EOverlapStatus::NotOverlapped;
}

void TRecentBlocksTracker::AddRecorded(
    ui64 requestId,
    const TBlockRange64& range)
{
    if (requestId == 0) {
        // Some unit tests do not set the request ID.
        return;
    }

    auto [it, inserted] = OrderedById.emplace(requestId, range);
    if (!inserted) {
        ReportRepeatedRequestId(requestId, range);
        return;
    }

    auto removedOldIterator = OrderedByArrival.PushBack(it);
    if (removedOldIterator) {
        OrderedById.erase(*removedOldIterator);
    }
}

bool TRecentBlocksTracker::CheckInflight(
    ui64 requestId,
    const TBlockRange64& range) const
{
    if (requestId == 0) {
        // Some unit tests do not set the request ID.
        return false;
    }

    return std::any_of(
        InflightBlocks.lower_bound(requestId),
        InflightBlocks.end(),
        [&](const auto& p)
        {
            return p.first == requestId   // Reject requests with same id
                   || p.second.Overlaps(range);
        });
}

bool TRecentBlocksTracker::HasInflight() const
{
    return !InflightBlocks.empty();
}

void TRecentBlocksTracker::AddInflight(
    ui64 requestId,
    const TBlockRange64& range)
{
    if (requestId == 0) {
        // Some unit tests do not set the request ID.
        return;
    }

    auto [it, inserted] = InflightBlocks.emplace(requestId, range);
    if (!inserted) {
        ReportRepeatedRequestId(requestId, range);
    }
}

void TRecentBlocksTracker::RemoveInflight(ui64 requestId)
{
    if (requestId == 0) {
        // Some unit tests do not set the request ID.
        return;
    }

    InflightBlocks.erase(requestId);
}

const TString& TRecentBlocksTracker::GetDeviceUUID() const {
    return DeviceUUID;
}

void TRecentBlocksTracker::Reset()
{
    OrderedById.clear();
    OrderedByArrival.Clear();
    InflightBlocks.clear();
}

void TRecentBlocksTracker::ReportRepeatedRequestId(
    ui64 requestId,
    const TBlockRange64& range) const
{
    ReportUnexpectedIdentifierRepetition();

    if (NActors::TlsActivationContext &&
        NActors::TActivationContext::ActorSystem())
    {
        LOG_ERROR(
            *NActors::TActivationContext::ActorSystem(),
            TBlockStoreComponents::DISK_AGENT,
            "[%s] Duplicate saved requestId: %ld block range [%ld, %ld]",
            DeviceUUID.c_str(),
            TCompositeId::FromRaw(requestId).Print().c_str(),
            range.Start,
            range.End);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
