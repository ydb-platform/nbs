#pragma once

#include <cloud/blockstore/libs/storage/api/partition.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/model/request_bounds_tracker.h>
#include <cloud/storage/core/libs/actors/public.h>

namespace NCloud::NBlockStore::NStorage {

class TDrainActorCompanion
{
private:
    struct TDrainRangeInfo
    {
        TRequestInfoPtr RequestInfo;
        TBlockRange64 RangeToDrain;
    };
    TVector<TDrainRangeInfo> DrainRangeRequests;
    TVector<TRequestInfoPtr> DrainRequests;
    TRequestInfoPtr WaitForInFlightWritesRequest;
    IRequestsInProgress& RequestsInProgress;
    const TRequestBoundsTracker* RequestBoundsTracker;
    const TString LoggingId;

public:
    TDrainActorCompanion(
        IRequestsInProgress& requestsInProgress,
        TString loggingId,
        const TRequestBoundsTracker* requestBoundsTracker = nullptr);

    TDrainActorCompanion(
        IRequestsInProgress& requestsInProgress,
        ui64 tabletID,
        const TRequestBoundsTracker* requestBoundsTracker = nullptr);

    void HandleDrain(
        const NPartition::TEvPartition::TEvDrainRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWaitForInFlightWrites(
        const NPartition::TEvPartition::TEvWaitForInFlightWritesRequest::TPtr&
            ev,
        const NActors::TActorContext& ctx);

    void ProcessDrainRequests(const NActors::TActorContext& ctx);

    void AddDrainRangeRequest(
        const NActors::TActorContext& ctx,
        TRequestInfoPtr reqInfo,
        TBlockRange64 range);

    void RemoveDrainRangeRequest(
        const NActors::TActorContext& ctx,
        TBlockRange64 range);

private:
    void DoProcessDrainRequests(const NActors::TActorContext& ctx);
    void DoProcessWaitForInFlightWritesRequests(const NActors::TActorContext& ctx);
    void DoProcessDrainRangeRequests(const NActors::TActorContext& ctx);
};

}  // namespace NCloud::NBlockStore::NStorage
