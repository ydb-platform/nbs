#pragma once

#include <cloud/blockstore/libs/storage/api/partition.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/model/requests_in_progress.h>
#include <cloud/storage/core/libs/actors/public.h>

namespace NCloud::NBlockStore::NStorage {

class TDrainActorCompanion
{
private:
    TVector<TRequestInfoPtr> DrainRequests;
    TRequestInfoPtr WaitForInFlightWritesRequest;
    IRequestsInProgress& RequestsInProgress;
    const TString LoggingId;

public:
    TDrainActorCompanion(
        IRequestsInProgress& requestsInProgress,
        TString loggingId);

    TDrainActorCompanion(
        IRequestsInProgress& requestsInProgress,
        ui64 tabletID);

    void HandleDrain(
        const NPartition::TEvPartition::TEvDrainRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWaitForInFlightWrites(
        const NPartition::TEvPartition::TEvWaitForInFlightWritesRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void ProcessDrainRequests(const NActors::TActorContext& ctx);

private:
    void DoProcessDrainRequests(const NActors::TActorContext& ctx);
    void DoProcessWaitForInFlightWritesRequests(const NActors::TActorContext& ctx);
};

}  // namespace NCloud::NBlockStore::NStorage
