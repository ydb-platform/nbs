#pragma once

#include <cloud/blockstore/libs/storage/api/partition.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/model/requests_in_progress.h>
#include <cloud/storage/core/libs/actors/public.h>

namespace NCloud::NBlockStore::NStorage {

class TDrainActorCompanion
{
    struct TDrainRangeInfo {
        TRequestInfoPtr ReqInfo;
        TBlockRange64 RangeToDrain;
    };
    TVector<TDrainRangeInfo> DrainRangeRequests;
    TVector<TRequestInfoPtr> DrainRequests;
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

    void AddDrainRangeRequest(
        const NActors::TActorContext& ctx,
        TRequestInfoPtr reqInfo,
        TBlockRange64 range);

    void ProcessDrainRequests(const NActors::TActorContext& ctx);

    void ProcessDrainRangeRequests(const NActors::TActorContext& ctx);
};

}  // namespace NCloud::NBlockStore::NStorage
