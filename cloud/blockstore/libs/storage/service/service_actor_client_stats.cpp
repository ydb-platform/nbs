#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/stats_service.h>
#include <cloud/blockstore/libs/storage/api/undelivered.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TServiceActor::HandleUploadClientMetrics(
    const TEvService::TEvUploadClientMetricsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ForwardRequestWithNondeliveryTracking(
        ctx,
        MakeStorageStatsServiceId(),
        *ev);
}

}   // namespace NCloud::NBlockStore::NStorage
