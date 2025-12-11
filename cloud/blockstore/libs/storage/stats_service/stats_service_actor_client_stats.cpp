#include "stats_service_actor.h"

#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TStatsServiceActor::HandleUploadClientMetrics(
    const TEvService::TEvUploadClientMetricsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& clientId = GetClientId(*msg);
    const auto& metrics = msg->Record.GetMetrics();

    ClientStatsAggregator->AddStats(clientId, metrics, ctx.Now());

    auto response =
        std::make_unique<TEvService::TEvUploadClientMetricsResponse>();
    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
