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

    BLOCKSTORE_TRACE_RECEIVED(ctx, &ev->TraceId, this, msg);

    ClientStatsAggregator->AddStats(clientId, metrics, ctx.Now());

    auto response = std::make_unique<TEvService::TEvUploadClientMetricsResponse>();

    BLOCKSTORE_TRACE_SENT(ctx, &ev->TraceId, this, response);
    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
