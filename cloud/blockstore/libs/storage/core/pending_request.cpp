#include "pending_request.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void SendPendingRequests(
    const NActors::TActorContext& ctx,
    TDeque<TPendingRequest>& pendingRequests)
{
    while (pendingRequests) {
        ctx.Send(pendingRequests.front().Event.release());
        pendingRequests.pop_front();
    }
}

void CancelPendingRequests(
    const NActors::TActorContext& ctx,
    TDeque<TPendingRequest>& pendingRequests)
{
    while (pendingRequests) {
        auto& request = pendingRequests.front();
        if (request.RequestInfo) {
            request.RequestInfo->CancelRequest(ctx);
        }
        pendingRequests.pop_front();
    }
}

}   // namespace NCloud::NBlockStore::NStorage
