#include "disk_agent_actor.h"

#include <cloud/blockstore/libs/storage/core/request_info.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentActor::HandleWaitReady(
    const TEvDiskAgent::TEvWaitReadyRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (!State) {
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "WaitReady request delayed until DiskAgent is ready");

        auto requestInfo = CreateRequestInfo<TEvDiskAgent::TWaitReadyMethod>(
            ev->Sender,
            ev->Cookie,
            ev->Get()->CallContext);

        PendingRequests.emplace_back(
            NActors::IEventHandlePtr(ev.Release()),
            requestInfo);
        return;
    }

    BLOCKSTORE_DISK_AGENT_COUNTER(WaitReady);

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::DISK_AGENT,
        "Received WaitReady request");

    auto response = std::make_unique<TEvDiskAgent::TEvWaitReadyResponse>();

    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
