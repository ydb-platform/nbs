#include "part_actor.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleWaitReady(
    const TEvPartition::TEvWaitReadyRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (CurrentState != STATE_WORK) {
        LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
            "[%lu] WaitReady request delayed until partition is ready",
            TabletID());

        auto requestInfo = CreateRequestInfo<TEvPartition::TWaitReadyMethod>(
            ev->Sender,
            ev->Cookie,
            ev->Get()->CallContext);

        PendingRequests.emplace_back(NActors::IEventHandlePtr(ev.Release()), requestInfo);
        return;
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu][d:%s] Received WaitReady request",
        TabletID(),
        PartitionConfig.GetDiskId().c_str());

    auto response = std::make_unique<TEvPartition::TEvWaitReadyResponse>();

    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
