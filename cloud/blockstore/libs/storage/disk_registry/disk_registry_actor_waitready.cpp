#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleWaitReady(
    const TEvDiskRegistry::TEvWaitReadyRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (CurrentState != STATE_WORK &&
        CurrentState != STATE_READ_ONLY)
    {
        LOG_DEBUG(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] WaitReady request delayed until DiskRegistry is ready",
            TabletID());

        auto requestInfo = CreateRequestInfo<TEvDiskRegistry::TWaitReadyMethod>(
            ev->Sender,
            ev->Cookie,
            ev->Get()->CallContext);

        PendingRequests.emplace_back(NActors::IEventHandlePtr(ev.Release()), requestInfo);
        return;
    }

    BLOCKSTORE_DISK_REGISTRY_COUNTER(WaitReady);

    LOG_DEBUG(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received WaitReady request",
        TabletID());

    auto response = std::make_unique<TEvDiskRegistry::TEvWaitReadyResponse>();

    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
