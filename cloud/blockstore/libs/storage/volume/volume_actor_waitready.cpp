#include "volume_actor.h"

#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleWaitReady(
    const TEvVolume::TEvWaitReadyRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (HasError(StorageAllocationResult)) {
        auto response = std::make_unique<TEvVolume::TEvWaitReadyResponse>(
            StorageAllocationResult);

        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    if (!State || !State->Ready()) {
        StartPartitionsIfNeeded(ctx);

        if (!State || !State->Ready()) {
            LOG_DEBUG(ctx, TBlockStoreComponents::VOLUME,
                "[%lu] WaitReady request delayed until volume and partitions are ready",
                TabletID());

            auto requestInfo = CreateRequestInfo<TEvVolume::TWaitReadyMethod>(
                ev->Sender,
                ev->Cookie,
                ev->Get()->CallContext,
                ev->TraceId.Clone());

            PendingRequests.emplace_back(NActors::IEventHandlePtr(ev.Release()), requestInfo);
            return;
        }
    }

    BLOCKSTORE_VOLUME_COUNTER(WaitReady);

    LOG_DEBUG(ctx, TBlockStoreComponents::VOLUME,
        "[%lu] Received WaitReady request",
        TabletID());

    const auto& volumeConfig = State->GetMeta().GetVolumeConfig();

    auto response = std::make_unique<TEvVolume::TEvWaitReadyResponse>();
    auto& volume = *response->Record.MutableVolume();
    VolumeConfigToVolume(volumeConfig, volume);

    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
