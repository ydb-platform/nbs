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
            LOG_DEBUG(
                ctx,
                TBlockStoreComponents::VOLUME,
                "%s WaitReady request delayed until volume and partitions are "
                "ready",
                LogTitle.GetWithTime().c_str());

            auto requestInfo = CreateRequestInfo<TEvVolume::TWaitReadyMethod>(
                ev->Sender,
                ev->Cookie,
                ev->Get()->CallContext);

            PendingRequests.emplace_back(NActors::IEventHandlePtr(ev.Release()), requestInfo);
            return;
        }
    }

    BLOCKSTORE_VOLUME_COUNTER(WaitReady);

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Received WaitReady request",
        LogTitle.GetWithTime().c_str());

    const auto& volumeConfig = State->GetMeta().GetVolumeConfig();

    auto response = std::make_unique<TEvVolume::TEvWaitReadyResponse>();
    auto& volume = *response->Record.MutableVolume();
    VolumeConfigToVolume(volumeConfig, State->GetPrincipalDiskId(), volume);

    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
