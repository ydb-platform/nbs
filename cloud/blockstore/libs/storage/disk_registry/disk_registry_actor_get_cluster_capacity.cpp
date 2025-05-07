#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleGetClusterCapacity(
    const TEvDiskRegistry::TEvGetClusterCapacityRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(GetClusterCapacity);

    LOG_DEBUG(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "Received GetClusterCapacity request");

    auto response = std::make_unique<TEvDiskRegistry::TEvGetClusterCapacityResponse>();

    ui64 freeBytesSSD = 0;
    ui64 totalBytesSSD = 0;
    ui64 freeBytesHDD = 0;
    ui64 totalBytesHDD = 0;
    for (const auto& poolName: State->GetPoolNames(NProto::DEVICE_POOL_KIND_DEFAULT)) {
        auto racks = State->GatherRacksInfo(poolName);

        for (const auto& rack: racks) {
            freeBytesSSD += rack.FreeBytes;
            totalBytesSSD += rack.TotalBytes;
        }
    }

    for (const auto& poolName: State->GetPoolNames(NProto::DEVICE_POOL_KIND_GLOBAL)) {
        auto racks = State->GatherRacksInfo(poolName);

        for (const auto& rack: racks) {
            freeBytesHDD += rack.FreeBytes;
            totalBytesHDD += rack.TotalBytes;
        }
    }

    const auto drBasedSSDMediaKinds = {
        NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
        NProto::STORAGE_MEDIA_SSD_MIRROR2,
        NProto::STORAGE_MEDIA_SSD_MIRROR3
    };
    const auto drBasedHDDMediaKinds = {
        NProto::STORAGE_MEDIA_HDD_NONREPLICATED,
    };

    for (const auto mediaKind: drBasedSSDMediaKinds) {
        NProto::TClusterCapacityInfo capacityInfo;
        capacityInfo.SetFree(freeBytesSSD);
        capacityInfo.SetTotal(totalBytesSSD);
        capacityInfo.SetKind(mediaKind);
        *response->Record.AddCapacity() = std::move(capacityInfo);
    }

    for (const auto mediaKind: drBasedHDDMediaKinds) {
        NProto::TClusterCapacityInfo capacityInfo;
        capacityInfo.SetFree(freeBytesHDD);
        capacityInfo.SetTotal(totalBytesHDD);
        capacityInfo.SetKind(mediaKind);
        *response->Record.AddCapacity() = std::move(capacityInfo);
    }

    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
