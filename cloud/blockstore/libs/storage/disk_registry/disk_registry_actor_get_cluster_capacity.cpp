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

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "Received GetClusterCapacity request");

    auto response =
        std::make_unique<TEvDiskRegistry::TEvGetClusterCapacityResponse>();

    auto sumBytesByPoolKind = [&](NProto::EDevicePoolKind poolKind)
    {
        ui64 freeBytes = 0;
        ui64 totalBytes = 0;

        for (const auto& poolName: State->GetPoolNames(poolKind)) {
            auto racks = State->GatherRacksInfo(poolName);
            for (const auto& rack: racks) {
                freeBytes += rack.FreeBytes;
                totalBytes += rack.TotalBytes;
            };
        }

        return std::make_pair(freeBytes, totalBytes);
    };

    auto [freeBytesSSD, totalBytesSSD] =
        sumBytesByPoolKind(NProto::DEVICE_POOL_KIND_DEFAULT);
    auto [freeBytesHDD, totalBytesHDD] =
        sumBytesByPoolKind(NProto::DEVICE_POOL_KIND_GLOBAL);

    const auto diskRegistryBasedSSDMediaKinds = {
        std::make_pair(NProto::STORAGE_MEDIA_SSD_NONREPLICATED, 1),
        std::make_pair(NProto::STORAGE_MEDIA_SSD_MIRROR2, 2),
        std::make_pair(NProto::STORAGE_MEDIA_SSD_MIRROR3, 3)};
    const auto diskRegistryBasedHDDMediaKinds = {
        std::make_pair(NProto::STORAGE_MEDIA_HDD_NONREPLICATED, 1),
    };

    for (const auto& [mediaKind, replicaCount]: diskRegistryBasedSSDMediaKinds)
    {
        NProto::TClusterCapacityInfo capacityInfo;
        capacityInfo.SetFreeBytes(freeBytesSSD / replicaCount);
        capacityInfo.SetTotalBytes(totalBytesSSD / replicaCount);
        capacityInfo.SetStorageMediaKind(mediaKind);
        *response->Record.AddCapacity() = std::move(capacityInfo);
    }

    for (const auto& [mediaKind, replicaCount]: diskRegistryBasedHDDMediaKinds)
    {
        NProto::TClusterCapacityInfo capacityInfo;
        capacityInfo.SetFreeBytes(freeBytesHDD / replicaCount);
        capacityInfo.SetTotalBytes(totalBytesHDD / replicaCount);
        capacityInfo.SetStorageMediaKind(mediaKind);
        *response->Record.AddCapacity() = std::move(capacityInfo);
    }

    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
