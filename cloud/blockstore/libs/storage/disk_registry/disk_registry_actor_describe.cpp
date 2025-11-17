#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleDescribeDisk(
    const TEvDiskRegistry::TEvDescribeDiskRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(DescribeDisk);

    const auto* msg = ev->Get();
    const TString& diskId = msg->Record.GetDiskId();

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Received DescribeDisk request: %s",
        LogTitle.GetWithTime().c_str(),
        msg->Record.ShortDebugString().c_str());

    if (!diskId) {
        auto response = std::make_unique<TEvDiskRegistry::TEvDescribeDiskResponse>(
            MakeError(E_ARGUMENT, "empty disk id"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    TDiskInfo diskInfo;

    auto error = State->GetDiskInfo(diskId, diskInfo);

    if (HasError(error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "%s GetDiskInfo %s error: %s",
            LogTitle.GetWithTime().c_str(),
            diskId.c_str(),
            FormatError(error).c_str());
    }

    auto response = std::make_unique<TEvDiskRegistry::TEvDescribeDiskResponse>(
        std::move(error)
    );

    response->Record.SetBlockSize(diskInfo.LogicalBlockSize);

    response->Record.SetState(diskInfo.State);
    response->Record.SetStateTs(diskInfo.StateTs.MicroSeconds());

    response->Record.SetCloudId(diskInfo.CloudId);
    response->Record.SetFolderId(diskInfo.FolderId);

    auto onDevice = [&] (NProto::TDeviceConfig& d, ui32 blockSize) {
        if (ToLogicalBlocks(d, blockSize)) {
            return;
        }

        TStringBuilder error;
        error << "HandleDescribeDisk: ToLogicalBlocks failed, device: "
            << d.GetDeviceUUID().Quote().c_str();
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "%s %s",
            LogTitle.GetWithTime().c_str(),
            error.c_str());
    };

    ui64 blockCount = 0;
    for (auto& device: diskInfo.Devices) {
        onDevice(device, diskInfo.LogicalBlockSize);

        blockCount += device.GetBlocksCount();

        *response->Record.AddDevices() = std::move(device);
    }
    response->Record.SetBlocksCount(blockCount);

    for (auto& migration: diskInfo.Migrations) {
        onDevice(
            *migration.MutableTargetDevice(),
            diskInfo.LogicalBlockSize
        );
        *response->Record.AddMigrations() = std::move(migration);
    }

    for (auto& replica: diskInfo.Replicas) {
        auto* r = response->Record.AddReplicas();
        for (auto& device: replica) {
            onDevice(device, diskInfo.LogicalBlockSize);
            *r->AddDevices() = std::move(device);
        }
    }

    for (auto& deviceId: diskInfo.DeviceReplacementIds) {
        *response->Record.AddDeviceReplacementUUIDs() = std::move(deviceId);
    }

    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
