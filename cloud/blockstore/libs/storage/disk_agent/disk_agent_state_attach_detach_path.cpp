#include "disk_agent_state.h"


#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/timer.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;

TVector<TString> TDiskAgentState::GetAllDeviceUUIDsForPath(const TString& path)
{
    TVector<TString> result;
    for (const auto& [uuid, device]: Devices) {
        if (device.Config.GetDeviceName() == path) {
            result.emplace_back(uuid);
        }
    }
    return result;
}

TFuture<void> TDiskAgentState::DetachPaths(
    const TVector<TString>& paths)
{
    TVector<TStorageAdapterPtr> storageAdaptersToDrop;

    for (const auto& path: paths) {
        auto* pathAttachState = PathAttachStates.FindPtr(path);
        if (!pathAttachState || *pathAttachState == EPathAttachState::Detached)
        {
            continue;
        }
        auto uuids = GetAllDeviceUUIDsForPath(path);

        for (const auto& uuid: uuids) {
            auto* d = Devices.FindPtr(uuid);

            storageAdaptersToDrop.emplace_back(std::move(d->StorageAdapter));
            d->StorageAdapter.reset();

            if (RdmaTarget) {
                RdmaTarget->DetachDevice(uuid);
            }
        }

        *pathAttachState = EPathAttachState::Detached;
    }

    return BackgroundThreadPool->Execute(
        [Log = Logging->CreateLog("BLOCKSTORE_DISK_AGENT"),
         storageAdaptersToDrop = std::move(storageAdaptersToDrop)]() mutable
        {
            auto timer = CreateWallClockTimer();

            for (const auto& storageAdapter: storageAdaptersToDrop) {
                auto requestsRemained = storageAdapter->Shutdown(timer);
                if (requestsRemained) {
                    STORAGE_WARN(
                        "remained " << requestsRemained
                                    << " requests in device after detach");
                }
            }

            storageAdaptersToDrop.clear();
        });
}

}   // namespace NCloud::NBlockStore::NStorage
