#include "disk_agent_state.h"

#include "storage_initializer.h"

#include <cloud/blockstore/libs/nvme/nvme.h>
#include <cloud/blockstore/libs/service_local/broken_storage.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_generator.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_scanner.h>

#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/timer.h>

#include <util/generic/hash_set.h>
#include <util/string/printf.h>
#include <util/system/fstat.h>

#include <ranges>
#include <regex>

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

auto TDiskAgentState::CheckCanAttachDetachPath(
    ui64 diskRegistryGeneration,
    ui64 diskAgentGeneration,
    const TVector<TString>& paths,
    bool attach) -> TResultOrError<TCheckCanAttachDetachPathResult>
{
    if (auto error = CheckDiskRegistryGenerationAndUpdateItIfNeeded(
            diskRegistryGeneration);
        HasError(error))
    {
        return error;
    }

    THashSet<TString> memoryDevices;
    for (const auto& md: AgentConfig->GetMemoryDevices()) {
        memoryDevices.emplace(md.GetDeviceId());
    }

    TCheckCanAttachDetachPathResult result;

    auto desiredState =
        attach ? EPathAttachState::Attached : EPathAttachState::Detached;

    for (const auto& path: paths) {
        auto uuids = GetAllDeviceUUIDsForPath(path);
        for (const auto& uuid: uuids) {
            if (memoryDevices.contains(uuid)) {
                return MakeError(
                    E_PRECONDITION_FAILED,
                    "Not supported for memory devices");
            }
        }

        auto* p = PathAttachStates.FindPtr(path);
        if (!p || *p == desiredState) {
            result.AlreadyAttachedDetachedPaths.emplace_back(path);
            continue;
        }

        if (diskAgentGeneration <= DiskAgentGeneration) {
            return MakeError(
                E_ARGUMENT,
                Sprintf(
                    "outdated disk agent generation %lu vs %lu",
                    diskAgentGeneration,
                    DiskAgentGeneration));
        }

        result.PathsToAttachDetach.emplace_back(path);
    }

    return result;
}

auto TDiskAgentState::CheckCanDetachPath(
    ui64 diskRegistryGeneration,
    ui64 diskAgentGeneration,
    const TVector<TString>& paths)
    -> TResultOrError<TCheckCanAttachDetachPathResult>
{
    return CheckCanAttachDetachPath(
        diskRegistryGeneration,
        diskAgentGeneration,
        paths,
        false);   // attach
}

TFuture<NProto::TError> TDiskAgentState::DetachPath(
    ui64 diskRegistryGeneration,
    ui64 diskAgentGeneration,
    const TVector<TString>& paths)
{
    auto [result, error] =
        CheckCanDetachPath(diskRegistryGeneration, diskAgentGeneration, paths);
    if (HasError(error)) {
        return MakeFuture(error);
    }

    TVector<TStorageAdapterPtr> storageAdaptersToDrop;

    for (const auto& path: result.PathsToAttachDetach) {
        auto* pathAttachState = PathAttachStates.FindPtr(path);
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

    DiskAgentGeneration = Max(diskAgentGeneration, DiskAgentGeneration);

    auto promise = NThreading::NewPromise<NProto::TError>();
    auto future = promise.GetFuture();

    BackgroundThreadPool->ExecuteSimple(
        [Log = Logging->CreateLog("BLOCKSTORE_DISK_AGENT"),
         promise = std::move(promise),
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

            promise.SetValue({});
        });
    return future;
}

NProto::TError TDiskAgentState::CheckDiskRegistryGenerationAndUpdateItIfNeeded(
    ui64 diskRegistryGeneration)
{
    if (diskRegistryGeneration < LastDiskRegistryGenerationSeen) {
        return MakeError(E_ARGUMENT, "outdated disk registry generation");
    }

    if (diskRegistryGeneration > LastDiskRegistryGenerationSeen) {
        DiskAgentGeneration = 0;
    }

    LastDiskRegistryGenerationSeen = diskRegistryGeneration;

    return {};
}

}   // namespace NCloud::NBlockStore::NStorage
