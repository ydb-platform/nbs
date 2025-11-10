#include "disk_agent_state.h"

#include "storage_initializer.h"

#include <cloud/blockstore/libs/nvme/nvme.h>
#include <cloud/blockstore/libs/service_local/broken_storage.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_generator.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_scanner.h>

#include <cloud/storage/core/libs/common/task_queue.h>

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

TVector<NProto::TDeviceConfig> TDiskAgentState::GetAllDevicesForPaths(
    const THashSet<TString>& paths)
{
    TVector<NProto::TDeviceConfig> result;
    for (const auto& [uuid, device]: Devices) {
        if (paths.contains(device.Config.GetDeviceName())) {
            result.emplace_back(device.Config);
        }
    }
    return result;
}

auto TDiskAgentState::CheckCanAttachPath(
    ui64 diskRegistryGeneration,
    ui64 diskAgentGeneration,
    const TVector<TString>& paths) -> TResultOrError<TCheckCanAttachPathResult>
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

    TCheckCanAttachPathResult result;

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
        if (!p) {
            return MakeError(E_NOT_FOUND, "Path not found");
        }

        if (*p == EPathAttachState::Attached) {
            result.AlreadyAttachedPaths.emplace_back(path);
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

        result.PathsToAttach.emplace_back(path);
    }

    return result;
}

auto TDiskAgentState::AttachPath(
    ui32 diskRegistryGeneration,
    ui64 diskAgentGeneration,
    const TVector<TString>& pathsToAttach)
    -> TFuture<TResultOrError<TAttachPathResult>>
{
    auto [checkResult, error] = CheckCanAttachPath(
        diskRegistryGeneration,
        diskAgentGeneration,
        pathsToAttach);
    if (HasError(error)) {
        return MakeFuture(TResultOrError<TAttachPathResult>(std::move(error)));
    }

    TAttachPathResult result = {
        .PathsToAttach = std::move(checkResult.PathsToAttach),
        .AlreadyAttachedPaths = std::move(checkResult.AlreadyAttachedPaths)};

    if (!result.PathsToAttach) {
        return MakeFuture(TResultOrError<TAttachPathResult>(std::move(result)));
    }

    THashSet<TString> pathsSet(
        result.PathsToAttach.begin(),
        result.PathsToAttach.end());
    TVector<NProto::TDeviceConfig> devices = GetAllDevicesForPaths(pathsSet);

    TVector<TStorageIoStatsPtr> devicesStats;
    for (const auto& config: devices) {
        devicesStats.emplace_back(
            Devices.FindPtr(config.GetDeviceUUID())->Stats);
    }

    auto promise = NewPromise<TResultOrError<TAttachPathResult>>();

    auto future = promise.GetFuture();

    BackgroundThreadPool->ExecuteSimple(
        [promise = std::move(promise),
         result = std::move(result),
         devices = std::move(devices),
         agentConfig = AgentConfig,
         loggingService = Logging,
         nvmeManager = NvmeManager,
         storageProvider = StorageProvider,
         storageConfig = StorageConfig]() mutable
        {
            auto protoConfig = agentConfig->GetProtoConfig();
            protoConfig.ClearFileDevices();
            protoConfig.ClearMemoryDevices();
            protoConfig.ClearNvmeDevices();

            THashMap<TString, bool> hasLayout;
            for (const auto& path: result.PathsToAttach) {
                ui64 fileLength = 0;
                try {
                    fileLength = GetFileLengthWithSeek(path);
                } catch (const std::exception& e) {
                    promise.SetValue(MakeError(
                        E_INVALID_STATE,
                        Sprintf(
                            "Failed to get file[%s] size: %s",
                            path.Quote().c_str(),
                            e.what())));
                    return;
                }

                for (const auto& c:
                     agentConfig->GetStorageDiscoveryConfig().GetPathConfigs())
                {
                    std::regex regex(c.GetPathRegExp().c_str());
                    if (std::regex_match(path.c_str(), regex)) {
                        const auto* poolConfig = FindPoolConfig(c, fileLength);
                        hasLayout[path] =
                            poolConfig ? poolConfig->HasLayout() : false;
                        break;
                    }
                }
            }

            for (const auto& device: devices) {
                auto* fileDevice = protoConfig.AddFileDevices();
                fileDevice->SetPath(device.GetDeviceName());
                fileDevice->SetBlockSize(device.GetBlockSize());
                fileDevice->SetDeviceId(device.GetDeviceUUID());
                fileDevice->SetPoolName(device.GetPoolName());

                if (hasLayout[device.GetDeviceName()]) {
                    fileDevice->SetOffset(device.GetPhysicalOffset());
                    fileDevice->SetFileSize(
                        device.GetBlocksCount() * device.GetBlockSize());
                }
                fileDevice->SetSerialNumber(device.GetSerialNumber());
            }

            auto agentConfigForValidation = std::make_shared<TDiskAgentConfig>(
                protoConfig,
                agentConfig->GetRack(),
                agentConfig->GetNetworkMbitThroughput());

            auto future = InitializeStorage(
                loggingService->CreateLog("BLOCKSTORE_DISK_AGENT"),
                storageConfig,
                agentConfigForValidation,
                storageProvider,
                nvmeManager,
                result.PathsToAttach,
                /*isAttachOperation=*/true);

            future.Subscribe(
                [promise = std::move(promise), result = std::move(result)](
                    TFuture<TInitializeStorageResult> future) mutable
                {
                    auto initializationResult = future.ExtractValue();

                    if (initializationResult.ConfigMismatchErrors) {
                        auto error =
                            MakeError(E_INVALID_STATE, "Config mismatch");
                        promise.SetValue(std::move(error));
                        return;
                    }

                    result.Configs = std::move(initializationResult.Configs);
                    result.Stats = std::move(initializationResult.Stats);
                    result.Devices = std::move(initializationResult.Devices);
                    promise.SetValue(std::move(result));
                });
        });

    return future;
}

void TDiskAgentState::PathAttached(
    ui64 diskAgentGeneration,
    TVector<NProto::TDeviceConfig> configs,
    TVector<IStoragePtr> devices,
    TVector<TStorageIoStatsPtr> stats,
    const TVector<TString>& pathsToAttach)
{
    TDuration ioTimeout;
    if (!AgentConfig->GetDeviceIOTimeoutsDisabled()) {
        ioTimeout = AgentConfig->GetDeviceIOTimeout();
    }

    for (size_t i = 0; i < devices.size(); ++i) {
        auto& device = devices[i];
        auto& config = configs[i];
        auto& stat = stats[i];

        auto* d = Devices.FindPtr(config.GetDeviceUUID());
        if (!d) {
            continue;
        }

        d->Config = std::move(config);
        d->Stats = std::move(stat);
        auto storageAdapter = std::make_shared<TStorageAdapter>(
            std::move(device),
            d->Config.GetBlockSize(),
            false,   // normalize
            ioTimeout,
            AgentConfig->GetShutdownTimeout());

        d->StorageAdapter = storageAdapter;

        if (RdmaTarget) {
            RdmaTarget->AttachDevice(
                d->Config.GetDeviceUUID(),
                std::move(storageAdapter));
        }
    }

    for (const auto& path: pathsToAttach) {
        PathAttachStates[path] = EPathAttachState::Attached;
    }

    DiskAgentGeneration = Max(diskAgentGeneration, DiskAgentGeneration);
}

NProto::TError TDiskAgentState::DetachPath(
    ui64 diskRegistryGeneration,
    ui64 diskAgentGeneration,
    const TVector<TString>& paths)
{
    if (auto error = CheckDiskRegistryGenerationAndUpdateItIfNeeded(
            diskRegistryGeneration);
        HasError(error))
    {
        return error;
    }

    for (const auto& path: paths) {
        auto* pathAttachState = PathAttachStates.FindPtr(path);
        if (!pathAttachState) {
            continue;
        }

        if (*pathAttachState == EPathAttachState::Detached) {
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
    }

    for (const auto& path: paths) {
        auto* pathAttachState = PathAttachStates.FindPtr(path);
        if (!pathAttachState) {
            continue;
        }
        auto uuids = GetAllDeviceUUIDsForPath(path);

        if (*pathAttachState == EPathAttachState::Detached) {
            continue;
        }

        for (const auto& uuid: uuids) {
            auto* d = Devices.FindPtr(uuid);
            d->StorageAdapter.reset();

            if (RdmaTarget) {
                RdmaTarget->DetachDevice(uuid);
            }
        }

        *pathAttachState = EPathAttachState::Detached;
    }

    DiskAgentGeneration = Max(diskAgentGeneration, DiskAgentGeneration);

    return {};
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
