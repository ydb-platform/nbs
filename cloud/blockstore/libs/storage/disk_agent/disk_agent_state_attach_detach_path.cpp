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

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;

namespace {

NProto::TError MakeDeviceChangedError(
    const TString& path,
    const TString& reason)
{
    return MakeError(
        E_INVALID_STATE,
        Sprintf("device %s changed: %s", path.c_str(), reason.c_str()));
}

NProto::TError CheckSerialNumber(
    const TVector<TString>& paths,
    const TVector<NProto::TDeviceConfig>& devices,
    const NNvme::INvmeManagerPtr& nvmeManager,
    const TLog& Log)
{
    THashMap<TString, TString> pathToSerialNumber;
    for (const auto& path: paths) {
        auto [serialNumber, error] = nvmeManager->GetSerialNumber(path);
        if (HasError(error)) {
            STORAGE_ERROR(
                "Failed to get serial number for device "
                << path.Quote() << ": " << error.GetMessage());
        }

        pathToSerialNumber[path] = serialNumber;
    }

    for (const auto& device: devices) {
        const auto& path = device.GetDeviceName();
        if (auto* serialNumber = pathToSerialNumber.FindPtr(path);
            serialNumber && *serialNumber &&
            device.GetSerialNumber() != *serialNumber)
        {
            return MakeDeviceChangedError(path, "serial number mismatch");
        }
    }

    return {};
}

TResultOrError<TVector<NProto::TFileDeviceArgs>> FillFileDeviceArgs(
    const TDiskAgentConfigPtr& agentConfig,
    const ILoggingServicePtr& loggingService)
{
    if (!agentConfig->GetFileDevices().empty()) {
        return TVector<NProto::TFileDeviceArgs>{
            agentConfig->GetFileDevices().begin(),
            agentConfig->GetFileDevices().end()};
    }

    TDeviceGenerator gen{
        loggingService->CreateLog("BLOCKSTORE_DISK_AGENT"),
        agentConfig->GetAgentId()};

    if (auto error = FindDevices(
            agentConfig->GetStorageDiscoveryConfig(),
            std::ref(gen));
        HasError(error))
    {
        return error;
    }

    return gen.ExtractResult();
}

NProto::TError CheckDevicesCountAndFileSizes(
    const TVector<TString>& paths,
    const TVector<NProto::TFileDeviceArgs>& files,
    const TVector<NProto::TDeviceConfig>& devices)
{
    THashMap<TString, i64> pathToFileSize;
    for (const auto& path: paths) {
        pathToFileSize[path] = GetFileLength(path);
    }

    THashMap<TString, NProto::TFileDeviceArgs> uuidToFileDeviceArgs;
    for (const auto& file: files) {
        if (!pathToFileSize.FindPtr(file.GetPath())) {
            continue;
        }
        uuidToFileDeviceArgs[file.GetDeviceId()] = file;
    }

    THashMap<TString, NProto::TDeviceConfig> uuidToDeviceConfigExpected;
    for (const auto& device: devices) {
        if (!pathToFileSize.FindPtr(device.GetDeviceName())) {
            continue;
        }
        uuidToDeviceConfigExpected[device.GetDeviceUUID()] = device;
    }

    if (uuidToDeviceConfigExpected.size() != uuidToFileDeviceArgs.size()) {
        MakeError(
            E_INVALID_STATE,
            Sprintf("device changed: device count mismatch"));
    }

    for (const auto& [uuid, device]: uuidToDeviceConfigExpected) {
        const auto& path = device.GetDeviceName();

        auto* fileDeviceArgs = uuidToFileDeviceArgs.FindPtr(uuid);
        if (!fileDeviceArgs) {
            return MakeDeviceChangedError(
                path,
                Sprintf("device with uuid %s not found", uuid.c_str()));
        }

        const ui32 blockSize = fileDeviceArgs->GetBlockSize();

        if (device.GetPhysicalOffset() != fileDeviceArgs->GetOffset()) {
            return MakeDeviceChangedError(path, "offset mismatch");
        }

        if (device.GetBlockSize() != blockSize) {
            return MakeDeviceChangedError(path, "block size mismatch");
        }

        auto actualLength = pathToFileSize[path];

        ui64 len = fileDeviceArgs->GetFileSize();
        if (!len && actualLength > 0) {
            len = actualLength;
        } else if (!len) {
            continue;
        }

        if (device.GetBlocksCount() != len / blockSize) {
            return MakeDeviceChangedError(path, "size mismatch");
        }

        if (actualLength <= 0) {
            continue;
        }

        if (fileDeviceArgs->GetOffset() && fileDeviceArgs->GetFileSize() &&
            (static_cast<ui64>(actualLength) <
             fileDeviceArgs->GetOffset() + fileDeviceArgs->GetFileSize()))
        {
            return MakeDeviceChangedError(path, "file size mismatch");
        }
    }

    return {};
}

NProto::TError CheckIsSamePath(
    const TVector<TString>& paths,
    const TVector<NProto::TDeviceConfig>& devices,
    const TDiskAgentConfigPtr& agentConfig,
    const ILoggingServicePtr& loggingService,
    const NNvme::INvmeManagerPtr& nvmeManager)
{
    auto [files, error] = FillFileDeviceArgs(agentConfig, loggingService);
    if (HasError(error)) {
        // device is broken, not config mismatch
        return error;
    }

    if (auto error = CheckDevicesCountAndFileSizes(paths, files, devices);
        HasError(error))
    {
        return error;
    }

    return CheckSerialNumber(
        paths,
        devices,
        nvmeManager,
        loggingService->CreateLog("BLOCKSTORE_DISK_AGENT"));
}

THashMap<TString, NThreading::TFuture<IStoragePtr>> AttachPathImpl(
    const TVector<NProto::TDeviceConfig>& devices,
    const TVector<TStorageIoStatsPtr>& stats,
    const TString& agentId,
    const IStorageProviderPtr& storageProvider)
{
    THashMap<TString, TFuture<IStoragePtr>> storages;

    for (size_t i = 0; i < devices.size(); ++i) {
        const auto& config = devices[i];
        const auto& deviceStats = stats[i];
        TFuture<IStoragePtr> storage;

        try {
            storage = CreateFileStorage(
                config.GetDeviceName(),
                config.GetPhysicalOffset() / config.GetBlockSize(),
                config,
                deviceStats,
                storageProvider,
                agentId);
        } catch (const std::exception& e) {
            storage = MakeErrorFuture<IStoragePtr>(std::current_exception());
        }

        storages[config.GetDeviceUUID()] = std::move(storage);
    }

    return storages;
}

}   // namespace

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
        .Devices = {},
        .PathsToAttach = std::move(checkResult.PathsToAttach),
        .AlreadyAttachedPaths = std::move(checkResult.AlreadyAttachedPaths)};

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
         devicesStats = std::move(devicesStats),
         agentConfig = AgentConfig,
         loggingService = Logging,
         nvmeManager = NvmeManager,
         storageProvider = StorageProvider]() mutable
        {
            auto error = CheckIsSamePath(
                result.PathsToAttach,
                devices,
                agentConfig,
                loggingService,
                nvmeManager);

            if (HasError(error)) {
                promise.SetValue(std::move(error));
                return;
            }

            auto storageFutures = AttachPathImpl(
                devices,
                devicesStats,
                agentConfig->GetAgentId(),
                storageProvider);

            TVector<TFuture<IStoragePtr>> futures;
            futures.reserve(storageFutures.size());
            for (auto& [_, future]: storageFutures) {
                futures.push_back(future);
            }

            WaitAll(futures).Subscribe(
                [promise = std::move(promise),
                 storageFutures = std::move(storageFutures),
                 result = std::move(result)](auto) mutable
                {
                    for (auto& [uuid, futureStorage]: storageFutures) {
                        result.Devices.insert(
                            {uuid, ResultOrError(futureStorage)});
                    }

                    promise.SetValue(std::move(result));
                });
        });

    return future;
}

void TDiskAgentState::PathAttached(
    ui64 diskAgentGeneration,
    THashMap<TString, TResultOrError<IStoragePtr>> devices,
    const TVector<TString>& pathsToAttach)
{
    for (auto& [uuid, errorOrDevice]: devices) {
        auto [storage, error] = std::move(errorOrDevice);
        auto* d = Devices.FindPtr(uuid);
        if (!d) {
            continue;
        }

        auto& config = d->Config;

        if (HasError(error)) {
            config.SetState(NProto::DEVICE_STATE_ERROR);
            config.SetStateMessage(std::move(*error.MutableMessage()));
            storage = CreateBrokenStorage();
            storage = CreateStorageWithIoStats(
                storage,
                d->Stats,
                d->Config.GetBlockSize());
        } else {
            config.SetState(NProto::DEVICE_STATE_ONLINE);
            config.ClearStateMessage();
        }

        TDuration ioTimeout;
        if (!AgentConfig->GetDeviceIOTimeoutsDisabled()) {
            ioTimeout = AgentConfig->GetDeviceIOTimeout();
        }

        auto storageAdapter = std::make_shared<TStorageAdapter>(
            std::move(storage),
            d->Config.GetBlockSize(),
            false,   // normalize
            ioTimeout,
            AgentConfig->GetShutdownTimeout());

        d->StorageAdapter = std::move(storageAdapter);

        if (RdmaTarget) {
            RdmaTarget->AttachDevice(uuid, std::move(storageAdapter));
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
