#include "storage_initializer.h"

#include "hash_table_storage.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/nvme/nvme.h>
#include <cloud/blockstore/libs/service_local/broken_storage.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/service/storage_provider.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_generator.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_scanner.h>
#include <cloud/blockstore/public/api/protos/mount.pb.h>

#include <util/string/builder.h>
#include <util/string/printf.h>
#include <util/system/file.h>
#include <util/system/mutex.h>

#include <cerrno>
#include <cstring>
#include <functional>
#include <tuple>

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

ui64 GetFileLength(const TString& path)
{
    TFileHandle file(path,
          EOpenModeFlag::RdOnly
        | EOpenModeFlag::OpenExisting);

    if (!file.IsOpen()) {
        ythrow TServiceError(E_ARGUMENT)
            << "unable to open file " << path << " error: " << strerror(errno);
    }

    const ui64 size = file.Seek(0, sEnd);

    if (!size) {
        ythrow TServiceError(E_ARGUMENT)
            << "unable to retrive file size " << path;
    }

    return size;
}

void SetBlocksCount(
    const NProto::TFileDeviceArgs& device,
    NProto::TDeviceConfig& config)
{
    const auto& path = device.GetPath();
    const ui32 blockSize = device.GetBlockSize();

    ui64 len = device.GetFileSize();
    if (!len) {
        len = GetFileLength(path);
    }
    config.SetBlocksCount(len / blockSize);
}

void SetBlocksCount(
    const NProto::TMemoryDeviceArgs& device,
    NProto::TDeviceConfig& config)
{
    const ui64 blocksCount = device.GetBlocksCount();

    config.SetBlocksCount(blocksCount);
}

////////////////////////////////////////////////////////////////////////////////

class TInitializer
{
private:
    const TLog Log;
    const TDiskAgentConfigPtr AgentConfig;
    const IStorageProviderPtr StorageProvider;
    const NNvme::INvmeManagerPtr NvmeManager;

    TVector<NProto::TFileDeviceArgs> FileDevices;

    TVector<TStorageIoStatsPtr> Stats;
    TVector<NProto::TDeviceConfig> Configs;
    TVector<IStoragePtr> Devices;
    TDeviceGuard Guard;

    TVector<TString> Errors;
    TMutex Lock;

public:
    TInitializer(
        TLog log,
        TDiskAgentConfigPtr agentConfig,
        IStorageProviderPtr storageProvider,
        NNvme::INvmeManagerPtr nvmeManager);

    TFuture<void> Initialize();
    TInitializeStorageResult GetResult();

private:
    TFuture<IStoragePtr> CreateFileStorage(
        TString path,
        ui64 startIndex,
        const NProto::TDeviceConfig& config,
        TStorageIoStatsPtr stats);

    TFuture<IStoragePtr> CreateMemoryStorage(
        const NProto::TDeviceConfig& config,
        TStorageIoStatsPtr stats);

    void OnError(int i, const TString& error);

    NProto::TDeviceConfig CreateConfig(const NProto::TFileDeviceArgs& device);
    NProto::TDeviceConfig CreateConfig(const NProto::TMemoryDeviceArgs& device);

    void ScanFileDevices();
    bool CompareConfigs(const TVector<NProto::TFileDeviceArgs>& fileDevices);
    bool ValidateConfigs(const TVector<NProto::TFileDeviceArgs>& fileDevices);
    bool ValidateStorageDiscoveryConfig() const;
};

////////////////////////////////////////////////////////////////////////////////

TInitializer::TInitializer(
        TLog log,
        TDiskAgentConfigPtr agentConfig,
        IStorageProviderPtr storageProvider,
        NNvme::INvmeManagerPtr nvmeManager)
    : Log {std::move(log)}
    , AgentConfig(std::move(agentConfig))
    , StorageProvider(std::move(storageProvider))
    , NvmeManager(std::move(nvmeManager))
{
    auto fileDevices = AgentConfig->GetFileDevices();

    FileDevices.assign(
        std::make_move_iterator(fileDevices.begin()),
        std::make_move_iterator(fileDevices.end()));
}

TFuture<IStoragePtr> TInitializer::CreateFileStorage(
    TString path,
    ui64 startIndex,
    const NProto::TDeviceConfig& config,
    TStorageIoStatsPtr stats)
{
    const ui32 blockSize = config.GetBlockSize();

    NProto::TVolume volume;
    volume.SetDiskId(std::move(path));
    volume.SetBlockSize(blockSize);
    volume.SetStartIndex(startIndex);
    volume.SetBlocksCount(config.GetBlocksCount());

    auto storage = StorageProvider->CreateStorage(
        volume,
        AgentConfig->GetAgentId(),
        NProto::VOLUME_ACCESS_READ_WRITE);

    return storage.Apply([=] (const auto& future) mutable {
        return CreateStorageWithIoStats(
            future.GetValue(),
            std::move(stats),
            blockSize);
    });
}

TFuture<IStoragePtr> TInitializer::CreateMemoryStorage(
    const NProto::TDeviceConfig& config,
    TStorageIoStatsPtr stats)
{
    const ui32 blockSize = config.GetBlockSize();

    return MakeFuture(CreateStorageWithIoStats(
        CreateHashTableStorage(blockSize, config.GetBlocksCount()),
        std::move(stats),
        blockSize
    ));
}

void TInitializer::OnError(int i, const TString& error)
{
    const auto& memoryDevices = AgentConfig->GetMemoryDevices();

    if (i < std::ssize(FileDevices)) {
        with_lock (Lock) {
            Errors.push_back(TStringBuilder()
                << "FileDevice " << FileDevices[i].GetPath()
                << " initialization failed: " << error);
        }
    } else {
        i -= FileDevices.size();
        Y_ABORT_UNLESS(i < memoryDevices.size());

        with_lock (Lock) {
            Errors.push_back(TStringBuilder()
                << "MemoryDevice " << memoryDevices[i].GetName()
                << " initialization failed: " << error);
        }
    }
}

bool TInitializer::ValidateStorageDiscoveryConfig() const
{
    const auto& config = AgentConfig->GetStorageDiscoveryConfig();

    for (const auto& path: config.GetPathConfigs()) {
        for (const auto& pool: path.GetPoolConfigs()) {
            if (pool.HasLayout() && !pool.GetLayout().GetDeviceSize()) {
                STORAGE_WARN("Bad pool configuration: " << pool);

                return false;
            }
        }
    }

    return true;
}

bool TInitializer::ValidateConfigs(
    const TVector<NProto::TFileDeviceArgs>& fileDevices)
{
    auto it = AdjacentFindBy(fileDevices, [] (auto& file) {
        return file.GetDeviceId();
    });

    if (it != fileDevices.end()) {
        const auto& uuid = it->GetDeviceId();
        const auto& lhs = it->GetPath();

        it = std::next(it);

        const auto& rhs = it != fileDevices.end()
            ? it->GetPath()
            : "?";

        STORAGE_WARN("Two files '" << lhs << "' and '" << rhs
            << "' have the same uuid: " << uuid);

        return false;
    }

    return true;
}

bool TInitializer::CompareConfigs(
    const TVector<NProto::TFileDeviceArgs>& dynConfig)
{
    if (dynConfig.size() != FileDevices.size()) {
        STORAGE_WARN("Unexpected device count: %lu != %lu",
            FileDevices.size(), dynConfig.size());
        return false;
    }

    auto staticConfig = FileDevices;

    SortBy(staticConfig, [] (auto& file) {
        return file.GetDeviceId();
    });

    auto key = [] (const auto& file) {
        return std::make_tuple(
            file.GetPath(),
            file.GetPoolName(),
            file.GetBlockSize(),
            file.GetOffset(),
            file.GetFileSize());
    };

    for (size_t i = 0; i != staticConfig.size(); ++i) {
        if (staticConfig[i].GetDeviceId() != dynConfig[i].GetDeviceId()) {
            STORAGE_WARN("Unexpected dynamic config: " << dynConfig[i]);

            return false;
        }

        if (key(staticConfig[i]) != key(dynConfig[i])) {
            STORAGE_WARN("Dynamic config: "
                << dynConfig[i] << " doesn't match the static one: "
                << staticConfig[i]);

            return false;
        }
    }

    return true;
}

void TInitializer::ScanFileDevices()
{
    if (!ValidateStorageDiscoveryConfig()) {
        ReportDiskAgentConfigMismatch("Bad storage discovery config");
        return;
    }

    TDeviceGenerator gen { Log, AgentConfig->GetAgentId() };

    if (auto error = FindDevices(
            AgentConfig->GetStorageDiscoveryConfig(),
            std::ref(gen)); HasError(error))
    {
        ReportDiskAgentConfigMismatch(TStringBuilder()
            << "Can't generate config: " << FormatError(error));

        return;
    }

    TVector<NProto::TFileDeviceArgs> files = gen.ExtractResult();

    if (files.empty()) {
        return;
    }

    SortBy(files, [] (auto& file) {
        return file.GetDeviceId();
    });

    if (!ValidateConfigs(files)) {
        ReportDiskAgentConfigMismatch("Bad generated config");

        return;
    }

    if (FileDevices.empty()) {
        // We have only the dynamic configuration
        FileDevices = std::move(files);

        return;
    }

    // We have both the static config and the dynamic config and they must
    // be the same
    if (!CompareConfigs(files)) {
        TStringStream ss;
        ss << "Generated config doesn't match the static one. Static:\n";
        for (auto& d: FileDevices) {
            ss << d << "\n";
        }
        ss << "\nDynamic:\n";
        for (auto& d: files) {
            ss << d << "\n";
        }

        ReportDiskAgentConfigMismatch(ss.Str());
    }
}

TFuture<void> TInitializer::Initialize()
{
    ScanFileDevices();

    const auto& memoryDevices = AgentConfig->GetMemoryDevices();

    auto deviceCount = std::ssize(FileDevices) + memoryDevices.size();

    Configs.resize(deviceCount);
    Devices.resize(deviceCount);
    Stats.resize(deviceCount);

    TVector<TFuture<IStoragePtr>> futures;

    int i = 0;
    for (; i != std::ssize(FileDevices); ++i) {
        const auto& device = FileDevices[i];

        Configs[i] = CreateConfig(device);
        Stats[i] = std::make_shared<TStorageIoStats>();

        auto onInitError = [=] () {
            OnError(i, CurrentExceptionMessage());

            Configs[i].SetState(NProto::DEVICE_STATE_ERROR);
            Configs[i].SetStateMessage(CurrentExceptionMessage());
            Devices[i] = CreateBrokenStorage();
        };

        try {
            SetBlocksCount(device, Configs[i]);

            if (AgentConfig->GetDeviceLockingEnabled() &&
                !Guard.Lock(Configs[i].GetDeviceName()))
            {
                ythrow TServiceError(E_ARGUMENT)
                    << "unable to lock file "
                    << Configs[i].GetDeviceName();
            }

            auto result = CreateFileStorage(
                device.GetPath(),
                device.GetOffset() / device.GetBlockSize(),
                Configs[i],
                Stats[i]
            ).Subscribe([=] (const auto& future) {
                    try {
                        Devices[i] = future.GetValue();
                    } catch (...) {
                        onInitError();
                    }
                });

            futures.push_back(result);
        } catch (...) {
            if (!Configs[i].GetBlocksCount()) {
                // NBS-2475#60fee40bec7b260b922b8a9c
                Configs[i].SetBlocksCount(1);
            }

            onInitError();
        }
    }

    for (; i != deviceCount; ++i) {
        const auto& device = memoryDevices[i - std::ssize(FileDevices)];

        Configs[i] = CreateConfig(device);
        Stats[i] = std::make_shared<TStorageIoStats>();
        SetBlocksCount(device, Configs[i]);

        try {
            auto result = CreateMemoryStorage(Configs[i], Stats[i])
                .Subscribe([=] (const auto& future) {
                    try {
                        Devices[i] = future.GetValue();
                    } catch (...) {
                        OnError(i, CurrentExceptionMessage());
                    }
                });

            futures.push_back(result);
        } catch (...) {
            OnError(i, CurrentExceptionMessage());
        }
    }

    return WaitAll(futures);
}

NProto::TDeviceConfig TInitializer::CreateConfig(
    const NProto::TFileDeviceArgs& device)
{
    const auto& path = device.GetPath();
    const ui32 blockSize = device.GetBlockSize();

    NProto::TDeviceConfig config;

    config.SetDeviceName(path);
    config.SetDeviceUUID(device.GetDeviceId());
    config.SetBlockSize(blockSize);
    config.SetRack(AgentConfig->GetRack());
    config.SetPoolName(device.GetPoolName());
    config.SetSerialNumber(device.GetSerialNumber());
    config.SetPhysicalOffset(device.GetOffset());

    if (!config.GetSerialNumber()) {
        auto [sn, error] = NvmeManager->GetSerialNumber(path);
        if (!HasError(error)) {
            config.SetSerialNumber(sn);
        } else {
            with_lock (Lock) {
                Errors.push_back(TStringBuilder()
                    << "Can't get serial number for " << path.Quote() << ": "
                    << FormatError(error));
            }
        }
    }

    return config;
}

NProto::TDeviceConfig TInitializer::CreateConfig(
    const NProto::TMemoryDeviceArgs& device)
{
    const auto& name = device.GetName();
    const ui32 blockSize = device.GetBlockSize();
    const ui64 blocksCount = device.GetBlocksCount();

    NProto::TDeviceConfig config;

    config.SetDeviceName(name);
    config.SetDeviceUUID(device.GetDeviceId());
    config.SetBlockSize(blockSize);
    config.SetBlocksCount(blocksCount);
    config.SetRack(AgentConfig->GetRack());
    config.SetPoolName(device.GetPoolName());

    return config;
}

TInitializeStorageResult TInitializer::GetResult()
{
    TInitializeStorageResult r;

    r.Configs.reserve(Devices.size());
    r.Devices.reserve(Devices.size());
    r.Stats.reserve(Devices.size());

    for (size_t i = 0; i != Devices.size(); ++i) {
        Y_ABORT_UNLESS(Devices[i]);
        Y_ABORT_UNLESS(Stats[i]);

        r.Configs.push_back(std::move(Configs[i]));
        r.Devices.push_back(std::move(Devices[i]));
        r.Stats.push_back(std::move(Stats[i]));
    }

    r.Errors = std::move(Errors);
    r.Guard = std::move(Guard);

    return r;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TFuture<TInitializeStorageResult> InitializeStorage(
    TLog log,
    TDiskAgentConfigPtr agentConfig,
    IStorageProviderPtr storageProvider,
    NNvme::INvmeManagerPtr nvmeManager)
{
    auto initializer = std::make_shared<TInitializer>(
        std::move(log),
        std::move(agentConfig),
        std::move(storageProvider),
        std::move(nvmeManager));

    return initializer->Initialize().Apply([=] (const auto& future) {
        Y_UNUSED(future);

        return initializer->GetResult();
    });
}

}   // namespace NCloud::NBlockStore::NStorage
