#include "storage_initializer.h"

#include "hash_table_storage.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/proto_helpers.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/nvme/nvme.h>
#include <cloud/blockstore/libs/service_local/broken_storage.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/service/storage_provider.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/compare_configs.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_generator.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_scanner.h>
#include <cloud/blockstore/public/api/protos/mount.pb.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/string/builder.h>
#include <util/string/printf.h>
#include <util/system/file.h>
#include <util/system/fs.h>
#include <util/system/mutex.h>

#include <cstring>
#include <tuple>

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString& GetDeviceId(const NProto::TFileDeviceArgs& file)
{
    return file.GetDeviceId();
}

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

TVector<TString> GetLostDevicesIds(
    const TVector<NProto::TFileDeviceArgs>& expectedConfig,
    const TVector<NProto::TFileDeviceArgs>& currentConfig)
{
    TVector<NProto::TFileDeviceArgs> tmp;
    std::set_difference(
        expectedConfig.begin(),
        expectedConfig.end(),
        currentConfig.begin(),
        currentConfig.end(),
        std::back_inserter(tmp),
        [](const auto& lhs, const auto& rhs)
        { return lhs.GetDeviceId() < rhs.GetDeviceId(); });

    TVector<TString> result;
    result.reserve(tmp.size());
    for (auto& device: tmp) {
        result.push_back(device.GetDeviceId());
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TInitializer
{
private:
    const TLog Log;
    const TStorageConfigPtr StorageConfig;
    const TDiskAgentConfigPtr AgentConfig;
    const IStorageProviderPtr StorageProvider;
    const NNvme::INvmeManagerPtr NvmeManager;

    TVector<NProto::TFileDeviceArgs> FileDevices;

    TVector<TStorageIoStatsPtr> Stats;
    TVector<NProto::TDeviceConfig> Configs;
    TVector<IStoragePtr> Devices;
    TDeviceGuard Guard;

    TVector<TString> Errors;
    TVector<TString> ConfigMismatchErrors;
    TVector<TString> DevicesWithSuspendedIO;
    TVector<TString> LostDevicesIds;
    TMutex Lock;

    THashMap<TString, TString> PathToSerial;

public:
    TInitializer(
        TLog log,
        TStorageConfigPtr storageConfig,
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
    bool ValidateGeneratedConfigs(const TVector<NProto::TFileDeviceArgs>& fileDevices);
    bool ValidateStorageDiscoveryConfig() const;
    void ValidateCurrentConfigs(
        const TVector<NProto::TFileDeviceArgs>& cachedDevices);

    void SaveCurrentConfig();

    void ReportDiskAgentConfigMismatchEvent(const TString& error);

    [[nodiscard]] TString GetCachedConfigsPath() const;

    TString GetSerialNumber(const TString& path);
    void SetupSerialNumbers(TVector<NProto::TFileDeviceArgs>& fileDevices);

    NProto::TError ProcessConfigCache();
};

////////////////////////////////////////////////////////////////////////////////

TInitializer::TInitializer(
        TLog log,
        TStorageConfigPtr storageConfig,
        TDiskAgentConfigPtr agentConfig,
        IStorageProviderPtr storageProvider,
        NNvme::INvmeManagerPtr nvmeManager)
    : Log {std::move(log)}
    , StorageConfig(std::move(storageConfig))
    , AgentConfig(std::move(agentConfig))
    , StorageProvider(std::move(storageProvider))
    , NvmeManager(std::move(nvmeManager))
{
    for (const auto& m: AgentConfig->GetPathToSerialNumberMapping()) {
        PathToSerial.emplace(m.GetPath(), m.GetSerialNumber());
    }

    auto fileDevices = AgentConfig->GetFileDevices();

    FileDevices.assign(
        std::make_move_iterator(fileDevices.begin()),
        std::make_move_iterator(fileDevices.end()));

    SortBy(FileDevices, GetDeviceId);
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
            if (pool.HasLayout()) {
                const auto& layout = pool.GetLayout();

                if (!layout.GetDeviceSize()) {
                    STORAGE_WARN(
                        "Bad pool configuration: the device size is not set. "
                        "Config: " << pool);

                    return false;
                }

                if (pool.GetMinSize() && pool.GetMinSize() <
                    layout.GetHeaderSize() + layout.GetDeviceSize())
                {
                    STORAGE_WARN(
                        "Bad pool configuration: the min size is less than the "
                        "minimum layout. "
                        "Config: " << pool);

                    return false;
                }
            }
        }
    }

    return true;
}

bool TInitializer::ValidateGeneratedConfigs(
    const TVector<NProto::TFileDeviceArgs>& fileDevices)
{
    auto it = AdjacentFindBy(fileDevices, GetDeviceId);

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

TString TInitializer::GetSerialNumber(const TString& path)
{
    auto it = PathToSerial.find(path);
    if (it == PathToSerial.end()) {
        auto [sn, error] = NvmeManager->GetSerialNumber(path);
        it = PathToSerial.emplace(path, sn).first;
        if (HasError(error)) {
            with_lock (Lock) {
                Errors.push_back(
                    TStringBuilder()
                    << "Can't get serial number for " << path.Quote() << ": "
                    << FormatError(error));
            }
        }
    }

    return it->second;
}

void TInitializer::SetupSerialNumbers(
    TVector<NProto::TFileDeviceArgs>& fileDevices)
{
    for (auto& file: fileDevices) {
        file.SetSerialNumber(GetSerialNumber(file.GetPath()));
    }
}

void TInitializer::ScanFileDevices()
{
    if (!ValidateStorageDiscoveryConfig()) {
        ReportDiskAgentConfigMismatchEvent("Bad storage discovery config");
        return;
    }

    TDeviceGenerator gen { Log, AgentConfig->GetAgentId() };

    if (auto error = FindDevices(
            AgentConfig->GetStorageDiscoveryConfig(),
            std::ref(gen)); HasError(error))
    {
        ReportDiskAgentConfigMismatchEvent(TStringBuilder()
            << "Can't generate config: " << FormatError(error));
        return;
    }

    TVector<NProto::TFileDeviceArgs> files = gen.ExtractResult();
    if (files.empty()) {
        return;
    }

    SortBy(files, GetDeviceId);
    SetupSerialNumbers(files);

    if (!ValidateGeneratedConfigs(files)) {
        ReportDiskAgentConfigMismatchEvent("Bad generated config");

        return;
    }

    if (FileDevices.empty()) {
        // We have only the dynamic configuration
        FileDevices = std::move(files);

        return;
    }

    // We have both the static config and the dynamic config and they must
    // be the same
    if (auto error = CompareConfigs(FileDevices, files); HasError(error)) {
        TStringStream ss;
        ss << "Generated config doesn't match the static one:"
            << FormatError(error) << ". Static:\n";
        for (auto& d: FileDevices) {
            ss << d << "\n";
        }
        ss << "\nDynamic:\n";
        for (auto& d: files) {
            ss << d << "\n";
        }

        ReportDiskAgentConfigMismatchEvent(ss.Str());
    }
}

void TInitializer::SaveCurrentConfig()
{
    const auto path = GetCachedConfigsPath();
    if (path.empty()) {
        return;
    }

    STORAGE_INFO("Store the current config to " << path);

    NProto::TDiskAgentConfig proto;
    proto.MutableFileDevices()->Assign(
        FileDevices.cbegin(),
        FileDevices.cend());

    if (AgentConfig->GetDisableBrokenDevices()) {
        proto.MutableDevicesWithSuspendedIO()->Assign(
            DevicesWithSuspendedIO.cbegin(),
            DevicesWithSuspendedIO.cend());
    }

    auto error = SaveDiskAgentConfig(path, proto);
    if (HasError(error)) {
        Errors.push_back(TStringBuilder()
            << "can't save the current config: " << FormatError(error));
    }
}

TString TInitializer::GetCachedConfigsPath() const
{
    const TString storagePath = StorageConfig->GetCachedDiskAgentConfigPath();
    const TString diskAgentPath = AgentConfig->GetCachedConfigPath();
    return diskAgentPath.empty() ? storagePath : diskAgentPath;
}

void TInitializer::ValidateCurrentConfigs(
    const TVector<NProto::TFileDeviceArgs>& cachedDevices)
{
    if (cachedDevices.empty()) {
        STORAGE_INFO("There is no cached config");
        SaveCurrentConfig();
        return;
    }

    STORAGE_INFO("Compare the current config with the cached one");
    const auto error = CompareConfigs(cachedDevices, FileDevices);
    if (!HasError(error)) {
        STORAGE_INFO("Current config is OK. Update cached config.");

        LostDevicesIds = GetLostDevicesIds(cachedDevices, FileDevices);

        SaveCurrentConfig();
        return;
    }

    TStringStream ss;
    ss << "Current config doesn't match the cached one: "
        << FormatError(error) << ". Current:\n";
    for (auto& d: FileDevices) {
        ss << d << "\n";
    }
    ss << "\nCached:\n";
    for (const auto& d: cachedDevices) {
        ss << d << "\n";
    }

    ReportDiskAgentConfigMismatchEvent(ss.Str());

    STORAGE_WARN("Current config is broken, fallback to the cached one.");
    FileDevices = cachedDevices;
    SetupSerialNumbers(FileDevices);

    Errors.push_back(TStringBuilder()
        << "broken config: " << FormatError(error));
}

NProto::TError TInitializer::ProcessConfigCache()
{
    const auto& path = GetCachedConfigsPath();
    if (path.empty()) {
        return {};
    }

    auto [config, error] = LoadDiskAgentConfig(path);
    if (HasError(error) && error.GetCode() != E_NOT_FOUND) {
        STORAGE_WARN(
            "Can't load Disk Agent config from the cache: "
            << FormatError(error));

        return error;
    }

    DevicesWithSuspendedIO.insert(
        DevicesWithSuspendedIO.end(),
        std::make_move_iterator(
            config.MutableDevicesWithSuspendedIO()->begin()),
        std::make_move_iterator(config.MutableDevicesWithSuspendedIO()->end()));

    TVector<NProto::TFileDeviceArgs> devices{
        std::make_move_iterator(config.MutableFileDevices()->begin()),
        std::make_move_iterator(config.MutableFileDevices()->end())};
    SortBy(devices, [] (const auto& d) {
        return d.GetDeviceId();
    });

    for (const auto& d: devices) {
        const auto currentSerialNumber = GetSerialNumber(d.GetPath());

        if (currentSerialNumber != d.GetSerialNumber()) {
            STORAGE_WARN(
                "Device " << d.GetDeviceId() << " [" << d.GetPath()
                        << "] has new serial number "
                        << TString(currentSerialNumber).Quote()
                        << " (was " << d.GetSerialNumber().Quote()
                        << ")");
            DevicesWithSuspendedIO.push_back(d.GetDeviceId());
        }
    }

    SortUnique(DevicesWithSuspendedIO);

    ValidateCurrentConfigs(devices);

    return {};
}

TFuture<void> TInitializer::Initialize()
{
    // Setup serial numbers for the static config
    for (auto& file: FileDevices) {
        if (file.GetSerialNumber().empty()) {
            file.SetSerialNumber(GetSerialNumber(file.GetPath()));
        }
    }

    ScanFileDevices();
    if (auto error = ProcessConfigCache(); HasError(error)) {
        return MakeErrorFuture<void>(
            std::make_exception_ptr(TServiceError(error)));
    }

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

        auto onInitError = [i, this] () {
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
            ).Subscribe([=, this] (const auto& future) {
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
                .Subscribe([=, this] (const auto& future) {
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

    return WaitAll(futures).Apply([] (const auto& future) {
        Y_UNUSED(future);   // ignore
    });
}

NProto::TDeviceConfig TInitializer::CreateConfig(
    const NProto::TFileDeviceArgs& device)
{
    NProto::TDeviceConfig config;

    config.SetDeviceName(device.GetPath());
    config.SetDeviceUUID(device.GetDeviceId());
    config.SetBlockSize(device.GetBlockSize());
    config.SetRack(AgentConfig->GetRack());
    config.SetPoolName(device.GetPoolName());
    config.SetSerialNumber(device.GetSerialNumber());
    config.SetPhysicalOffset(device.GetOffset());

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
    r.ConfigMismatchErrors = std::move(ConfigMismatchErrors);
    r.DevicesWithSuspendedIO = std::move(DevicesWithSuspendedIO);
    r.LostDevicesIds = std::move(LostDevicesIds);
    r.Guard = std::move(Guard);

    return r;
}

void TInitializer::ReportDiskAgentConfigMismatchEvent(const TString& error) {
    ReportDiskAgentConfigMismatch(error);
    ConfigMismatchErrors.push_back(error);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TFuture<TInitializeStorageResult> InitializeStorage(
    TLog log,
    TStorageConfigPtr storageConfig,
    TDiskAgentConfigPtr agentConfig,
    IStorageProviderPtr storageProvider,
    NNvme::INvmeManagerPtr nvmeManager)
{
    auto initializer = std::make_shared<TInitializer>(
        std::move(log),
        std::move(storageConfig),
        std::move(agentConfig),
        std::move(storageProvider),
        std::move(nvmeManager));

    return initializer->Initialize().Apply([=] (const auto& future) {
        future.GetValue();

        return initializer->GetResult();
    });
}

}   // namespace NCloud::NBlockStore::NStorage
