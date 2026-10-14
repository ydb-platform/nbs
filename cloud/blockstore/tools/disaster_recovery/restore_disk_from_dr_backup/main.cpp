#include "options.h"

#include <cloud/blockstore/libs/storage/protos/disk.pb.h>
#include <cloud/blockstore/libs/storage/protos_ydb/disk.pb.h>

#include <cloud/storage/core/libs/common/format.h>

#include <util/digest/multi.h>
#include <util/folder/path.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TPhysicalKey
{
    TString SerialNumber;
    ui64 PhysicalOffset = 0;

    bool operator==(const TPhysicalKey& other) const = default;
};

struct TPhysicalKeyHash
{
    size_t operator()(const TPhysicalKey& key) const
    {
        return MultiHash(key.SerialNumber, key.PhysicalOffset);
    }
};

struct TDeviceIndex
{
    THashMap<TString, const NProto::TDeviceConfig*> ByUUID;
    THashMap<TPhysicalKey, const NProto::TDeviceConfig*, TPhysicalKeyHash>
        ByPhysical;
};

class TDiskValidator
{
public:
    bool AddDeviceToDisk(const TString& diskId, const TString& deviceUuid);

private:
    THashMap<TString, TString> DeviceToDisk;
};

bool TDiskValidator::AddDeviceToDisk(
    const TString& diskId,
    const TString& deviceUuid)
{
    if (DeviceToDisk.contains(deviceUuid)) {
        return false;
    }
    DeviceToDisk.emplace(deviceUuid, diskId);
    return true;
}

bool ValidateDevice(
    const NProto::TDeviceConfig& oldDevice,
    const NProto::TDeviceConfig& newDevice)
{
    return oldDevice.GetSerialNumber() == newDevice.GetSerialNumber() &&
           oldDevice.GetPhysicalOffset() == newDevice.GetPhysicalOffset() &&
           oldDevice.GetPoolKind() == newDevice.GetPoolKind() &&
           oldDevice.GetBlockSize() == newDevice.GetBlockSize();
}

TString ReadFile(const TString& filePath)
{
    TFile file(filePath, EOpenModeFlag::RdOnly);
    return TFileInput(file).ReadAll();
}

NProto::TDiskRegistryStateBackup LoadBackup(const TString& path)
{
    TString backupData = ReadFile(path);

    NProto::TBackupDiskRegistryStateResponse backup;

    auto status =
        google::protobuf::util::JsonStringToMessage(backupData, &backup);
    if (!status.ok()) {
        ythrow yexception() << "Error loading backup from: " << path.Quote()
                            << " error: " << status.ToString();
    }

    return backup.GetLocalDBBackup();
}

TDeviceIndex BuildDeviceIndex(const NProto::TDiskRegistryStateBackup& backup)
{
    TDeviceIndex index;
    for (const auto& agent: backup.GetAgents()) {
        for (const auto& device: agent.GetDevices()) {
            index.ByUUID.emplace(device.GetDeviceUUID(), &device);

            TPhysicalKey key{
                .SerialNumber = device.GetSerialNumber(),
                .PhysicalOffset = device.GetPhysicalOffset()};

            index.ByPhysical.emplace(std::move(key), &device);
        }

        // TODO: get rid of duplication
        for (const auto& device: agent.GetUnknownDevices()) {
            index.ByUUID.emplace(device.GetDeviceUUID(), &device);

            TPhysicalKey key{
                .SerialNumber = device.GetSerialNumber(),
                .PhysicalOffset = device.GetPhysicalOffset()};

            index.ByPhysical.emplace(std::move(key), &device);
        }
    }

    return index;
}

THashMap<TString, const NProto::TDiskConfig*> BuildDiskIdToConfig(
    const NProto::TDiskRegistryStateBackup& backup)
{
    THashMap<TString, const NProto::TDiskConfig*> diskIdToConfig;
    diskIdToConfig.reserve(backup.GetDisks().size());

    for (const auto& disk: backup.GetDisks()) {
        diskIdToConfig.emplace(disk.GetDiskId(), &disk);
    }

    return diskIdToConfig;
}

const NProto::TDeviceConfig* ResolveDevice(
    const NProto::TDeviceConfig& oldDevice,
    const TDeviceIndex& currentIndex)
{
    const TPhysicalKey pKey{
        .SerialNumber = oldDevice.GetSerialNumber(),
        .PhysicalOffset = oldDevice.GetPhysicalOffset()};

    const auto* currentDevice = currentIndex.ByPhysical.FindPtr(pKey);
    if (!currentDevice) {
        return nullptr;
    }
    return *currentDevice;
}

TVector<NProto::TDeviceConfig> ResolveDiskDevices(
    const NProto::TDiskConfig& oldDisk,
    const TDeviceIndex& oldIndex,
    const TDeviceIndex& currentIndex,
    TDiskValidator& validator)
{
    if (oldDisk.DeviceUUIDsSize() == 0) {
        ythrow yexception() << "disk has no devices in old backup";
    }

    TVector<NProto::TDeviceConfig> devices;
    devices.reserve(oldDisk.DeviceUUIDsSize());

    for (const auto& uuid: oldDisk.GetDeviceUUIDs()) {
        const auto* oldDevice = oldIndex.ByUUID.FindPtr(uuid);
        if (!oldDevice) {
            ythrow yexception()
                << "device " << uuid.Quote() << " not found in old backup";
        }

        const auto* currentDevice = ResolveDevice(**oldDevice, currentIndex);
        if (!currentDevice) {
            ythrow yexception()
                << "device " << uuid.Quote() << " with serial number "
                << (*oldDevice) -> GetSerialNumber().Quote()
                << " and physical offset " << (*oldDevice)->GetPhysicalOffset()
                << " not found in current backup";
        }

        if (!ValidateDevice(**oldDevice, *currentDevice)) {
            ythrow yexception()
                << "device " << uuid.Quote()
                << " is inconsistent with device in previous backup";
        }

        if (!validator.AddDeviceToDisk(oldDisk.GetDiskId(), uuid)) {
            ythrow yexception() << "device " << uuid.Quote()
                                << " already exists on other disk ";
        }

        devices.push_back(*currentDevice);
    }

    return devices;
}

NProto::TCreateDiskFromDevicesRequest BuildRequest(

    const NProto::TDiskConfig& oldDisk,
    const TVector<NProto::TDeviceConfig>& newDevices)
{
    NProto::TCreateDiskFromDevicesRequest request;

    ::NKikimrBlockStore::TVolumeConfig volumeConfig;
    volumeConfig.SetDiskId(oldDisk.GetDiskId());
    volumeConfig.SetBlockSize(oldDisk.GetBlockSize());
    volumeConfig.SetStorageMediaKind(oldDisk.GetStorageMediaKind());

    request.MutableVolumeConfig()->CopyFrom(volumeConfig);
    request.SetForce(true);

    for (const auto& device: newDevices) {
        *request.AddDevices() = device;
    }
    return request;
}

void DumpRequest(
    const NProto::TCreateDiskFromDevicesRequest& request,
    const TOptions& options)
{
    TProtoStringType json;
    auto status = google::protobuf::util::MessageToJsonString(request, &json);
    if (!status.ok()) {
        ythrow yexception()
            << "Error serializing request for disk "
            << request.GetVolumeConfig().GetDiskId().Quote() << " error: " << status.ToString();
    }

    if (!options.OutputDir) {
        Cout << json << Endl;
        return;
    }

    const TFsPath outputDir(options.OutputDir);
    outputDir.MkDirs();

    const TFsPath outputPath = outputDir / (request.GetVolumeConfig().GetDiskId() + ".json");
    TFileOutput(outputPath).Write(json);

    Cout << "Disk " << request.GetVolumeConfig().GetDiskId().Quote() << " dumped to "
         << outputPath.GetPath().Quote()
         << ", devices count: " << request.GetDevices().size() << Endl;
}

void Run(const TOptions& options)
{
    auto oldBackup = LoadBackup(options.OldBackupPath);
    TDeviceIndex oldIndex = BuildDeviceIndex(oldBackup);

    auto currentBackup = LoadBackup(options.CurrentBackupPath);
    TDeviceIndex currentIndex = BuildDeviceIndex(currentBackup);

    THashMap<TString, const NProto::TDiskConfig*> oldDiskIdToConfig =
        BuildDiskIdToConfig(oldBackup);

    TDiskValidator validator;

    for (const auto& diskId: options.DiskIds) {
        const auto* oldDisk = oldDiskIdToConfig.FindPtr(diskId);
        if (!oldDisk) {
            ythrow yexception() << "disk not found in old backup";
        }

        const auto devices =
            ResolveDiskDevices(**oldDisk, oldIndex, currentIndex, validator);

        DumpRequest(BuildRequest(**oldDisk, devices), options);
    }
}

}   // namespace

}   // namespace NCloud::NBlockStore::NStorage

using namespace NCloud::NBlockStore::NStorage;

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    try {
        const TOptions options{argc, argv};

        Run(options);

    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
    return 0;
}
