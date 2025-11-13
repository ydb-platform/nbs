#include "nvme_device_list.h"

#include <cloud/blockstore/libs/nvme/nvme.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>

#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/proto_helpers.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/stream/file.h>
#include <util/stream/format.h>
#include <util/string/builder.h>
#include <util/string/strip.h>
#include <util/system/fs.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

const TStringBuf VFIODriverName = "vfio-pci";

////////////////////////////////////////////////////////////////////////////////

struct TBySerialNumber
{
    TStringBuf SerialNumber;

    explicit TBySerialNumber(TStringBuf serialNumber)
        : SerialNumber(serialNumber)
    {}

    [[nodiscard]] bool operator () (const NNvme::TControllerData& data) const
    {
        return data.SerialNumber == SerialNumber;
    }

    [[nodiscard]] bool operator () (const NProto::TNVMeDevice& device) const
    {
        return device.GetSerialNumber() == SerialNumber;
    }
};

////////////////////////////////////////////////////////////////////////////////

NNvme::TPCIAddress GetPCIAddress(const NProto::TNVMeDevice& d)
{
    return {
        .VendorId = static_cast<ui16>(d.GetPCIVendorId()),
        .DeviceId = static_cast<ui16>(d.GetPCIDeviceId()),
        .Address = d.GetPCIAddress()};
}

////////////////////////////////////////////////////////////////////////////////

[[nodiscard]] TResultOrError<TVector<NProto::TNVMeDevice>> LoadNVMeDevices(
    const TString& path)
{
    return SafeExecute<TResultOrError<TVector<NProto::TNVMeDevice>>>(
        [&]
        {
            if (!NFs::Exists(path)) {
                return TVector<NProto::TNVMeDevice>{};
            }

            NProto::TNVMeDeviceList proto;
            ParseProtoTextFromFileRobust(path, proto);

            return TVector<NProto::TNVMeDevice>(
                std::make_move_iterator(proto.MutableDevices()->begin()),
                std::make_move_iterator(proto.MutableDevices()->end()));
        });
}

[[nodiscard]] NProto::TError SaveNVMeDevices(
    const TString& path,
    const TVector<NProto::TNVMeDevice>& devices)
{
    return SafeExecute<NProto::TError>(
        [&]
        {
            const TString tmpPath {path + ".tmp"};
            NProto::TNVMeDeviceList proto;
            proto.MutableDevices()->Assign(devices.begin(), devices.end());

            SerializeToTextFormat(proto, tmpPath);

            if (!NFs::Rename(tmpPath, path)) {
                const auto ec = errno;
                char buf[64]{};

                return MakeError(
                    MAKE_SYSTEM_ERROR(ec),
                    TStringBuilder()
                        << "can't rename a file from " << tmpPath << " to "
                        << path << ::strerror_r(ec, buf, sizeof(buf)));
            }

            return NProto::TError{};
        });
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TNVMeDeviceList::TNVMeDeviceList(
    NNvme::INvmeManagerPtr nvmeManager,
    TDiskAgentConfigPtr config,
    TLog log)
    : NVMeManager(std::move(nvmeManager))
    , Config(std::move(config))
    , Log(std::move(log))
{
    Y_ABORT_UNLESS(NVMeManager);
    Y_ABORT_UNLESS(Config);

    DiscoverNVMeDevices();
}

TNVMeDeviceList::~TNVMeDeviceList() = default;

void TNVMeDeviceList::DiscoverNVMeDevices()
{
    if (Config->GetNVMeDevices().empty()) {
        return;
    }

    TVector<NNvme::TControllerData> availableDevices;
    {
        auto [r, error] = NVMeManager->ListControllers();
        if (HasError(error)) {
            STORAGE_ERROR("Can't list NVMe controllers: " << FormatError(error));
            return;
        }
        availableDevices = std::move(r);
    }

    for (auto& device: availableDevices) {
        StripInPlace(device.SerialNumber);
        StripInPlace(device.ModelNumber);
    }

    TVector<NProto::TNVMeDevice> cachedDevices;

    if (Config->GetCachedNVMeDevicesPath()) {
        auto [r, error] = LoadNVMeDevices(Config->GetCachedNVMeDevicesPath());
        if (HasError(error)) {
            STORAGE_ERROR(
                "Can't load NVMe devices from cache: " << FormatError(error));
        } else {
            cachedDevices = std::move(r);
        }
    }

    for (const auto& spec: Config->GetNVMeDevices()) {
        if (spec.GetSerialNumber().empty()) {
            STORAGE_WARN("Empty serial number for NVMe device: " << spec);
            continue;
        }

        auto* device = FindIfPtr(
            availableDevices,
            TBySerialNumber(spec.GetSerialNumber()));

        if (device) {
            auto [pci, error] = NVMeManager->GetPCIAddress(device->DevicePath);
            if (HasError(error)) {
                STORAGE_ERROR(
                    "Can't get PCIe address for NVMe device "
                    << spec.GetSerialNumber().Quote() << ": "
                    << FormatError(error));

                continue;
            }

            STORAGE_INFO(
                "Found NVMe device "
                << spec.GetSerialNumber().Quote() << ": " << device->ModelNumber
                << " " << FormatByteSize(device->Capacity) << " ("
                << Hex(pci.VendorId) << ":" << Hex(pci.DeviceId) << " "
                << pci.Address << ")");

            NProto::TNVMeDevice& d = NVMeDevices.emplace_back();
            d.SetSerialNumber(device->SerialNumber);
            d.SetModel(device->ModelNumber);
            d.SetCapacity(device->Capacity);
            d.SetPCIVendorId(pci.VendorId);
            d.SetPCIDeviceId(pci.DeviceId);
            d.SetPCIAddress(pci.Address);

            continue;
        }

        auto cachedDeviceIt =
            FindIf(cachedDevices, TBySerialNumber(spec.GetSerialNumber()));

        if (cachedDeviceIt == cachedDevices.end()) {
            STORAGE_WARN(
                "NVMe device " << spec.GetSerialNumber().Quote()
                               << " not found");
            continue;
        }

        // should be bound to VFIO driver

        auto [driver, error] =
            NVMeManager->GetDriverName(GetPCIAddress(*cachedDeviceIt));

        if (HasError(error)) {
            STORAGE_WARN(
                "Can't get a driver name for NVMe device "
                << spec.GetSerialNumber().Quote() << ": "
                << FormatError(error));
            continue;
        }

        if (driver != VFIODriverName) {
            STORAGE_WARN(
                "Unexpected driver (" << driver.Quote() << ") for NVMe device "
                                      << spec.GetSerialNumber().Quote());
            continue;
        }

        STORAGE_INFO("NVMe device found in cache " << *cachedDeviceIt);

        NVMeDevices.push_back(std::move(*cachedDeviceIt));
        cachedDevices.erase(cachedDeviceIt);
    }

    UpdateDeviceCache();
}

void TNVMeDeviceList::UpdateDeviceCache()
{
    if (!Config->GetCachedNVMeDevicesPath()) {
        return;
    }
    auto error =
        SaveNVMeDevices(Config->GetCachedNVMeDevicesPath(), NVMeDevices);
    if (HasError(error)) {
        STORAGE_ERROR(
            "Can't save NVMe devices to cache: " << FormatError(error));
    }
}

const TVector<NProto::TNVMeDevice>& TNVMeDeviceList::GetNVMeDevices() const
{
    return NVMeDevices;
}

NProto::TError TNVMeDeviceList::BindNVMeDeviceToVFIO(
    const TString& serialNumber)
{
    auto* device = FindIfPtr(NVMeDevices, TBySerialNumber(serialNumber));
    if (!device) {
        return MakeError(E_NOT_FOUND, "NVMe device not found");
    }

    return NVMeManager->BindToVFIO(GetPCIAddress(*device));
}

NProto::TError TNVMeDeviceList::ResetNVMeDevice(const TString& serialNumber)
{
    auto* device = FindIfPtr(NVMeDevices, TBySerialNumber(serialNumber));
    if (!device) {
        return MakeError(E_NOT_FOUND, "NVMe device not found");
    }

    auto error = NVMeManager->BindToNVME(GetPCIAddress(*device));
    if (HasError(error)) {
        STORAGE_ERROR(
            "Can't bind NVMe device "
            << serialNumber.Quote()
            << " to the nvme driver: " << FormatError(error));
        return error;
    }

    // TODO: remove namespaces; secure erase

    return {};
}

}   // namespace NCloud::NBlockStore::NStorage
