#include "sysfs_helpers.h"

#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/string/ascii.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/strip.h>
#include <util/system/fs.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

void WriteFile(const TFsPath& path, TStringBuf data)
{
    TFile file{path, EOpenModeFlag::OpenExisting | EOpenModeFlag::WrOnly};

    file.Write(data.data(), data.size());
}

TString ReadFile(const TFsPath& path)
{
    return Strip(TFileInput{path}.ReadAll());
}

ui32 ReadHexFromFile(const TFsPath& path)
{
    return std::stoul(ReadFile(path), nullptr, 16);
}

auto GetVFioDeviceName(const TFsPath& pciDevicePath) -> TString
{
    const TFsPath vfioDevDir = pciDevicePath / "vfio-dev";
    if (!vfioDevDir.Exists()) {
        return {};
    }

    TVector<TString> children;
    vfioDevDir.ListNames(children);

    TString devName;
    for (const auto& child: children) {
        TStringBuf number;
        if (!TStringBuf{child}.AfterPrefix("vfio", number)) {
            continue;
        }
        Y_ENSURE(
            !number.empty() && std::ranges::all_of(number, IsAsciiDigit<char>),
            "unexpected entry " << child.Quote() << " in "
                                << vfioDevDir.GetPath().Quote()
                                << ": expected name matching 'vfio<N>' "
                                << "where <N> is a non-empty digit sequence");

        Y_ENSURE(
            devName.empty(),
            "multiple vfio<N> entries found in "
                << vfioDevDir.GetPath().Quote() << " (already have "
                << devName.Quote() << ", now also " << child.Quote() << ")");

        devName = child;
    }

    Y_ENSURE(
        !devName.empty(),
        "no vfio<N> entry found in " << vfioDevDir.GetPath().Quote());

    return devName;
}

auto GetIOMMUGroup(const TFsPath& pciDevicePath) -> std::optional<ui32>
{
    const TFsPath iommuPath = pciDevicePath / "iommu_group";
    if (iommuPath.Exists()) {
        const auto value = iommuPath.ReadLink().Basename();
        return FromString<ui32>(value);
    }

    return {};
}

auto GetNumaNode(const TFsPath& pciDevicePath) -> std::optional<ui32>
{
    const TFsPath numaPath = pciDevicePath / "numa_node";
    if (numaPath.Exists()) {
        const i32 value = FromString<i32>(ReadFile(numaPath));
        if (value != -1) {
            Y_ENSURE(value >= 0, "unexpected NUMA node: " << value);
            return static_cast<ui32>(value);
        }
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

class TSysFs final: public ISysFs
{
private:
    const TFsPath SysFsRoot;

public:
    explicit TSysFs(TFsPath sysFsRoot)
        : SysFsRoot(std::move(sysFsRoot))
    {}

    auto GetDriverForPCIDevice(const TString& pciAddr) -> TString final
    {
        const TFsPath path = SysFsRoot / "bus/pci/devices" / pciAddr / "driver";

        if (!path.Exists()) {
            return {};
        }

        return path.ReadLink().Basename();
    }

    void BindPCIDeviceToDriver(
        const TString& pciAddr,
        const TString& driverName) final
    {
        const TString currentDriver = GetDriverForPCIDevice(pciAddr);
        if (driverName == currentDriver) {
            return;
        }

        const TFsPath basePath = SysFsRoot / "bus/pci";
        const TFsPath devicePath = basePath / "devices" / pciAddr;

        // Unbind from the current driver
        if (currentDriver) {
            WriteFile(devicePath / "driver/unbind", pciAddr);
        }

        // Bind to the new driver
        if (driverName) {
            WriteFile(devicePath / "driver_override", driverName);
            WriteFile(basePath / "drivers" / driverName / "bind", pciAddr);
        } else {
            WriteFile(devicePath / "driver_override", {});
        }
    }

    auto GetNVMeCtrlNameFromPCIAddr(const TString& pciAddr) -> TString final
    {
        const TFsPath path = SysFsRoot / "bus/pci/devices" / pciAddr / "nvme";

        if (path.Exists()) {
            TVector<TString> children;
            path.ListNames(children);

            if (children) {
                return children[0];
            }
        }

        return {};
    }

    auto GetNVMeDeviceFromPCIAddr(const TString& pciAddr)
        -> NProto::TNVMeDevice final
    {
        const TFsPath pciDevicePath = SysFsRoot / "bus/pci/devices" / pciAddr;

        NProto::TNVMeDevice device;
        device.SetPCIAddress(pciAddr);
        device.SetVendorId(ReadHexFromFile(pciDevicePath / "vendor"));
        device.SetDeviceId(ReadHexFromFile(pciDevicePath / "device"));

        const auto name = GetNVMeCtrlNameFromPCIAddr(pciAddr);
        if (!name.empty()) {
            const auto nvme = pciDevicePath / "nvme" / name;

            device.SetSerialNumber(ReadFile(nvme / "serial"));
            device.SetModel(ReadFile(nvme / "model"));
        }

        if (auto group = GetIOMMUGroup(pciDevicePath)) {
            device.SetIOMMUGroup(*group);
        }

        if (auto name = GetVFioDeviceName(pciDevicePath)) {
            device.SetVfioDevName(std::move(name));
        }

        if (auto node = GetNumaNode(pciDevicePath)) {
            device.SetNumaNode(*node);
        }

        return device;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ISysFsPtr CreateSysFs(TFsPath sysFsRoot)
{
    return std::make_shared<TSysFs>(std::move(sysFsRoot));
}

}   // namespace NCloud::NBlockStore
