#include "sysfs_helpers.h"

#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/strip.h>
#include <util/system/fs.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

void WriteFile(const TFsPath& path, TStringBuf data)
{
    TFile file{path, EOpenModeFlag::OpenExisting | EOpenModeFlag::WrOnly};

    TFileOutput(file).Write(data);
}

TString ReadFile(const TFsPath& path)
{
    return Strip(TFileInput{path}.ReadAll());
}

ui32 ReadHexFromFile(const TFsPath& path)
{
    return std::stoul(ReadFile(path), nullptr, 16);
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

        // Unbind from the current driver
        if (currentDriver) {
            WriteFile(
                basePath / "devices" / pciAddr / "driver/unbind",
                pciAddr);
        }

        // Bind to the new driver
        if (driverName) {
            WriteFile(basePath / "drivers" / driverName / "bind", pciAddr);
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
        const TFsPath path = SysFsRoot / "bus/pci/devices" / pciAddr;

        NProto::TNVMeDevice device;
        device.SetPCIAddress(pciAddr);
        device.SetVendorId(ReadHexFromFile(path / "vendor"));
        device.SetDeviceId(ReadHexFromFile(path / "device"));

        const auto name = GetNVMeCtrlNameFromPCIAddr(pciAddr);
        if (!name.empty()) {
            const auto nvme = path / "nvme" / name;

            device.SetSerialNumber(ReadFile(nvme / "serial"));
            device.SetModel(ReadFile(nvme / "model"));
        }

        const TFsPath iommuPath = path / "iommu_group";
        if (iommuPath.Exists()) {
            const auto value = iommuPath.ReadLink().Basename();
            device.SetIOMMUGroup(std::stoul(value));
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
