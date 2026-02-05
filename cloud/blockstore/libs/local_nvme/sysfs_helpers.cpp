#include "sysfs_helpers.h"

#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/system/fs.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

void WriteFile(const TFsPath& path, TStringBuf data)
{
    TFile file{path, EOpenModeFlag::OpenExisting | EOpenModeFlag::WrOnly};

    TFileOutput(file).Write(data);
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
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ISysFsPtr CreateSysFs(TFsPath sysFsRoot)
{
    return std::make_shared<TSysFs>(std::move(sysFsRoot));
}

}   // namespace NCloud::NBlockStore
