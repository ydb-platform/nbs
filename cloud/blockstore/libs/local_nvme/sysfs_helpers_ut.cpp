#include "sysfs_helpers.h"

#include <cloud/blockstore/libs/storage/protos/local_nvme.pb.h>

#include <cloud/storage/core/libs/common/proto_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/generic/vector.h>
#include <util/stream/file.h>
#include <util/system/fs.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

auto ReadFile(const TFsPath& path) -> TString
{
    auto data = TFileInput(path).ReadAll();

    NFs::Remove(path);
    path.Touch();

    return data;
}

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    std::optional<TTempDir> TempDir;

    TFsPath SysFsRoot;
    TFsPath NVMeDriverPath;
    TFsPath VFIODriverPath;

    ISysFsPtr SysFs;

    TVector<NProto::TNVMeDevice> Devices;

    void SetUp(NUnitTest::TTestContext& /*testContext*/) final
    {
        TempDir.emplace();

        SysFsRoot = TempDir->Path();
        SysFs = CreateSysFs(TempDir->Path());

        NVMeDriverPath = PrepareDriver("nvme");
        VFIODriverPath = PrepareDriver("vfio-pci");

        Devices = PrepareDevices();
        UNIT_ASSERT_VALUES_EQUAL(3, Devices.size());

        for (const auto& device: Devices) {
            const TFsPath devicePath = GetPCIDevicePath(device);
            NFs::MakeDirectoryRecursive(devicePath);

            TFileOutput(devicePath / "vendor").Write(device.GetVendorId());
            TFileOutput(devicePath / "device").Write(device.GetDeviceId());

            const TFsPath groupPath = PrepareIOMMUGroup(device.GetIOMMUGroup());
            NFs::SymLink(groupPath, devicePath / "iommu_group");
        }

        // NVME_0 bound to the nvme driver
        {
            const auto& device = Devices[0];

            const TFsPath devicePath = GetPCIDevicePath(device);

            NFs::SymLink(NVMeDriverPath, devicePath / "driver");

            const TFsPath ctrlPath = devicePath / "nvme/nvme0";
            NFs::MakeDirectoryRecursive(ctrlPath);

            TFileOutput(ctrlPath / "serial").Write(device.GetSerialNumber());
            TFileOutput(ctrlPath / "model").Write(device.GetModel());
        }

        // NVME_1 bound to the vfio-pci driver
        {
            const auto& device = Devices[1];
            const TFsPath devicePath = GetPCIDevicePath(device);
            NFs::SymLink(VFIODriverPath, devicePath / "driver");
        }
    }

    auto GetPCIDevicePath(const NProto::TNVMeDevice& device) -> TFsPath
    {
        return SysFsRoot / "bus/pci/devices/" / device.GetPCIAddress();
    }

    auto PrepareDriver(const TString& name) -> TFsPath
    {
        TFsPath path = SysFsRoot / "bus/pci/drivers/" / name;

        NFs::MakeDirectoryRecursive(path);
        for (const TStringBuf sub: {"bind", "unbind", "new_id"}) {
            (path / sub).Touch();
        }

        return path;
    }

    auto PrepareDevices() -> TVector<NProto::TNVMeDevice>
    {
        NProto::TNVMeDeviceList list;
        ParseProtoTextFromString(
            R"(
            Devices {
                SerialNumber: "NVME_0"
                PCIAddress: "0000:f1:00.0"
                IOMMUGroup: 10
                VendorId: 0x100
                DeviceId: 0x200
                Model: "Test NVMe 1"
            }
            Devices {
                SerialNumber: "NVME_1"
                PCIAddress: "0000:31:00.0"
                IOMMUGroup: 20
                VendorId: 0x300
                DeviceId: 0x400
                Model: "Test NVMe 2"
            }
            Devices {
                SerialNumber: "NVME_2"
                PCIAddress: "0000:33:00.0"
                IOMMUGroup: 30
                VendorId: 0x500
                DeviceId: 0x600
                Model: "Test NVMe 3"
            }
        )",
            list);

        return {list.GetDevices().begin(), list.GetDevices().end()};
    }

    // /sys/kernel/iommu_groups/<group>; /devices/
    auto PrepareIOMMUGroup(ui32 group) -> TFsPath
    {
        TFsPath path = SysFsRoot / "kernel/iommu_groups/" / ToString(group);
        NFs::MakeDirectoryRecursive(path);
        return path;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSysFsHelpersTest)
{
    Y_UNIT_TEST_F(ShouldGetCurrentDeviceDriver, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(
            "nvme",
            SysFs->GetDriverForPCIDevice(Devices[0].GetPCIAddress()));

        UNIT_ASSERT_VALUES_EQUAL(
            "vfio-pci",
            SysFs->GetDriverForPCIDevice(Devices[1].GetPCIAddress()));

        UNIT_ASSERT_VALUES_EQUAL(
            "",
            SysFs->GetDriverForPCIDevice(Devices[2].GetPCIAddress()));
    }

    Y_UNIT_TEST_F(ShouldBindDevice, TFixture)
    {
        {
            const auto& device = Devices[0];
            const auto& pciAddr = device.GetPCIAddress();

            // nvme -> nvme (noop)

            SysFs->BindPCIDeviceToDriver(pciAddr, "nvme");

            UNIT_ASSERT_VALUES_EQUAL("", ReadFile(NVMeDriverPath / "unbind"));
            UNIT_ASSERT_VALUES_EQUAL("", ReadFile(NVMeDriverPath / "bind"));
            UNIT_ASSERT_VALUES_EQUAL("", ReadFile(VFIODriverPath / "unbind"));
            UNIT_ASSERT_VALUES_EQUAL("", ReadFile(VFIODriverPath / "bind"));

            // nvme -> vfio-pci

            SysFs->BindPCIDeviceToDriver(pciAddr, "vfio-pci");

            UNIT_ASSERT_VALUES_EQUAL(
                pciAddr,
                ReadFile(NVMeDriverPath / "unbind"));
            UNIT_ASSERT_VALUES_EQUAL("", ReadFile(NVMeDriverPath / "bind"));
            UNIT_ASSERT_VALUES_EQUAL("", ReadFile(VFIODriverPath / "unbind"));
            UNIT_ASSERT_VALUES_EQUAL(
                pciAddr,
                ReadFile(VFIODriverPath / "bind"));
        }

        {
            const auto& device = Devices[1];
            const auto& pciAddr = device.GetPCIAddress();

            // vfio-pci -> vfio-pci (noop)

            SysFs->BindPCIDeviceToDriver(pciAddr, "vfio-pci");

            UNIT_ASSERT_VALUES_EQUAL("", ReadFile(NVMeDriverPath / "unbind"));
            UNIT_ASSERT_VALUES_EQUAL("", ReadFile(NVMeDriverPath / "bind"));
            UNIT_ASSERT_VALUES_EQUAL("", ReadFile(VFIODriverPath / "unbind"));
            UNIT_ASSERT_VALUES_EQUAL("", ReadFile(VFIODriverPath / "bind"));

            // vfio-pci -> ""

            SysFs->BindPCIDeviceToDriver(pciAddr, "");

            UNIT_ASSERT_VALUES_EQUAL("", ReadFile(NVMeDriverPath / "unbind"));
            UNIT_ASSERT_VALUES_EQUAL("", ReadFile(NVMeDriverPath / "bind"));
            UNIT_ASSERT_VALUES_EQUAL(
                pciAddr,
                ReadFile(VFIODriverPath / "unbind"));
            UNIT_ASSERT_VALUES_EQUAL("", ReadFile(VFIODriverPath / "bind"));
        }

        {
            const auto& device = Devices[2];
            const auto& pciAddr = device.GetPCIAddress();

            // "" -> vfio-pci

            SysFs->BindPCIDeviceToDriver(pciAddr, "vfio-pci");

            UNIT_ASSERT_VALUES_EQUAL("", ReadFile(NVMeDriverPath / "unbind"));
            UNIT_ASSERT_VALUES_EQUAL("", ReadFile(NVMeDriverPath / "bind"));
            UNIT_ASSERT_VALUES_EQUAL("", ReadFile(VFIODriverPath / "unbind"));
            UNIT_ASSERT_VALUES_EQUAL(
                pciAddr,
                ReadFile(VFIODriverPath / "bind"));
        }
    }

    Y_UNIT_TEST_F(ShouldGetNVMeCtrlNameFromPCIAddr, TFixture)
    {
        for (size_t i = 0; i != Devices.size(); ++i) {
            const auto& device = Devices[i];
            const auto& pciAddr = device.GetPCIAddress();

            if (i == 0) {
                auto ctrlName = SysFs->GetNVMeCtrlNameFromPCIAddr(pciAddr);
                UNIT_ASSERT_VALUES_EQUAL("nvme0", ctrlName);
                continue;
            }

            auto ctrlName = SysFs->GetNVMeCtrlNameFromPCIAddr(pciAddr);
            UNIT_ASSERT_VALUES_EQUAL("", ctrlName);
        }
    }
}

}   // namespace NCloud::NBlockStore
