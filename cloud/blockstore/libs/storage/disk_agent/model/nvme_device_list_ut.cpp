#include "nvme_device_list.h"

#include <cloud/blockstore/libs/nvme/nvme.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>

#include <chrono>
#include <memory>

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;
using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestNVMeManager final
    : public NNvme::INvmeManager
{
    struct TController: NNvme::TControllerData
    {
        NNvme::TPCIAddress PCIAddress;
    };

    TVector<TController> Controllers;
    TVector<TController> BoundToVFIO;

    TFuture<NProto::TError> Format(
        const TString& path,
        NNvme::nvme_secure_erase_setting ses) final
    {
        Y_UNUSED(path, ses);
        return MakeFuture(MakeError(E_NOT_IMPLEMENTED));
    }

    TFuture<NProto::TError> Deallocate(
        const TString& path,
        ui64 offsetBytes,
        ui64 sizeBytes) final
    {
        Y_UNUSED(path, offsetBytes, sizeBytes);
        return MakeFuture(MakeError(E_NOT_IMPLEMENTED));
    }

    TResultOrError<bool> IsSsd(const TString& path) final
    {
        return !FindIfPtr(Controllers, [&path] (const auto& d) {
            return d.DevicePath == path;
        });
    }

    TResultOrError<TString> GetSerialNumber(const TString& path) final
    {
        auto* device = FindIfPtr(Controllers, [&path] (const auto& d) {
            return d.DevicePath == path;
        });

        if (device) {
            return device->SerialNumber;
        }

        return MakeError(E_NOT_FOUND);
    }

    TResultOrError<NNvme::TPCIAddress> GetPCIAddress(const TString& path) final
    {
        auto* device = FindIfPtr(Controllers, [&] (const auto& d) {
            return d.DevicePath == path;
        });

        if (device) {
            return device->PCIAddress;
        }

        return MakeError(E_NOT_FOUND);
    }

    TResultOrError<TString> GetDriverName(const NNvme::TPCIAddress& pci) final
    {
        auto cmp = [&](const TController& d)
        {
            return d.PCIAddress == pci;
        };

        if (FindIfPtr(BoundToVFIO, cmp)) {
            return TString("vfio-pci");
        }

        return TString("nvme");
    }

    TResultOrError<TVector<NNvme::TControllerData>> ListControllers() final
    {
        return TVector<NNvme::TControllerData>(
            Controllers.begin(),
            Controllers.end());
    }

    NProto::TError BindToVFIO(const NNvme::TPCIAddress& pci) final
    {
        return BindImpl(pci, Controllers, BoundToVFIO);
    }

    NProto::TError BindToNVME(const NNvme::TPCIAddress& pci) final
    {
        return BindImpl(pci, BoundToVFIO, Controllers);
    }

    static NProto::TError BindImpl(
        const NNvme::TPCIAddress& pci,
        TVector<TController>& src,
        TVector<TController>& dst)
    {
        auto cmp = [&](const TController& d)
        {
            return d.PCIAddress == pci;
        };

        auto it = FindIf(src, cmp);

        if (it == src.end()) {
            if (FindIfPtr(dst, cmp)) {
                return MakeError(S_ALREADY);
            }
            return MakeError(E_NOT_FOUND);
        }

        dst.push_back(std::move(*it));
        src.erase(it);

        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    const TTempDir TempDir;
    const TString CachedNVMeDevicesPath =
        TempDir.Path() / "nbs-nvme-devices.txt";

    std::shared_ptr<TTestNVMeManager> NVMeManager;
    ILoggingServicePtr Logging;

    void SetUp(NUnitTest::TTestContext& /*testContext*/) override
    {
        Logging = CreateLoggingService("console");

        NVMeManager = std::make_shared<TTestNVMeManager>();
        NVMeManager->Controllers = {
            {{.DevicePath = "/dev/nvme0",
              .SerialNumber = "S6EYNE0R103562",
              .ModelNumber = "SAMSUNG MZWLR3T8HBLS-00007",
              .Capacity = 3276_GB},
             {0x144d, 0xa828, "31:00.0"}},
            {{.DevicePath = "/dev/nvme1",
              .SerialNumber = "S4YPNC0R705326",
              .ModelNumber = "SAMSUNG MZWLJ3T8HBLS-00007",
              .Capacity = 3276_GB},
             {0x144d, 0xa828, "32:00.0"}},
            {{.DevicePath = "/dev/nvme2",
              .SerialNumber = "S6EYNA0R104173",
              .ModelNumber = "SAMSUNG MZWLR3T8HBLS-00007",
              .Capacity = 3276_GB},
             {0x144d, 0xa828, "33:00.0"}},
            {{.DevicePath = "/dev/nvme3",
              .SerialNumber = "S6EYNA0R104150",
              .ModelNumber = "SAMSUNG MZWLR3T8HBLS-00007",
              .Capacity = 3276_GB},
             {0x144d, 0xa828, "34:00.0"}},
            {{.DevicePath = "/dev/nvme4",
              .SerialNumber = "S4YPNC0R501553",
              .ModelNumber = "SAMSUNG MZWLJ3T8HBLS-00007",
              .Capacity = 3276_GB},
             {0x144d, 0xa828, "f1:00.0"}},
            {{.DevicePath = "/dev/nvme5",
              .SerialNumber = "S4YPNC0R500432",
              .ModelNumber = "SAMSUNG MZWLJ3T8HBLS-00007",
              .Capacity = 3276_GB},
             {0x144d, 0xa828, "f2:00.0"}},
        };
    }

    static TDiskAgentConfigPtr CreateConfig(NProto::TDiskAgentConfig proto)
    {
        return std::make_shared<TDiskAgentConfig>(
            std::move(proto),
            "rack",
            1000);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TNVMeDeviceListTest)
{
    Y_UNIT_TEST_F(ShouldListNVMeDevices, TFixture)
    {
        NProto::TDiskAgentConfig configProto;
        configProto.AddNvmeDevices()->SetSerialNumber(
            NVMeManager->Controllers[4].SerialNumber);   // nvme4
        configProto.AddNvmeDevices()->SetSerialNumber(
            NVMeManager->Controllers[3].SerialNumber);   // nvme3

        TNVMeDeviceList list{
            NVMeManager,
            CreateConfig(configProto),
            Logging->CreateLog("NVMe")};

        auto devices = list.GetNVMeDevices();
        SortBy(devices, [] (const auto& d) {
            return d.GetSerialNumber();
        });

        UNIT_ASSERT_VALUES_EQUAL(2, devices.size());

        const auto& nvme3 = devices[1];
        const auto& nvme4 = devices[0];

        UNIT_ASSERT_VALUES_EQUAL(
            NVMeManager->Controllers[3].SerialNumber,
            nvme3.GetSerialNumber());

        UNIT_ASSERT_VALUES_EQUAL(
            NVMeManager->Controllers[3].PCIAddress.Address,
            nvme3.GetPCIAddress());

        UNIT_ASSERT_VALUES_EQUAL(
            NVMeManager->Controllers[4].SerialNumber,
            nvme4.GetSerialNumber());

        UNIT_ASSERT_VALUES_EQUAL(
            NVMeManager->Controllers[4].PCIAddress.Address,
            nvme4.GetPCIAddress());
    }

    Y_UNIT_TEST_F(ShouldBindNVMeDeviceToVFIO, TFixture)
    {
        const TString nvme4 = NVMeManager->Controllers[4].SerialNumber;

        NProto::TDiskAgentConfig configProto;
        configProto.AddNvmeDevices()->SetSerialNumber(
            NVMeManager->Controllers[3].SerialNumber);
        configProto.AddNvmeDevices()->SetSerialNumber(nvme4);

        TNVMeDeviceList list{
            NVMeManager,
            CreateConfig(configProto),
            Logging->CreateLog("NVMe")};

        UNIT_ASSERT_VALUES_EQUAL(2, list.GetNVMeDevices().size());
        UNIT_ASSERT_VALUES_EQUAL(0, NVMeManager->BoundToVFIO.size());
        UNIT_ASSERT_VALUES_EQUAL(6, NVMeManager->Controllers.size());

        {
            auto error = list.BindNVMeDeviceToVFIO(nvme4);
            UNIT_ASSERT_C(!HasError(error), FormatError(error));
        }

        UNIT_ASSERT_VALUES_EQUAL(1, NVMeManager->BoundToVFIO.size());
        UNIT_ASSERT_VALUES_EQUAL(5, NVMeManager->Controllers.size());
        UNIT_ASSERT_VALUES_EQUAL(2, list.GetNVMeDevices().size());

        {
            auto error = list.BindNVMeDeviceToVFIO(nvme4);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_ALREADY,
                error.GetCode(),
                FormatError(error));
        }

        UNIT_ASSERT_VALUES_EQUAL(1, NVMeManager->BoundToVFIO.size());
        UNIT_ASSERT_VALUES_EQUAL(5, NVMeManager->Controllers.size());

        {
            auto error = list.ResetNVMeDevice(nvme4);
            UNIT_ASSERT_C(!HasError(error), FormatError(error));
        }

        UNIT_ASSERT_VALUES_EQUAL(0, NVMeManager->BoundToVFIO.size());
        UNIT_ASSERT_VALUES_EQUAL(6, NVMeManager->Controllers.size());
    }

    Y_UNIT_TEST_F(ShouldFindBoundNVMeDevicesInCache, TFixture)
    {
        const TString nvme4sn = NVMeManager->Controllers[4].SerialNumber;
        const NNvme::TPCIAddress nvme4pci =
            NVMeManager->Controllers[4].PCIAddress;

        NProto::TDiskAgentConfig configProto;
        configProto.AddNvmeDevices()->SetSerialNumber(
            NVMeManager->Controllers[3].SerialNumber);
        configProto.AddNvmeDevices()->SetSerialNumber(nvme4sn);

        {
            TNVMeDeviceList list{
                NVMeManager,
                CreateConfig(configProto),
                Logging->CreateLog("NVMe")};

            auto error = list.BindNVMeDeviceToVFIO(nvme4sn);
            UNIT_ASSERT_C(!HasError(error), FormatError(error));
        }

        // nvme4 is bound to vfio

        UNIT_ASSERT_VALUES_EQUAL(1, NVMeManager->BoundToVFIO.size());
        UNIT_ASSERT_VALUES_EQUAL(5, NVMeManager->Controllers.size());

        {
            TNVMeDeviceList list{
                NVMeManager,
                CreateConfig(configProto),
                Logging->CreateLog("NVMe")};

            // w/o cache nvme4 can't be found
            const auto& devices = list.GetNVMeDevices();
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());

            auto* device = FindIfPtr(devices, [&] (const auto& device) {
                return device.GetSerialNumber() == nvme4sn;
            });
            UNIT_ASSERT(!device);
        }

        {
            const auto error = NVMeManager->BindToNVME(nvme4pci);
            UNIT_ASSERT_C(!HasError(error), FormatError(error));
        }

        configProto.SetCachedNVMeDevicesPath(CachedNVMeDevicesPath);

        UNIT_ASSERT_VALUES_EQUAL(0, NVMeManager->BoundToVFIO.size());
        UNIT_ASSERT_VALUES_EQUAL(6, NVMeManager->Controllers.size());

        {
            TNVMeDeviceList list{
                NVMeManager,
                CreateConfig(configProto),
                Logging->CreateLog("NVMe")};

            auto error = list.BindNVMeDeviceToVFIO(nvme4sn);
            UNIT_ASSERT_C(!HasError(error), FormatError(error));
        }

        UNIT_ASSERT_VALUES_EQUAL(1, NVMeManager->BoundToVFIO.size());
        UNIT_ASSERT_VALUES_EQUAL(5, NVMeManager->Controllers.size());

        // nvme4 should be found in the cache
        {
            TNVMeDeviceList list{
                NVMeManager,
                CreateConfig(configProto),
                Logging->CreateLog("NVMe")};

            const auto& devices = list.GetNVMeDevices();
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());

            auto* device = FindIfPtr(devices, [&] (const auto& device) {
                return device.GetSerialNumber() == nvme4sn;
            });
            UNIT_ASSERT(device);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
