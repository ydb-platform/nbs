#include "service_linux.h"

#include "config.h"
#include "device_provider.h"
#include "sysfs_helpers.h"

#include <cloud/blockstore/libs/nvme/nvme_stub.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/proto_helpers.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/hash.h>
#include <util/stream/file.h>
#include <util/system/tempfile.h>

#include <chrono>
#include <latch>
#include <thread>

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;
using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestLocalNVMeDeviceProvider final: ILocalNVMeDeviceProvider
{
    TPromise<TVector<NProto::TNVMeDevice>> Promise =
        NewPromise<TVector<NProto::TNVMeDevice>>();

    [[nodiscard]] auto ListNVMeDevices() const
        -> TFuture<TVector<NProto::TNVMeDevice>> final
    {
        return Promise;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestSysFs final: ISysFs
{
    THashMap<TString, TString> DeviceToDriver;

    auto GetDriverForPCIDevice(const TString& pciAddr) -> TString final
    {
        return DeviceToDriver.Value(pciAddr, TString());
    }

    void BindPCIDeviceToDriver(
        const TString& pciAddr,
        const TString& driverName) final
    {
        DeviceToDriver[pciAddr] = driverName;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    TString StateCacheFile;
    TVector<NProto::TNVMeDevice> Devices;

    TLocalNVMeConfigPtr Config;
    ILoggingServicePtr Logging;
    std::shared_ptr<TTestLocalNVMeDeviceProvider> DeviceProvider;
    NNvme::INvmeManagerPtr NVMeManager;
    TExecutorPtr Executor;
    std::shared_ptr<TTestSysFs> SysFs;

    ILocalNVMeServicePtr Service;

    void SetUp(NUnitTest::TTestContext& /*testContext*/) final
    {
        SysFs = std::make_shared<TTestSysFs>();

        PrepareDevices();
        PrepareConfigs();

        Logging =
            CreateLoggingService("console", {.FiltrationLevel = TLOG_DEBUG});
        Logging->Start();

        DeviceProvider = std::make_shared<TTestLocalNVMeDeviceProvider>();

        NVMeManager = NNvme::CreateNvmeManagerStub();

        Executor = TExecutor::Create("TestExecutor");
        Executor->Start();

        Service = CreateService();
        Service->Start();
    }

    void TearDown(NUnitTest::TTestContext& /* testContext */) final
    {
        Service->Stop();
        Executor->Stop();
        Logging->Stop();

        if (StateCacheFile) {
            NFs::Remove(StateCacheFile);
        }
    }

    auto CreateService() -> ILocalNVMeServicePtr
    {
        return CreateLocalNVMeService(
            Config,
            Logging,
            std::static_pointer_cast<ILocalNVMeDeviceProvider>(DeviceProvider),
            NVMeManager,
            Executor,
            std::static_pointer_cast<ISysFs>(SysFs));
    }

    void PrepareDevices()
    {
        NProto::TNVMeDeviceList list;
        ParseProtoTextFromString(
            R"(
            Devices {
                SerialNumber: "NVME_0"
                PCIAddress: "f1:00.0"
                IOMMUGroup: 10
                VendorId: 0x100
                DeviceId: 0x200
                Model: "Test NVMe 1"
            }
            Devices {
                SerialNumber: "NVME_1"
                PCIAddress: "31:00.0"
                IOMMUGroup: 20
                VendorId: 0x300
                DeviceId: 0x400
                Model: "Test NVMe 2"
            }
            Devices {
                SerialNumber: "NVME_2"
                PCIAddress: "33:00.0"
                IOMMUGroup: 30
                VendorId: 0x100
                DeviceId: 0x200
                Model: "Test NVMe 1"
            }
            Devices {
                SerialNumber: "NVME_3"
                PCIAddress: "34:00.0"
                IOMMUGroup: 40
                VendorId: 0x100
                DeviceId: 0x200
                Model: "Test NVMe 1"
            }
        )",
            list);

        UNIT_ASSERT_VALUES_EQUAL(4, list.DevicesSize());

        Devices.assign(
            std::make_move_iterator(list.MutableDevices()->begin()),
            std::make_move_iterator(list.MutableDevices()->end()));

        for (const auto& device: Devices) {
            SysFs->DeviceToDriver[device.GetPCIAddress()] = "nvme";
        }
    }

    void PrepareConfigs()
    {
        StateCacheFile = MakeTempName(nullptr, "nvme");

        NProto::TLocalNVMeConfig proto;
        proto.SetStateCacheFilePath(StateCacheFile);

        Config = std::make_shared<TLocalNVMeConfig>(proto);
    }

    auto ListNVMeDevices()
    {
        for (;;) {
            auto future = Service->ListNVMeDevices();
            auto r = future.GetValueSync();
            if (!HasError(r) || r.GetError().GetCode() != E_REJECTED) {
                return r;
            }
            Sleep(100ms);
        }
    }

    auto LoadStateFromCache() const
    {
        NProto::TLocalNVMeServiceState proto;
        ParseProtoTextFromFile(Config->GetStateCacheFilePath(), proto);

        auto bySerialNumber = [](const auto& device)
        {
            return device.GetSerialNumber();
        };

        SortBy(*proto.MutableDevices(), bySerialNumber);
        SortBy(*proto.MutableAcquiredDevices(), bySerialNumber);

        return proto;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLocalNVMeServiceTest)
{
    Y_UNIT_TEST_F(ShouldListDevices, TFixture)
    {
        {
            auto future = Service->ListNVMeDevices();
            auto [_, error] = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                error.GetCode(),
                FormatError(error));
        }

        DeviceProvider->Promise.SetValue(Devices);

        {
            auto [devices, error] = ListNVMeDevices();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));

            UNIT_ASSERT_VALUES_EQUAL(Devices.size(), devices.size());
            for (size_t i = 0; i != Devices.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    Devices[i].DebugString(),
                    devices[i].DebugString(),
                    "#" << i);
            }
        }
    }

    Y_UNIT_TEST_F(ShouldAcquireAndReleaseDevice, TFixture)
    {
        const auto& device = Devices[0];
        const auto& pciAddr = device.GetPCIAddress();
        const auto& serialNumber = device.GetSerialNumber();

        for (const auto& sn: {serialNumber, TString("UNK")}) {
            {
                auto future = Service->AcquireNVMeDevice(sn);
                const auto& error = future.GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(
                    E_REJECTED,
                    error.GetCode(),
                    FormatError(error));
            }

            {
                auto future = Service->ReleaseNVMeDevice(sn);
                const auto& error = future.GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(
                    E_REJECTED,
                    error.GetCode(),
                    FormatError(error));
            }
        }

        DeviceProvider->Promise.SetValue(Devices);

        {
            auto [_, error] = ListNVMeDevices();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        {
            auto future = Service->AcquireNVMeDevice("UNK");
            const auto& error = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_NOT_FOUND,
                error.GetCode(),
                FormatError(error));
        }

        {
            auto future = Service->ReleaseNVMeDevice("UNK");
            const auto& error = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_NOT_FOUND,
                error.GetCode(),
                FormatError(error));
        }

        UNIT_ASSERT_VALUES_EQUAL("nvme", SysFs->DeviceToDriver[pciAddr]);

        {
            auto future = Service->AcquireNVMeDevice(serialNumber);
            const auto& error = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        UNIT_ASSERT_VALUES_EQUAL("vfio-pci", SysFs->DeviceToDriver[pciAddr]);

        {
            auto future = Service->ReleaseNVMeDevice(serialNumber);
            const auto& error = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        UNIT_ASSERT_VALUES_EQUAL("nvme", SysFs->DeviceToDriver[pciAddr]);
    }

    Y_UNIT_TEST_F(ShouldHandleListDevicesError, TFixture)
    {
        DeviceProvider->Promise.SetException(
            std::make_exception_ptr(std::runtime_error{"fail"}));

        {
            auto [_, error] = ListNVMeDevices();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FAIL,
                error.GetCode(),
                FormatError(error));
        }

        {
            auto [_, error] = ListNVMeDevices();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FAIL,
                error.GetCode(),
                FormatError(error));
        }
    }

    Y_UNIT_TEST_F(ShouldAcquireAndReleaseDeviceMT, TFixture)
    {
        const int threadNum = 4;
        const int requestNum = 32;

        std::latch start{threadNum + 1};

        TVector<std::thread> threads;
        for (int i = 0; i != threadNum; ++i) {
            threads.emplace_back(
                [&]
                {
                    start.arrive_and_wait();

                    TVector<TFuture<NProto::TError>> futures;

                    for (int i = 0; i != requestNum; ++i) {
                        futures.push_back(Service->AcquireNVMeDevice("UNK"));
                        futures.push_back(Service->ReleaseNVMeDevice("UNK"));
                        Sleep(10ms);
                    }

                    for (const auto& f: futures) {
                        const auto& error = f.GetValueSync();
                        UNIT_ASSERT_VALUES_EQUAL_C(
                            E_NOT_FOUND,
                            error.GetCode(),
                            FormatError(error));
                    }
                });
        }

        DeviceProvider->Promise.SetValue(Devices);
        {
            auto [_, error] = ListNVMeDevices();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        start.arrive_and_wait();

        for (auto& t: threads) {
            t.join();
        }
    }

    Y_UNIT_TEST_F(ShouldUpdateStateCache, TFixture)
    {
        DeviceProvider->Promise.SetValue(Devices);
        {
            auto [_, error] = ListNVMeDevices();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        {
            auto proto = LoadStateFromCache();
            UNIT_ASSERT_VALUES_EQUAL_C(
                Devices.size(),
                proto.DevicesSize(),
                proto);
            for (size_t i = 0; i != Devices.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    Devices[i].DebugString(),
                    proto.GetDevices(i).DebugString(),
                    "#" << i);
            }
            UNIT_ASSERT_VALUES_EQUAL(0, proto.AcquiredDevicesSize());
        }

        {
            auto future = Service->AcquireNVMeDevice("NVME_0");
            const auto& error = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        {
            auto future = Service->AcquireNVMeDevice("NVME_2");
            const auto& error = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        {
            auto proto = LoadStateFromCache();
            UNIT_ASSERT_VALUES_EQUAL_C(
                Devices.size(),
                proto.DevicesSize(),
                proto);
            for (size_t i = 0; i != Devices.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    Devices[i].DebugString(),
                    proto.GetDevices(i).DebugString(),
                    "#" << i);
            }
            UNIT_ASSERT_VALUES_EQUAL(2, proto.AcquiredDevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "NVME_0",
                proto.GetAcquiredDevices(0).GetSerialNumber());
            UNIT_ASSERT_VALUES_EQUAL(
                "NVME_2",
                proto.GetAcquiredDevices(1).GetSerialNumber());
        }

        {
            auto future = Service->ReleaseNVMeDevice("NVME_0");
            const auto& error = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        {
            auto proto = LoadStateFromCache();
            UNIT_ASSERT_VALUES_EQUAL_C(
                Devices.size(),
                proto.DevicesSize(),
                proto);
            for (size_t i = 0; i != Devices.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    Devices[i].DebugString(),
                    proto.GetDevices(i).DebugString(),
                    "#" << i);
            }
            UNIT_ASSERT_VALUES_EQUAL(1, proto.AcquiredDevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "NVME_2",
                proto.GetAcquiredDevices(0).GetSerialNumber());
        }
    }

    Y_UNIT_TEST_F(ShouldRestoreStateFromCache, TFixture)
    {
        Service->Stop();
        Service = nullptr;

        {
            NProto::TLocalNVMeServiceState proto;
            proto.MutableDevices()->Assign(Devices.begin(), Devices.end());

            proto.AddAcquiredDevices()->SetSerialNumber(
                Devices[0].GetSerialNumber());
            proto.AddAcquiredDevices()->SetSerialNumber(
                Devices[2].GetSerialNumber());
            SerializeToTextFormat(proto, Config->GetStateCacheFilePath());
        }

        Service = CreateService();
        Service->Start();

        {
            auto [devices, error] = ListNVMeDevices();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_VALUES_EQUAL(Devices.size(), devices.size());
            for (size_t i = 0; i != Devices.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    Devices[i].DebugString(),
                    devices[i].DebugString(),
                    "#" << i);
            }
        }

        {
            auto future =
                Service->AcquireNVMeDevice(Devices[0].GetSerialNumber());
            const auto& error = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                error.GetCode(),
                FormatError(error));
        }

        {
            auto future =
                Service->AcquireNVMeDevice(Devices[1].GetSerialNumber());
            const auto& error = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        {
            auto future =
                Service->AcquireNVMeDevice(Devices[2].GetSerialNumber());
            const auto& error = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                error.GetCode(),
                FormatError(error));
        }
    }

    Y_UNIT_TEST_F(ShouldFetchDevicesFromProviderIfCacheIsEmpty, TFixture)
    {
        Service->Stop();
        Service = nullptr;

        {
            NProto::TLocalNVMeServiceState proto;
            SerializeToTextFormat(proto, Config->GetStateCacheFilePath());
        }

        Service = CreateService();
        Service->Start();

        DeviceProvider->Promise.SetValue(Devices);

        {
            auto [devices, error] = ListNVMeDevices();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_VALUES_EQUAL(Devices.size(), devices.size());
            for (size_t i = 0; i != Devices.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    Devices[i].DebugString(),
                    devices[i].DebugString(),
                    "#" << i);
            }
        }
    }

    Y_UNIT_TEST_F(ShouldRejectBrokenCacheFile, TFixture)
    {
        Service->Stop();
        Service = nullptr;

        {
            TFileOutput file{Config->GetStateCacheFilePath()};
            file << "broken ;;";
        }

        Service = CreateService();
        Service->Start();

        DeviceProvider->Promise.SetValue(Devices);

        {
            auto [devices, error] = ListNVMeDevices();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_VALUES_EQUAL(Devices.size(), devices.size());
            for (size_t i = 0; i != Devices.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    Devices[i].DebugString(),
                    devices[i].DebugString(),
                    "#" << i);
            }
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
