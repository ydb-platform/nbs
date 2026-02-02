#include "service.h"

#include "device_provider.h"

#include <cloud/blockstore/libs/nvme/nvme_stub.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/proto_helpers.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>

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

struct TFixture: public NUnitTest::TBaseFixture
{
    TVector<NProto::TNVMeDevice> Devices;

    ILoggingServicePtr Logging;
    std::shared_ptr<TTestLocalNVMeDeviceProvider> DeviceProvider;
    NNvme::INvmeManagerPtr NVMeManager;
    TExecutorPtr Executor;

    ILocalNVMeServicePtr Service;

    void SetUp(NUnitTest::TTestContext& /*testContext*/) final
    {
        PrepareDevices();

        Logging =
            CreateLoggingService("console", {.FiltrationLevel = TLOG_DEBUG});
        Logging->Start();

        DeviceProvider = std::make_shared<TTestLocalNVMeDeviceProvider>();

        NVMeManager = NNvme::CreateNvmeManagerStub();

        Executor = TExecutor::Create("TestExecutor");
        Executor->Start();

        Service = CreateLocalNVMeService(
            Logging,
            std::static_pointer_cast<ILocalNVMeDeviceProvider>(DeviceProvider),
            NVMeManager,
            Executor);
        Service->Start();
    }

    void TearDown(NUnitTest::TTestContext& /* testContext */) final
    {
        Service->Stop();
        Executor->Stop();
        Logging->Stop();
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
        )",
            list);

        UNIT_ASSERT_VALUES_EQUAL(3, list.DevicesSize());

        Devices.assign(
            std::make_move_iterator(list.MutableDevices()->begin()),
            std::make_move_iterator(list.MutableDevices()->end()));
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

            for (size_t i = 0; i != devices.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    Devices[i].DebugString(),
                    devices[i].DebugString(),
                    "#" << i);
            }
        }
    }

    Y_UNIT_TEST_F(ShouldAcquireAndReleaseDevice, TFixture)
    {
        for (TString sn: {"NVME_0", "UNK"}) {
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

        {
            auto future = Service->AcquireNVMeDevice("NVME_0");
            const auto& error = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        {
            auto future = Service->ReleaseNVMeDevice("NVME_0");
            const auto& error = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }
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
}

}   // namespace NCloud::NBlockStore::NStorage
