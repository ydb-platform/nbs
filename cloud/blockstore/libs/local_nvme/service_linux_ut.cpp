#include "service_linux.h"

#include "config.h"
#include "device_provider.h"
#include "sysfs_helpers.h"
#include "test_grpc_device_provider.h"

#include <cloud/blockstore/libs/nvme/nvme_stub.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/helpers.h>
#include <cloud/storage/core/libs/common/proto_helpers.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/protobuf/util/pb_io.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/system/env.h>
#include <util/system/event.h>
#include <util/system/tempfile.h>

#include <chrono>
#include <latch>
#include <thread>

namespace NCloud::NBlockStore {

using namespace NNvme;
using namespace NThreading;
using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

const TDuration DefaultTimeout = 5s;
const ui8 AllowedAdminOpcodes[] = {0x1, 0x2, 0x3, 0xB};
const ui8 AllowedSetFeatureIds[] = {0x1, 0x4, 0x5};

struct TTestLocalNVMeDeviceProvider final: ILocalNVMeDeviceProvider
{
    using TListNVMeDevicesFn =
        std::function<TFuture<TVector<NProto::TNVMeDevice>>()>;

    TPromise<TListNVMeDevicesFn> ListNVMeDevicesImpl =
        NewPromise<TListNVMeDevicesFn>();

    void Start() final
    {}

    void Stop() final
    {}

    [[nodiscard]] auto ListNVMeDevices()
        -> TFuture<TVector<NProto::TNVMeDevice>> final
    {
        return ListNVMeDevicesImpl.GetFuture().Apply(
            [](const auto& future)
            {
                const auto& func = future.GetValue();
                return func();
            });
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TSanitizeInfo: TSanitizeStatus
{
    ui32 Count = 0;
};

struct TLockdownImpl
{
    std::function<TResultOrError<TLockdownState>(const TString& ctrlPath)>
        GetLockdownState = [](const TString&)
    {
        return TLockdownState{.Supported = false};
    };

    std::function<
        NProto::TError(const TString& ctrlPath, const TLockdownConfig& config)>
        EnsureLockdown = [](const TString&, const NNvme::TLockdownConfig&)
    {
        return MakeError(
            MAKE_SYSTEM_ERROR(ENOTSUP),
            "Lockdown command is not supported");
    };
};

class TTestNVMeManager: public INvmeManager
{
private:
    std::mutex Mutex;
    THashMap<TString, TSanitizeInfo> SanitizeInfo;
    std::shared_ptr<TLockdownImpl> LockdownImpl;

    TAutoEvent SanitizeRequested;

public:
    explicit TTestNVMeManager(std::shared_ptr<TLockdownImpl> lockdownImpl)
        : LockdownImpl(std::move(lockdownImpl))
    {}

    void Start() final
    {}

    void Stop() final
    {}

    auto Format(const TString& path, nvme_secure_erase_setting ses)
        -> TFuture<NProto::TError> final
    {
        Y_UNUSED(path, ses);

        return MakeFuture(MakeError(S_OK));
    }

    auto Deallocate(const TString& path, ui64 offsetBytes, ui64 sizeBytes)
        -> TFuture<NProto::TError> final
    {
        Y_UNUSED(path, offsetBytes, sizeBytes);

        return MakeFuture(MakeError(S_OK));
    }

    auto StartSanitize(const TString& ctrlPath) -> NProto::TError final
    {
        std::unique_lock lock{Mutex};

        auto& info = SanitizeInfo[ctrlPath];
        if (info.Status.GetCode() == E_TRY_AGAIN) {
            return MakeError(
                E_INVALID_STATE,
                "previous sanitize operation has not been completed yet");
        }

        info.Status = MakeError(E_TRY_AGAIN);
        info.Progress = 0;
        info.Count += 1;

        SanitizeRequested.Signal();

        return {};
    }

    auto GetSanitizeStatus(const TString& ctrlPath)
        -> TResultOrError<TSanitizeStatus> final
    {
        std::unique_lock lock{Mutex};

        return SanitizeInfo.Value(ctrlPath, TSanitizeInfo{});
    }

    auto IsSsd(const TString& path) -> TResultOrError<bool> final
    {
        Y_UNUSED(path);

        return true;
    }

    auto GetSerialNumber(const TString& path) -> TResultOrError<TString> final
    {
        Y_UNUSED(path);

        return TString();
    }

    auto GetDeviceModel(const TString& path) -> TResultOrError<TString> final
    {
        Y_UNUSED(path);

        return TString();
    }

    NProto::TError ResetToSingleNamespace(const TString& ctrlPath) final
    {
        Y_UNUSED(ctrlPath);

        return {};
    }

    TResultOrError<TLockdownState> GetLockdownState(
        const TString& ctrlPath) final
    {
        std::unique_lock lock{Mutex};
        return LockdownImpl->GetLockdownState(ctrlPath);
    }

    NProto::TError EnsureLockdown(
        const TString& ctrlPath,
        const TLockdownConfig& config) final
    {
        std::unique_lock lock{Mutex};
        return LockdownImpl->EnsureLockdown(ctrlPath, config);
    }

public:
    void WaitSanitizeRequested()
    {
        SanitizeRequested.WaitI();
    }

    void UpdateSanitizeStatus(
        const TString& ctrlPath,
        NProto::TError status,
        double progress)
    {
        std::unique_lock lock{Mutex};

        auto& info = SanitizeInfo[ctrlPath];

        info.Status = status;
        info.Progress = progress;
    }

    auto GetSanitizeInfo(const TString& ctrlPath) -> TSanitizeInfo
    {
        std::unique_lock lock{Mutex};

        return SanitizeInfo.Value(ctrlPath, TSanitizeInfo{});
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestSysFs final: ISysFs
{
    THashMap<TString, TString> AddrToDriver;
    THashMap<TString, TString> DeviceToCtrl;
    THashMap<TString, NProto::TNVMeDevice> AddrToDevice;

    std::function<TString(const TString&)> GetVfioDeviceForPCIDeviceImpl;

    auto GetDriverForPCIDevice(const TString& pciAddr) -> TString final
    {
        return AddrToDriver.Value(pciAddr, TString());
    }

    void BindPCIDeviceToDriver(
        const TString& pciAddr,
        const TString& driverName) final
    {
        AddrToDriver[pciAddr] = driverName;
    }

    auto GetNVMeCtrlNameFromPCIAddr(const TString& pciAddr) -> TString final
    {
        return DeviceToCtrl.Value(pciAddr, TString());
    }

    auto GetNVMeDeviceFromPCIAddr(const TString& pciAddr)
        -> NProto::TNVMeDevice final
    {
        const auto* device = AddrToDevice.FindPtr(pciAddr);
        Y_ENSURE(device);
        return *device;
    }

    auto GetVfioDeviceForPCIDevice(const TString& pciAddr) -> TString final
    {
        return GetVfioDeviceForPCIDeviceImpl
                   ? GetVfioDeviceForPCIDeviceImpl(pciAddr)
                   : TString{};
    }

    [[nodiscard]] auto IsVfioDevSupported() const -> bool final
    {
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFixtureBase: public NUnitTest::TBaseFixture
{
    const TString EmptyIdempotenceId;

    TString StateCacheFile;
    TVector<NProto::TNVMeDevice> Devices;

    TLocalNVMeConfigPtr Config;
    ILoggingServicePtr Logging;
    IMonitoringServicePtr Monitoring;
    std::shared_ptr<TLockdownImpl> LockdownImpl;
    std::shared_ptr<TTestNVMeManager> NVMeManager;
    TExecutorPtr Executor;
    ITaskQueuePtr BackgroundThreadPool;
    std::shared_ptr<TTestSysFs> SysFs;

    ILocalNVMeServicePtr Service;

    void SetUp(NUnitTest::TTestContext& /*testContext*/) override
    {
        SysFs = std::make_shared<TTestSysFs>();

        PrepareDevices();
        PrepareConfigs();

        Logging =
            CreateLoggingService("console", {.FiltrationLevel = TLOG_DEBUG});
        Logging->Start();

        Monitoring = CreateMonitoringServiceStub();
        Monitoring->Start();

        LockdownImpl = std::make_shared<TLockdownImpl>();
        NVMeManager = std::make_shared<TTestNVMeManager>(LockdownImpl);

        BackgroundThreadPool = CreateLongRunningTaskExecutor("BG");
        BackgroundThreadPool->Start();

        Executor = TExecutor::Create("TestExecutor");
        Executor->Start();
    }

    void TearDown(NUnitTest::TTestContext& /* testContext */) override
    {
        Service->Stop();
        Executor->Stop();
        BackgroundThreadPool->Stop();
        Monitoring->Stop();
        Logging->Stop();

        if (StateCacheFile) {
            NFs::Remove(StateCacheFile);
        }
    }

    auto CreateService(ILocalNVMeDeviceProviderPtr deviceProvider) const
        -> ILocalNVMeServicePtr
    {
        return CreateLocalNVMeService(
            Config,
            Logging,
            Monitoring,
            std::move(deviceProvider),
            std::static_pointer_cast<INvmeManager>(NVMeManager),
            Executor,
            BackgroundThreadPool,
            std::static_pointer_cast<ISysFs>(SysFs));
    }

    void PrepareDevices()
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
                FirmwareRev: "FW4242"
            }
            Devices {
                SerialNumber: "NVME_1"
                PCIAddress: "0000:31:00.0"
                IOMMUGroup: 20
                VendorId: 0x300
                DeviceId: 0x400
                Model: "Test NVMe 2"
                FirmwareRev: "FW4243"
                DeviceState: NVME_DEVICE_STATE_MAINTENANCE
            }
            Devices {
                SerialNumber: "NVME_2"
                PCIAddress: "0000:33:00.0"
                IOMMUGroup: 30
                VendorId: 0x100
                DeviceId: 0x200
                Model: "Test NVMe 1"
                FirmwareRev: "FW4242"
            }
            Devices {
                SerialNumber: "NVME_3"
                PCIAddress: "0000:34:00.0"
                IOMMUGroup: 40
                VendorId: 0x100
                DeviceId: 0x200
                Model: "Test NVMe 1"
                FirmwareRev: "FW4242"
            }
        )",
            list);

        UNIT_ASSERT_VALUES_EQUAL(4, list.DevicesSize());

        Devices.assign(
            std::make_move_iterator(list.MutableDevices()->begin()),
            std::make_move_iterator(list.MutableDevices()->end()));

        for (size_t i = 0; i != Devices.size(); ++i) {
            const auto& device = Devices[i];
            const auto& pciAddr = device.GetPCIAddress();

            SysFs->AddrToDriver[pciAddr] = "nvme";
            SysFs->DeviceToCtrl[pciAddr] = TStringBuilder() << "nvme" << i;
            SysFs->AddrToDevice[pciAddr] = device;
        }
    }

    void PrepareConfigs()
    {
        StateCacheFile = MakeTempName(nullptr, "nvme");

        NProto::TLocalNVMeConfig proto;
        proto.SetStateCacheFilePath(StateCacheFile);
        proto.SetUpdateDevicesInterval(TDuration(1s).MilliSeconds());
        proto.SetUpdateCountersInterval(TDuration(100ms).MilliSeconds());

        auto& lockdownConfig = *proto.MutableLockdownConfig();

        lockdownConfig.MutableAllowedAdminOpcodes()->Assign(
            std::begin(AllowedAdminOpcodes),
            std::end(AllowedAdminOpcodes));

        lockdownConfig.MutableAllowedSetFeatureIds()->Assign(
            std::begin(AllowedSetFeatureIds),
            std::end(AllowedSetFeatureIds));

        lockdownConfig.SetBlockLockdownCommand(true);

        Config = std::make_shared<TLocalNVMeConfig>(proto);
    }

    auto ListNVMeDevices() const
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

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public TFixtureBase
{
    std::shared_ptr<TTestLocalNVMeDeviceProvider> DeviceProvider;

    void SetUp(NUnitTest::TTestContext& testContext) override
    {
        TFixtureBase::SetUp(testContext);

        DeviceProvider = std::make_shared<TTestLocalNVMeDeviceProvider>();

        Service = CreateService();
        Service->Start();
    }

    auto CreateService() -> ILocalNVMeServicePtr
    {
        return TFixtureBase::CreateService(
            std::static_pointer_cast<ILocalNVMeDeviceProvider>(DeviceProvider));
    }

    void TearDown(NUnitTest::TTestContext& testContext) override
    {
        TFixtureBase::TearDown(testContext);
        DeviceProvider.reset();
    }

    auto CreateDeviceList() const -> TVector<NProto::TNVMeDevice>
    {
        TVector<NProto::TNVMeDevice> devices;

        devices.reserve(Devices.size());

        for (const auto& src: Devices) {
            // We only populate serial number and PCI address here;
            // the rest of the device info is resolved by the Service via sysfs.

            auto& device = devices.emplace_back();
            device.SetSerialNumber(src.GetSerialNumber());

            // chop "0000:"
            device.SetPCIAddress(src.GetPCIAddress().substr(5));

            device.SetDeviceState(src.GetDeviceState());
        }

        // Add some noise

        {
            auto& device = devices.emplace_back();
            device.SetPCIAddress("unexpected");
        }

        {
            auto& device = devices.emplace_back();
            device.SetSerialNumber("unexpected");
        }

        return devices;
    }

    void SetProviderReady() const
    {
        DeviceProvider->ListNVMeDevicesImpl.SetValue(
            [&] { return MakeFuture(CreateDeviceList()); });
    }

    void WaitCountersUpdate()
    {
        Executor
            ->Execute(
                [&]
                {
                    RunningCont()->SleepT(
                        Config->GetUpdateCountersInterval() * 2);
                })
            .Wait();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFixtureInfra: public TFixtureBase
{
    ILocalNVMeDeviceProviderPtr DeviceProvider;

    void SetUp(NUnitTest::TTestContext& testContext) override
    {
        TFixtureBase::SetUp(testContext);

        DeviceProvider = CreateTestGrpcDeviceProvider(
            Logging,
            "unix://nbs@" + GetEnv("INFRA_DEVICE_PROVIDER_SOCKET"));
        DeviceProvider->Start();

        Service = CreateService(
            std::static_pointer_cast<ILocalNVMeDeviceProvider>(DeviceProvider));
        Service->Start();
    }

    void TearDown(NUnitTest::TTestContext& testContext) override
    {
        TFixtureBase::TearDown(testContext);

        DeviceProvider->Stop();
        DeviceProvider.reset();
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

        SetProviderReady();

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

    Y_UNIT_TEST_F(ShouldRetryGetVfioDevName, TFixture)
    {
        SetProviderReady();

        {
            auto [devices, error] = ListNVMeDevices();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        const TString vfioDev = "vfio0";
        const auto& device = Devices[0];

        const ui32 successfulAttempt = 3;
        std::atomic<ui32> attempts = 0;
        SysFs->GetVfioDeviceForPCIDeviceImpl =
            [&](const TString& pciAddr) mutable -> TString
        {
            if (device.GetPCIAddress() != pciAddr) {
                return {};
            }

            if (attempts != successfulAttempt) {
                ++attempts;
                return {};
            }

            return vfioDev;
        };

        auto future = Service->AcquireNVMeDevice(
            device.GetSerialNumber(),
            EmptyIdempotenceId);

        const auto& [r, error] = future.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));
        UNIT_ASSERT_VALUES_EQUAL(device.GetSerialNumber(), r.GetSerialNumber());
        UNIT_ASSERT_VALUES_EQUAL(vfioDev, r.GetVfioDevName());
        UNIT_ASSERT_VALUES_EQUAL(successfulAttempt, attempts.load());
    }

    Y_UNIT_TEST_F(ShouldAcquireAndReleaseDevice, TFixture)
    {
        const auto& device = Devices[0];
        const auto& pciAddr = device.GetPCIAddress();
        const auto& serialNumber = device.GetSerialNumber();

        for (const auto& sn: {serialNumber, TString("UNK")}) {
            {
                auto future =
                    Service->AcquireNVMeDevice(sn, EmptyIdempotenceId);
                const auto& [_, error] = future.GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(
                    E_REJECTED,
                    error.GetCode(),
                    FormatError(error));
            }

            {
                auto future =
                    Service->ReleaseNVMeDevice(sn, EmptyIdempotenceId);
                const auto& error = future.GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(
                    E_REJECTED,
                    error.GetCode(),
                    FormatError(error));
            }
        }

        SetProviderReady();

        {
            auto [devices, error] = ListNVMeDevices();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));

            for (const auto& device: devices) {
                UNIT_ASSERT_VALUES_EQUAL("", device.GetVfioDevName());
            }
        }

        {
            auto future = Service->AcquireNVMeDevice("UNK", EmptyIdempotenceId);
            const auto& [_, error] = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_NOT_FOUND,
                error.GetCode(),
                FormatError(error));
        }

        {
            auto future = Service->ReleaseNVMeDevice("UNK", EmptyIdempotenceId);
            const auto& error = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_NOT_FOUND,
                error.GetCode(),
                FormatError(error));
        }

        UNIT_ASSERT_VALUES_EQUAL("nvme", SysFs->AddrToDriver[pciAddr]);

        {
            const TString vfioDev = "vfio0";

            SysFs->GetVfioDeviceForPCIDeviceImpl =
                [&](const TString& pciAddr) -> TString
            {
                if (pciAddr == device.GetPCIAddress()) {
                    return vfioDev;
                }
                return {};
            };

            auto future =
                Service->AcquireNVMeDevice(serialNumber, EmptyIdempotenceId);
            const auto& [device, error] = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));

            UNIT_ASSERT_VALUES_EQUAL(serialNumber, device.GetSerialNumber());
            UNIT_ASSERT_VALUES_EQUAL(vfioDev, device.GetVfioDevName());
        }

        const TString ctrlPath = "/dev/nvme0";

        UNIT_ASSERT_VALUES_EQUAL("vfio-pci", SysFs->AddrToDriver[pciAddr]);

        {
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                NVMeManager->GetSanitizeInfo(ctrlPath).Count);

            auto future =
                Service->ReleaseNVMeDevice(serialNumber, EmptyIdempotenceId);

            NVMeManager->WaitSanitizeRequested();
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                NVMeManager->GetSanitizeInfo(ctrlPath).Count);

            UNIT_ASSERT(!future.HasValue());

            NVMeManager->UpdateSanitizeStatus(
                ctrlPath,
                MakeError(E_TRY_AGAIN),
                50.0);

            UNIT_ASSERT(!future.Wait(100ms));

            NVMeManager->UpdateSanitizeStatus(ctrlPath, MakeError(S_OK), 100.0);

            const auto& error = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        UNIT_ASSERT_VALUES_EQUAL("nvme", SysFs->AddrToDriver[pciAddr]);
    }

    Y_UNIT_TEST_F(ShouldRetryListDevicesError, TFixture)
    {
        const ui32 successfulAttempt = 3;

        std::atomic<ui32> attempts = 0;

        DeviceProvider->ListNVMeDevicesImpl.SetValue(
            [&]
            {
                ++attempts;
                if (attempts < successfulAttempt) {
                    return MakeErrorFuture<TVector<NProto::TNVMeDevice>>(
                        std::make_exception_ptr(
                            TServiceError{E_IO} << "fail #"
                                                << attempts.load()));
                }

                return MakeFuture(CreateDeviceList());
            });

        for (;;) {
            auto future = Service->ListNVMeDevices();
            const auto& [devices, error] = future.GetValueSync();

            UNIT_ASSERT_GE(successfulAttempt, attempts);

            if (!HasError(error)) {
                UNIT_ASSERT_VALUES_EQUAL(successfulAttempt, attempts.load());
                UNIT_ASSERT_VALUES_EQUAL(Devices.size(), devices.size());

                break;
            }

            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                error.GetCode(),
                FormatError(error));

            Sleep(100ms);
        }
    }

    Y_UNIT_TEST_F(ShouldCancelInitializationOnStop, TFixture)
    {
        TAutoEvent shouldStop;

        TPromise<void> promise = NewPromise();
        DeviceProvider->ListNVMeDevicesImpl.SetValue(
            [&]
            {
                shouldStop.Signal();

                return promise.GetFuture().Apply(
                    [](const auto&) -> TVector<NProto::TNVMeDevice>
                    { ythrow TServiceError{E_IO} << "fail"; });
            });

        const bool ok = shouldStop.WaitT(DefaultTimeout);
        UNIT_ASSERT(ok);

        Service->Stop();

        auto future = Service->ListNVMeDevices();
        const auto& [devices, error] = future.GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            error.GetCode(),
            FormatError(error));

        UNIT_ASSERT_STRING_CONTAINS_C(
            error.GetMessage(),
            "is stopped",
            FormatError(error));

        promise.SetValue();
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
                        futures.push_back(
                            Service
                                ->AcquireNVMeDevice("UNK", EmptyIdempotenceId)
                                .Apply(
                                    [](const auto& future)
                                    { return future.GetValue().GetError(); }));
                        futures.push_back(Service->ReleaseNVMeDevice(
                            "UNK",
                            EmptyIdempotenceId));
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

        SetProviderReady();
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
        SetProviderReady();
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
            auto future =
                Service->AcquireNVMeDevice("NVME_0", EmptyIdempotenceId);
            const auto& [device, error] = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_VALUES_EQUAL("NVME_0", device.GetSerialNumber());
        }

        {
            auto future =
                Service->AcquireNVMeDevice("NVME_2", EmptyIdempotenceId);
            const auto& [device, error] = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_VALUES_EQUAL("NVME_2", device.GetSerialNumber());
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
            const TString ctrlPath = "/dev/nvme0";
            auto future =
                Service->ReleaseNVMeDevice("NVME_0", EmptyIdempotenceId);

            NVMeManager->WaitSanitizeRequested();
            NVMeManager->UpdateSanitizeStatus(ctrlPath, MakeError(S_OK), 100.0);

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
            auto future = Service->AcquireNVMeDevice(
                Devices[0].GetSerialNumber(),
                EmptyIdempotenceId);
            const auto& [_, error] = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_INVALID_STATE,
                error.GetCode(),
                FormatError(error));
        }

        {
            auto future = Service->AcquireNVMeDevice(
                Devices[1].GetSerialNumber(),
                EmptyIdempotenceId);
            const auto& [_, error] = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        {
            auto future = Service->AcquireNVMeDevice(
                Devices[2].GetSerialNumber(),
                EmptyIdempotenceId);
            const auto& [_, error] = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_INVALID_STATE,
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

        SetProviderReady();

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

        SetProviderReady();

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

    Y_UNIT_TEST_F(ShouldHandleRequestsAsync, TFixture)
    {
        SetProviderReady();
        {
            auto [_, error] = ListNVMeDevices();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        const TString ctrlPath1 = "/dev/nvme0";
        const TString ctrlPath2 = "/dev/nvme1";

        auto future1 = Service->ReleaseNVMeDevice(
            Devices[0].GetSerialNumber(),
            EmptyIdempotenceId);
        NVMeManager->WaitSanitizeRequested();
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            NVMeManager->GetSanitizeInfo(ctrlPath1).Count);
        UNIT_ASSERT(!future1.HasValue());

        auto future2 = Service->ReleaseNVMeDevice(
            Devices[1].GetSerialNumber(),
            EmptyIdempotenceId);
        NVMeManager->WaitSanitizeRequested();
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            NVMeManager->GetSanitizeInfo(ctrlPath2).Count);
        UNIT_ASSERT(!future2.HasValue());

        Sleep(100ms);

        NVMeManager->UpdateSanitizeStatus(
            ctrlPath1,
            MakeError(E_TRY_AGAIN),
            66.0);

        NVMeManager->UpdateSanitizeStatus(
            ctrlPath2,
            MakeError(E_TRY_AGAIN),
            66.0);

        Sleep(100ms);

        NVMeManager->UpdateSanitizeStatus(
            ctrlPath1,
            MakeError(E_TRY_AGAIN),
            70.0);

        NVMeManager->UpdateSanitizeStatus(
            ctrlPath2,
            MakeError(E_TRY_AGAIN),
            90.0);

        Sleep(100ms);

        NVMeManager->UpdateSanitizeStatus(
            ctrlPath1,
            MakeError(E_TRY_AGAIN),
            90.0);

        NVMeManager->UpdateSanitizeStatus(ctrlPath2, MakeError(S_OK), 100.0);

        UNIT_ASSERT(!future1.HasValue());

        {
            const auto& error = future2.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        UNIT_ASSERT(!future1.HasValue());

        Sleep(100ms);

        UNIT_ASSERT(!future1.HasValue());

        NVMeManager->UpdateSanitizeStatus(ctrlPath1, MakeError(S_OK), 100.0);

        {
            const auto& error = future2.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }
    }

    Y_UNIT_TEST_F(ShouldFailReleaseIfSanitizeFails, TFixture)
    {
        SetProviderReady();
        {
            auto [_, error] = ListNVMeDevices();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        const TString ctrlPath = "/dev/nvme0";

        auto future = Service->ReleaseNVMeDevice(
            Devices[0].GetSerialNumber(),
            EmptyIdempotenceId);
        NVMeManager->WaitSanitizeRequested();
        NVMeManager->UpdateSanitizeStatus(
            ctrlPath,
            MakeError(E_TRY_AGAIN),
            10.0);
        Sleep(200ms);
        NVMeManager->UpdateSanitizeStatus(
            ctrlPath,
            MakeError(E_FAIL, "fail"),
            0);

        const auto& error = future.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(E_FAIL, error.GetCode(), FormatError(error));
    }

    Y_UNIT_TEST_F(ShouldNotAllowConcurrentRequestsForSameDevice, TFixture)
    {
        SetProviderReady();
        {
            auto [_, error] = ListNVMeDevices();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        const auto& sn = Devices[0].GetSerialNumber();
        const TString ctrlPath = "/dev/nvme0";

        auto future1 = Service->ReleaseNVMeDevice(sn, EmptyIdempotenceId);
        NVMeManager->WaitSanitizeRequested();

        {
            auto future2 = Service->ReleaseNVMeDevice(sn, EmptyIdempotenceId);
            const auto& error = future2.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_TRY_AGAIN,
                error.GetCode(),
                FormatError(error));

            UNIT_ASSERT_C(
                !HasProtoFlag(error.GetFlags(), NProto::EF_SILENT),
                FormatError(error));
        }

        NVMeManager->UpdateSanitizeStatus(ctrlPath, MakeError(S_OK), 100.0);

        {
            const auto& error = future1.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }
    }

    Y_UNIT_TEST_F(ShouldExecuteOperationIdempotentlyByIdempotenceId, TFixture)
    {
        SetProviderReady();
        {
            auto [_, error] = ListNVMeDevices();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        const auto& device = Devices[0];
        const auto& sn = device.GetSerialNumber();
        const TString ctrlPath = "/dev/nvme0";

        SysFs->GetVfioDeviceForPCIDeviceImpl =
            [&](const TString& pciAddr) -> TString
        {
            if (pciAddr == device.GetPCIAddress()) {
                return "vfio0";
            }
            return {};
        };

        {
            auto future = Service->ReleaseNVMeDevice(sn, "idempotence-id-1");
            const auto& error = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_TRY_AGAIN,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_C(
                HasProtoFlag(error.GetFlags(), NProto::EF_SILENT),
                FormatError(error));
            UNIT_ASSERT_STRING_CONTAINS_C(
                error.GetMessage(),
                "Release in progress",
                FormatError(error));
        }

        {
            auto future = Service->ReleaseNVMeDevice(sn, "xxx");
            const auto& error = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_TRY_AGAIN,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_C(
                !HasProtoFlag(error.GetFlags(), NProto::EF_SILENT),
                FormatError(error));
            UNIT_ASSERT_STRING_CONTAINS_C(
                error.GetMessage(),
                "Another operation is in progress",
                FormatError(error));

            UNIT_ASSERT_STRING_CONTAINS_C(
                error.GetMessage(),
                "Release",
                FormatError(error));
        }

        {
            auto future = Service->AcquireNVMeDevice(sn, "idempotence-id-1");
            const auto& [_, error] = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                error.GetCode(),
                FormatError(error));

            UNIT_ASSERT_STRING_CONTAINS_C(
                error.GetMessage(),
                "idempotence id was used for a different operation type",
                FormatError(error));

            UNIT_ASSERT_STRING_CONTAINS_C(
                error.GetMessage(),
                "Release",
                FormatError(error));
        }

        {
            auto future = Service->ReleaseNVMeDevice(sn, EmptyIdempotenceId);
            const auto& error = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_TRY_AGAIN,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_C(
                !HasProtoFlag(error.GetFlags(), NProto::EF_SILENT),
                FormatError(error));
            UNIT_ASSERT_STRING_CONTAINS_C(
                error.GetMessage(),
                "Another operation is in progress",
                FormatError(error));
        }

        NVMeManager->WaitSanitizeRequested();
        NVMeManager->UpdateSanitizeStatus(ctrlPath, MakeError(S_OK), 100.0);

        for (;;) {
            auto future = Service->ReleaseNVMeDevice(sn, "idempotence-id-1");
            const auto& error = future.GetValueSync();
            if (!HasError(error)) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    S_OK,
                    error.GetCode(),
                    FormatError(error));
                break;
            }
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_TRY_AGAIN,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_C(
                HasProtoFlag(error.GetFlags(), NProto::EF_SILENT),
                FormatError(error));
            Sleep(100ms);
        }

        {
            auto future = Service->ReleaseNVMeDevice(sn, "idempotence-id-1");
            const auto& error = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        {
            auto future = Service->AcquireNVMeDevice(sn, "idempotence-id-1");
            const auto& [_, error] = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                error.GetCode(),
                FormatError(error));

            UNIT_ASSERT_STRING_CONTAINS_C(
                error.GetMessage(),
                "idempotence id was used for a different operation type",
                FormatError(error));

            UNIT_ASSERT_STRING_CONTAINS_C(
                error.GetMessage(),
                "Release",
                FormatError(error));
        }

        // Start new operation (sync)

        {
            auto future = Service->AcquireNVMeDevice(sn, EmptyIdempotenceId);
            const auto& [_, error] = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        {
            auto future = Service->ReleaseNVMeDevice(sn, EmptyIdempotenceId);

            NVMeManager->WaitSanitizeRequested();
            NVMeManager->UpdateSanitizeStatus(ctrlPath, MakeError(S_OK), 100.0);

            const auto& error = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        for (;;) {
            auto future = Service->AcquireNVMeDevice(sn, "idempotence-id-2");
            const auto& [_, error] = future.GetValueSync();
            if (!HasError(error)) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    S_OK,
                    error.GetCode(),
                    FormatError(error));
                break;
            }
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_TRY_AGAIN,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_C(
                HasProtoFlag(error.GetFlags(), NProto::EF_SILENT),
                FormatError(error));
            Sleep(100ms);
        }

        {
            auto future = Service->AcquireNVMeDevice(sn, "idempotence-id-2");
            const auto& [device, error] = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        {
            auto future = Service->AcquireNVMeDevice(sn, "idempotence-id-3");
            const auto& [device, error] = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_TRY_AGAIN,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_C(
                HasProtoFlag(error.GetFlags(), NProto::EF_SILENT),
                FormatError(error));
        }

        for (;;) {
            auto future = Service->AcquireNVMeDevice(sn, "idempotence-id-3");
            const auto& [_, error] = future.GetValueSync();

            UNIT_ASSERT_C(HasError(error), FormatError(error));

            if (error.GetCode() == E_INVALID_STATE) {
                break;
            }

            Sleep(100ms);
        }
    }

    Y_UNIT_TEST_F(ShouldCancelOperationOnStopping, TFixture)
    {
        SetProviderReady();
        {
            auto [_, error] = ListNVMeDevices();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        const TString ctrlPath = "/dev/nvme0";
        const TString idempotenceId1 = "1";

        {
            auto future1 =
                Service->ReleaseNVMeDevice(Devices[0].GetSerialNumber(), "xxx");
            const auto& error = future1.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_TRY_AGAIN,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_C(
                HasProtoFlag(error.GetFlags(), NProto::EF_SILENT),
                FormatError(error));
        }

        NVMeManager->WaitSanitizeRequested();

        auto future2 =
            Service->ReleaseNVMeDevice(Devices[1].GetSerialNumber(), "");

        NVMeManager->WaitSanitizeRequested();

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            NVMeManager->GetSanitizeInfo(ctrlPath).Count);

        UNIT_ASSERT(!future2.HasValue());

        Service->Stop();

        {
            auto future1 =
                Service->ReleaseNVMeDevice(Devices[0].GetSerialNumber(), "xxx");
            const auto& error = future1.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                error.GetCode(),
                FormatError(error));
        }

        UNIT_ASSERT(future2.HasValue());

        const auto& error = future2.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            error.GetCode(),
            FormatError(error));
    }

    Y_UNIT_TEST_F(ShouldStartWithoutNvmeDevicesAndDiscoverThemLater, TFixture)
    {
        const ui32 successfulAttempt = 3;

        TAutoEvent lastRequest;
        std::atomic<ui32> attempts = 0;

        DeviceProvider->ListNVMeDevicesImpl.SetValue(
            [&]
            {
                ++attempts;
                if (attempts < successfulAttempt) {
                    return MakeFuture(TVector<NProto::TNVMeDevice>());
                }

                lastRequest.Signal();

                return MakeFuture(CreateDeviceList());
            });

        {
            auto [devices, error] = ListNVMeDevices();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_VALUES_EQUAL(0, devices.size());
        }

        UNIT_ASSERT_VALUES_EQUAL(1, attempts.load());

        lastRequest.WaitT(
            Config->GetUpdateDevicesInterval() * successfulAttempt);

        UNIT_ASSERT_VALUES_EQUAL(successfulAttempt, attempts.load());

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

    Y_UNIT_TEST_F(ShouldStopWhilePollingForNvmeDevices, TFixture)
    {
        const ui32 lastAttempt = 2;

        TAutoEvent lastRequest;
        std::atomic<ui32> attempts = 0;

        DeviceProvider->ListNVMeDevicesImpl.SetValue(
            [&]
            {
                ++attempts;
                if (attempts >= lastAttempt) {
                    lastRequest.Signal();
                }

                return MakeFuture(TVector<NProto::TNVMeDevice>());
            });

        {
            auto [devices, error] = ListNVMeDevices();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_VALUES_EQUAL(0, devices.size());
        }

        UNIT_ASSERT_VALUES_EQUAL(1, attempts.load());

        lastRequest.WaitT(Config->GetUpdateDevicesInterval() * lastAttempt);

        UNIT_ASSERT_VALUES_EQUAL(lastAttempt, attempts.load());

        Service->Stop();
    }

    Y_UNIT_TEST_F(
        ShouldUpdateNvmeFirmwareRevisionMetricWhenDiskFirmwareRevisionChanges,
        TFixture)
    {
        SetProviderReady();

        {
            auto [devices, error] = ListNVMeDevices();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        WaitCountersUpdate();

        auto rootGroup =
            Monitoring->GetCounters()->FindSubgroup("counters", "blockstore");
        UNIT_ASSERT(rootGroup);

        auto counters = rootGroup->FindSubgroup("component", "local_nvme");
        UNIT_ASSERT(counters);

        UNIT_ASSERT_VALUES_EQUAL(
            3,
            counters->GetCounter("DevicesInOnlineState")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            counters->GetCounter("DevicesInOfflineState")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            counters->GetCounter("DevicesInMaintenanceState")->Val());

        for (const auto& d: Devices) {
            auto deviceGroup =
                counters->FindSubgroup("device", d.GetSerialNumber());
            UNIT_ASSERT(deviceGroup);
            auto fwGroup =
                deviceGroup->FindSubgroup("firmware", d.GetFirmwareRev());
            UNIT_ASSERT(fwGroup);
            auto revisionCounter = fwGroup->FindCounter("revision");
            UNIT_ASSERT(revisionCounter);
            UNIT_ASSERT_VALUES_EQUAL(1, revisionCounter->Val());
        }

        auto& device = Devices[1];
        const TString sn = device.GetSerialNumber();

        {
            auto future = Service->AcquireNVMeDevice(sn, EmptyIdempotenceId);

            const auto& [_, error] = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        const TString ctrlPath = "/dev/nvme1";
        const TString oldFwRev = Devices[1].GetFirmwareRev();
        const TString newFwRev = "new-fw";

        // Simulate a firmware revision change while the NVMe device is detached
        // from the nvme driver. The service should not observe this change yet
        SysFs->AddrToDevice[Devices[1].GetPCIAddress()].SetFirmwareRev(
            newFwRev);

        WaitCountersUpdate();

        // The firmware revision metric should remain unchanged while the device
        // is acquired and detached from the nvme driver
        {
            auto deviceGroup = counters->FindSubgroup("device", sn);
            UNIT_ASSERT(deviceGroup);

            UNIT_ASSERT(!deviceGroup->FindSubgroup("firmware", newFwRev));

            auto fwGroup = deviceGroup->FindSubgroup("firmware", oldFwRev);
            UNIT_ASSERT(fwGroup);
            auto revisionCounter = fwGroup->FindCounter("revision");
            UNIT_ASSERT(revisionCounter);
            UNIT_ASSERT_VALUES_EQUAL(1, revisionCounter->Val());
        }

        // Release the NVMe device back to the nvme driver so the service can
        // observe the updated firmware revision
        {
            auto future = Service->ReleaseNVMeDevice(sn, EmptyIdempotenceId);

            NVMeManager->WaitSanitizeRequested();
            NVMeManager->UpdateSanitizeStatus(ctrlPath, MakeError(S_OK), 100.0);

            const auto& error = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        WaitCountersUpdate();

        // After the device is released, the metric should expose the new
        // firmware revision while keeping the old revision series present
        {
            auto deviceGroup = counters->FindSubgroup("device", sn);
            UNIT_ASSERT(deviceGroup);

            auto newFwGroup = deviceGroup->FindSubgroup("firmware", newFwRev);
            {
                UNIT_ASSERT(newFwGroup);
                auto revisionCounter = newFwGroup->FindCounter("revision");
                UNIT_ASSERT(revisionCounter);
                UNIT_ASSERT_VALUES_EQUAL(1, revisionCounter->Val());
            }

            auto oldFwGroup = deviceGroup->FindSubgroup("firmware", oldFwRev);
            {
                UNIT_ASSERT(oldFwGroup);
                auto revisionCounter = oldFwGroup->FindCounter("revision");
                UNIT_ASSERT(revisionCounter);
                UNIT_ASSERT_VALUES_EQUAL(1, revisionCounter->Val());
            }
        }
    }

    Y_UNIT_TEST_F(ShouldLockdownDevices, TFixture)
    {
        const TString vfioDev = "vfio0";
        const TString ctrlPath = "/dev/nvme0";
        const auto& device = Devices[0];

        const TVector<ui8>
            supportedAdminOpcodes{0x1, 0x2, 0x3, 0x4, 0x5, 0xA, 0xB};
        const TVector<ui8>
            supportedSetFeatureIds{0x1, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8};

        const TVector<ui8> prohibitedAdminOpcodesBeforeLockdown{0x4};
        const TVector<ui8> prohibitedSetFeatureIdsBeforeLockdown{0x7};

        const TVector<ui8> prohibitedAdminOpcodesAfterLockdown{0x4, 0x5, 0xA};
        const TVector<ui8> prohibitedSetFeatureIdsAfterLockdown{
            0x3,
            0x6,
            0x7,
            0x8};

        SysFs->GetVfioDeviceForPCIDeviceImpl =
            [&](const TString& pciAddr) -> TString
        {
            if (pciAddr == device.GetPCIAddress()) {
                return vfioDev;
            }
            return {};
        };

        TLockdownState lockdownState{
            .Supported = true,
            .AdminCmd =
                {
                    .Supported = supportedAdminOpcodes,
                    .Prohibited = prohibitedAdminOpcodesBeforeLockdown,
                },
            .FeatureId =
                {
                    .Supported = supportedSetFeatureIds,
                    .Prohibited = prohibitedSetFeatureIdsBeforeLockdown,
                },
        };

        LockdownImpl->GetLockdownState =
            [&](const auto& path) -> TResultOrError<TLockdownState>
        {
            if (path != ctrlPath) {
                return MakeError(E_ARGUMENT, "Unexpected path");
            }

            return lockdownState;
        };

        LockdownImpl->EnsureLockdown =
            [&](const auto& path, const TLockdownConfig& config)
        {
            if (!std::ranges::equal(
                    AllowedAdminOpcodes,
                    config.AllowedAdminOpcodes))
            {
                return MakeError(E_ARGUMENT, "Unexpected AllowedAdminOpcodes");
            }

            if (!std::ranges::equal(
                    AllowedSetFeatureIds,
                    config.AllowedSetFeatureIds))
            {
                return MakeError(E_ARGUMENT, "Unexpected AllowedSetFeatureIds");
            }

            if (!config.BlockLockdownCommand) {
                return MakeError(E_ARGUMENT, "Unexpected BlockLockdownCommand");
            }

            if (path != ctrlPath) {
                return MakeError(E_ARGUMENT, "Unexpected path");
            }

            lockdownState.AdminCmd.Prohibited =
                prohibitedAdminOpcodesAfterLockdown;
            lockdownState.FeatureId.Prohibited =
                prohibitedSetFeatureIdsAfterLockdown;

            return MakeError(S_OK);
        };

        SetProviderReady();

        {
            auto future = Service->AcquireNVMeDevice(
                device.GetSerialNumber(),
                EmptyIdempotenceId);
            const auto& [_, error] = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }
    }

    Y_UNIT_TEST_F(ShouldFailAcquireWhenLockdownFails, TFixture)
    {
        const TString vfioDev = "vfio0";
        const TString ctrlPath = "/dev/nvme0";
        const auto& device = Devices[0];

        SysFs->GetVfioDeviceForPCIDeviceImpl =
            [&](const TString& pciAddr) -> TString
        {
            if (pciAddr == device.GetPCIAddress()) {
                return vfioDev;
            }
            return {};
        };

        const TLockdownState lockdownState{
            .Supported = true,
            .AdminCmd = {.Supported = {0x1, 0x2, 0x3, 0x4, 0x5, 0xA, 0xB}},
            .FeatureId = {.Supported = {0x1, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8}},
        };

        LockdownImpl->GetLockdownState =
            [&](const auto& path) -> TResultOrError<TLockdownState>
        {
            if (path == ctrlPath) {
                return lockdownState;
            }

            return TLockdownState{};
        };

        const auto lockdownError = MakeError(E_INVALID_STATE, "test");

        LockdownImpl->EnsureLockdown = [&](const auto&, const TLockdownConfig&)
        {
            return lockdownError;
        };

        SetProviderReady();

        {
            auto future = Service->AcquireNVMeDevice(
                device.GetSerialNumber(),
                EmptyIdempotenceId);
            const auto& [_, error] = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                lockdownError.GetCode(),
                error.GetCode(),
                FormatError(error));
        }
    }

    Y_UNIT_TEST_F(ShouldReconcileDevices, TFixture)
    {
        TVector<NProto::TNVMeDevice> providedDeviceList;
        std::mutex mutex;
        std::condition_variable deviceListFetched;
        ui32 publishedVersion = 0;
        ui32 fetchedVersion = 0;

        auto publishDeviceList = [&](const auto& devices)
        {
            std::unique_lock lock{mutex};

            providedDeviceList = devices;
            ++publishedVersion;

            // Do not continue until the service has fetched this exact
            // version.
            deviceListFetched.wait(
                lock,
                [&] { return fetchedVersion == publishedVersion; });
        };

        DeviceProvider->ListNVMeDevicesImpl.SetValue(
            [&]
            {
                TVector<NProto::TNVMeDevice> devices;
                {
                    std::unique_lock lock{mutex};
                    devices = providedDeviceList;
                    fetchedVersion = publishedVersion;
                }

                deviceListFetched.notify_one();
                return MakeFuture(std::move(devices));
            });

        struct TTestCase
        {
            TStringBuf Name;
            TVector<NProto::TNVMeDevice> ProvidedList;
            TVector<NProto::TNVMeDevice> ExpectedList;
        };

        auto online = [](auto device)
        {
            device.SetDeviceState(NProto::NVME_DEVICE_STATE_ONLINE);
            return device;
        };

        auto maintenance = [](auto device)
        {
            device.SetDeviceState(NProto::NVME_DEVICE_STATE_MAINTENANCE);
            return device;
        };

        auto offline = [](auto device)
        {
            device.SetDeviceState(NProto::NVME_DEVICE_STATE_OFFLINE);
            return device;
        };

        const TTestCase testCases[]{
            {
                .Name = "empty device list",
                .ProvidedList = {},
                .ExpectedList = {},
            },
            // NVME_0: online
            {
                .Name = "discover online device",
                .ProvidedList = {online(Devices[0])},
                .ExpectedList = {online(Devices[0])},
            },
            // NVME_0: online
            // NVME_1: new, maintenance
            // NVME_2: new, online
            {
                .Name = "discover online and maintenance devices",
                .ProvidedList =
                    {
                        online(Devices[0]),
                        maintenance(Devices[1]),
                        online(Devices[2]),
                    },
                .ExpectedList =
                    {
                        online(Devices[0]),
                        maintenance(Devices[1]),
                        online(Devices[2]),
                    },
            },
            // NVME_0: online
            // NVME_1: lost, maintenance -> offline
            // NVME_2: online -> maintenance
            {
                .Name = "mark missing device offline and update device state",
                .ProvidedList =
                    {
                        online(Devices[0]),
                        maintenance(Devices[2]),
                    },
                .ExpectedList =
                    {
                        online(Devices[0]),
                        offline(Devices[1]),
                        maintenance(Devices[2]),
                    },
            },
            // NVME_0: online
            // NVME_1: offline -> online
            // NVME_2: maintenance -> offline
            // NVME_3: new, online
            {
                .Name = "restore offline device, mark missing device offline, "
                        "and discover new device",
                .ProvidedList =
                    {
                        online(Devices[0]),
                        online(Devices[1]),
                        online(Devices[3]),
                    },
                .ExpectedList =
                    {
                        online(Devices[0]),
                        online(Devices[1]),
                        offline(Devices[2]),
                        online(Devices[3]),
                    },
            },
            // NVME_0: online -> offline
            // NVME_1: online -> offline
            // NVME_2: offline
            // NVME_3: online -> offline
            {
                .Name = "mark all missing devices offline",
                .ProvidedList = {},
                .ExpectedList =
                    {
                        offline(Devices[0]),
                        offline(Devices[1]),
                        offline(Devices[2]),
                        offline(Devices[3]),
                    },
            },
        };

        for (ui32 i = 0; i != std::size(testCases); ++i) {
            const auto& t = testCases[i];

            publishDeviceList(t.ProvidedList);

            const auto [devices, error] = ListNVMeDevices();
            const auto caseInfo = TStringBuilder()
                                  << "Case #" << i << " (" << t.Name << ")";

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                caseInfo << ": " << FormatError(error));

            UNIT_ASSERT_VALUES_EQUAL_C(
                t.ExpectedList.size(),
                devices.size(),
                caseInfo);

            for (size_t j = 0; j != devices.size(); ++j) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    t.ExpectedList[j].DebugString(),
                    devices[j].DebugString(),
                    caseInfo << ", device #" << j);
            }
        }
    }

    Y_UNIT_TEST_F(ShouldGetDevicesFromInfra, TFixtureInfra)
    {
        auto [devices, error] = ListNVMeDevices();
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));

        UNIT_ASSERT_VALUES_EQUAL(Devices.size(), devices.size());
        for (size_t i = 0; i != Devices.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                Devices[i].DebugString(),
                devices[i].DebugString(),
                "#" << i);
        }
    }
}

}   // namespace NCloud::NBlockStore
