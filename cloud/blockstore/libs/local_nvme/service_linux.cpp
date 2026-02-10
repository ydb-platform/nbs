#include "service_linux.h"

#include "config.h"
#include "device_provider.h"
#include "sysfs_helpers.h"

#include <cloud/blockstore/libs/nvme/nvme.h>

#include <cloud/storage/core/libs/common/proto_helpers.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>
#include <library/cpp/threading/future/future.h>

#include <util/folder/path.h>
#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/system/fs.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

const TErrorResponse ServiceDestroyedError =
    MakeError(E_REJECTED, "Local NVMe service is destroyed");

const TDuration SanitizeStatusProbeTimeout = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

template <typename R>
TString Join(TStringBuf delim, const R& range)
{
    TStringStream ss;
    for (int i = 0; auto&& x: range) {
        if (i++) {
            ss << delim;
        }
        ss << x;
    }

    return ss.Str();
}

// // xx:xx.x -> 0000:xx:xx.x
TString NormalizePCIAddr(const TString& pciAddr)
{
    if (std::ranges::count(pciAddr, ':') == 1) {
        return "0000:" + pciAddr;
    }
    return pciAddr;
}

////////////////////////////////////////////////////////////////////////////////

class TLocalNVMeService final
    : public std::enable_shared_from_this<TLocalNVMeService>
    , public ILocalNVMeService
{
    using TListDevicesResult = TResultOrError<TVector<NProto::TNVMeDevice>>;

private:
    const TLocalNVMeConfigPtr Config;
    const ILoggingServicePtr Logging;
    const ILocalNVMeDeviceProviderPtr DeviceProvider;
    const NNvme::INvmeManagerPtr NVMeManager;
    const TExecutorPtr Executor;
    const ISysFsPtr SysFs;

    TLog Log;
    TFuture<NProto::TError> Ready;

    THashMap<TString, NProto::TNVMeDevice> Devices;
    THashSet<TString> AcquiredDevices;

public:
    TLocalNVMeService(
        TLocalNVMeConfigPtr config,
        ILoggingServicePtr logging,
        ILocalNVMeDeviceProviderPtr deviceProvider,
        NNvme::INvmeManagerPtr nvmeManager,
        TExecutorPtr executor,
        ISysFsPtr sysFs);

    // IStartable

    void Start() final;
    void Stop() final;

    // ILocalNVMeService

    [[nodiscard]] auto ListNVMeDevices() -> TFuture<TListDevicesResult> final;

    [[nodiscard]] auto AcquireNVMeDevice(const TString& serialNumber)
        -> TFuture<NProto::TError> final;

    [[nodiscard]] auto ReleaseNVMeDevice(const TString& serialNumber)
        -> TFuture<NProto::TError> final;

private:
    auto AcquireDevice(TCont* c, const TString& serialNumber) -> NProto::TError;
    auto ReleaseDevice(TCont* c, const TString& serialNumber) -> NProto::TError;
    auto ListDevices(TCont* c) const -> TVector<NProto::TNVMeDevice>;

    auto EnsureIsReady() const -> NProto::TError;
    auto Initialize() -> NProto::TError;
    auto FetchDevices() -> NProto::TError;
    bool TryRestoreStateFromCache();
    void UpdateStateCache();
    auto CreateStateSnapshot() -> NProto::TLocalNVMeServiceState;
    auto SanitizeNVMeDevice(TCont* c, const NProto::TNVMeDevice& device)
        -> NProto::TError;
    auto ResetToSingleNamespace(const NProto::TNVMeDevice& device)
        -> NProto::TError;
    auto GetNVMeCtrlPath(const NProto::TNVMeDevice& device) -> TFsPath;
    auto BindDeviceToDriver(
        const NProto::TNVMeDevice& device,
        const TString& driverName) -> NProto::TError;

    // Spawn a coroutine with a name `name` to execute `fn`
    template <typename R, typename TSelf, typename F, typename... TArgs>
    static auto
    ExecuteAsync(TSelf&& service, const char* name, F fn, TArgs&&... args)
        -> TFuture<R>
    {
        auto promise = NewPromise<R>();
        auto weakSelf = service.weak_from_this();

        service.Executor->Execute(
            [weakSelf, name, promise, fn, args...]() mutable
            {
                if (auto self = weakSelf.lock()) {
                    auto* e = self->Executor->GetContExecutor();
                    e->CreateOwned(
                        [promise, fn, self, args...](TCont* c) mutable
                        {
                            promise.SetValue(std::invoke(fn, self, c, args...));
                        },
                        name);
                } else {
                    promise.SetValue(ServiceDestroyedError);
                }
            });

        return promise.GetFuture();
    }
};

////////////////////////////////////////////////////////////////////////////////

TLocalNVMeService::TLocalNVMeService(
    TLocalNVMeConfigPtr config,
    ILoggingServicePtr logging,
    ILocalNVMeDeviceProviderPtr deviceProvider,
    NNvme::INvmeManagerPtr nvmeManager,
    TExecutorPtr executor,
    ISysFsPtr sysFs)
    : Config(std::move(config))
    , Logging(std::move(logging))
    , DeviceProvider(std::move(deviceProvider))
    , NVMeManager(std::move(nvmeManager))
    , Executor(std::move(executor))
    , SysFs(std::move(sysFs))
{}

void TLocalNVMeService::Start()
{
    Log = Logging->CreateLog("BLOCKSTORE_LOCAL_NVME");

    Ready = Executor->Execute(
        [weakSelf = weak_from_this()]() -> NProto::TError
        {
            if (auto self = weakSelf.lock()) {
                return self->Initialize();
            }

            return ServiceDestroyedError;
        });
}

void TLocalNVMeService::Stop()
{}

auto TLocalNVMeService::ListNVMeDevices() -> TFuture<TListDevicesResult>
{
    STORAGE_INFO("List NVMe devices");

    if (auto error = EnsureIsReady(); HasError(error)) {
        return MakeFuture<TListDevicesResult>(error);
    }

    return ExecuteAsync<TListDevicesResult>(
        *this,
        "list",
        &TLocalNVMeService::ListDevices);
}

auto TLocalNVMeService::AcquireNVMeDevice(const TString& serialNumber)
    -> TFuture<NProto::TError>
{
    STORAGE_INFO("Acquire NVMe device " << serialNumber.Quote());

    if (auto error = EnsureIsReady(); HasError(error)) {
        return MakeFuture(error);
    }

    return ExecuteAsync<NProto::TError>(
        *this,
        "acquire",
        &TLocalNVMeService::AcquireDevice,
        serialNumber);
}

auto TLocalNVMeService::ReleaseNVMeDevice(const TString& serialNumber)
    -> TFuture<NProto::TError>
{
    STORAGE_INFO("Release NVMe device " << serialNumber.Quote());

    if (auto error = EnsureIsReady(); HasError(error)) {
        return MakeFuture(error);
    }

    return ExecuteAsync<NProto::TError>(
        *this,
        "release",
        &TLocalNVMeService::ReleaseDevice,
        serialNumber);
}

////////////////////////////////////////////////////////////////////////////////

auto TLocalNVMeService::EnsureIsReady() const -> NProto::TError
{
    if (Ready.HasValue()) {
        return Ready.GetValue();
    }

    Y_DEBUG_ABORT_UNLESS(!Ready.HasException());

    return MakeError(E_REJECTED, "not ready");
}

auto TLocalNVMeService::FetchDevices() -> NProto::TError
{
    auto future = DeviceProvider->ListNVMeDevices();

    auto [devices, error] = Executor->ResultOrError(future);

    if (HasError(error)) {
        STORAGE_ERROR(
            "Failed to fetch NVMe devices from the provider: "
            << FormatError(error));
        return error;
    }

    STORAGE_INFO(
        "Fetched NVMe devices (" << devices.size()
                                 << "): " << Join(", ", devices));

    // We expect Device provider to provide serial numbers and PCI addresses

    for (const auto& src: devices) {
        if (src.GetPCIAddress().empty()) {
            STORAGE_WARN("Ignore device with empty PCI address: " << src);
            continue;
        }

        if (src.GetSerialNumber().empty()) {
            STORAGE_WARN("Ignore device with empty serial number: " << src);
            continue;
        }

        const auto pciAddr = NormalizePCIAddr(src.GetPCIAddress());

        auto [device, deviceError] =
            SafeExecute<TResultOrError<NProto::TNVMeDevice>>(
                [&] { return SysFs->GetNVMeDeviceFromPCIAddr(pciAddr); });

        if (HasError(deviceError)) {
            STORAGE_ERROR(
                "Failed to retrieve information about "
                << src << ": " << FormatError(deviceError));
            continue;
        }

        if (device.GetSerialNumber() != src.GetSerialNumber()) {
            STORAGE_ERROR(
                "Serial numbers don't match: " << src << " vs " << device
                                               << ". Ignore device");
            continue;
        }

        Devices[device.GetSerialNumber()] = std::move(device);
    }

    UpdateStateCache();

    return {};
}

auto TLocalNVMeService::CreateStateSnapshot() -> NProto::TLocalNVMeServiceState
{
    NProto::TLocalNVMeServiceState proto;
    for (const auto& [_, device]: Devices) {
        *proto.MutableDevices()->Add() = device;
    }

    for (const auto& sn: AcquiredDevices) {
        proto.AddAcquiredDevices()->SetSerialNumber(sn);
    }

    return proto;
}

void TLocalNVMeService::UpdateStateCache()
{
    if (!Config->GetStateCacheFilePath()) {
        return;
    }

    try {
        const TString tmpPath{Config->GetStateCacheFilePath() + ".tmp"};

        SerializeToTextFormat(CreateStateSnapshot(), tmpPath);

        NFs::Rename(tmpPath, Config->GetStateCacheFilePath());

        STORAGE_INFO("Service state stored in cache successfully");
    } catch (...) {
        STORAGE_ERROR(
            "Failed to store the service state from the cache: "
            << CurrentExceptionMessage());
    }
}

bool TLocalNVMeService::TryRestoreStateFromCache()
{
    if (!Config->GetStateCacheFilePath() ||
        !NFs::Exists(Config->GetStateCacheFilePath()))
    {
        return false;
    }

    NProto::TLocalNVMeServiceState proto;
    try {
        ParseProtoTextFromFile(Config->GetStateCacheFilePath(), proto);
    } catch (...) {
        STORAGE_ERROR(
            "Failed to restore the service state from the cache: "
            << CurrentExceptionMessage());
        return false;
    }

    STORAGE_INFO(
        "Found in cache NVMe devices ("
        << proto.DevicesSize() << "): " << Join(", ", proto.GetDevices())
        << "; acquired devices: " << Join(", ", proto.GetAcquiredDevices())
        << ";");

    for (auto& device: proto.GetDevices()) {
        Devices.emplace(device.GetSerialNumber(), device);
    }

    for (const auto& device: proto.GetAcquiredDevices()) {
        AcquiredDevices.emplace(device.GetSerialNumber());
    }

    return true;
}

auto TLocalNVMeService::Initialize() -> NProto::TError
{
    if (!TryRestoreStateFromCache()) {
        STORAGE_DEBUG("Cache restore failed, fetching from provider");

        return FetchDevices();
    }

    if (Devices.empty()) {
        STORAGE_INFO("No NVMe devices in the cache, fetching from provider");

        return FetchDevices();
    }

    return {};
}

auto TLocalNVMeService::BindDeviceToDriver(
    const NProto::TNVMeDevice& device,
    const TString& driverName) -> NProto::TError
{
    STORAGE_INFO(
        "Bind " << device.GetSerialNumber().Quote() << " to " << driverName
                << " driver");

    return SafeExecute<NProto::TError>(
        [&]
        {
            SysFs->BindPCIDeviceToDriver(device.GetPCIAddress(), driverName);
            return MakeError(S_OK);
        });

    return {};
}

auto TLocalNVMeService::AcquireDevice(TCont* c, const TString& serialNumber)
    -> NProto::TError
{
    Y_UNUSED(c);

    const auto* device = Devices.FindPtr(serialNumber);

    if (!device) {
        return MakeError(
            E_NOT_FOUND,
            TStringBuilder()
                << "Device " << serialNumber.Quote() << " not found");
    }

    auto [_, ok] = AcquiredDevices.insert(serialNumber);
    if (!ok) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "Device " << serialNumber.Quote() << " already acquired");
    }

    UpdateStateCache();

    return BindDeviceToDriver(*device, "vfio-pci");
}

auto TLocalNVMeService::GetNVMeCtrlPath(const NProto::TNVMeDevice& device)
    -> TFsPath
{
    const TString& pciAddr = device.GetPCIAddress();
    const TString ctrlName = SysFs->GetNVMeCtrlNameFromPCIAddr(pciAddr);
    if (!ctrlName) {
        ythrow TServiceError(E_NOT_FOUND)
            << "Failed to get controller name for "
            << device.GetSerialNumber().Quote() << " " << pciAddr;
    }

    return TFsPath{"/dev"} / ctrlName;
}

auto TLocalNVMeService::SanitizeNVMeDevice(
    TCont* c,
    const NProto::TNVMeDevice& device) -> NProto::TError
try {
    STORAGE_INFO("Sanitize " << device.GetSerialNumber().Quote());

    const TFsPath ctrlPath = GetNVMeCtrlPath(device);

    CheckError(NVMeManager->Sanitize(ctrlPath));

    for (;;) {
        auto [r, error] = NVMeManager->GetSanitizeStatus(ctrlPath);
        CheckError(error);

        STORAGE_DEBUG(
            "Sanitize status for " << device.GetSerialNumber() << ": "
                                   << FormatError(r.Status)
                                   << " progress: " << r.Progress);

        if (!HasError(r.Status)) {
            break;
        }

        if (r.Status.GetCode() != E_TRY_AGAIN) {
            return r.Status;
        }

        c->SleepT(SanitizeStatusProbeTimeout);
    }

    return {};
} catch (...) {
    return MakeError(E_FAIL, CurrentExceptionMessage());
}

auto TLocalNVMeService::ResetToSingleNamespace(
    const NProto::TNVMeDevice& device) -> NProto::TError
{
    STORAGE_INFO(
        "Reset NVMe " << device.GetSerialNumber().Quote()
                      << " to single namespace");

    return SafeExecute<NProto::TError>(
        [&]
        {
            const TFsPath ctrlPath = GetNVMeCtrlPath(device);

            return NVMeManager->ResetToSingleNamespace(ctrlPath);
        });
}

auto TLocalNVMeService::ReleaseDevice(TCont* c, const TString& serialNumber)
    -> NProto::TError
{
    const auto* device = Devices.FindPtr(serialNumber);

    if (!device) {
        return MakeError(
            E_NOT_FOUND,
            TStringBuilder()
                << "Device " << serialNumber.Quote() << " not found");
    }

    if (auto error = BindDeviceToDriver(*device, "nvme"); HasError(error)) {
        return error;
    }

    if (auto error = SanitizeNVMeDevice(c, *device); HasError(error)) {
        return error;
    }

    if (auto error = ResetToSingleNamespace(*device); HasError(error)) {
        return error;
    }

    AcquiredDevices.erase(serialNumber);

    UpdateStateCache();

    return {};
}

auto TLocalNVMeService::ListDevices(TCont* c) const
    -> TVector<NProto::TNVMeDevice>
{
    Y_UNUSED(c);

    TVector<NProto::TNVMeDevice> devices;
    devices.reserve(Devices.size());

    for (auto& [_, device]: Devices) {
        devices.push_back(device);
    }

    SortBy(devices, [](const auto& d) { return d.GetSerialNumber(); });

    return devices;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ILocalNVMeServicePtr CreateLocalNVMeService(
    TLocalNVMeConfigPtr config,
    ILoggingServicePtr logging,
    ILocalNVMeDeviceProviderPtr deviceProvider,
    NNvme::INvmeManagerPtr nvmeManager,
    TExecutorPtr executor,
    ISysFsPtr sysFs)
{
    return std::make_shared<TLocalNVMeService>(
        std::move(config),
        std::move(logging),
        std::move(deviceProvider),
        std::move(nvmeManager),
        std::move(executor),
        std::move(sysFs));
}

ILocalNVMeServicePtr CreateLocalNVMeService(
    TLocalNVMeConfigPtr config,
    ILoggingServicePtr logging,
    ILocalNVMeDeviceProviderPtr deviceProvider,
    NNvme::INvmeManagerPtr nvmeManager,
    TExecutorPtr executor)
{
    return CreateLocalNVMeService(
        std::move(config),
        std::move(logging),
        std::move(deviceProvider),
        std::move(nvmeManager),
        std::move(executor),
        CreateSysFs("/sys"));
}

}   // namespace NCloud::NBlockStore
