#include "service_linux.h"

#include "config.h"
#include "device_provider.h"
#include "sysfs_helpers.h"

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

const NProto::TError ServiceDestroyedError =
    MakeError(E_REJECTED, "Local NVMe service is destroyed");

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
    const NNvme::INvmeManagerPtr NvmeManager;
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

    [[nodiscard]] auto ListNVMeDevices() const
        -> TFuture<TListDevicesResult> final;

    [[nodiscard]] auto AcquireNVMeDevice(const TString& serialNumber)
        -> TFuture<NProto::TError> final;

    [[nodiscard]] auto ReleaseNVMeDevice(const TString& serialNumber)
        -> TFuture<NProto::TError> final;

private:
    auto AcquireDevice(const TString& serialNumber) -> NProto::TError;
    auto ReleaseDevice(const TString& serialNumber) -> NProto::TError;
    auto ListDevices() const -> TVector<NProto::TNVMeDevice>;
    auto EnsureIsReady() const -> NProto::TError;
    auto Initialize() -> NProto::TError;
    auto FetchDevices() -> NProto::TError;
    bool TryRestoreStateFromCache();
    void UpdateStateCache();
    auto CreateStateSnapshot() -> NProto::TLocalNVMeServiceState;
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
    , NvmeManager(std::move(nvmeManager))
    , Executor(std::move(executor))
    , SysFs(std::move(sysFs))
{
    Y_UNUSED(NvmeManager);
}

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

auto TLocalNVMeService::ListNVMeDevices() const -> TFuture<TListDevicesResult>
{
    STORAGE_INFO("List NVMe devices");

    if (auto error = EnsureIsReady(); HasError(error)) {
        return MakeFuture<TListDevicesResult>(error);
    }

    return Executor->Execute(
        [weakSelf = weak_from_this()]() -> TListDevicesResult
        {
            if (auto self = weakSelf.lock()) {
                return self->ListDevices();
            }

            return ServiceDestroyedError;
        });
}

auto TLocalNVMeService::AcquireNVMeDevice(const TString& serialNumber)
    -> TFuture<NProto::TError>
{
    STORAGE_INFO("Acquire NVMe device " << serialNumber.Quote());

    if (auto error = EnsureIsReady(); HasError(error)) {
        return MakeFuture(error);
    }

    return Executor->Execute(
        [weakSelf = weak_from_this(), serialNumber]() -> NProto::TError
        {
            if (auto self = weakSelf.lock()) {
                return self->AcquireDevice(serialNumber);
            }
            return ServiceDestroyedError;
        });
}

auto TLocalNVMeService::ReleaseNVMeDevice(const TString& serialNumber)
    -> TFuture<NProto::TError>
{
    STORAGE_INFO("Release NVMe device " << serialNumber.Quote());

    if (auto error = EnsureIsReady(); HasError(error)) {
        return MakeFuture(error);
    }

    return Executor->Execute(
        [weakSelf = weak_from_this(), serialNumber]() -> NProto::TError
        {
            if (auto self = weakSelf.lock()) {
                return self->ReleaseDevice(serialNumber);
            }
            return ServiceDestroyedError;
        });
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

    for (const auto& device: devices) {
        Devices[device.GetSerialNumber()] = device;
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

auto TLocalNVMeService::AcquireDevice(const TString& serialNumber)
    -> NProto::TError
{
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

    try {
        SysFs->BindPCIDeviceToDriver(device->GetPCIAddress(), "vfio-pci");
    } catch (...) {
        return MakeError(E_FAIL, CurrentExceptionMessage());
    }

    return {};
}

auto TLocalNVMeService::ReleaseDevice(const TString& serialNumber)
    -> NProto::TError
{
    const auto* device = Devices.FindPtr(serialNumber);

    if (!device) {
        return MakeError(
            E_NOT_FOUND,
            TStringBuilder()
                << "Device " << serialNumber.Quote() << " not found");
    }

    try {
        SysFs->BindPCIDeviceToDriver(device->GetPCIAddress(), "nvme");
    } catch (...) {
        return MakeError(E_FAIL, CurrentExceptionMessage());
    }

    AcquiredDevices.erase(serialNumber);

    UpdateStateCache();

    return {};
}

auto TLocalNVMeService::ListDevices() const -> TVector<NProto::TNVMeDevice>
{
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
