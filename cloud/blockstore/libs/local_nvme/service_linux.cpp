#include "service.h"

#include "device_provider.h"

#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>
#include <util/stream/str.h>
#include <util/string/builder.h>

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
    const ILoggingServicePtr Logging;
    const ILocalNVMeDeviceProviderPtr DeviceProvider;
    const NNvme::INvmeManagerPtr NvmeManager;
    const TExecutorPtr Executor;

    TLog Log;
    TFuture<NProto::TError> Ready;

    THashMap<TString, NProto::TNVMeDevice> Devices;

public:
    TLocalNVMeService(
        ILoggingServicePtr logging,
        ILocalNVMeDeviceProviderPtr deviceProvider,
        NNvme::INvmeManagerPtr nvmeManager,
        TExecutorPtr executor);

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
};

////////////////////////////////////////////////////////////////////////////////

TLocalNVMeService::TLocalNVMeService(
    ILoggingServicePtr logging,
    ILocalNVMeDeviceProviderPtr deviceProvider,
    NNvme::INvmeManagerPtr nvmeManager,
    TExecutorPtr executor)
    : Logging(std::move(logging))
    , DeviceProvider(std::move(deviceProvider))
    , NvmeManager(std::move(nvmeManager))
    , Executor(std::move(executor))
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

auto TLocalNVMeService::Initialize() -> NProto::TError
{
    auto future = DeviceProvider->ListNVMeDevices();

    auto [devices, error] = Executor->ResultOrError(future);

    if (HasError(error)) {
        STORAGE_ERROR(
            "Failed to list NVMe devices from the provider: "
            << FormatError(error));
        return error;
    }

    STORAGE_INFO(
        "Found NVMe devices (" << devices.size()
                               << "): " << Join(", ", devices));

    for (const auto& device: devices) {
        Devices[device.GetSerialNumber()] = device;
    }

    return {};
}

auto TLocalNVMeService::AcquireDevice(const TString& serialNumber)
    -> NProto::TError
{
    if (!Devices.contains(serialNumber)) {
        return MakeError(
            E_NOT_FOUND,
            TStringBuilder()
                << "Device " << serialNumber.Quote() << " not found");
    }

    // TODO

    return {};
}

auto TLocalNVMeService::ReleaseDevice(const TString& serialNumber)
    -> NProto::TError
{
    if (!Devices.contains(serialNumber)) {
        return MakeError(
            E_NOT_FOUND,
            TStringBuilder()
                << "Device " << serialNumber.Quote() << " not found");
    }

    // TODO

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
    ILoggingServicePtr logging,
    ILocalNVMeDeviceProviderPtr deviceProvider,
    NNvme::INvmeManagerPtr nvmeManager,
    TExecutorPtr executor)
{
    return std::make_shared<TLocalNVMeService>(
        std::move(logging),
        std::move(deviceProvider),
        std::move(nvmeManager),
        std::move(executor));
}

}   // namespace NCloud::NBlockStore
