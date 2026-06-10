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

using TSerialNumber = TString;
using TListDevicesResult = TResultOrError<TVector<NProto::TNVMeDevice>>;
using TAcquireResult = TResultOrError<NProto::TNVMeDevice>;

namespace {

////////////////////////////////////////////////////////////////////////////////

const NProto::TError ServiceDestroyedError =
    MakeError(E_REJECTED, "Local NVMe service is destroyed");

const TDuration SanitizeStatusProbeTimeout = TDuration::MilliSeconds(100);
const TDuration VfioDeviceRetryDelay = TDuration::MilliSeconds(100);
const ui32 VfioDeviceMaxRetries = 100;

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

struct TAcquireOperationResult
{
    TFuture<TAcquireResult> Result;
};

struct TReleaseOperationResult
{
    TFuture<NProto::TError> Result;
};

using TOperationResult =
    std::variant<TAcquireOperationResult, TReleaseOperationResult>;

struct TOperation
{
    TString Name;
    TString IdempotenceId;
    TInstant StartTime;
    TInstant FinishTime;
    TCont* Cont = nullptr;

    TOperationResult Result;
};

using TOperationPtr = std::shared_ptr<TOperation>;

using TOperations = THashMap<TSerialNumber, TOperationPtr>;

////////////////////////////////////////////////////////////////////////////////

class TLocalNVMeService final
    : public std::enable_shared_from_this<TLocalNVMeService>
    , public ILocalNVMeService
{
private:
    const TLocalNVMeConfigPtr Config;
    const ILoggingServicePtr Logging;
    const ILocalNVMeDeviceProviderPtr DeviceProvider;
    const NNvme::INvmeManagerPtr NVMeManager;
    const TExecutorPtr Executor;
    const ISysFsPtr SysFs;

    TLog Log;
    TFuture<NProto::TError> Ready;
    bool IsVfioDevSupported = false;

    THashMap<TSerialNumber, NProto::TNVMeDevice> Devices;
    THashSet<TSerialNumber> AcquiredDevices;

    TOperations Operations;

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

    [[nodiscard]] auto AcquireNVMeDevice(
        const TSerialNumber& serialNumber,
        const TString& idempotenceId) -> TFuture<TAcquireResult> final;

    [[nodiscard]] auto ReleaseNVMeDevice(
        const TSerialNumber& serialNumber,
        const TString& idempotenceId) -> TFuture<NProto::TError> final;

private:
    auto AcquireDevice(
        const TSerialNumber& serialNumber,
        const TString& idempotenceId) -> TFuture<TAcquireResult>;

    auto AcquireDeviceImpl(TCont* c, NProto::TNVMeDevice device)
        -> TAcquireResult;

    auto ReleaseDevice(
        const TSerialNumber& serialNumber,
        const TString& idempotenceId) -> TFuture<NProto::TError>;

    auto ReleaseDeviceImpl(TCont* c, const NProto::TNVMeDevice& device)
        -> NProto::TError;

    auto ListDevices() const -> TVector<NProto::TNVMeDevice>;

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
    auto GetVfioDevName(TCont* c, const TString& pciAddress)
        -> TResultOrError<TString>;

    auto StopImpl() -> TFuture<void>;

    template <typename TOpResult, typename TFnResult>
    auto TryEarlyResponse(
        const TSerialNumber& serialNumber,
        const TString& idempotenceId) -> std::optional<TFuture<TFnResult>>;

    template <typename TOpResult, typename TFnResult, typename TFn>
    auto StartDeviceOperation(
        const char* name,
        TFn fn,
        const TSerialNumber& serialNumber,
        const TString& idempotenceId) -> TFuture<TFnResult>;
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

    auto [supported, error] = SafeExecute<TResultOrError<bool>>(
        [&] { return SysFs->IsVfioDevSupported(); });
    if (HasError(error)) {
        STORAGE_ERROR(
            "Failed to check for vfio-dev support: " << FormatError(error));
    } else {
        IsVfioDevSupported = supported;
    }

    Ready = Executor->Execute(
        [weakSelf = weak_from_this()]
        {
            if (auto self = weakSelf.lock()) {
                return self->Initialize();
            }

            return ServiceDestroyedError;
        });
}

auto TLocalNVMeService::StopImpl() -> TFuture<void>
{
    TVector<TFuture<void>> futures;
    futures.reserve(Operations.size());

    for (const auto& [_, op]: Operations) {
        futures.emplace_back(
            std::visit(
                [](const auto& r) { return r.Result.IgnoreResult(); },
                op->Result));

        if (op->Cont) {
            op->Cont->Cancel();
        }
    }

    return WaitAll(futures);
}

void TLocalNVMeService::Stop()
{
    auto future = Executor->Execute(
        [weakSelf = weak_from_this()]
        {
            if (auto self = weakSelf.lock()) {
                return self->StopImpl();
            }
            return MakeFuture();
        });
    future.Wait();
}

auto TLocalNVMeService::ListNVMeDevices() -> TFuture<TListDevicesResult>
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

auto TLocalNVMeService::AcquireNVMeDevice(
    const TSerialNumber& serialNumber,
    const TString& idempotenceId) -> TFuture<TAcquireResult>
{
    STORAGE_INFO(
        "Acquire NVMe device " << serialNumber.Quote()
                               << " idempotenceId: " << idempotenceId);

    if (auto error = EnsureIsReady(); HasError(error)) {
        return MakeFuture<TAcquireResult>(error);
    }

    auto weakSelf = weak_from_this();

    return Executor->Execute(
        [weakSelf, serialNumber, idempotenceId]() mutable
        {
            auto self = weakSelf.lock();
            if (!self) {
                return MakeFuture<TAcquireResult>(ServiceDestroyedError);
            }
            return self->AcquireDevice(serialNumber, idempotenceId);
        });
}

auto TLocalNVMeService::ReleaseNVMeDevice(
    const TSerialNumber& serialNumber,
    const TString& idempotenceId) -> TFuture<NProto::TError>
{
    STORAGE_INFO(
        "Release NVMe device " << serialNumber.Quote()
                               << " idempotenceId: " << idempotenceId);

    if (auto error = EnsureIsReady(); HasError(error)) {
        return MakeFuture(error);
    }

    auto weakSelf = weak_from_this();

    return Executor->Execute(
        [weakSelf, serialNumber, idempotenceId]() mutable
        {
            auto self = weakSelf.lock();
            if (!self) {
                return MakeFuture(ServiceDestroyedError);
            }
            return self->ReleaseDevice(serialNumber, idempotenceId);
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

        auto [device, deviceError] = SafeExecute<TAcquireResult>(
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
}

template <typename TOpResult, typename TFnResult>
auto TLocalNVMeService::TryEarlyResponse(
    const TSerialNumber& serialNumber,
    const TString& idempotenceId) -> std::optional<TFuture<TFnResult>>
{
    auto makeErrorFuture = [](ui32 code, TString msg)
    {
        return MakeFuture<TFnResult>(MakeError(code, std::move(msg)));
    };

    auto it = Operations.find(serialNumber);
    if (it == Operations.end()) {
        return std::nullopt;
    }

    const auto& op = *it->second;

    const bool sameId = idempotenceId && idempotenceId == op.IdempotenceId;

    if (sameId) {
        const auto* typedOp = std::get_if<TOpResult>(&op.Result);
        if (!typedOp) {
            return makeErrorFuture(
                E_ARGUMENT,
                "idempotence id was used for a different operation type");
        }

        if (!op.FinishTime) {
            return makeErrorFuture(E_TRY_AGAIN, "Operation is in progress");
        }

        return typedOp->Result;
    }

    if (!op.FinishTime) {
        return makeErrorFuture(
            E_TRY_AGAIN,
            TStringBuilder()
                << "Another operation (" << op.Name << ", started at "
                << op.StartTime << ") is in progress");
    }

    return std::nullopt;
}

template <typename TOpResult, typename TFnResult, typename TFn>
auto TLocalNVMeService::StartDeviceOperation(
    const char* name,
    TFn fn,
    const TSerialNumber& serialNumber,
    const TString& idempotenceId) -> TFuture<TFnResult>
{
    const auto* device = Devices.FindPtr(serialNumber);

    if (!device) {
        return MakeFuture<TFnResult>(MakeError(
            E_NOT_FOUND,
            TStringBuilder()
                << "Device " << serialNumber.Quote() << " not found"));
    }

    if (auto earlyResponse =
            TryEarlyResponse<TOpResult, TFnResult>(serialNumber, idempotenceId))
    {
        return *earlyResponse;
    }

    auto promise = NewPromise<TFnResult>();

    auto op = std::make_shared<TOperation>();
    op->Name = name;
    op->IdempotenceId = idempotenceId;
    op->StartTime = Now();
    op->Result = TOpResult{promise.GetFuture()};

    auto weakSelf = weak_from_this();

    auto* e = Executor->GetContExecutor();
    auto* cont = e->CreateOwned(
        [op, promise, weakSelf, fn, device = *device](TCont* c) mutable
        {
            auto self = weakSelf.lock();
            TFnResult r =
                self ? std::invoke(fn, self, c, device) : ServiceDestroyedError;

            op->FinishTime = Now();
            op->Cont = nullptr;
            promise.SetValue(r);
        },
        name);

    op->Cont = cont;

    Operations[serialNumber] = op;

    // If idempotenceId isn't provided - wait operation synchronously
    if (!idempotenceId) {
        return promise.GetFuture();
    }

    return MakeFuture<TFnResult>(MakeError(E_TRY_AGAIN, "started"));
}

auto TLocalNVMeService::AcquireDevice(
    const TSerialNumber& serialNumber,
    const TString& idempotenceId) -> TFuture<TAcquireResult>
{
    return StartDeviceOperation<TAcquireOperationResult, TAcquireResult>(
        "Acquire",
        &TLocalNVMeService::AcquireDeviceImpl,
        serialNumber,
        idempotenceId);
}

auto TLocalNVMeService::AcquireDeviceImpl(TCont* c, NProto::TNVMeDevice device)
    -> TResultOrError<NProto::TNVMeDevice>
{
    const auto& serialNumber = device.GetSerialNumber();

    auto [_, ok] = AcquiredDevices.insert(serialNumber);
    if (!ok) {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder()
                << "Device " << serialNumber.Quote() << " already acquired");
    }

    UpdateStateCache();

    if (auto error = BindDeviceToDriver(device, "vfio-pci"); HasError(error)) {
        return error;
    }

    if (!IsVfioDevSupported) {
        return device;
    }

    auto [vfioDev, error] = GetVfioDevName(c, device.GetPCIAddress());
    if (HasError(error)) {
        STORAGE_ERROR(
            "Failed to get vfio device for " << device << ": "
                                             << FormatError(error));
    } else {
        device.SetVfioDevName(std::move(vfioDev));
    }

    return device;
}

auto TLocalNVMeService::GetVfioDevName(TCont* c, const TString& pciAddress)
    -> TResultOrError<TString>
{
    // Binding to vfio-pci completes before the vfio-dev sysfs entry may
    // appear.
    // Retry to avoid racing with asynchronous vfio<N> creation.

    for (ui32 attempt = 0;; ++attempt) {
        auto [vfioDevice, error] = SafeExecute<TResultOrError<TString>>(
            [&] { return SysFs->GetVfioDeviceForPCIDevice(pciAddress); });

        if (HasError(error)) {
            return error;
        }

        if (!vfioDevice && attempt < VfioDeviceMaxRetries) {
            STORAGE_DEBUG(
                "vfio device for PCI address "
                << pciAddress << " was not found, retry (attempts: "
                << attempt + 1 << "/" << VfioDeviceMaxRetries << ")");

            const int ec = c->SleepT(VfioDeviceRetryDelay);
            if (ec == ECANCELED) {
                return ServiceDestroyedError;
            }

            Y_DEBUG_ABORT_UNLESS(ec == ETIMEDOUT, "SleepT: %d", ec);

            continue;
        }

        if (!vfioDevice) {
            return MakeError(
                E_NOT_FOUND,
                TStringBuilder() << "vfio device for PCI address " << pciAddress
                                 << " was not found after "
                                 << VfioDeviceMaxRetries << " retries");
        }

        return vfioDevice;
    }
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

        const int ec = c->SleepT(SanitizeStatusProbeTimeout);

        if (ec == ECANCELED) {
            STORAGE_INFO(
                "Status polling of sanitize operation for "
                << device.GetSerialNumber().Quote() << " was cancelled");

            return ServiceDestroyedError;
        }

        Y_DEBUG_ABORT_UNLESS(ec == ETIMEDOUT, "SleepT: %d", ec);
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

auto TLocalNVMeService::ReleaseDevice(
    const TSerialNumber& serialNumber,
    const TString& idempotenceId) -> TFuture<NProto::TError>
{
    return StartDeviceOperation<TReleaseOperationResult, NProto::TError>(
        "Release",
        &TLocalNVMeService::ReleaseDeviceImpl,
        serialNumber,
        idempotenceId);
}

auto TLocalNVMeService::ReleaseDeviceImpl(
    TCont* c,
    const NProto::TNVMeDevice& device) -> NProto::TError
{
    if (auto error = BindDeviceToDriver(device, "nvme"); HasError(error)) {
        return error;
    }

    if (auto error = SanitizeNVMeDevice(c, device); HasError(error)) {
        return error;
    }

    if (auto error = ResetToSingleNamespace(device); HasError(error)) {
        return error;
    }

    AcquiredDevices.erase(device.GetSerialNumber());

    UpdateStateCache();

    return {};
}

auto TLocalNVMeService::ListDevices() const -> TVector<NProto::TNVMeDevice>
{
    TVector<NProto::TNVMeDevice> devices;
    devices.reserve(Devices.size());

    for (const auto& [_, device]: Devices) {
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
