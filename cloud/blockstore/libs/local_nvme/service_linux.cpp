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
#include <util/generic/scope.h>
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

const NProto::TError ServiceNotReadyError =
    MakeError(E_REJECTED, "Local NVMe service is not ready");

const NProto::TError ServiceStoppedError =
    MakeError(E_REJECTED, "Local NVMe service is stopped");

const NProto::TError ServiceDestroyedError =
    MakeError(E_REJECTED, "Local NVMe service is destroyed");

const TDuration FetchDevicesRetryTimeout = TDuration::Seconds(1);
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

bool SleepCont(TDuration dt)
{
    TCont* c = RunningCont();
    Y_DEBUG_ABORT_UNLESS(c);

    if (!c) {
        return false;
    }

    const int ec = c->SleepT(dt);
    if (ec == ECANCELED) {
        return false;
    }

    Y_DEBUG_ABORT_UNLESS(ec == ETIMEDOUT, "SleepT: %d", ec);

    return true;
}

////////////////////////////////////////////////////////////////////////////////

struct TAcquireOperationResult: TFuture<TAcquireResult>
{
};

struct TReleaseOperationResult: TFuture<NProto::TError>
{
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

////////////////////////////////////////////////////////////////////////////////

class TLocalNVMeService final
    : public std::enable_shared_from_this<TLocalNVMeService>
    , public ILocalNVMeService
{
    enum class EServiceState
    {
        NotReady,
        Initializing,
        Running,
        Stopped
    };

private:
    const TLocalNVMeConfigPtr Config;
    const ILoggingServicePtr Logging;
    const ILocalNVMeDeviceProviderPtr DeviceProvider;
    const NNvme::INvmeManagerPtr NVMeManager;
    const TExecutorPtr Executor;
    const ISysFsPtr SysFs;

    std::atomic<EServiceState> ServiceState = EServiceState::NotReady;

    TLog Log;
    bool IsVfioDevSupported = false;

    TFuture<void> Ready;
    TCont* InitializeCont = nullptr;

    THashMap<TSerialNumber, NProto::TNVMeDevice> Devices;
    THashSet<TSerialNumber> AcquiredDevices;
    THashMap<TSerialNumber, TOperationPtr> Operations;

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
    auto ListDevices() const -> TVector<NProto::TNVMeDevice>;

    auto AcquireDevice(
        const TSerialNumber& serialNumber,
        const TString& idempotenceId) -> TFuture<TAcquireResult>;

    auto ReleaseDevice(
        const TSerialNumber& serialNumber,
        const TString& idempotenceId) -> TFuture<NProto::TError>;

    auto AcquireDeviceImpl(NProto::TNVMeDevice device) -> TAcquireResult;
    auto ReleaseDeviceImpl(const NProto::TNVMeDevice& device) -> NProto::TError;

    static auto EnsureIsReady(
        const std::weak_ptr<TLocalNVMeService>& weakService)
        -> TResultOrError<std::shared_ptr<TLocalNVMeService>>;

    void Initialize();
    auto FetchDevices() -> NProto::TError;
    bool TryRestoreStateFromCache();
    void UpdateStateCache();
    auto CreateStateSnapshot() -> NProto::TLocalNVMeServiceState;
    auto SanitizeNVMeDevice(const NProto::TNVMeDevice& device)
        -> NProto::TError;
    auto ResetToSingleNamespace(const NProto::TNVMeDevice& device)
        -> NProto::TError;
    auto GetNVMeCtrlPath(const NProto::TNVMeDevice& device) -> TFsPath;
    auto BindDeviceToDriver(
        const NProto::TNVMeDevice& device,
        const TString& driverName) -> NProto::TError;
    auto GetVfioDevName(const TString& pciAddress) -> TResultOrError<TString>;

    auto StopImpl() -> TFuture<void>;

    template <typename TOpResult>
    auto GetOperationResult(
        const TSerialNumber& serialNumber,
        const TString& idempotenceId)
        -> std::optional<TFuture<typename TOpResult::value_type>>;

    template <typename TOpResult, typename TFnResult, typename TFn>
    auto StartOperation(
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

void TLocalNVMeService::Initialize()
{
    // Store a pointer to the current continuation so Initialize can be stopped
    // if Stop is called.
    InitializeCont = RunningCont();
    Y_DEBUG_ABORT_UNLESS(InitializeCont);

    Y_DEFER
    {
        InitializeCont = nullptr;
    };

    if (!TryRestoreStateFromCache()) {
        for (;;) {
            STORAGE_INFO("Fetching NVMe devices from the provider");

            auto error = FetchDevices();
            if (!HasError(error)) {
                break;
            }

            STORAGE_ERROR(
                "Failed to fetch NVMe devices from the provider: "
                << FormatError(error));

            if (!SleepCont(FetchDevicesRetryTimeout)) {
                STORAGE_INFO("Initialization has been cancelled");
                return;
            }
        }
    }

    EServiceState state = EServiceState::Initializing;
    if (!ServiceState.compare_exchange_strong(state, EServiceState::Running)) {
        Y_DEBUG_ABORT_UNLESS(state == EServiceState::Stopped);
    }
}

void TLocalNVMeService::Start()
{
    if (EServiceState expected = EServiceState::NotReady;
        !ServiceState.compare_exchange_strong(
            expected,
            EServiceState::Initializing))
    {
        Y_DEBUG_ABORT(
            "Unexpected ServiceState: %d",
            static_cast<int>(expected));
        return;
    }

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
                self->Initialize();
            }
        });
}

auto TLocalNVMeService::StopImpl() -> TFuture<void>
{
    Y_DEBUG_ABORT_UNLESS(ServiceState == EServiceState::Stopped);

    TVector<TFuture<void>> futures;
    futures.reserve(Operations.size() + 1);

    futures.push_back(Ready);
    if (InitializeCont) {
        InitializeCont->Cancel();
    }

    for (const auto& [_, op]: Operations) {
        futures.emplace_back(
            std::visit(
                [](const auto& r) { return r.IgnoreResult(); },
                op->Result));

        if (op->Cont) {
            op->Cont->Cancel();
        }
    }

    return WaitAll(futures);
}

void TLocalNVMeService::Stop()
{
    const EServiceState prevState =
        ServiceState.exchange(EServiceState::Stopped);
    if (prevState == EServiceState::Stopped ||
        prevState == EServiceState::NotReady)
    {
        // Service is not running
        return;
    }

    auto future = Executor->Execute([this] { return StopImpl(); });
    future.Wait();
}

auto TLocalNVMeService::ListNVMeDevices() -> TFuture<TListDevicesResult>
{
    STORAGE_INFO("List NVMe devices");

    return Executor->Execute(
        [weakSelf = weak_from_this()]() -> TListDevicesResult
        {
            auto [self, error] = EnsureIsReady(weakSelf);

            if (HasError(error)) {
                return error;
            }

            return self->ListDevices();
        });
}

auto TLocalNVMeService::AcquireNVMeDevice(
    const TSerialNumber& serialNumber,
    const TString& idempotenceId) -> TFuture<TAcquireResult>
{
    STORAGE_INFO(
        "Acquire NVMe device " << serialNumber.Quote()
                               << " idempotenceId: " << idempotenceId);

    auto weakSelf = weak_from_this();

    return Executor->Execute(
        [weakSelf, serialNumber, idempotenceId]() mutable
        {
            auto [self, error] = EnsureIsReady(weakSelf);

            if (HasError(error)) {
                return MakeFuture<TAcquireResult>(error);
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

    auto weakSelf = weak_from_this();

    return Executor->Execute(
        [weakSelf, serialNumber, idempotenceId]() mutable
        {
            auto [self, error] = EnsureIsReady(weakSelf);

            if (HasError(error)) {
                return MakeFuture(error);
            }

            return self->ReleaseDevice(serialNumber, idempotenceId);
        });
}

////////////////////////////////////////////////////////////////////////////////

auto TLocalNVMeService::EnsureIsReady(
    const std::weak_ptr<TLocalNVMeService>& weakService)
    -> TResultOrError<std::shared_ptr<TLocalNVMeService>>
{
    auto service = weakService.lock();

    if (!service) {
        return ServiceDestroyedError;
    }

    const EServiceState state = service->ServiceState;

    if (state == EServiceState::Stopped) {
        return ServiceStoppedError;
    }

    if (state != EServiceState::Running) {
        return ServiceNotReadyError;
    }

    return service;
}

auto TLocalNVMeService::FetchDevices() -> NProto::TError
{
    auto future = DeviceProvider->ListNVMeDevices();
    auto [devices, error] = Executor->ResultOrError(future);

    if (HasError(error)) {
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
        STORAGE_DEBUG("Cache not configured");

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

    if (!proto.DevicesSize()) {
        STORAGE_INFO("No NVMe devices in the cache");
        return false;
    }

    STORAGE_INFO(
        "Found in cache NVMe devices ("
        << proto.DevicesSize() << "): " << Join(", ", proto.GetDevices())
        << "; acquired devices: " << Join(", ", proto.GetAcquiredDevices())
        << ";");

    for (const auto& device: proto.GetDevices()) {
        Devices.emplace(device.GetSerialNumber(), device);
    }

    for (const auto& device: proto.GetAcquiredDevices()) {
        AcquiredDevices.emplace(device.GetSerialNumber());
    }

    return true;
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

template <typename TOpResult>
auto TLocalNVMeService::GetOperationResult(
    const TSerialNumber& serialNumber,
    const TString& idempotenceId)
    -> std::optional<TFuture<typename TOpResult::value_type>>
{
    auto makeErrorFuture = [](ui32 code, TString msg)
    {
        return MakeFuture<typename TOpResult::value_type>(
            MakeError(code, std::move(msg)));
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
                TStringBuilder()
                    << "idempotence id was used for a different operation type "
                       "("
                    << op.Name << ", started at " << op.StartTime << ")");
        }

        if (!op.FinishTime) {
            return makeErrorFuture(E_TRY_AGAIN, "Operation is in progress");
        }

        return *typedOp;
    }

    if (!op.FinishTime) {
        return makeErrorFuture(
            E_TRY_AGAIN,
            TStringBuilder() << "Another operation is in progress (" << op.Name
                             << ", started at " << op.StartTime << ")");
    }

    return std::nullopt;
}

template <typename TOpResult, typename TFnResult, typename TFn>
auto TLocalNVMeService::StartOperation(
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

    if (auto opResult =
            GetOperationResult<TOpResult>(serialNumber, idempotenceId))
    {
        return *opResult;
    }

    auto op = std::make_shared<TOperation>();
    op->Name = TypeName<TOpResult>();
    op->IdempotenceId = idempotenceId;
    op->StartTime = Now();

    auto weakSelf = weak_from_this();

    auto future = Executor->Execute(
        [op, weakSelf, fn, device = *device]() mutable
        {
            op->Cont = RunningCont();

            auto self = weakSelf.lock();
            TFnResult r =
                self ? std::invoke(fn, self, device) : ServiceDestroyedError;

            op->FinishTime = Now();
            op->Cont = nullptr;
            return r;
        });

    op->Result = TOpResult{future};
    Operations[serialNumber] = op;

    return future;
}

auto TLocalNVMeService::AcquireDevice(
    const TSerialNumber& serialNumber,
    const TString& idempotenceId) -> TFuture<TAcquireResult>
{
    auto future = StartOperation<TAcquireOperationResult, TAcquireResult>(
        &TLocalNVMeService::AcquireDeviceImpl,
        serialNumber,
        idempotenceId);

    // If idempotenceId isn't provided - wait operation synchronously
    if (!idempotenceId || future.IsReady()) {
        return future;
    }

    return MakeFuture<TAcquireResult>(
        MakeError(E_TRY_AGAIN, "Acquire in progress"));
}

auto TLocalNVMeService::AcquireDeviceImpl(NProto::TNVMeDevice device)
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

    auto [vfioDev, error] = GetVfioDevName(device.GetPCIAddress());
    if (HasError(error)) {
        STORAGE_ERROR(
            "Failed to get vfio device for " << device << ": "
                                             << FormatError(error));
    } else {
        device.SetVfioDevName(std::move(vfioDev));
    }

    return device;
}

auto TLocalNVMeService::GetVfioDevName(const TString& pciAddress)
    -> TResultOrError<TString>
{
    // Binding to vfio-pci completes before the vfio-dev sysfs entry may
    // appear.
    // Retry to avoid racing with asynchronous vfio<N> creation.

    for (ui32 attempt = 0; attempt != VfioDeviceMaxRetries; ++attempt) {
        auto [vfioDevice, error] = SafeExecute<TResultOrError<TString>>(
            [&] { return SysFs->GetVfioDeviceForPCIDevice(pciAddress); });

        if (HasError(error)) {
            return error;
        }

        if (vfioDevice) {
            return vfioDevice;
        }

        STORAGE_DEBUG(
            "vfio device for PCI address "
            << pciAddress << " was not found, retry (attempts: " << attempt + 1
            << "/" << VfioDeviceMaxRetries << ")");

        if (!SleepCont(VfioDeviceRetryDelay)) {
            return ServiceDestroyedError;
        }
    }

    return MakeError(
        E_NOT_FOUND,
        TStringBuilder() << "vfio device for PCI address " << pciAddress
                         << " was not found after " << VfioDeviceMaxRetries
                         << " retries");
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

auto TLocalNVMeService::SanitizeNVMeDevice(const NProto::TNVMeDevice& device)
    -> NProto::TError
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

        if (!SleepCont(SanitizeStatusProbeTimeout)) {
            STORAGE_INFO(
                "Status polling of sanitize operation for "
                << device.GetSerialNumber().Quote() << " was cancelled");

            return ServiceDestroyedError;
        }
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
    auto future = StartOperation<TReleaseOperationResult, NProto::TError>(
        &TLocalNVMeService::ReleaseDeviceImpl,
        serialNumber,
        idempotenceId);

    // If idempotenceId isn't provided - wait operation synchronously
    if (!idempotenceId || future.IsReady()) {
        return future;
    }

    return MakeFuture<NProto::TError>(
        MakeError(E_TRY_AGAIN, "Release in progress"));
}

auto TLocalNVMeService::ReleaseDeviceImpl(const NProto::TNVMeDevice& device)
    -> NProto::TError
{
    if (auto error = BindDeviceToDriver(device, "nvme"); HasError(error)) {
        return error;
    }

    if (auto error = SanitizeNVMeDevice(device); HasError(error)) {
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
