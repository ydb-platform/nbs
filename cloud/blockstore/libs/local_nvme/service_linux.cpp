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

const TDuration UpdateDevicesRetryTimeout = TDuration::Seconds(1);
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
    const ITaskQueuePtr BackgroundExecutor;
    const ISysFsPtr SysFs;

    std::atomic<EServiceState> ServiceState = EServiceState::NotReady;

    TLog Log;
    bool IsVfioDevSupported = false;

    TFuture<void> Ready;
    TCont* StartCont = nullptr;

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
        ITaskQueuePtr backgroundExecutor,
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

    void StartImpl();
    auto Initialize() -> NProto::TError;
    auto FetchDevices() const -> TResultOrError<TVector<NProto::TNVMeDevice>>;
    auto PrepareDevice(
        const TSerialNumber& serialNumber,
        const TString& pciAddr) const -> TResultOrError<NProto::TNVMeDevice>;
    auto UpdateDevices() -> NProto::TError;
    bool TryRestoreStateFromCache();
    void UpdateStateCache();
    auto CreateStateSnapshot() const -> NProto::TLocalNVMeServiceState;
    auto SanitizeNVMeDevice(const NProto::TNVMeDevice& device)
        -> NProto::TError;
    auto ResetToSingleNamespace(const NProto::TNVMeDevice& device)
        -> NProto::TError;
    auto GetNVMeCtrlPath(const NProto::TNVMeDevice& device) const -> TFsPath;
    auto BindDeviceToDriver(
        const NProto::TNVMeDevice& device,
        const TString& driverName) -> NProto::TError;
    auto GetVfioDevName(const TString& pciAddress) const
        -> TResultOrError<TString>;

    auto StopImpl() -> TFuture<void>;

    template <typename TOpResult>
    auto GetOperationResult(
        const TSerialNumber& serialNumber,
        const TString& idempotenceId) const
        -> std::optional<TFuture<typename TOpResult::value_type>>;

    template <typename TOpResult, typename TFnResult, typename TFn>
    auto StartOperation(
        TString opName,
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
    ITaskQueuePtr backgroundExecutor,
    ISysFsPtr sysFs)
    : Config(std::move(config))
    , Logging(std::move(logging))
    , DeviceProvider(std::move(deviceProvider))
    , NVMeManager(std::move(nvmeManager))
    , Executor(std::move(executor))
    , BackgroundExecutor(std::move(backgroundExecutor))
    , SysFs(std::move(sysFs))
{}

void TLocalNVMeService::StartImpl()
{
    // Save the StartImpl continuation so Stop() can interrupt startup.
    StartCont = RunningCont();
    Y_DEBUG_ABORT_UNLESS(StartCont);

    Y_DEFER
    {
        StartCont = nullptr;
    };

    if (auto error = Initialize(); HasError(error)) {
        STORAGE_ERROR(
            "Failed to initialize Local NVMe service: " << FormatError(error));
        return;
    }

    // Initialization is complete; publish the Running state.
    EServiceState state = EServiceState::Initializing;
    if (!ServiceState.compare_exchange_strong(state, EServiceState::Running)) {
        Y_DEBUG_ABORT_UNLESS(state == EServiceState::Stopped);
        return;
    }

    // The service is already running, but devices may appear later. Keep
    // polling the provider until at least one device becomes available.
    while (Devices.empty()) {
        if (!SleepCont(Config->GetUpdateDevicesInterval())) {
            return;
        }

        const auto error = UpdateDevices();
        if (HasError(error)) {
            STORAGE_ERROR(
                "Failed to update NVMe device list: " << FormatError(error));
        }
    }
}

auto TLocalNVMeService::Initialize() -> NProto::TError
{
    if (TryRestoreStateFromCache()) {
        return {};
    }

    for (;;) {
        const auto error = UpdateDevices();
        if (!HasError(error)) {
            break;
        }

        STORAGE_ERROR(
            "Failed to update NVMe device list: " << FormatError(error));

        if (!SleepCont(UpdateDevicesRetryTimeout)) {
            return MakeError(E_CANCELLED, "Initialization has been cancelled");
        }
    }

    return {};
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

    STORAGE_INFO("Starting Local NVMe service");

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
                self->StartImpl();
            }
        });
}

auto TLocalNVMeService::StopImpl() -> TFuture<void>
{
    Y_DEBUG_ABORT_UNLESS(ServiceState == EServiceState::Stopped);

    TVector<TFuture<void>> futures;
    futures.reserve(Operations.size() + 1);

    futures.push_back(Ready);
    if (StartCont) {
        StartCont->Cancel();
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

    STORAGE_INFO("Stopping Local NVMe service");

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

auto TLocalNVMeService::UpdateDevices() -> NProto::TError
{
    auto [devices, error] = FetchDevices();
    if (HasError(error)) {
        return error;
    }

    Devices.clear();
    for (auto& d: devices) {
        Devices[d.GetSerialNumber()] = std::move(d);
    }

    UpdateStateCache();

    return {};
}

auto TLocalNVMeService::PrepareDevice(
    const TSerialNumber& serialNumber,
    const TString& pciAddr) const -> TResultOrError<NProto::TNVMeDevice>
{
    if (pciAddr.empty()) {
        return MakeError(E_ARGUMENT, "empty PCI address");
    }

    if (serialNumber.empty()) {
        return MakeError(E_ARGUMENT, "empty serial number");
    }

    const auto normalizedPCIAddr = NormalizePCIAddr(pciAddr);

    auto [device, error] = SafeExecute<TResultOrError<NProto::TNVMeDevice>>(
        [&] { return SysFs->GetNVMeDeviceFromPCIAddr(normalizedPCIAddr); });

    if (HasError(error)) {
        return MakeError(
            error.GetCode(),
            TStringBuilder() << "Failed to get NVMe device from sysfs"
                             << " by PCI address " << normalizedPCIAddr << ": "
                             << error.GetMessage());
    }

    if (device.GetSerialNumber() != serialNumber) {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder()
                << "NVMe serial number mismatch for PCI address "
                << normalizedPCIAddr << ": expected " << serialNumber
                << ", got " << device.GetSerialNumber());
    }

    return device;
}

auto TLocalNVMeService::FetchDevices() const
    -> TResultOrError<TVector<NProto::TNVMeDevice>>
{
    STORAGE_INFO("Fetching NVMe devices from the provider");

    auto future = DeviceProvider->ListNVMeDevices();
    auto [devices, error] = Executor->ResultOrError(future);

    if (HasError(error)) {
        STORAGE_ERROR(
            "Failed to fetch NVMe devices from the provider: "
            << FormatError(error));
        return error;
    }

    STORAGE_INFO(
        "Fetched NVMe devices from the provider ("
        << devices.size() << "): " << Join(", ", devices));

    TVector<NProto::TNVMeDevice> result;
    result.reserve(devices.size());

    for (const auto& src: devices) {
        auto [device, error] =
            PrepareDevice(src.GetSerialNumber(), src.GetPCIAddress());

        if (HasError(error)) {
            STORAGE_WARN(
                "Ignoring NVMe device reported by the provider"
                << ": " << src << ": " << FormatError(error));
            continue;
        }

        result.push_back(std::move(device));
    }

    return result;
}

auto TLocalNVMeService::CreateStateSnapshot() const
    -> NProto::TLocalNVMeServiceState
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

    auto future = BackgroundExecutor->Execute(
        [sysfs = SysFs, pciAddr = device.GetPCIAddress(), driverName]
        { sysfs->BindPCIDeviceToDriver(pciAddr, driverName); });

    return Executor->ResultOrError(future).GetError();
}

template <typename TOpResult>
auto TLocalNVMeService::GetOperationResult(
    const TSerialNumber& serialNumber,
    const TString& idempotenceId) const
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
    TString opName,
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
        STORAGE_INFO(
            "Idempotent " << opName
                          << " NVMe device operation already completed for "
                          << serialNumber.Quote()
                          << ", idempotenceId: " << idempotenceId);

        return *opResult;
    }

    STORAGE_INFO(
        "Starting " << opName << " NVMe device operation for "
                    << serialNumber.Quote()
                    << ", idempotenceId: " << idempotenceId);

    auto op = std::make_shared<TOperation>();
    op->Name = std::move(opName);
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
        "Acquire",
        &TLocalNVMeService::AcquireDeviceImpl,
        serialNumber,
        idempotenceId);

    if (future.IsReady()) {
        return future;
    }

    // If idempotenceId isn't provided, wait for the operation synchronously.
    if (!idempotenceId) {
        STORAGE_INFO(
            "Waiting for Acquire NVMe device operation to complete "
            "synchronously for "
            << serialNumber.Quote() << ": idempotenceId is not provided");
        return future;
    }

    STORAGE_DEBUG(
        "Acquire NVMe device operation is still in progress for "
        << serialNumber.Quote() << ", idempotenceId: " << idempotenceId);

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

auto TLocalNVMeService::GetVfioDevName(const TString& pciAddress) const
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

auto TLocalNVMeService::GetNVMeCtrlPath(const NProto::TNVMeDevice& device) const
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

    const auto ctrlPath = GetNVMeCtrlPath(device);

    CheckError(NVMeManager->StartSanitize(ctrlPath));

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
} catch (const TServiceError& e) {
    return MakeError(e.GetCode(), ToString(e.GetMessage()));
} catch (...) {
    return MakeError(E_FAIL, CurrentExceptionMessage());
}

auto TLocalNVMeService::ResetToSingleNamespace(
    const NProto::TNVMeDevice& device) -> NProto::TError
{
    STORAGE_INFO(
        "Reset NVMe " << device.GetSerialNumber().Quote()
                      << " to single namespace");

    auto [ctrlPath, error] = SafeExecute<TResultOrError<TFsPath>>(
        [&] { return GetNVMeCtrlPath(device); });
    if (HasError(error)) {
        return error;
    }

    auto future = BackgroundExecutor->Execute(
        [nvme = NVMeManager, ctrlPath = std::move(ctrlPath)]
        { return nvme->ResetToSingleNamespace(ctrlPath); });

    return Executor->WaitFor(future);
}

auto TLocalNVMeService::ReleaseDevice(
    const TSerialNumber& serialNumber,
    const TString& idempotenceId) -> TFuture<NProto::TError>
{
    auto future = StartOperation<TReleaseOperationResult, NProto::TError>(
        "Release",
        &TLocalNVMeService::ReleaseDeviceImpl,
        serialNumber,
        idempotenceId);

    if (future.IsReady()) {
        return future;
    }

    // If idempotenceId isn't provided, wait for the operation synchronously.
    if (!idempotenceId) {
        STORAGE_INFO(
            "Waiting for Release NVMe device operation to complete "
            "synchronously for "
            << serialNumber.Quote() << ": idempotenceId is not provided");
        return future;
    }

    STORAGE_DEBUG(
        "Release NVMe device operation is still in progress for "
        << serialNumber.Quote() << ", idempotenceId: " << idempotenceId);

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
    ITaskQueuePtr backgroundExecutor,
    ISysFsPtr sysFs)
{
    return std::make_shared<TLocalNVMeService>(
        std::move(config),
        std::move(logging),
        std::move(deviceProvider),
        std::move(nvmeManager),
        std::move(executor),
        std::move(backgroundExecutor),
        std::move(sysFs));
}

ILocalNVMeServicePtr CreateLocalNVMeService(
    TLocalNVMeConfigPtr config,
    ILoggingServicePtr logging,
    ILocalNVMeDeviceProviderPtr deviceProvider,
    NNvme::INvmeManagerPtr nvmeManager,
    TExecutorPtr executor,
    ITaskQueuePtr backgroundExecutor)
{
    return CreateLocalNVMeService(
        std::move(config),
        std::move(logging),
        std::move(deviceProvider),
        std::move(nvmeManager),
        std::move(executor),
        std::move(backgroundExecutor),
        CreateSysFs("/sys"));
}

}   // namespace NCloud::NBlockStore
