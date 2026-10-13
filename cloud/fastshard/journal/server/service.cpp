#include "service.h"

#include "device_manager.h"

#include <cloud/fastshard/journal/iface/journalled_device.h>

#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <util/generic/scope.h>
#include <util/string/builder.h>

#include <atomic>
#include <functional>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TJournalledDeviceSpec
{
    TJournalledDeviceConfig Config;

    // Set while the device is being recreated.
    std::atomic_bool Recreating = false;
    IJournalledDevicePtr Device;
};

////////////////////////////////////////////////////////////////////////////////

using TDeviceOperation =
    std::function<TFuture<NProto::TError>(TJournalledDeviceSpec* spec)>;

////////////////////////////////////////////////////////////////////////////////

// Runs an operation on every device with at most |limit| of them in flight:
// each device that completes lets the next one in.
class TDeviceOperationWindow
    : public std::enable_shared_from_this<TDeviceOperationWindow>
{
private:
    const TVector<TJournalledDeviceSpec*> Specs;
    const TDeviceOperation Operation;

    std::atomic<size_t> NextIndex = 0;
    std::atomic<size_t> CompletedCount = 0;
    TPromise<void> AllCompleted = NewPromise<void>();

public:
    TDeviceOperationWindow(
        TVector<TJournalledDeviceSpec*> specs,
        TDeviceOperation operation)
        : Specs(std::move(specs))
        , Operation(std::move(operation))
    {}

    TFuture<void> Run(size_t limit)
    {
        if (Specs.empty()) {
            return MakeFuture();
        }

        const size_t inFlight = Min(Max<size_t>(limit, 1), Specs.size());
        for (size_t i = 0; i < inFlight; ++i) {
            RunNext();
        }

        return AllCompleted.GetFuture();
    }

private:
    void RunNext()
    {
        for (;;) {
            const size_t index = NextIndex.fetch_add(1);
            if (index >= Specs.size()) {
                return;
            }

            auto future = RunOperation(Specs[index]);

            // Runs the devices one after another while their operations
            // complete right away, so a run over ready futures loops
            // instead of recursing.
            if (future.HasValue() || future.HasException()) {
                if (Complete()) {
                    return;
                }
                continue;
            }

            future.Subscribe(
                [self = shared_from_this()](const auto&)
                {
                    if (!self->Complete()) {
                        self->RunNext();
                    }
                });
            return;
        }
    }

    TFuture<NProto::TError> RunOperation(TJournalledDeviceSpec* spec)
    {
        try {
            return Operation(spec);
        } catch (...) {
            return MakeFuture(MakeError(E_FAIL, CurrentExceptionMessage()));
        }
    }

    bool Complete()
    {
        if (CompletedCount.fetch_add(1) + 1 != Specs.size()) {
            return false;
        }

        AllCompleted.SetValue();
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

// Stands in for a journalled device that is not started (yet) or failed to
// start. Every request is rejected so the device can be safely stopped and
// replaced.
class TFailedDevice final: public IJournalledDevice
{
public:
    TFuture<NProto::TError> Start() override
    {
        return MakeFuture(MakeInvalidStateError());
    }

    TFuture<NProto::TError> Stop() override
    {
        return MakeFuture<NProto::TError>();
    }

    [[nodiscard]] auto ReadPages(NProto::TReadPagesRequest request)
        -> TFuture<NProto::TReadPagesResponse> override
    {
        Y_UNUSED(request);
        return MakeFuture<NProto::TReadPagesResponse>(
            TErrorResponse(MakeInvalidStateError()));
    }

    [[nodiscard]] auto WriteLogRecord(NProto::TWriteLogRecordRequest request)
        -> TFuture<NProto::TWriteLogRecordResponse> override
    {
        Y_UNUSED(request);
        return MakeFuture<NProto::TWriteLogRecordResponse>(
            TErrorResponse(MakeInvalidStateError()));
    }

    [[nodiscard]] auto ReadJournalTail(NProto::TReadJournalTailRequest request)
        -> TFuture<NProto::TReadJournalTailResponse> override
    {
        Y_UNUSED(request);
        return MakeFuture<NProto::TReadJournalTailResponse>(
            TErrorResponse(MakeInvalidStateError()));
    }

    [[nodiscard]] auto AdvanceLsnLowWatermark(
        NProto::TAdvanceLsnLowWatermarkRequest request)
        -> TFuture<NProto::TAdvanceLsnLowWatermarkResponse> override
    {
        Y_UNUSED(request);
        return MakeFuture<NProto::TAdvanceLsnLowWatermarkResponse>(
            TErrorResponse(MakeInvalidStateError()));
    }

private:
    static NProto::TError MakeInvalidStateError()
    {
        return MakeError(E_INVALID_STATE, "Journalled device is not started");
    }
};

////////////////////////////////////////////////////////////////////////////////

class TJournalledDeviceHandler final
    : public IServerBackend
    , public std::enable_shared_from_this<TJournalledDeviceHandler>
{
private:
    const TExecutorPtr Executor;
    const IDeviceManagerPtr DeviceManager;
    const IJournalledDeviceFactoryPtr JournalledDeviceFactory;
    const IJournalledDevicePtr FailedDevice;
    const ui32 RestoreConcurrency;

    // Filled once in the constructor, only the specs themselves change later.
    THashMap<TString, TJournalledDeviceSpec> DeviceSpecs;

public:
    TJournalledDeviceHandler(
        TExecutorPtr executor,
        IDeviceManagerPtr deviceManager,
        IJournalledDeviceFactoryPtr journalledDeviceFactory,
        TVector<TJournalledDeviceConfig> journalledDeviceConfigs,
        ui32 restoreConcurrency)
        : Executor(std::move(executor))
        , DeviceManager(std::move(deviceManager))
        , JournalledDeviceFactory(std::move(journalledDeviceFactory))
        , FailedDevice(std::make_shared<TFailedDevice>())
        , RestoreConcurrency(restoreConcurrency)
    {
        for (auto& config: journalledDeviceConfigs) {
            auto& spec = DeviceSpecs[config.DeviceUUID];
            spec.Config = std::move(config);
            spec.Device = FailedDevice;
        }
    }

    // IServerBackend

    void Start() override
    {
        auto future = RunOnDevices(
            RestoreConcurrency,
            [weakSelf = weak_from_this()](TJournalledDeviceSpec* spec)
            {
                auto self = weakSelf.lock();
                if (!self) {
                    return MakeFuture(MakeError(
                        E_FAIL,
                        "TJournalledDeviceHandler is destroyed"));
                }

                return self->StartJournalledDevice(spec).Apply(
                    [uuid = spec->Config.DeviceUUID](const auto& future)
                    {
                        auto error = ExtractResponse(future);
                        if (HasError(error)) {
                            ReportJournalledDeviceCreationError(
                                TStringBuilder()
                                << "unable to create journalled device "
                                << uuid.Quote() << ": " << FormatError(error));
                        }
                        return error;
                    });
            });

        future.Wait();
    }

    void Stop() override
    {
        auto future = RunOnDevices(
            DeviceSpecs.size(),
            [](TJournalledDeviceSpec* spec) { return spec->Device->Stop(); });

        future.Wait();
    }

    [[nodiscard]] auto AcquireDevices(NProto::TAcquireDevicesRequest request)
        -> TFuture<NProto::TAcquireDevicesResponse> final
    {
        return DeviceManager->AcquireDevices(std::move(request));
    }

    [[nodiscard]] auto ReleaseDevices(NProto::TReleaseDevicesRequest request)
        -> TFuture<NProto::TReleaseDevicesResponse> final
    {
        return DeviceManager->ReleaseDevices(std::move(request));
    }

    [[nodiscard]] auto FormatDevice(NProto::TFormatDeviceRequest request)
        -> TFuture<NProto::TFormatDeviceResponse> final
    {
        return Execute<NProto::TFormatDeviceResponse>(
            [req = std::move(request)](auto& self) mutable
            { return self.DoFormatDevice(std::move(req)); });
    }

    [[nodiscard]] auto ReadPages(NProto::TReadPagesRequest request)
        -> TFuture<NProto::TReadPagesResponse> final
    {
        auto [device, error] = GetDevice(
            request.GetDeviceUUID(),
            request.GetHeaders().GetClientId(),
            NProto::ACCESS_READ_ONLY);

        if (HasError(error)) {
            return MakeFuture<NProto::TReadPagesResponse>(
                TErrorResponse(error));
        }

        return device->ReadPages(std::move(request));
    }

    [[nodiscard]] auto WriteLogRecord(NProto::TWriteLogRecordRequest request)
        -> TFuture<NProto::TWriteLogRecordResponse> final
    {
        auto [device, error] = GetDevice(
            request.GetDeviceUUID(),
            request.GetHeaders().GetClientId(),
            NProto::ACCESS_READ_WRITE);

        if (HasError(error)) {
            return MakeFuture<NProto::TWriteLogRecordResponse>(
                TErrorResponse(error));
        }

        return device->WriteLogRecord(std::move(request));
    }

    [[nodiscard]] auto ReadJournalTail(NProto::TReadJournalTailRequest request)
        -> TFuture<NProto::TReadJournalTailResponse> final
    {
        auto [device, error] = GetDevice(
            request.GetDeviceUUID(),
            request.GetHeaders().GetClientId(),
            NProto::ACCESS_READ_ONLY);

        if (HasError(error)) {
            return MakeFuture<NProto::TReadJournalTailResponse>(
                TErrorResponse(error));
        }

        return device->ReadJournalTail(std::move(request));
    }

    [[nodiscard]] auto AdvanceLsnLowWatermark(
        NProto::TAdvanceLsnLowWatermarkRequest request)
        -> TFuture<NProto::TAdvanceLsnLowWatermarkResponse> final
    {
        auto [device, error] = GetDevice(
            request.GetDeviceUUID(),
            request.GetHeaders().GetClientId(),
            NProto::ACCESS_READ_WRITE);

        if (HasError(error)) {
            return MakeFuture<NProto::TAdvanceLsnLowWatermarkResponse>(
                TErrorResponse(error));
        }

        return device->AdvanceLsnLowWatermark(std::move(request));
    }

private:
    TFuture<void> RunOnDevices(size_t limit, TDeviceOperation operation)
    {
        TVector<TJournalledDeviceSpec*> specs;
        specs.reserve(DeviceSpecs.size());
        for (auto& [uuid, spec]: DeviceSpecs) {
            specs.push_back(&spec);
        }

        auto window = std::make_shared<TDeviceOperationWindow>(
            std::move(specs),
            std::move(operation));

        return window->Run(limit);
    }

    TFuture<NProto::TError> StartJournalledDevice(TJournalledDeviceSpec* spec)
    {
        return Execute<NProto::TError>(
            [spec](auto& self) { return self.DoStartJournalledDevice(spec); });
    }

    // Runs in the executor: waits for the device to start.
    NProto::TError DoStartJournalledDevice(TJournalledDeviceSpec* spec)
    {
        auto [device, error] = JournalledDeviceFactory->Create(spec->Config);
        if (HasError(error)) {
            return error;
        }

        if (error = Executor->ExtractResponse(device->Start()); HasError(error))
        {
            return error;
        }

        spec->Device = std::move(device);
        return {};
    }

    // Runs in the executor: waits for the device to stop.
    NProto::TError DoStopJournalledDevice(TJournalledDeviceSpec* spec)
    {
        auto error = Executor->ExtractResponse(spec->Device->Stop());
        if (HasError(error)) {
            return error;
        }

        spec->Device = FailedDevice;
        return {};
    }

    NProto::TFormatDeviceResponse DoFormatDevice(
        NProto::TFormatDeviceRequest request);

    TResultOrError<IJournalledDevicePtr> GetDevice(
        const TString& deviceUUID,
        const TString& clientId,
        NProto::EAccessMode accessMode)
    {
        auto [spec, error] = GetDeviceSpec(deviceUUID, clientId, accessMode);
        if (HasError(error)) {
            return error;
        }

        return spec->Device;
    }

    TResultOrError<TJournalledDeviceSpec*> GetDeviceSpec(
        const TString& deviceUUID,
        const TString& clientId,
        NProto::EAccessMode accessMode)
    {
        if (deviceUUID.empty()) {
            return MakeError(E_ARGUMENT, "empty device UUID");
        }

        if (clientId.empty()) {
            return MakeError(E_ARGUMENT, "empty client id");
        }

        auto* spec = DeviceSpecs.FindPtr(deviceUUID);
        if (!spec) {
            return MakeError(
                E_NOT_FOUND,
                TStringBuilder()
                    << "Device " << deviceUUID.Quote() << " not found");
        }

        auto error =
            DeviceManager->AccessDevice(deviceUUID, clientId, accessMode);

        if (HasError(error)) {
            return error;
        }

        return spec;
    }

    // Runs |func| in the executor, so it can wait for futures inline.
    template <typename T, typename F>
    TFuture<T> Execute(F func)
    {
        return Executor->Execute(
            [weakSelf = weak_from_this(), func = std::move(func)]() mutable -> T
            {
                auto self = weakSelf.lock();
                if (!self) {
                    return TErrorResponse(
                        E_FAIL,
                        "TJournalledDeviceHandler is destroyed");
                }

                return func(*self);
            });
    }
};

////////////////////////////////////////////////////////////////////////////////

NProto::TFormatDeviceResponse TJournalledDeviceHandler::DoFormatDevice(
    NProto::TFormatDeviceRequest request)
{
    //
    // Find the device to format and check the access
    //

    auto [spec, error] = GetDeviceSpec(
        request.GetDeviceUUID(),
        request.GetHeaders().GetClientId(),
        NProto::ACCESS_READ_WRITE);

    if (HasError(error)) {
        return TErrorResponse(error);
    }

    //
    // Figure out the region to wipe
    //

    const auto& config = spec->Config;

    // Only the journal metadata is wiped by default.
    const ui64 blocksCount = request.GetWholeDevice()
                                 ? config.BlocksCount
                                 : config.LogMetaSize / config.BlockSize;
    if (!blocksCount) {
        return TErrorResponse(S_FALSE, "nothing to format");
    }

    //
    // Mark the device as busy to reject parallel recreates
    //

    if (spec->Recreating.exchange(true)) {
        return TErrorResponse(
            E_REJECTED,
            TStringBuilder() << "Device " << config.DeviceUUID.Quote()
                             << " is being formatted");
    }
    Y_DEFER
    {
        spec->Recreating.store(false);
    };

    //
    // Stop the current journalled device
    //

    if (auto stopError = DoStopJournalledDevice(spec); HasError(stopError)) {
        return TErrorResponse(stopError);
    }

    //
    // Wipe the region
    //

    TPageRangeRef region{.FirstPageNo = 0, .PageCount = blocksCount};

    auto device = DeviceManager->CreateDevice(
        config.DeviceUUID,
        region,
        config.BlockSize);

    auto zeroError = Executor->ExtractResponse(device->ZeroPages({region}));
    if (HasError(zeroError)) {
        return TErrorResponse(zeroError);
    }

    device.reset();

    //
    // Start a new journalled device over the wiped region
    //

    NProto::TFormatDeviceResponse response;
    *response.MutableError() = DoStartJournalledDevice(spec);
    return response;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IServerBackendPtr CreateService(
    TExecutorPtr executor,
    IDeviceManagerPtr deviceManager,
    IJournalledDeviceFactoryPtr journalledDeviceFactory,
    TVector<TJournalledDeviceConfig> journalledDeviceConfigs,
    ui32 restoreConcurrency)
{
    return std::make_shared<TJournalledDeviceHandler>(
        std::move(executor),
        std::move(deviceManager),
        std::move(journalledDeviceFactory),
        std::move(journalledDeviceConfigs),
        restoreConcurrency);
}

}   // namespace NCloud::NJournalled
