#include "service.h"

#include "device_manager.h"

#include <cloud/fastshard/journal/iface/journalled_device.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <library/cpp/coroutine/engine/events.h>
#include <library/cpp/threading/future/async_semaphore.h>

#include <util/generic/algorithm.h>
#include <util/generic/scope.h>
#include <util/string/builder.h>

#include <functional>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

// Touched only in the executor, so no locking is needed: the requests, the
// formats and the stop all run there.
struct TJournalledDeviceSpec
{
    TJournalledDeviceConfig Config;

    // Set while a format of the device is in flight: a parallel format of
    // the same device is rejected.
    bool Formatting = false;

    // Null while the device is not started: it failed to start or to be
    // recreated, and stays so until the next format succeeds.
    IJournalledDevicePtr Device;
};

////////////////////////////////////////////////////////////////////////////////

using TDeviceOperation =
    std::function<TFuture<void>(TJournalledDeviceSpec& spec)>;

////////////////////////////////////////////////////////////////////////////////

THashMap<TString, TJournalledDeviceSpec> MakeDeviceSpecs(
    TVector<TJournalledDeviceConfig> configs)
{
    THashMap<TString, TJournalledDeviceSpec> specs;

    for (auto& config: configs) {
        auto& spec = specs[config.DeviceUUID];
        spec.Config = std::move(config);
    }

    return specs;
}

TVector<const TJournalledDeviceSpec*> SortDevices(
    const THashMap<TString, TJournalledDeviceSpec>& devices)
{
    TVector<const TJournalledDeviceSpec*> sorted;
    sorted.reserve(devices.size());

    for (const auto& [uuid, spec]: devices) {
        sorted.push_back(&spec);
    }

    Sort(sorted, [](const auto* lhs, const auto* rhs) {
        return lhs->Config.DeviceUUID < rhs->Config.DeviceUUID;
    });

    return sorted;
}

template <typename T>
T MakeErrorResponse(ui32 code, TString message)
{
    return TErrorResponse(code, std::move(message));
}

template <>
void MakeErrorResponse(ui32, TString)
{}

////////////////////////////////////////////////////////////////////////////////

class TJournalledDeviceHandler final
    : public IServerBackend
    , public std::enable_shared_from_this<TJournalledDeviceHandler>
{
private:
    const TExecutorPtr Executor;
    const IDeviceManagerPtr DeviceManager;
    const IJournalledDeviceFactoryPtr JournalledDeviceFactory;
    const ui32 RestoreConcurrency;

    // Filled once in the constructor, only the specs themselves change later.
    THashMap<TString, TJournalledDeviceSpec> DeviceSpecs;
    const TVector<const TJournalledDeviceSpec*> SortedDevices;

    // Set in the executor once Stop is called: no device is recreated after.
    bool Stopped = false;

    // The formats in flight, counted in the executor. Stop waits on
    // |FormatsDone| until the count drops to zero; every completed format
    // wakes the waiter up.
    ui32 FormatsInFlight = 0;
    TContWaitQueue FormatsDone;

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
        , RestoreConcurrency(restoreConcurrency)
        , DeviceSpecs(MakeDeviceSpecs(std::move(journalledDeviceConfigs)))
        , SortedDevices(SortDevices(DeviceSpecs))
    {}

    // IServerBackend

    void Start() override
    {
        auto future = RunOnDevices(
            RestoreConcurrency,
            [weakSelf = weak_from_this()](TJournalledDeviceSpec& spec)
                -> TFuture<void>
            {
                auto self = weakSelf.lock();
                if (!self) {
                    return MakeFuture();
                }

                return self->StartJournalledDevice(spec).Apply(
                    [uuid = spec.Config.DeviceUUID](const auto& future)
                    {
                        auto error = ExtractResponse(future);
                        if (HasError(error)) {
                            ReportJournalledDeviceCreationError(
                                TStringBuilder()
                                << "unable to create journalled device "
                                << uuid.Quote() << ": " << FormatError(error));
                        }
                    });
            });

        future.Wait();
    }

    void Stop() override
    {
        auto future = Execute<void>([](auto& self) { self.DoStop(); });
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

    [[nodiscard]] auto ListDevices(NProto::TListDevicesRequest request)
        -> TFuture<NProto::TListDevicesResponse> final
    {
        Y_UNUSED(request);

        NProto::TListDevicesResponse response;

        for (const auto& device: SortedDevices) {
            const auto& config = device->Config;
            const ui64 deviceSize = config.BlocksCount * config.BlockSize;

            auto& info = *response.AddDevices();
            info.SetDeviceUUID(config.DeviceUUID);
            info.SetBlockSize(config.BlockSize);
            info.SetLogMetaSize(config.LogMetaSize);
            info.SetLogDataSize(config.LogDataSize);
            info.SetDataSize(
                deviceSize - config.LogMetaSize - config.LogDataSize);
        }

        return MakeFuture(std::move(response));
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
    // Runs an operation on every device with at most |limit| of them in
    // flight: each device that completes lets the next one in.
    TFuture<void> RunOnDevices(ui32 limit, TDeviceOperation operation)
    {
        if (DeviceSpecs.empty()) {
            return MakeFuture();
        }

        auto semaphore = TAsyncSemaphore::Make(Max<size_t>(limit, 1));

        TVector<TFuture<void>> futures;
        futures.reserve(DeviceSpecs.size());

        for (auto& [uuid, spec]: DeviceSpecs) {
            auto future = semaphore->AcquireAsync().Apply(
                [&spec, operation](const auto& acquired)
                {
                    //
                    // Hold the slot until the operation completes; an
                    // operation that throws gives it back right away
                    //

                    auto release = acquired.GetValue()->MakeAutoRelease();
                    auto future = operation(spec);
                    future.Subscribe(release.DeferRelease());

                    return future;
                });

            futures.push_back(std::move(future));
        }

        return WaitAll(futures);
    }

    // Runs in the executor, where the formats run too: a format that is in
    // flight finishes first, so the device it starts is stopped here and no
    // format starts a device afterwards.
    void DoStop()
    {
        Stopped = true;

        while (FormatsInFlight) {
            auto* cont = Executor->GetContExecutor()->Running();
            if (FormatsDone.WaitI(cont) == ECANCELED) {
                break;
            }
        }

        Executor->WaitFor(RunOnDevices(
            static_cast<ui32>(DeviceSpecs.size()),
            [](TJournalledDeviceSpec& spec) -> TFuture<void>
            {
                if (!spec.Device) {
                    return MakeFuture();
                }

                return spec.Device->Stop();
            }));
    }

    TFuture<NProto::TError> StartJournalledDevice(TJournalledDeviceSpec& spec)
    {
        return Execute<NProto::TError>(
            [&spec](auto& self) { return self.DoStartJournalledDevice(spec); });
    }

    // Runs in the executor: waits for the device to start.
    NProto::TError DoStartJournalledDevice(TJournalledDeviceSpec& spec)
    {
        auto [device, error] = JournalledDeviceFactory->Create(spec.Config);
        if (HasError(error)) {
            return error;
        }

        error = Executor->ExtractResponse(device->Start());
        if (HasError(error)) {
            return error;
        }

        spec.Device = std::move(device);
        return {};
    }

    // Runs in the executor: waits for the device to stop. A device that is
    // not started has nothing to stop.
    void DoStopJournalledDevice(TJournalledDeviceSpec& spec)
    {
        if (!spec.Device) {
            return;
        }

        Executor->WaitFor(spec.Device->Stop());
        spec.Device = nullptr;
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

        if (!spec->Device) {
            return MakeError(
                E_INVALID_STATE,
                TStringBuilder()
                    << "Device " << deviceUUID.Quote() << " is not started");
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
                    return MakeErrorResponse<T>(
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

    //
    // Wipe only the journal metadata unless the whole device is requested:
    // a journal without metadata is empty, which is all a format needs
    //

    const ui64 blocksCount = request.GetWholeDevice()
                                 ? config.BlocksCount
                                 : config.LogMetaSize / config.BlockSize;
    if (!blocksCount) {
        return TErrorResponse(S_FALSE, "nothing to format");
    }

    //
    // Reject a format after Stop: it would start a device nobody stops
    //

    if (Stopped) {
        return TErrorResponse(E_INVALID_STATE, "the service is stopped");
    }

    //
    // Mark the device as busy to reject parallel recreates. The error is not
    // retriable: a retry would wipe the device again right after this format
    // completes
    //

    if (spec->Formatting) {
        return TErrorResponse(
            E_INVALID_STATE,
            TStringBuilder() << "Device " << config.DeviceUUID.Quote()
                             << " is being formatted");
    }

    //
    // Count the format so a stop waits for it, and wake the stop up once
    // the format completes
    //

    auto& formatting = spec->Formatting;
    formatting = true;
    ++FormatsInFlight;

    Y_DEFER
    {
        formatting = false;
        --FormatsInFlight;
        FormatsDone.BroadCast();
    };

    //
    // Stop the current journalled device
    //

    DoStopJournalledDevice(*spec);

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
    *response.MutableError() = DoStartJournalledDevice(*spec);
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
