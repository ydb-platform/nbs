#include "service.h"

#include "device_manager.h"

#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <util/string/builder.h>

#include <atomic>
#include <functional>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TNamedDevice
{
    TString UUID;
    IJournalledDevicePtr Device;
};

////////////////////////////////////////////////////////////////////////////////

using TDeviceOperation =
    std::function<TFuture<NProto::TError>(const TNamedDevice& device)>;

////////////////////////////////////////////////////////////////////////////////

// Runs an operation on every device with at most |limit| of them in flight
class TDeviceOperationWindow
    : public std::enable_shared_from_this<TDeviceOperationWindow>
{
private:
    const TVector<TNamedDevice> Devices;
    const TDeviceOperation Operation;

    std::atomic<size_t> NextIndex = 0;
    std::atomic<size_t> CompletedCount = 0;
    TPromise<NProto::TError> AllCompleted = NewPromise<NProto::TError>();

public:
    TDeviceOperationWindow(
            TVector<TNamedDevice> devices,
            TDeviceOperation operation)
        : Devices(std::move(devices))
        , Operation(std::move(operation))
    {}

    TFuture<NProto::TError> Run(size_t limit)
    {
        if (Devices.empty()) {
            return MakeFuture<NProto::TError>();
        }

        const size_t inFlight = Min(Max<size_t>(limit, 1), Devices.size());
        for (size_t i = 0; i < inFlight; ++i) {
            RunNext();
        }

        return AllCompleted.GetFuture();
    }

private:
    void RunNext()
    {
        const size_t index = NextIndex.fetch_add(1);
        if (index >= Devices.size()) {
            return;
        }

        Operation(Devices[index]).Subscribe(
            [self = shared_from_this()](const auto&)
            {
                if (self->CompletedCount.fetch_add(1) + 1 ==
                    self->Devices.size())
                {
                    self->AllCompleted.SetValue(NProto::TError());
                    return;
                }

                self->RunNext();
            });
    }
};

////////////////////////////////////////////////////////////////////////////////

class TJournalledDeviceHandler final: public IServerBackend
{
private:
    const IDeviceManagerPtr DeviceManager;
    const THashMap<TString, TJournalledDeviceSpec> Devices;
    const ui32 RestoreConcurrency;

public:
    TJournalledDeviceHandler(
        IDeviceManagerPtr deviceManager,
        THashMap<TString, TJournalledDeviceSpec> devices,
        ui32 restoreConcurrency)
        : DeviceManager(std::move(deviceManager))
        , Devices(std::move(devices))
        , RestoreConcurrency(restoreConcurrency)
    {}

    // IServerBackend

    TFuture<NProto::TError> Start() override
    {
        return RunOnDevices(
            RestoreConcurrency,
            [](const TNamedDevice& device)
            {
                return device.Device->Start().Apply(
                    [uuid = device.UUID](const auto& future)
                    {
                        auto error = ExtractResponse(future);
                        if (HasError(error)) {
                            ReportJournalledDeviceCreationError(
                                TStringBuilder()
                                << "unable to start device " << uuid.Quote()
                                << ": " << FormatError(error));
                        }
                        return error;
                    });
            });
    }

    TFuture<NProto::TError> Stop() override
    {
        return RunOnDevices(
            Devices.size(),
            [](const TNamedDevice& device) { return device.Device->Stop(); });
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
        auto [spec, error] = GetDeviceSpec(
            request.GetDeviceUUID(),
            request.GetHeaders().GetClientId(),
            NProto::ACCESS_READ_WRITE);

        if (HasError(error)) {
            return MakeFuture<NProto::TFormatDeviceResponse>(
                TErrorResponse(error));
        }

        const auto& config = spec->Config;

        // Only the journal metadata is wiped by default.
        const ui64 blocksCount =
            request.GetWholeDevice()
                ? config.BlocksCount
                : config.LogMetaSize / config.BlockSize;

        if (!blocksCount) {
            return MakeFuture(NProto::TFormatDeviceResponse());
        }

        TPageRangeRef region{.FirstPageNo = 0, .PageCount = blocksCount};

        auto device = DeviceManager->CreateDevice(
            request.GetDeviceUUID(),
            region,
            config.BlockSize);

        return device->ZeroPages({region}).Apply(
            [](const auto& future)
            {
                NProto::TFormatDeviceResponse response;
                *response.MutableError() = SafeExecute<NProto::TError>(
                    [&] { return future.GetValue(); });

                return response;
            });
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
    TFuture<NProto::TError> RunOnDevices(
        size_t limit,
        TDeviceOperation operation) const
    {
        TVector<TNamedDevice> devices;
        devices.reserve(Devices.size());
        for (const auto& [uuid, spec]: Devices) {
            devices.push_back({.UUID = uuid, .Device = spec.Device});
        }

        auto window = std::make_shared<TDeviceOperationWindow>(
            std::move(devices),
            std::move(operation));

        return window->Run(limit);
    }

    TResultOrError<NJournalled::IJournalledDevicePtr> GetDevice(
        const TString& deviceUUID,
        const TString& clientId,
        NProto::EAccessMode accessMode) const
    {
        auto [spec, error] = GetDeviceSpec(deviceUUID, clientId, accessMode);
        if (HasError(error)) {
            return error;
        }

        return spec->Device;
    }

    TResultOrError<const TJournalledDeviceSpec*> GetDeviceSpec(
        const TString& deviceUUID,
        const TString& clientId,
        NProto::EAccessMode accessMode) const
    {
        if (deviceUUID.empty()) {
            return MakeError(E_ARGUMENT, "empty device UUID");
        }

        if (clientId.empty()) {
            return MakeError(E_ARGUMENT, "empty client id");
        }

        const auto* spec = Devices.FindPtr(deviceUUID);
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
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IServerBackendPtr CreateService(
    IDeviceManagerPtr deviceManager,
    TVector<TJournalledDeviceSpec> journalledDevices,
    ui32 restoreConcurrency)
{
    THashMap<TString, TJournalledDeviceSpec> deviceMap;
    for (auto device: journalledDevices) {
        auto uuid = device.Config.DeviceUUID;
        deviceMap.emplace(std::move(uuid), std::move(device));
    }

    return std::make_shared<TJournalledDeviceHandler>(
        std::move(deviceManager),
        std::move(deviceMap),
        restoreConcurrency);
}

}   // namespace NCloud::NJournalled
