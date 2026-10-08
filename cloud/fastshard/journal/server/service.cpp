#include "service.h"

#include "device_manager.h"

#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <util/generic/algorithm.h>
#include <util/string/builder.h>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TJournalledDeviceHandler final: public IServerBackend
{
private:
    const IDeviceManagerPtr DeviceManager;
    const THashMap<TString, TJournalledDeviceSpec> Devices;

public:
    TJournalledDeviceHandler(
        IDeviceManagerPtr deviceManager,
        THashMap<TString, TJournalledDeviceSpec> devices)
        : DeviceManager(std::move(deviceManager))
        , Devices(std::move(devices))
    {}

    // IServerBackend

    TFuture<NProto::TError> Start() override
    {
        TVector<TFuture<NProto::TError>> futures;
        futures.reserve(Devices.size());

        for (const auto& [uuid, spec]: Devices) {
            auto future = spec.Device->Start().Apply(
                [uuid](const auto& future)
                {
                    auto error = ExtractResponse(future);
                    if (HasError(error)) {
                        ReportJournalledDeviceCreationError(
                            TStringBuilder()
                            << "unable to start device " << uuid.Quote() << ": "
                            << FormatError(error));
                    }
                    return error;
                });

            futures.push_back(std::move(future));
        }

        return WaitAll(futures).Apply([](const auto&)
                                      { return NProto::TError(); });
    }

    TFuture<NProto::TError> Stop() override
    {
        TVector<TFuture<NProto::TError>> futures;
        futures.reserve(Devices.size());

        for (const auto& [uuid, spec]: Devices) {
            futures.push_back(spec.Device->Stop());
        }

        return WaitAll(futures).Apply([](const auto&)
                                      { return NProto::TError(); });
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

    [[nodiscard]] auto ListDevices(NProto::TListDevicesRequest request)
        -> TFuture<NProto::TListDevicesResponse> final
    {
        Y_UNUSED(request);

        TVector<TString> uuids;
        uuids.reserve(Devices.size());
        for (const auto& [uuid, spec]: Devices) {
            uuids.push_back(uuid);
        }
        Sort(uuids);

        NProto::TListDevicesResponse response;

        for (const auto& uuid: uuids) {
            const auto& config = Devices.at(uuid).Config;
            const ui64 deviceSize = config.BlocksCount * config.BlockSize;

            auto& info = *response.AddDevices();
            info.SetDeviceUUID(uuid);
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
    TVector<TJournalledDeviceSpec> journalledDevices)
{
    THashMap<TString, TJournalledDeviceSpec> deviceMap;
    for (auto device: journalledDevices) {
        auto uuid = device.Config.DeviceUUID;
        deviceMap.emplace(std::move(uuid), std::move(device));
    }

    return std::make_shared<TJournalledDeviceHandler>(
        std::move(deviceManager),
        std::move(deviceMap));
}

}   // namespace NCloud::NJournalled
