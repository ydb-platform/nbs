#include "disk_agent_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/storage/disk_agent/journalled_device_adapter.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_client.h>

#include <cloud/fastshard/journal/impl/device_page_store.h>
#include <cloud/fastshard/journal/impl/journal.h>
#include <cloud/fastshard/journal/impl/journalled_device_v1.h>
#include <cloud/fastshard/journal/impl/journalled_device_v2.h>
#include <cloud/fastshard/journal/impl/key_buffer_store.h>
#include <cloud/fastshard/journal/server/device_manager.h>
#include <cloud/fastshard/journal/server/server.h>

#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <util/generic/hash.h>
#include <util/string/builder.h>

#include <optional>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NJournalled;
using namespace NKikimr;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

// Fastshard orders writers by generation only, so the mount sequence number of
// the disk agent is never used.
constexpr ui64 DefaultMountSeqNumber = 0;

////////////////////////////////////////////////////////////////////////////////

void CopyHeaders(
    NProto::THeaders& dst,
    const NCloud::NProto::TDeviceRequestHeaders& src)
{
    dst.SetClientId(src.GetClientId());
    dst.SetRequestTimeout(src.GetRequestTimeout());
}

std::optional<NProto::EVolumeAccessMode> GetVolumeAccessMode(
    NProto::EAccessMode accessMode)
{
    switch (accessMode) {
        case NProto::ACCESS_READ_WRITE:
            return NProto::VOLUME_ACCESS_READ_WRITE;
        case NProto::ACCESS_READ_ONLY:
            return NProto::VOLUME_ACCESS_READ_ONLY;

        default:
            return std::nullopt;
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TJournalledDeviceConfig
{
    TString DeviceUUID;

    ui64 BlocksCount = 0;
    ui32 BlockSize = 0;

    ui64 LogMetaSize = 0;
    ui64 LogDataSize = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TJournalledDeviceSpec
{
    IJournalledDevicePtr Device;
    TJournalledDeviceConfig Config;
};

////////////////////////////////////////////////////////////////////////////////

class TDeviceManager final: public IDeviceManager
{
private:
    const ITimerPtr Timer;
    const TDeviceClientPtr DeviceClient;
    TActorSystem* ActorSystem = nullptr;
    const TActorId DiskAgentActorId;

public:
    TDeviceManager(
        ITimerPtr timer,
        TDeviceClientPtr deviceClient,
        TActorSystem* actorSystem,
        const TActorId& diskAgentActorId)
        : Timer(std::move(timer))
        , DeviceClient(std::move(deviceClient))
        , ActorSystem(actorSystem)
        , DiskAgentActorId(diskAgentActorId)
    {}

    [[nodiscard]] auto AcquireDevices(
        NCloud::NProto::TAcquireDevicesRequest request)
        -> TFuture<NCloud::NProto::TAcquireDevicesResponse> final
    {
        const auto accessMode = GetVolumeAccessMode(request.GetAccessMode());
        if (!accessMode) {
            return MakeFuture<NCloud::NProto::TAcquireDevicesResponse>(
                TErrorResponse(
                    E_ARGUMENT,
                    TStringBuilder()
                        << "unknown access mode: "
                        << static_cast<int>(request.GetAccessMode())));
        }

        auto ev = std::make_unique<TEvDiskAgent::TEvAcquireDevicesRequest>();

        CopyHeaders(*ev->Record.MutableHeaders(), request.GetHeaders());
        ev->Record.MutableDeviceUUIDs()->Assign(
            request.GetDeviceUUIDs().begin(),
            request.GetDeviceUUIDs().end());
        ev->Record.SetAccessMode(*accessMode);
        ev->Record.SetMountSeqNumber(DefaultMountSeqNumber);
        ev->Record.SetDiskId(request.GetFastshardId());
        ev->Record.SetVolumeGeneration(request.GetGeneration());

        auto future = ActorSystem->Ask<TEvDiskAgent::TEvAcquireDevicesResponse>(
            DiskAgentActorId,
            THolder(ev.release()));

        return future.Apply(
            [](const auto& future)
            {
                NCloud::NProto::TAcquireDevicesResponse response;
                const auto& ev = future.GetValue();
                response.MutableError()->CopyFrom(ev->Record.GetError());

                return response;
            });
    }

    [[nodiscard]] auto ReleaseDevices(
        NCloud::NProto::TReleaseDevicesRequest request)
        -> TFuture<NCloud::NProto::TReleaseDevicesResponse> final
    {
        auto ev = std::make_unique<TEvDiskAgent::TEvReleaseDevicesRequest>();

        CopyHeaders(*ev->Record.MutableHeaders(), request.GetHeaders());
        ev->Record.MutableDeviceUUIDs()->Assign(
            request.GetDeviceUUIDs().begin(),
            request.GetDeviceUUIDs().end());
        ev->Record.SetDiskId(request.GetFastshardId());
        ev->Record.SetVolumeGeneration(request.GetGeneration());

        auto future = ActorSystem->Ask<TEvDiskAgent::TEvReleaseDevicesResponse>(
            DiskAgentActorId,
            THolder(ev.release()));

        return future.Apply(
            [](const auto& future)
            {
                NCloud::NProto::TReleaseDevicesResponse response;
                const auto& ev = future.GetValue();
                response.MutableError()->CopyFrom(ev->Record.GetError());

                return response;
            });
    }

    [[nodiscard]] NProto::TError AccessDevice(
        const TString& deviceUUID,
        const TString& clientId,
        NProto::EAccessMode accessMode) final
    {
        const auto volumeAccessMode = GetVolumeAccessMode(accessMode);
        if (!volumeAccessMode) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "unknown access mode: " << static_cast<int>(accessMode));
        }

        auto [_, error] =
            DeviceClient->AccessDevice(deviceUUID, clientId, *volumeAccessMode);
        return error;
    }

    [[nodiscard]] IDevicePtr CreateDevice(
        const TString& deviceUUID,
        TPageRangeRef region,
        ui32 blockSize) final
    {
        return CreateDeviceAdapter(
            Timer,
            DeviceClient,
            deviceUUID,
            region,
            blockSize);
    }
};

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

    TFuture<NCloud::NProto::TError> Start() override
    {
        TVector<TFuture<NCloud::NProto::TError>> futures;
        futures.reserve(Devices.size());

        for (const auto& [uuid, spec]: Devices) {
            auto future = spec.Device->Start().Apply(
                [uuid](const auto& future)
                {
                    auto error = ExtractResponse(future);
                    if (HasError(error)) {
                        ReportDiskAgentJournalledDeviceCreationError(
                            TStringBuilder()
                                << "unable to start: " << FormatError(error),
                            {{"device", uuid}});
                    }
                    return error;
                });

            futures.push_back(std::move(future));
        }

        return WaitAll(futures).Apply([](const auto&)
                                      { return NProto::TError(); });
    }

    TFuture<NCloud::NProto::TError> Stop() override
    {
        TVector<TFuture<NCloud::NProto::TError>> futures;
        futures.reserve(Devices.size());

        for (const auto& [uuid, spec]: Devices) {
            futures.push_back(spec.Device->Stop());
        }

        return WaitAll(futures).Apply([](const auto&)
                                      { return NProto::TError(); });
    }

    [[nodiscard]] auto AcquireDevices(
        NCloud::NProto::TAcquireDevicesRequest request)
        -> TFuture<NCloud::NProto::TAcquireDevicesResponse> final
    {
        return DeviceManager->AcquireDevices(std::move(request));
    }

    [[nodiscard]] auto ReleaseDevices(
        NCloud::NProto::TReleaseDevicesRequest request)
        -> TFuture<NCloud::NProto::TReleaseDevicesResponse> final
    {
        return DeviceManager->ReleaseDevices(std::move(request));
    }

    [[nodiscard]] auto FormatDevice(
        NCloud::NProto::TFormatDeviceRequest request)
        -> TFuture<NCloud::NProto::TFormatDeviceResponse> final
    {
        auto [spec, error] = GetDeviceSpec(
            request.GetDeviceUUID(),
            request.GetHeaders().GetClientId(),
            NProto::ACCESS_READ_WRITE);

        if (HasError(error)) {
            return MakeFuture<NCloud::NProto::TFormatDeviceResponse>(
                TErrorResponse(error));
        }

        const auto& config = spec->Config;

        // Only the journal metadata is wiped by default.
        const ui64 blocksCount =
            request.GetWholeDevice()
                ? config.BlocksCount
                : config.LogMetaSize / config.BlockSize;

        if (!blocksCount) {
            return MakeFuture(NCloud::NProto::TFormatDeviceResponse());
        }

        TPageRangeRef region{.FirstPageNo = 0, .PageCount = blocksCount};

        auto device = DeviceManager->CreateDevice(
            request.GetDeviceUUID(),
            region,
            config.BlockSize);

        return device->ZeroPages({region}).Apply(
            [](const auto& future)
            {
                NCloud::NProto::TFormatDeviceResponse response;
                *response.MutableError() = SafeExecute<NProto::TError>(
                    [&] { return future.GetValue(); });

                return response;
            });
    }

    [[nodiscard]] auto ReadPages(
        NCloud::NProto::TReadPagesRequest request)
        -> TFuture<NCloud::NProto::TReadPagesResponse> final
    {
        auto [device, error] = GetDevice(
            request.GetDeviceUUID(),
            request.GetHeaders().GetClientId(),
            NProto::ACCESS_READ_ONLY);

        if (HasError(error)) {
            return MakeFuture<NCloud::NProto::TReadPagesResponse>(
                TErrorResponse(error));
        }

        return device->ReadPages(std::move(request));
    }

    [[nodiscard]] auto WriteLogRecord(
        NCloud::NProto::TWriteLogRecordRequest request)
        -> TFuture<NCloud::NProto::TWriteLogRecordResponse> final
    {
        auto [device, error] = GetDevice(
            request.GetDeviceUUID(),
            request.GetHeaders().GetClientId(),
            NProto::ACCESS_READ_WRITE);

        if (HasError(error)) {
            return MakeFuture<NCloud::NProto::TWriteLogRecordResponse>(
                TErrorResponse(error));
        }

        return device->WriteLogRecord(std::move(request));
    }

    [[nodiscard]] auto ReadJournalTail(
        NCloud::NProto::TReadJournalTailRequest request)
        -> TFuture<NCloud::NProto::TReadJournalTailResponse> final
    {
        auto [device, error] = GetDevice(
            request.GetDeviceUUID(),
            request.GetHeaders().GetClientId(),
            NProto::ACCESS_READ_ONLY);

        if (HasError(error)) {
            return MakeFuture<NCloud::NProto::TReadJournalTailResponse>(
                TErrorResponse(error));
        }

        return device->ReadJournalTail(std::move(request));
    }

    [[nodiscard]] auto AdvanceLsnLowWatermark(
        NCloud::NProto::TAdvanceLsnLowWatermarkRequest request)
        -> TFuture<NCloud::NProto::TAdvanceLsnLowWatermarkResponse> final
    {
        auto [device, error] = GetDevice(
            request.GetDeviceUUID(),
            request.GetHeaders().GetClientId(),
            NProto::ACCESS_READ_WRITE);

        if (HasError(error)) {
            return MakeFuture<NCloud::NProto::TAdvanceLsnLowWatermarkResponse>(
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
            return MakeError(E_NOT_FOUND, TStringBuilder()
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

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IJournalledDevicePtr> CreateJournalledDevice(
    TLog& Log,
    ILoggingServicePtr logging,
    TExecutorPtr executor,
    IDeviceManagerPtr deviceManager,
    bool journalEnabled,
    const TJournalledDeviceConfig& config)
{
    const ui32 blockSize = config.BlockSize;
    const ui64 blockCount = config.BlocksCount;
    const auto& uuid = config.DeviceUUID;

    if (!blockSize) {
        return MakeError(E_ARGUMENT, "the device block size is zero");
    }

    if (!journalEnabled) {
        // No journal: the whole device holds the data, writes go straight to it.
        return NJournalled::CreateJournalledDeviceV1(
            deviceManager->CreateDevice(
                uuid,
                {.FirstPageNo = 0, .PageCount = blockCount},
                blockSize));
    }

    // The device is split into three parts: the journal metadata, the journal
    // data and the data itself.
    const ui64 logMetaSize = config.LogMetaSize;
    const ui64 logDataSize = config.LogDataSize;

    if (logMetaSize % blockSize || logDataSize % blockSize) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "the journal parts " << logMetaSize << " and "
                << logDataSize << " bytes are not multiples of the block size "
                << blockSize);
    }

    const ui64 logMetaBlockCount = logMetaSize / blockSize;
    const ui64 logDataBlockCount = logDataSize / blockSize;

    // The key buffer store needs a couple of pages for its superblock and at
    // least one for the entries.
    constexpr ui64 MinLogMetaBlockCount = 3;

    if (logMetaBlockCount < MinLogMetaBlockCount || logDataBlockCount < 1 ||
        logMetaBlockCount + logDataBlockCount >= blockCount)
    {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "the journal parts " << logMetaBlockCount << " and "
                << logDataBlockCount << " blocks leave no room on the device "
                << "of " << blockCount << " blocks");
    }

    const ui64 dataBlockCount =
        blockCount - logMetaBlockCount - logDataBlockCount;

    if (logMetaBlockCount >= logDataBlockCount ||
        logDataBlockCount >= dataBlockCount)
    {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "the journal metadata (" << logMetaBlockCount
                << " blocks) must be smaller than the journal data ("
                << logDataBlockCount << " blocks), which must be smaller "
                << "than the data (" << dataBlockCount << " blocks)");
    }

    auto createAdapter = [&](ui64 firstBlockIndex, ui64 regionBlockCount)
    {
        return deviceManager->CreateDevice(
            uuid,
            {.FirstPageNo = firstBlockIndex, .PageCount = regionBlockCount},
            blockSize);
    };

    auto logMetaStore = NJournalled::CreateDeviceKeyBufferStore(
        logging,
        createAdapter(0, logMetaBlockCount),
        logMetaBlockCount,
        blockSize);

    auto logDataStore = NJournalled::CreateDevicePageStore(
        createAdapter(logMetaBlockCount, logDataBlockCount),
        logDataBlockCount,
        blockSize);

    auto dataStore = createAdapter(
        logMetaBlockCount + logDataBlockCount,
        dataBlockCount);

    auto journal = NJournalled::CreateJournal(
        logging,
        executor,
        std::move(logMetaStore),
        std::move(logDataStore),
        dataBlockCount);

    auto journalledDevice = NJournalled::CreateJournalledDeviceV2(
        std::move(logging),
        std::move(executor),
        std::move(journal),
        std::move(dataStore),
        uuid);

    STORAGE_DEBUG(
        "Journalled device "
        << uuid.Quote() << " created: journal "
        << "metadata " << FormatByteSize(logMetaBlockCount * blockSize)
        << ", journal data "
        << FormatByteSize(logDataBlockCount * blockSize) << ", data "
        << FormatByteSize(dataBlockCount * blockSize) << ", block size "
        << blockSize);

    return journalledDevice;
}

////////////////////////////////////////////////////////////////////////////////

class TServerBuilder
{
private:
    ILoggingServicePtr Logging;
    TExecutorPtr Executor;
    IDeviceManagerPtr DeviceManager;
    const TNetworkAddress ListenAddress;
    const bool JournalEnabled;
    TVector<TJournalledDeviceConfig> DeviceConfigs;

public:
    TServerBuilder(
            ILoggingServicePtr logging,
            TExecutorPtr executor,
            IDeviceManagerPtr deviceManager,
            const TNetworkAddress& listenAddress,
            bool journalEnabled,
            TVector<TJournalledDeviceConfig> deviceConfigs)
        : Logging(std::move(logging))
        , Executor(std::move(executor))
        , DeviceManager(std::move(deviceManager))
        , ListenAddress(listenAddress)
        , JournalEnabled(journalEnabled)
        , DeviceConfigs(std::move(deviceConfigs))
    {}

    // Devices that fail to be created are reported and skipped; throws
    // TServiceError if none of them could be created.
    IStartablePtr Build()
    {
        auto Log = Logging->CreateLog("BLOCKSTORE_JOURNALLED_DEVICE");

        THashMap<TString, TJournalledDeviceSpec> devices;

        for (const auto& config: DeviceConfigs) {
            const auto& uuid = config.DeviceUUID;

            if (!JournalEnabled && (config.LogMetaSize || config.LogDataSize)) {
                ReportDiskAgentJournalledDeviceCreationError(
                    TStringBuilder()
                    << "the journal is disabled, but the device " << uuid.Quote()
                    << " has the journal parts " << config.LogMetaSize << " and "
                    << config.LogDataSize << " bytes");
                continue;
            }

            auto [device, error] = CreateJournalledDevice(
                Log,
                Logging,
                Executor,
                DeviceManager,
                JournalEnabled,
                config);

            if (HasError(error)) {
                ReportDiskAgentJournalledDeviceCreationError(
                    TStringBuilder() << "unable to create device " << uuid.Quote()
                                     << ": " << FormatError(error));
                continue;
            }

            devices.emplace(uuid, TJournalledDeviceSpec{std::move(device), config});
        }

        if (devices.empty()) {
            STORAGE_THROW_SERVICE_ERROR(MakeError(
                E_NOT_FOUND,
                TStringBuilder() << "none of the " << DeviceConfigs.size()
                                 << " journalled devices could be created"));
        }

        return NJournalled::CreateServer(
            ListenAddress,
            std::move(Logging),
            std::move(Executor),
            std::make_shared<TJournalledDeviceHandler>(
                std::move(DeviceManager),
                std::move(devices)));
    }
};

////////////////////////////////////////////////////////////////////////////////

TNetworkAddress CreateNetworkAddress(TStringBuf s)
{
    TStringBuf hostRef;
    TStringBuf portRef;
    s.RSplit(':', hostRef, portRef);

    return {
        hostRef ? TString(hostRef).c_str() : nullptr,
        FromString<ui16>(portRef)};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NProto::TError TDiskAgentActor::StartJournalledDeviceTcpServer(
    const NActors::TActorContext& ctx,
    const THashMap<TString, NProto::TJournalConfig>& journalledDevices)
{
    if (journalledDevices.empty()) {
        return {};
    }

    auto address = AgentConfig->GetJournalledDeviceTcpServerListenAddress();
    if (address.empty()) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "the listen address is not configured, but there are "
                << journalledDevices.size() << " journalled devices");
    }

    if (!State) {
        return MakeError(
            E_INVALID_STATE,
            "the disk agent state is not initialized");
    }

    auto journalConfigs = journalledDevices;

    TVector<TJournalledDeviceConfig> configs;
    for (const auto& config: State->GetDevices()) {
        auto it = journalConfigs.find(config.GetDeviceUUID());
        if (it != journalConfigs.end()) {
            const auto& journalConfig = it->second;
            configs.emplace_back(
                TJournalledDeviceConfig{
                    .DeviceUUID = config.GetDeviceUUID(),
                    .BlocksCount = config.GetBlocksCount(),
                    .BlockSize = config.GetBlockSize(),
                    .LogMetaSize = journalConfig.GetLogMetaSize(),
                    .LogDataSize = journalConfig.GetLogDataSize()});
            journalConfigs.erase(it);
        }
    }

    if (!journalConfigs.empty()) {
        TStringBuilder missing;
        for (const auto& [id, _]: journalConfigs) {
            missing << (missing.empty() ? "" : ", ") << id.Quote();
        }

        ReportDiskAgentJournalledDeviceCreationError(
            "Journalled devices not found among the disk agent devices",
            {{"devices", missing}});
    }

    if (configs.empty()) {
        return MakeError(
            E_NOT_FOUND,
            "none of the journalled devices is among the disk agent devices");
    }

    Executor = TExecutor::Create("JD");

    auto deviceManager = std::make_shared<TDeviceManager>(
        CreateWallClockTimer(),
        State->GetDeviceClient(),
        TActivationContext::ActorSystem(),
        ctx.SelfID);

    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::DISK_AGENT,
        "Starting journalled device TCP server on " << address.Quote()
                                                    << "...");

    try {
        const auto listenAddress = CreateNetworkAddress(address);

        JournalledDeviceTcpServer = TServerBuilder(
            Logging,
            Executor,
            deviceManager,
            listenAddress,
            AgentConfig->GetJournalEnabled(),
            std::move(configs)).Build();

        Executor->Start();

        JournalledDeviceTcpServer->Start();

        LOG_INFO_S(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Journalled device TCP server started on " << address.Quote());

        return {};

    } catch (const TServiceError& e) {
        return MakeError(e.GetCode(), TString(e.GetMessage()));
    } catch (...) {
        return MakeError(E_FAIL, CurrentExceptionMessage());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
