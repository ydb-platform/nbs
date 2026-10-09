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
#include <cloud/fastshard/journal/server/server.h>

#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/coroutine/executor.h>

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
    NProto::TDeviceConfig Device;
    NProto::TJournalConfig Journal;
};

////////////////////////////////////////////////////////////////////////////////

struct TJournalledDeviceSpec
{
    NProto::TJournalConfig Config;
    ui32 BlockSize = 0;
    ui64 BlocksCount = 0;

    NJournalled::IJournalledDevicePtr Device;
};

////////////////////////////////////////////////////////////////////////////////

class TJournalledDeviceHandler final: public IServerBackend
{
private:
    TActorSystem* ActorSystem = nullptr;
    const TActorId DiskAgentActorId;
    const TDeviceClientPtr DeviceClient;
    const ITimerPtr Timer;
    const THashMap<TString, TJournalledDeviceSpec> Devices;

public:
    TJournalledDeviceHandler(
        TActorSystem* actorSystem,
        const TActorId& diskAgentActorId,
        TDeviceClientPtr deviceClient,
        ITimerPtr timer,
        THashMap<TString, TJournalledDeviceSpec> devices)
        : ActorSystem(actorSystem)
        , DiskAgentActorId(diskAgentActorId)
        , DeviceClient(std::move(deviceClient))
        , Timer(std::move(timer))
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
        ev->Record.SetMountSeqNumber(request.GetSeqNumber());
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
        auto promise = NewPromise<NCloud::NProto::TReleaseDevicesResponse>();

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

    [[nodiscard]] auto FormatDevice(
        NCloud::NProto::TFormatDeviceRequest request)
        -> TFuture<NCloud::NProto::TFormatDeviceResponse> final
    {
        auto [spec, error] = GetDeviceSpec(
            request.GetDeviceUUID(),
            request.GetHeaders().GetClientId(),
            NProto::VOLUME_ACCESS_READ_WRITE);

        if (HasError(error)) {
            return MakeFuture<NCloud::NProto::TFormatDeviceResponse>(
                TErrorResponse(error));
        }

        // Only the journal metadata is wiped by default.
        const ui64 blocksCount =
            request.GetWholeDevice()
                ? spec->BlocksCount
                : spec->Config.GetLogMetaSize() / spec->BlockSize;

        if (!blocksCount) {
            return MakeFuture(NCloud::NProto::TFormatDeviceResponse());
        }

        auto [storageAdapter, accessError] =
            DeviceClient->AccessDevice(request.GetDeviceUUID());

        if (HasError(accessError)) {
            return MakeFuture<NCloud::NProto::TFormatDeviceResponse>(
                TErrorResponse(accessError));
        }

        auto zeroRequest = std::make_shared<NProto::TZeroBlocksRequest>();
        zeroRequest->SetStartIndex(0);
        zeroRequest->SetBlocksCount(blocksCount);

        auto future = storageAdapter->ZeroBlocks(
            Timer->Now(),
            MakeIntrusive<TCallContext>(),
            std::move(zeroRequest),
            spec->BlockSize);

        return future.Apply(
            [](const auto& future)
            {
                const auto zeroResponse =
                    SafeExecute<NProto::TZeroBlocksResponse>(
                        [&] { return future.GetValue(); });

                NCloud::NProto::TFormatDeviceResponse response;
                *response.MutableError() = zeroResponse.GetError();

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
            NProto::VOLUME_ACCESS_READ_ONLY);

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
            NProto::VOLUME_ACCESS_READ_WRITE);

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
            NProto::VOLUME_ACCESS_READ_ONLY);

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
            NProto::VOLUME_ACCESS_READ_WRITE);

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
        NProto::EVolumeAccessMode accessMode) const
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
        NProto::EVolumeAccessMode accessMode) const
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

        auto [storageAdapter, error] =
            DeviceClient->AccessDevice(deviceUUID, clientId, accessMode);

        if (HasError(error)) {
            return error;
        }

        return spec;
    }
};

////////////////////////////////////////////////////////////////////////////////

TResultOrError<NJournalled::IJournalledDevicePtr> CreateJournalledDevice(
    const TActorContext& ctx,
    ITimerPtr timer,
    ILoggingServicePtr logging,
    TExecutorPtr executor,
    TDeviceClientPtr deviceClient,
    const TDiskAgentConfig& agentConfig,
    const TJournalledDeviceConfig& config)
{
    const ui32 blockSize = config.Device.GetBlockSize();
    const ui64 blockCount = config.Device.GetBlocksCount();
    const auto& uuid = config.Device.GetDeviceUUID();

    if (!blockSize) {
        return MakeError(E_ARGUMENT, "the device block size is zero");
    }

    if (!agentConfig.GetJournalEnabled()) {
        // No journal: the whole device holds the data, writes go straight to it.
        return NJournalled::CreateJournalledDeviceV1(CreateDeviceAdapter(
            std::move(timer),
            uuid,
            blockSize,
            std::move(deviceClient),
            {.FirstBlockIndex = 0, .BlockCount = blockCount}));
    }

    // The device is split into three parts: the journal metadata, the journal
    // data and the data itself.
    const ui64 logMetaSize = config.Journal.GetLogMetaSize();
    const ui64 logDataSize = config.Journal.GetLogDataSize();

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

    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::DISK_AGENT,
        "Journalled device " << uuid.Quote() << ": journal "
            << "metadata " << FormatByteSize(logMetaBlockCount * blockSize)
            << ", journal data "
            << FormatByteSize(logDataBlockCount * blockSize) << ", data "
            << FormatByteSize(dataBlockCount * blockSize) << ", block size "
            << blockSize);

    auto createAdapter = [&](ui64 firstBlockIndex, ui64 regionBlockCount)
    {
        return CreateDeviceAdapter(
            timer,
            uuid,
            blockSize,
            deviceClient,
            {.FirstBlockIndex = firstBlockIndex,
             .BlockCount = regionBlockCount});
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

    return NJournalled::CreateJournalledDeviceV2(
        std::move(logging),
        std::move(executor),
        std::move(journal),
        std::move(dataStore),
        uuid);
}

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
            configs.emplace_back(
                TJournalledDeviceConfig{
                    .Device = config,
                    .Journal = it->second});
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

    THashMap<TString, TJournalledDeviceSpec> devices;
    auto timer = CreateWallClockTimer();

    for (const auto& config: configs) {
        const auto& uuid = config.Device.GetDeviceUUID();

        auto [device, error] = CreateJournalledDevice(
            ctx,
            timer,
            Logging,
            Executor,
            State->GetDeviceClient(),
            *AgentConfig,
            config);

        if (HasError(error)) {
            ReportDiskAgentJournalledDeviceCreationError(
                FormatError(error),
                {{"device", uuid}});
            continue;
        }

        devices.emplace(
            uuid,
            TJournalledDeviceSpec{
                .Config = AgentConfig->GetJournalEnabled()
                              ? config.Journal
                              : NProto::TJournalConfig{},
                .BlockSize = config.Device.GetBlockSize(),
                .BlocksCount = config.Device.GetBlocksCount(),
                .Device = std::move(device)});
    }

    if (devices.empty()) {
        return MakeError(
            E_NOT_FOUND,
            TStringBuilder() << "none of the " << configs.size()
                             << " journalled devices could be created");
    }

    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::DISK_AGENT,
        "Starting journalled device TCP server on " << address.Quote()
                                                    << "...");

    try {
        const TNetworkAddress listenAddress = CreateNetworkAddress(address);

        JournalledDeviceTcpServer = NJournalled::CreateServer(
            listenAddress,
            Logging,
            Executor,
            std::make_shared<TJournalledDeviceHandler>(
                TActivationContext::ActorSystem(),
                ctx.SelfID,
                State->GetDeviceClient(),
                timer,
                std::move(devices)));

        Executor->Start();

        JournalledDeviceTcpServer->Start();

        LOG_INFO_S(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Journalled device TCP server started on " << address.Quote());

        return {};

    } catch (...) {
        return MakeError(E_FAIL, CurrentExceptionMessage());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
