#include "disk_agent_actor.h"

#include <cloud/blockstore/libs/storage/disk_agent/journalled_device_adapter.h>

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
#include <util/generic/hash_set.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NJournalled;
using namespace NKikimr;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr NProto::EVolumeAccessMode DefaultAccessMode =
    NProto::VOLUME_ACCESS_READ_WRITE;
constexpr ui64 DefaultMountSeqNumber = 0;
constexpr ui64 DefaultVolumeGeneration = 0;

////////////////////////////////////////////////////////////////////////////////

void CopyHeaders(
    NProto::THeaders& dst,
    const NCloud::NProto::TDeviceRequestHeaders& src)
{
    dst.SetClientId(src.GetClientId());
    dst.SetRequestTimeout(src.GetRequestTimeout());
}

////////////////////////////////////////////////////////////////////////////////

class TJournalledDeviceHandler final: public IServerBackend
{
private:
    TActorSystem* ActorSystem = nullptr;
    const TActorId DiskAgentActorId;
    const TDeviceClientPtr DeviceClient;
    const THashMap<TString, NJournalled::IJournalledDevicePtr> Devices;

public:
    TJournalledDeviceHandler(
        TActorSystem* actorSystem,
        const TActorId& diskAgentActorId,
        TDeviceClientPtr deviceClient,
        THashMap<TString, NJournalled::IJournalledDevicePtr> devices)
        : ActorSystem(actorSystem)
        , DiskAgentActorId(diskAgentActorId)
        , DeviceClient(std::move(deviceClient))
        , Devices(std::move(devices))
    {}

    // IServerBackend

    void Start() override
    {
        for (const auto& [uuid, device]: Devices) {
            device->Start();
        }
    }

    void Stop() override
    {
        for (const auto& [uuid, device]: Devices) {
            device->Stop();
        }
    }

    [[nodiscard]] auto AcquireDevices(
        NCloud::NProto::TAcquireDevicesRequest request)
        -> TFuture<NCloud::NProto::TAcquireDevicesResponse> final
    {
        auto ev = std::make_unique<TEvDiskAgent::TEvAcquireDevicesRequest>();

        CopyHeaders(*ev->Record.MutableHeaders(), request.GetHeaders());
        ev->Record.MutableDeviceUUIDs()->Assign(
            request.GetDeviceUUIDs().begin(),
            request.GetDeviceUUIDs().end());
        ev->Record.SetAccessMode(DefaultAccessMode);
        ev->Record.SetMountSeqNumber(DefaultMountSeqNumber);
        ev->Record.SetDiskId(request.GetHeaders().GetClientId());
        ev->Record.SetVolumeGeneration(DefaultVolumeGeneration);

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
        if (deviceUUID.empty()) {
            return MakeError(E_ARGUMENT, "empty device UUID");
        }

        if (clientId.empty()) {
            return MakeError(E_ARGUMENT, "empty client id");
        }

        auto* device = Devices.FindPtr(deviceUUID);
        if (!device) {
            return MakeError(E_NOT_FOUND, TStringBuilder()
                << "Device " << deviceUUID.Quote() << " not found");
        }

        auto [storageAdapter, error] =
            DeviceClient->AccessDevice(deviceUUID, clientId, accessMode);

        if (HasError(error)) {
            return error;
        }

        return *device;
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
    const NProto::TDeviceConfig& device)
{
    if (!agentConfig.GetJournalEnabled()) {
        // No journal: the whole device holds the data, writes go straight to it.
        return NJournalled::CreateJournalledDeviceV1(
            CreateDeviceAdapter(
                std::move(timer),
                device.GetDeviceUUID(),
                device.GetBlockSize(),
                std::move(deviceClient)));
    }

    const ui32 blockSize = device.GetBlockSize();
    const ui64 blockCount = device.GetBlocksCount();
    if (!blockSize) {
        return MakeError(E_ARGUMENT, "the device block size is zero");
    }

    // The device is split into three parts: the journal metadata, the journal
    // data and the data itself.
    const ui64 logMetaBlockCount =
        blockCount * agentConfig.GetLogMetaSizePercents() / 100;
    const ui64 logDataBlockCount =
        blockCount * agentConfig.GetLogDataSizePercents() / 100;

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
        "Journalled device " << device.GetDeviceUUID().Quote() << ": journal "
            << "metadata " << FormatByteSize(logMetaBlockCount * blockSize)
            << ", journal data "
            << FormatByteSize(logDataBlockCount * blockSize) << ", data "
            << FormatByteSize(dataBlockCount * blockSize) << ", block size "
            << blockSize);

    auto createAdapter = [&](ui64 firstBlockIndex, ui64 regionBlockCount)
    {
        return CreateDeviceAdapter(
            timer,
            device.GetDeviceUUID(),
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
        device.GetDeviceUUID());
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

void TDiskAgentActor::StartJournalledDeviceTcpServer(
    const NActors::TActorContext& ctx,
    const TVector<TString>& journalledDeviceIds)
{
    if (!State || journalledDeviceIds.empty()) {
        return;
    }

    auto address = AgentConfig->GetJournalledDeviceTcpServerListenAddress();
    if (address.empty()) {
        return;
    }

    const THashSet<TString> journalledIds(
        journalledDeviceIds.begin(),
        journalledDeviceIds.end());

    TVector<NProto::TDeviceConfig> configs;
    for (const auto& config: State->GetDevices()) {
        if (journalledIds.contains(config.GetDeviceUUID())) {
            configs.push_back(config);
        }
    }

    if (configs.empty()) {
        return;
    }

    Executor = TExecutor::Create("JD");

    THashMap<TString, NJournalled::IJournalledDevicePtr> devices;
    auto timer = CreateWallClockTimer();

    for (const auto& config: configs) {
        const auto& uuid = config.GetDeviceUUID();

        auto [device, error] = CreateJournalledDevice(
            ctx,
            timer,
            Logging,
            Executor,
            State->GetDeviceClient(),
            *AgentConfig,
            config);

        if (HasError(error)) {
            LOG_ERROR_S(
                ctx,
                TBlockStoreComponents::DISK_AGENT,
                "Can't create journalled device " << uuid.Quote() << ": "
                    << FormatError(error));
            continue;
        }

        devices.emplace(uuid, std::move(device));
    }

    if (devices.empty()) {
        return;
    }

    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::DISK_AGENT,
        "Starting journaled device TCP server on " << address.Quote() << "...");

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
                std::move(devices)));

        Executor->Start();

        JournalledDeviceTcpServer->Start();

        LOG_INFO_S(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Journalled device TCP server started on " << address.Quote());

    } catch (...) {
        LOG_ERROR_S(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Could not start journalled device TCP server"
                << ": " << CurrentExceptionMessage());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
