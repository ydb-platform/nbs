#include "disk_agent_actor.h"

#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/storage/disk_agent/actors/io_request_parser.h>
#include <cloud/blockstore/libs/storage/disk_agent/actors/multi_agent_write_handler.h>
#include <cloud/blockstore/libs/storage/disk_agent/journalled_device_adapter.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/future_helper.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/journalled_device/device_page_store.h>
#include <cloud/storage/core/libs/journalled_device/journal.h>
#include <cloud/storage/core/libs/journalled_device/journalled_device.h>
#include <cloud/storage/core/libs/journalled_device/journalled_device_v2.h>
#include <cloud/storage/core/libs/journalled_device/key_buffer_store.h>

#include <contrib/ydb/core/base/appdata.h>

#include <util/generic/size_literals.h>
#include <util/generic/vector.h>
#include <util/system/align.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

// The device is split into three parts: the journal metadata, the journal
// data and the data itself. The journal parts take these fractions of the
// device.
constexpr ui64 LogMetaSizeDivisor = 128;
constexpr ui64 LogDataSizeDivisor = 8;

TResultOrError<NJournalled::IJournalledDevicePtr> CreateJournalledDevice(
    const TActorContext& ctx,
    ITimerPtr timer,
    ILoggingServicePtr logging,
    TExecutorPtr executor,
    TDeviceClientPtr deviceClient,
    const NProto::TDeviceConfig& device)
{
    const ui64 blockSize = device.GetBlockSize();
    const ui64 deviceSize = blockSize * device.GetBlocksCount();
    if (!blockSize) {
        return MakeError(E_ARGUMENT, "the device block size is zero");
    }

    const ui64 logMetaSize =
        AlignDown(deviceSize / LogMetaSizeDivisor, blockSize);
    const ui64 logDataSize =
        AlignDown(deviceSize / LogDataSizeDivisor, blockSize);

    if (logMetaSize < 3 * blockSize || logDataSize < blockSize ||
        logMetaSize + logDataSize >= deviceSize)
    {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "the journal parts " << logMetaSize << " and "
                << logDataSize << " bytes leave no room on the device of "
                << deviceSize << " bytes");
    }

    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::DISK_AGENT,
        "Journalled device " << device.GetDeviceUUID().Quote() << ": journal "
            << "metadata " << FormatByteSize(logMetaSize) << ", journal data "
            << FormatByteSize(logDataSize) << ", data "
            << FormatByteSize(deviceSize - logMetaSize - logDataSize)
            << ", block size " << blockSize);

    auto createAdapter = [&](ui64 offset, ui64 size)
    {
        return CreateDeviceAdapter(
            timer,
            device.GetDeviceUUID(),
            deviceClient,
            {.Offset = offset, .Size = size});
    };

    auto logMetaStore = NJournalled::CreateDeviceKeyBufferStore(
        createAdapter(0, logMetaSize),
        logMetaSize / blockSize,
        blockSize);

    auto logDataStore = NJournalled::CreateDevicePageStore(
        createAdapter(logMetaSize, logDataSize),
        logDataSize / blockSize,
        blockSize);

    auto dataStore = createAdapter(
        logMetaSize + logDataSize,
        deviceSize - logMetaSize - logDataSize);

    auto journal = NJournalled::CreateJournal(
        logging,
        executor,
        std::move(logMetaStore),
        std::move(logDataStore));

    return NJournalled::CreateJournalledDeviceV2(
        std::move(logging),
        std::move(executor),
        std::move(journal),
        std::move(dataStore),
        device.GetDeviceUUID(),
        TString{});
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentActor::InitAgent(const TActorContext& ctx)
{
    Y_DEBUG_ABORT_UNLESS(
        OldRequestCounters.Delayed && OldRequestCounters.Rejected);

    TRdmaTargetConfigPtr rdmaTargetConfig = nullptr;
    if (RdmaConfig && RdmaConfig->GetDiskAgentTargetEnabled()) {
        rdmaTargetConfig = std::make_shared<TRdmaTargetConfig>(
            Config->GetRejectLateRequestsAtDiskAgentEnabled(),
            RdmaConfig->GetDiskAgentTarget());
    }

    State = std::make_unique<TDiskAgentState>(
        Config,
        AgentConfig,
        Spdk,
        Allocator,
        StorageProvider,
        ProfileLog,
        BlockDigestGenerator,
        Logging,
        RdmaServer,
        NvmeManager,
        std::move(rdmaTargetConfig),
        OldRequestCounters,
        CreateMultiAgentWriteHandler(
            TActivationContext::ActorSystem(),
            ctx.SelfID),
        BackgroundThreadPool);

    auto* actorSystem = TActivationContext::ActorSystem();
    auto replyTo = ctx.SelfID;

    State->Initialize().Subscribe(
        [actorSystem, replyTo]   //
        (const NThreading::TFuture<TDiskAgentState::TInitializeResult>& future)
        {
            using TCompletionEvent = TEvDiskAgentPrivate::TEvInitAgentCompleted;

            NProto::TError error;

            try {
                TDiskAgentState::TInitializeResult r = UnsafeExtractValue(future);

                auto response = std::make_unique<TCompletionEvent>(
                    std::move(r.Configs),
                    std::move(r.Errors),
                    std::move(r.ConfigMismatchErrors),
                    std::move(r.DevicesWithSuspendedIO));

                actorSystem->Send(
                    new IEventHandle(replyTo, replyTo, response.release()));
            } catch (const TServiceError& e) {
                error = MakeError(e.GetCode(), TString(e.GetMessage()));
            } catch (...) {
                error = MakeError(E_FAIL, CurrentExceptionMessage());
            }

            if (error.GetCode()) {
                auto response = std::make_unique<TCompletionEvent>(error);

                actorSystem->Send(
                    new IEventHandle(replyTo, replyTo, response.release()));
            }
        });
}

void TDiskAgentActor::HandleInitAgentCompleted(
    const TEvDiskAgentPrivate::TEvInitAgentCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    for (const auto& error: msg->Errors) {
        LOG_WARN_S(ctx, TBlockStoreComponents::DISK_AGENT, error);
    }

    // Crit events that reported on startup have issue with them being invisible
    // on second restart. Here, we schedule the event to allow monitoring
    // initially to read counters without event and then with the event.
    for (const auto& configMismatchError: msg->ConfigMismatchErrors) {
        const TDuration startupCritEventDelay = UpdateCountersInterval * 2;
        ctx.Schedule(
            startupCritEventDelay,
            new TEvDiskAgentPrivate::TEvReportDelayedDiskAgentConfigMismatch(
                configMismatchError));
    }

    if (const auto& error = msg->GetError(); HasError(error)) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::DISK_AGENT,
            "DiskAgent initialization failed. Error: " << FormatError(error).data());
    } else {
        TStringStream out;
        for (const auto& config: msg->Configs) {
            out << config.GetDeviceName()
                << "(" << FormatByteSize(config.GetBlocksCount() * config.GetBlockSize())
                << "); ";
        }

        LOG_INFO_S(ctx, TBlockStoreComponents::DISK_AGENT,
            "Initialization completed. Devices found: " << out.Str());
    }

    // resend pending requests
    SendPendingRequests(ctx, PendingRequests);

    if (msg->Configs.empty()) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "No devices: become idle");

        Become(&TThis::StateIdle);

        return;
    }

    if (ui32 count = AgentConfig->GetIOParserActorCount()) {
        LOG_INFO_S(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Create " << count << " IORequestParserActor actors");

        NDiskAgent::TStorageBufferAllocator allocator;
        if (AgentConfig->GetIOParserActorAllocateStorageEnabled() &&
            AgentConfig->GetBackend() == NProto::DISK_AGENT_BACKEND_AIO)
        {
            allocator = [](ui64 byteCount)
            {
                return std::shared_ptr<char>(
                    static_cast<char*>(
                        std::aligned_alloc(DefaultBlockSize, byteCount)),
                    std::free);
            };
        }

        IOParserActors.reserve(count);
        for (ui32 i = 0; i != count; ++i) {
            auto actor =
                NDiskAgent::CreateIORequestParserActor(ctx.SelfID, allocator);

            IOParserActors.push_back(ctx.Register(
                actor.release(),
                TMailboxType::TinyReadAsFilled,
                NKikimr::AppData()->UserPoolId));
        }
    }

    if (State &&
        !AgentConfig->GetJournalledDeviceTcpServerListenAddress().empty())
    {
        Executor = TExecutor::Create("JD");
        Executor->Start();

        auto timer = CreateWallClockTimer();
        auto deviceClient = State->GetDeviceClient();

        THashMap<TString, NJournalled::IJournalledDevicePtr> devices;

        for (const auto& deviceConfig: State->GetDevices()) {
            const auto& uuid = deviceConfig.GetDeviceUUID();

            auto [device, error] = CreateJournalledDevice(
                ctx,
                timer,
                Logging,
                Executor,
                deviceClient,
                deviceConfig);

            if (!HasError(error)) {
                try {
                    device->Start();
                } catch (...) {
                    error = MakeError(E_FAIL, CurrentExceptionMessage());
                }
            }

            if (HasError(error)) {
                LOG_ERROR_S(
                    ctx,
                    TBlockStoreComponents::DISK_AGENT,
                    "Can't create journalled device " << uuid.Quote() << ": "
                        << FormatError(error));
                continue;
            }

            JournalledDevices.push_back(device);
            devices.emplace(uuid, std::move(device));
        }

        StartJournalledDeviceTcpServer(ctx, std::move(devices));
    }

    LOG_INFO(ctx, TBlockStoreComponents::DISK_AGENT, "Ready to work");

    Become(&TThis::StateWork);

    NCloud::Send(
        ctx,
        MakeDiskRegistryProxyServiceId(),
        std::make_unique<TEvDiskRegistryProxy::TEvSubscribeRequest>(
            ctx.SelfID));

    ScheduleUpdateStats(ctx);

    RunSessionCacheActor(ctx);

    RestartDeviceHealthChecking(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
