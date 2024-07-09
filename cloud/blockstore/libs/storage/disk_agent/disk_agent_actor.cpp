#include "disk_agent_actor.h"

#include "actors/session_cache_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/nvme/nvme.h>
#include <cloud/blockstore/libs/service/storage_provider.h>
#include <cloud/blockstore/libs/storage/disk_agent/actors/config_cache_actor.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <contrib/ydb/core/base/appdata.h>
#include <contrib/ydb/core/mon/mon.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

TDiskAgentActor::TDiskAgentActor(
        TStorageConfigPtr config,
        TDiskAgentConfigPtr agentConfig,
        NRdma::TRdmaConfigPtr rdmaConfig,
        NSpdk::ISpdkEnvPtr spdk,
        ICachingAllocatorPtr allocator,
        IStorageProviderPtr storageProvider,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        ILoggingServicePtr logging,
        NRdma::IServerPtr rdmaServer,
        NNvme::INvmeManagerPtr nvmeManager)
    : Config(std::move(config))
    , AgentConfig(std::move(agentConfig))
    , RdmaConfig(std::move(rdmaConfig))
    , Spdk(std::move(spdk))
    , Allocator(std::move(allocator))
    , StorageProvider(std::move(storageProvider))
    , ProfileLog(std::move(profileLog))
    , BlockDigestGenerator(std::move(blockDigestGenerator))
    , Logging(std::move(logging))
    , RdmaServer(std::move(rdmaServer))
    , NvmeManager(std::move(nvmeManager))
{}

TDiskAgentActor::~TDiskAgentActor()
{}

void TDiskAgentActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateInit);

    RegisterPages(ctx);
    RegisterCounters(ctx);

    InitAgent(ctx);
}

void TDiskAgentActor::RegisterPages(const TActorContext& ctx)
{
    auto mon = AppData(ctx)->Mon;
    if (mon) {
        auto* rootPage = mon->RegisterIndexPage("blockstore", "BlockStore");

        mon->RegisterActorPage(rootPage, "disk_agent", "DiskAgent",
            false, ctx.ExecutorThread.ActorSystem, SelfId());
    }
}

void TDiskAgentActor::RegisterCounters(const TActorContext& ctx)
{
    Counters = CreateDiskAgentCounters();

    auto counters = AppData(ctx)->Counters;
    if (counters) {
        auto rootGroup = counters->GetSubgroup("counters", "blockstore");
        auto totalCounters = rootGroup->GetSubgroup("component", "disk_agent");
        auto outOfOrderCounters = totalCounters->GetSubgroup("utils", "old_requests");

        OldRequestCounters.Delayed = outOfOrderCounters->GetCounter("Delayed");
        OldRequestCounters.Rejected = outOfOrderCounters->GetCounter("Rejected");
        OldRequestCounters.Already = outOfOrderCounters->GetCounter("Already");

        UpdateCounters(ctx);
        ScheduleCountersUpdate(ctx);
    }
}

void TDiskAgentActor::ScheduleCountersUpdate(const TActorContext& ctx)
{
    ctx.Schedule(UpdateCountersInterval, new TEvents::TEvWakeup());
}

void TDiskAgentActor::UpdateCounters(const TActorContext& ctx)
{
    auto counters = AppData(ctx)->Counters;
    if (counters) {
        // TODO
    }
}

void TDiskAgentActor::UpdateActorStats()
{
    if (Counters) {
        auto& actorQueue = Counters->Percentile()
            [TDiskAgentCounters::PERCENTILE_COUNTER_Actor_ActorQueue];
        auto& mailboxQueue = Counters->Percentile()
            [TDiskAgentCounters::PERCENTILE_COUNTER_Actor_MailboxQueue];
        auto ctx(ActorContext());
        auto actorQueues = ctx.CountMailboxEvents(1001);
        IncrementFor(actorQueue, actorQueues.first);
        IncrementFor(mailboxQueue, actorQueues.second);
    }
}

void TDiskAgentActor::UpdateSessionCache(const TActorContext& ctx)
{
    // Temporary agent shouldn't change sessions cache to avoid race conditions
    // with the primary agent.
    if (!SessionCacheActor || AgentConfig->GetTemporaryAgent()) {
        return;
    }

    NCloud::Send<TEvDiskAgentPrivate::TEvUpdateSessionCacheRequest>(
        ctx,
        SessionCacheActor,
        0,  // cookie
        State->GetSessions());
}

void TDiskAgentActor::RunSessionCacheActor(const TActorContext& ctx)
{
    if (AgentConfig->GetTemporaryAgent()) {
        return;
    }

    auto path = GetDiskAgentCachedSessionsPath(AgentConfig, Config);
    if (path.empty()) {
        return;
    }

    auto actor = NDiskAgent::CreateSessionCacheActor(
        std::move(path),
        AgentConfig->GetReleaseInactiveSessionsTimeout());

    // Starting SessionCacheActor on the IO pool to avoid file operations in the
    // User pool
    SessionCacheActor = ctx.Register(
        actor.release(),
        TMailboxType::HTSwap,
        NKikimr::AppData()->IOPoolId);
}

void TDiskAgentActor::UpdateConfigCache(
    const NActors::TActorContext& ctx,
    NProto::TDiskAgentConfig config)
{
    Y_UNUSED(ctx);
    Y_UNUSED(config);

    if (!ConfigCacheActor) {
        return;
    }

    // NCloud::Send<TEvDiskAgentPrivate::TEvUpdateConfigCacheRequest>(
    //     ctx,
    //     ConfigCacheActor,
    //     0,   // cookie
    //     std::move(config));
}

void TDiskAgentActor::RunConfigCacheActor(const NActors::TActorContext& ctx)
{
    // What about temp agent?

    auto path = GetDiskAgentCachedConfigPath(AgentConfig, Config);
    if (path.empty()) {
        return;
    }

    auto actor = NDiskAgent::CreateConfigCacheActor(std::move(path));
    Y_DEBUG_ABORT_UNLESS(actor);
    ConfigCacheActor = ctx.Register(
        actor.release(),
        TMailboxType::Simple,
        NKikimr::AppData()->IOPoolId);

    NProto::TDiskAgentConfig proto;
    proto.MutableFileDevices()->Assign(
        AgentConfig->GetFileDevices().cbegin(),
        AgentConfig->GetFileDevices().cend());
    UpdateConfigCache(ctx, std::move(proto));
}

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentActor::HandleReportDelayedDiskAgentConfigMismatch(
    const TEvDiskAgentPrivate::TEvReportDelayedDiskAgentConfigMismatch::TPtr&
        ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);
    const auto* msg = ev->Get();
    ReportDiskAgentConfigMismatch(
        TStringBuilder() << "[duplicate] " << msg->ErrorText);
}

void TDiskAgentActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_INFO(ctx, TBlockStoreComponents::DISK_AGENT, "Poisoned");

    if (StatsActor) {
        NCloud::Send<TEvents::TEvPoisonPill>(ctx, StatsActor);
        StatsActor = {};
    }

    if (SessionCacheActor) {
        NCloud::Send<TEvents::TEvPoisonPill>(ctx, SessionCacheActor);
        SessionCacheActor = {};
    }

    State->StopTarget();

    for (auto& [uuid, pendingRequests]: SecureErasePendingRequests) {
        for (auto& requestInfo: pendingRequests) {
            NCloud::Reply(
                ctx,
                *requestInfo,
                std::make_unique<TEvDiskAgent::TEvSecureEraseDeviceResponse>(
                    MakeError(E_REJECTED, "DiskAgent is dead")
                ));
        }
    }

    Die(ctx);
}

void TDiskAgentActor::HandleDeleteDevices(
    const TEvDiskAgent::TEvDeleteDevicesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (!ConfigCacheActor) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvDeleteDevicesResponse>(
                MakeError(S_FALSE, "There is no config cache actor.")));
        return;
    }

    const auto* msg = ev->Get();
    if (msg->Record.GetDevicePath().empty()) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvDeleteDevicesResponse>(
                MakeError(E_ARGUMENT, "DevicePath is empty.")));
        return;
    }

    auto requestInfo = CreateRequestInfo<TEvDiskRegistry::TWaitReadyMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    DeleteDevicesRequestQueue.emplace(
        NActors::IEventHandlePtr(ev.Release()),
        std::move(requestInfo));
    SendDeleteDevicesFromQueue(ctx);

    // if (DeleteDevicesRequestInfo) {
    //     DeleteDevicesRequestQueue.push(ev->Release());
    //     return;
    // }


    // DeleteDevicesRequestInfo =
    // Y_DEBUG_ABORT_UNLESS(
    //     !DeleteDevicesRequestInfos.contains(DeleteDevicesRequestCookie));
    // DeleteDevicesRequestInfos[DeleteDevicesRequestCookie] =
    //     CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);
}

void TDiskAgentActor::SendDeleteDevicesFromQueue(const NActors::TActorContext& ctx) {
    if (DeleteDevicesRequestInfo || DeleteDevicesRequestQueue.empty()) {
        return;
    }

    TPendingRequest request = std::move(DeleteDevicesRequestQueue.front());
    DeleteDevicesRequestQueue.pop();
    DeleteDevicesRequestInfo = std::move(request.RequestInfo);
    auto* msg = request.Event->Get<TEvDiskAgent::TEvDeleteDevicesRequest>();

    auto shouldDeletePath = [msg](const TString& path) -> bool
    {
        return AnyOf(
            msg->Record.GetDevicePath(),
            [path](const TString& devicePath) { return path == devicePath; });
    };

    NProto::TDiskAgentConfig proto;
    for (const auto& device: AgentConfig->GetFileDevices()) {
        if (shouldDeletePath(device.GetPath())) {
            if (!State->GetReaderSessions(device.GetDeviceId()).empty() ||
                !State->GetWriterSession(device.GetDeviceId()).Id.empty())
            {
                ctx.Send(
                    SelfId(),
                    new TEvDiskAgentPrivate::TEvUpdateConfigCacheResponse(
                        MakeError(
                            E_INVALID_STATE,
                            "Can't remove device from cache that has an active "
                            "session.")));
                return;
            }
            continue;
        }
        *proto.AddFileDevices() = device;
    }

    // NCloud::Send<TEvDiskAgentPrivate::TEvUpdateConfigCacheRequest>(
    //     ctx,
    //     ConfigCacheActor,
    //     0,   // cookie
    //     std::move(proto));
}

void TDiskAgentActor::HandleUpdateConfigCacheResponse(
    const TEvDiskAgentPrivate::TEvUpdateConfigCacheResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    if (!DeleteDevicesRequestInfo) {
        return;
    }

    const auto* msg = ev->Get();
    NCloud::Reply(
        ctx,
        *DeleteDevicesRequestInfo,
        std::make_unique<TEvDiskAgent::TEvDeleteDevicesResponse>(
            msg->GetError()));
    DeleteDevicesRequestInfo.Reset();
    SendDeleteDevicesFromQueue(ctx);

    // if (ev->Cookie == 0) {
    //     return;
    // }

    // const auto* msg = ev->Get();
    // Y_DEBUG_ABORT_UNLESS(DeleteDevicesRequestInfos.contains(ev->Cookie));
    // auto it = DeleteDevicesRequestInfos.find(ev->Cookie);
    // if (it == DeleteDevicesRequestInfos.end()) {
    //     return;
    // }

    // NCloud::Reply(
    //     ctx,
    //     it->second,
    //     std::make_unique<TEvDiskAgent::TEvDeleteDevicesResponse>(
    //         msg->GetError()));
    // DeleteDevicesRequestInfos.erase(it);
}

void TDiskAgentActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    UpdateCounters(ctx);
    ScheduleCountersUpdate(ctx);
}

bool TDiskAgentActor::HandleRequests(STFUNC_SIG)
{
    switch (ev->GetTypeRewrite()) {
        BLOCKSTORE_DISK_AGENT_REQUESTS(BLOCKSTORE_HANDLE_REQUEST, TEvDiskAgent)
        BLOCKSTORE_DISK_AGENT_REQUESTS_PRIVATE(BLOCKSTORE_HANDLE_REQUEST, TEvDiskAgentPrivate)

        default:
            return false;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TDiskAgentActor::StateInit)
{
    UpdateActorStatsSampled();
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvents::TEvWakeup, HandleWakeup);

        HFunc(NMon::TEvHttpInfo, HandleHttpInfo);

        HFunc(TEvDiskAgentPrivate::TEvInitAgentCompleted, HandleInitAgentCompleted);

        HFunc(
            TEvDiskAgentPrivate::TEvReportDelayedDiskAgentConfigMismatch,
            HandleReportDelayedDiskAgentConfigMismatch);

        BLOCKSTORE_HANDLE_REQUEST(WaitReady, TEvDiskAgent)

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::DISK_AGENT);
            break;
    }
}

STFUNC(TDiskAgentActor::StateWork)
{
    UpdateActorStatsSampled();
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvents::TEvWakeup, HandleWakeup);

        HFunc(NMon::TEvHttpInfo, HandleHttpInfo);

        HFunc(TEvDiskAgentPrivate::TEvSecureEraseCompleted, HandleSecureEraseCompleted);

        HFunc(TEvDiskAgentPrivate::TEvRegisterAgentResponse,
            HandleRegisterAgentResponse);

        HFunc(TEvDiskRegistryProxy::TEvSubscribeResponse, HandleSubscribeResponse);
        HFunc(TEvDiskRegistryProxy::TEvConnectionEstablished, HandleConnectionEstablished);
        HFunc(TEvDiskRegistryProxy::TEvConnectionLost, HandleConnectionLost);

        HFunc(TEvDiskAgentPrivate::TEvWriteOrZeroCompleted, HandleWriteOrZeroCompleted);

        HFunc(
            TEvDiskAgentPrivate::TEvReportDelayedDiskAgentConfigMismatch,
            HandleReportDelayedDiskAgentConfigMismatch);

        HFunc(
            TEvDiskAgentPrivate::TEvUpdateSessionCacheResponse,
            HandleUpdateSessionCacheResponse);

        HFunc(
            TEvDiskAgentPrivate::TEvCancelSuspensionRequest,
            HandleCancelSuspension);

        HFunc(
            TEvDiskAgentPrivate::TEvUpdateConfigCacheResponse,
            HandleUpdateConfigCacheResponse);

        default:
            if (!HandleRequests(ev)) {
                HandleUnexpectedEvent(ev, TBlockStoreComponents::DISK_AGENT);
            }
            break;
    }
}

STFUNC(TDiskAgentActor::StateIdle)
{
    UpdateActorStatsSampled();
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvents::TEvWakeup, HandleWakeup);

        HFunc(NMon::TEvHttpInfo, HandleHttpInfo);

        // IgnoreFunc(TEvDiskRegistryProxy::TEvConnectionLost); ???

        BLOCKSTORE_HANDLE_REQUEST(WaitReady, TEvDiskAgent)

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::DISK_AGENT);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
