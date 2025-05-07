#include "disk_agent_actor.h"

#include "actors/device_health_check_actor.h"
#include "actors/session_cache_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/nvme/nvme.h>
#include <cloud/blockstore/libs/service/storage_provider.h>
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
            false, ctx.ActorSystem(), SelfId());
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

    auto path = GetCachedSessionsPath();
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

TString TDiskAgentActor::GetCachedSessionsPath() const
{
    const TString storagePath = Config->GetCachedDiskAgentSessionsPath();
    const TString agentPath = AgentConfig->GetCachedSessionsPath();
    return agentPath.empty() ? storagePath : agentPath;
}

void TDiskAgentActor::RestartDeviceHealthChecking(const TActorContext& ctx)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_AGENT,
        "Restart device health checking");

    if (HealthCheckActor) {
        NCloud::Send<TEvents::TEvPoisonPill>(ctx, HealthCheckActor);
        HealthCheckActor = {};
    }

    if (!AgentConfig->GetDeviceHealthCheckDisabled()) {
        HealthCheckActor = NCloud::Register(
            ctx,
            NDiskAgent::CreateDeviceHealthCheckActor(
                ctx.SelfID,
                State->GetEnabledDevices(),
                UpdateCountersInterval));
    }
}

TDuration TDiskAgentActor::GetMaxRequestTimeout() const
{
    return Max(
        Config->GetNonReplicatedMaxRequestTimeoutSSD(),
        Config->GetNonReplicatedMaxRequestTimeoutHDD());
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

    if (HealthCheckActor) {
        NCloud::Send<TEvents::TEvPoisonPill>(ctx, HealthCheckActor);
        HealthCheckActor = {};
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

    for (const auto& actor: IOParserActors) {
        NCloud::Send<TEvents::TEvPoisonPill>(ctx, actor);
    }
    IOParserActors.clear();

    Die(ctx);
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

bool TDiskAgentActor::RejectRequests(STFUNC_SIG)
{
    switch (ev->GetTypeRewrite()) {
        BLOCKSTORE_DISK_AGENT_REQUESTS(BLOCKSTORE_REJECT_REQUEST, TEvDiskAgent)
        BLOCKSTORE_DISK_AGENT_REQUESTS_PRIVATE(
            BLOCKSTORE_REJECT_REQUEST,
            TEvDiskAgentPrivate)

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
            if (!RejectRequests(ev)) {
                auto ctx = ActorContext();
                LOG_WARN(
                    ctx,
                    TBlockStoreComponents::DISK_AGENT,
                    "Unexpected request in Init state");
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::DISK_AGENT,
                    __PRETTY_FUNCTION__);
            }
            break;
    }
}

bool TDiskAgentActor::ShouldOffloadRequest(ui32 eventType) const
{
    if (IOParserActors.empty()) {
        return false;
    }

    if (eventType == TEvDiskAgent::EvWriteDeviceBlocksRequest) {
        return true;
    }

    if (!AgentConfig->GetOffloadAllIORequestsParsingEnabled()) {
        return false;
    }

    return eventType == TEvDiskAgent::EvReadDeviceBlocksRequest ||
           eventType == TEvDiskAgent::EvZeroDeviceBlocksRequest;
}

STFUNC(TDiskAgentActor::StateWork)
{
    UpdateActorStatsSampled();

    if (ShouldOffloadRequest(ev->GetTypeRewrite())) {
        ev->Rewrite(ev->Type, IOParserActors[ParserActorIdx]);
        ParserActorIdx = (ParserActorIdx + 1) % IOParserActors.size();

        ActorContext().Send(ev);

        return;
    }

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
            TEvDiskAgent::TEvDisableConcreteAgentRequest,
            HandleDisableConcreteAgent);

        HFunc(
            TEvDiskAgentPrivate::TEvParsedWriteDeviceBlocksRequest,
            HandleParsedWriteDeviceBlocks);

        case TEvDiskAgentPrivate::EvParsedReadDeviceBlocksRequest:
            HandleReadDeviceBlocks(
                *reinterpret_cast<
                    typename TEvDiskAgent::TEvReadDeviceBlocksRequest::TPtr*>(
                    &ev),
                ActorContext());
            break;

        case TEvDiskAgentPrivate::EvParsedZeroDeviceBlocksRequest:
            HandleZeroDeviceBlocks(
                *reinterpret_cast<
                    typename TEvDiskAgent::TEvZeroDeviceBlocksRequest::TPtr*>(
                    &ev),
                ActorContext());
            break;
        HFunc(
            TEvDiskAgent::TEvDirectCopyBlocksRequest,
            HandleDirectCopyBlocks);

        default:
            if (!HandleRequests(ev)) {
                auto ctx = ActorContext();
                LOG_WARN(
                    ctx,
                    TBlockStoreComponents::DISK_AGENT,
                    "Unexpected request in Work state");
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::DISK_AGENT,
                    __PRETTY_FUNCTION__);
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

        BLOCKSTORE_HANDLE_REQUEST(WaitReady, TEvDiskAgent)

        default:
            if (!RejectRequests(ev)) {
                auto ctx = ActorContext();
                LOG_WARN(
                    ctx,
                    TBlockStoreComponents::DISK_AGENT,
                    "Unexpected request in Idle state");
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::DISK_AGENT,
                    __PRETTY_FUNCTION__);
            }
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
