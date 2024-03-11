#include "disk_agent_actor.h"

#include "actors/session_cache_actor.h"

#include <cloud/blockstore/libs/nvme/nvme.h>
#include <cloud/blockstore/libs/service/storage_provider.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/mon/mon.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

TDiskAgentActor::TDiskAgentActor(
        TStorageConfigPtr config,
        TDiskAgentConfigPtr agentConfig,
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
    , Spdk(std::move(spdk))
    , Allocator(std::move(allocator))
    , StorageProvider(std::move(storageProvider))
    , ProfileLog(std::move(profileLog))
    , BlockDigestGenerator(std::move(blockDigestGenerator))
    , Logging(std::move(logging))
    , RdmaServer(std::move(rdmaServer))
    , NvmeManager(std::move(nvmeManager))
{
    ActivityType = TBlockStoreActivities::DISK_AGENT;
}

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

void TDiskAgentActor::UpdateSessionCacheAndRespond(
    const NActors::TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    NActors::IEventBasePtr response)
{
    LOG_INFO(ctx, TBlockStoreComponents::DISK_AGENT, "Update the session cache");

    auto actor = NDiskAgent::CreateSessionCacheActor(
        State->GetSessions(),
        AgentConfig->GetCachedSessionsPath(),
        std::move(requestInfo),
        std::move(response));

    ctx.Register(
        actor.release(),
        TMailboxType::HTSwap,
        NKikimr::AppData()->IOPoolId);
}

////////////////////////////////////////////////////////////////////////////////

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

        HFunc(TEvDiskAgent::TEvAcquireDevicesRequest, HandleAcquireDevices);
        HFunc(TEvDiskAgentPrivate::TEvRegisterAgentResponse,
            HandleRegisterAgentResponse);

        HFunc(TEvDiskRegistryProxy::TEvSubscribeResponse, HandleSubscribeResponse);
        HFunc(TEvDiskRegistryProxy::TEvConnectionLost, HandleConnectionLost);

        HFunc(TEvDiskAgentPrivate::TEvWriteOrZeroCompleted, HandleWriteOrZeroCompleted);

        default:
            if (!HandleRequests(ev)) {
                HandleUnexpectedEvent(ev, TBlockStoreComponents::DISK_AGENT);
            }
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
