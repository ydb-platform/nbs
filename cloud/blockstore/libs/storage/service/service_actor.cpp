#include "service_actor.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/storage/api/undelivered.h>
#include <cloud/blockstore/libs/storage/core/disk_counters.h>
#include <cloud/storage/core/libs/common/alloc.h>

#include <contrib/ydb/core/base/appdata.h>
#include <contrib/ydb/core/mon/mon.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

TServiceActor::TServiceActor(
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticsConfig,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        NDiscovery::IDiscoveryServicePtr discoveryService,
        ITraceSerializerPtr traceSerializer,
        NServer::IEndpointEventHandlerPtr endpointEventHandler,
        NRdma::IClientPtr rdmaClient,
        IVolumeStatsPtr volumeStats,
        TManuallyPreemptedVolumesPtr preemptedVolumes,
        IRootKmsKeyProviderPtr rootKmsKeyProvider)
    : Config(std::move(config))
    , DiagnosticsConfig(std::move(diagnosticsConfig))
    , ProfileLog(std::move(profileLog))
    , BlockDigestGenerator(std::move(blockDigestGenerator))
    , DiscoveryService(std::move(discoveryService))
    , TraceSerializer(std::move(traceSerializer))
    , EndpointEventHandler(std::move(endpointEventHandler))
    , RdmaClient(std::move(rdmaClient))
    , VolumeStats(std::move(volumeStats))
    , RootKmsKeyProvider(std::move(rootKmsKeyProvider))
    , SharedCounters(MakeIntrusive<TSharedServiceCounters>(Config))
    , State(std::move(preemptedVolumes))
{}

TServiceActor::~TServiceActor() = default;

void TServiceActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    RegisterPages(ctx);
    RegisterCounters(ctx);
    ScheduleLivenessCheck(ctx, {});
}

void TServiceActor::RegisterPages(const TActorContext& ctx)
{
    auto mon = AppData(ctx)->Mon;
    if (mon) {
        auto* rootPage = mon->RegisterIndexPage("blockstore", "BlockStore");

        mon->RegisterActorPage(rootPage, "service", "Service",
            false, ctx.ActorSystem(), SelfId());
    }
}

void TServiceActor::RegisterCounters(const TActorContext& ctx)
{
    Counters = CreateServiceCounters();

    SelfPingMaxUsCounter = AppData(ctx)
        ->Counters
        ->GetSubgroup("counters", "blockstore")
        ->GetSubgroup("component", "service")
        ->GetCounter("SelfPingMaxUs", false);

    ScheduleSelfPing(ctx);

    UpdateCounters(ctx);

    ScheduleCountersUpdate(ctx);
}

void TServiceActor::ScheduleSelfPing(const TActorContext& ctx)
{
    ctx.Schedule(
        TDuration::MilliSeconds(10),
        new TEvServicePrivate::TEvSelfPing(GetCycleCount()));
}

void TServiceActor::ScheduleCountersUpdate(const TActorContext& ctx)
{
    ctx.Schedule(UpdateCountersInterval, new TEvents::TEvWakeup());
}

void TServiceActor::UpdateCounters(const TActorContext& ctx)
{
    auto& bytesAllocated = Counters->Simple()[TServiceCounters::SIMPLE_COUNTER_Stats_BlockBufferBytes];
    bytesAllocated.Set(TProfilingAllocator::Instance()->GetBytesAllocated());

    auto counters = AppData(ctx)->Counters;
    if (counters) {
        auto rootGroup = counters->GetSubgroup("counters", "blockstore");
        auto serviceCounters = rootGroup->GetSubgroup("component", "service");

        const auto& simpleCounters = Counters->Simple();
        for (ui32 idx = 0; idx < simpleCounters.Size(); ++idx) {
            auto counter = serviceCounters->GetCounter(
                Counters->SimpleCounterName(idx));
            *counter = simpleCounters[idx].Get();
        }

        const auto& cumulativeCounters = Counters->Cumulative();
        for (ui32 idx = 0; idx < cumulativeCounters.Size(); ++idx) {
            auto counter = serviceCounters->GetCounter(
                Counters->CumulativeCounterName(idx),
                true);
            *counter = cumulativeCounters[idx].Get();
        }

        const auto& percentileCounters = Counters->Percentile();
        for (ui32 idx = 0; idx < percentileCounters.Size(); ++idx) {
            auto histogramGroup = serviceCounters->GetSubgroup(
                "histogram",
                Counters->PercentileCounterName(idx));

            const auto& percentileCounter = percentileCounters[idx];
            for (ui32 idxRange = 0; idxRange < percentileCounter.GetRangeCount(); ++idxRange) {
                auto counter = histogramGroup->GetCounter(
                    percentileCounter.GetRangeName(idxRange),
                    true);
                *counter = percentileCounter.GetRangeValue(idxRange);
            }
        }
    }

    *SelfPingMaxUsCounter = CyclesToDuration(SelfPingMaxUs).MicroSeconds();
    SelfPingMaxUs = 0;
}

void TServiceActor::UpdateActorStats(const TActorContext& ctx)
{
    if (Counters) {
        auto& actorQueue = Counters->Percentile()
            [TServiceCounters::PERCENTILE_COUNTER_Actor_ActorQueue];
        auto& mailboxQueue = Counters->Percentile()
            [TServiceCounters::PERCENTILE_COUNTER_Actor_MailboxQueue];

        auto actorQueues = ctx.CountMailboxEvents(1001);
        IncrementFor(actorQueue, actorQueues.first);
        IncrementFor(mailboxQueue, actorQueues.second);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TServiceActor::HandleVolumeMountStateChanged(
    const TEvService::TEvVolumeMountStateChanged::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);
    const auto& msg = ev->Get();

    if (auto volume = State.GetVolume(msg->DiskId)) {
        volume->SetTabletReportedLocalMount(msg->HasLocalMount);
    }
}


void TServiceActor::HandleRegisterVolume(
    const TEvService::TEvRegisterVolume::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);
    const auto& msg = ev->Get();

    auto volume = State.GetOrAddVolume(msg->DiskId);
    volume->TabletId = msg->TabletId;
    volume->VolumeInfo = msg->Config;
}

void TServiceActor::HandleVolumeConfigUpdated(
    const TEvService::TEvVolumeConfigUpdated::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);
    const auto& msg = ev->Get();

    auto volume = State.GetVolume(msg->DiskId);
    if (!volume) {
        LOG_WARN(ctx, TBlockStoreComponents::SERVICE,
            "Volume %s not found",
            msg->DiskId.Quote().data());
        return;
    }
    volume->VolumeInfo = msg->Config;
}

void TServiceActor::HandleUnregisterVolume(
    const TEvService::TEvUnregisterVolume::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    const auto& msg = ev->Get();
    auto volume = State.GetVolume(msg->DiskId);
    if (volume && !volume->VolumeSessionActor) {
        State.RemoveVolume(std::move(volume));
    }
}

////////////////////////////////////////////////////////////////////////////////

void TServiceActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    UpdateCounters(ctx);
    ScheduleCountersUpdate(ctx);
}

void TServiceActor::HandleSelfPing(
    const TEvServicePrivate::TEvSelfPing::TPtr& ev,
    const TActorContext& ctx)
{
    SelfPingMaxUs = std::max(SelfPingMaxUs, GetCycleCount() - ev->Get()->StartCycles);
    ScheduleSelfPing(ctx);
}

void TServiceActor::HandlePing(
    const TEvService::TEvPingRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ReadWriteCounters.Update(
        ctx.Now(),
        Config->GetPingMetricsHalfDecayInterval(),
        0);

    auto response = std::make_unique<TEvService::TEvPingResponse>();
    response->Record.SetLastByteCount(ReadWriteCounters.GetBytes());
    response->Record.SetLastRequestCount(ReadWriteCounters.GetRequests());

    NCloud::Reply(ctx, *ev, std::move(response));
}

void TServiceActor::HandleDiscoverInstances(
    const TEvService::TEvDiscoverInstancesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto response = std::make_unique<TEvService::TEvDiscoverInstancesResponse>();
    DiscoveryService->ServeRequest(msg->Record, &response->Record);

    NCloud::Reply(ctx, *ev, std::move(response));
}

template <typename TResponse, typename TRequestPtr>
void TServiceActor::HandleNotSupportedRequest(
    const TRequestPtr& ev,
    const TActorContext& ctx)
{
    auto response = std::make_unique<TResponse>();

    auto& responseError = *response->Record.MutableError();
    responseError.SetCode(E_FAIL);
    responseError.SetMessage("Not supported in ServiceActor");

    NCloud::Reply(ctx, *ev, std::move(response));
}

bool TServiceActor::HandleRequests(STFUNC_SIG)
{
    switch (ev->GetTypeRewrite()) {
        BLOCKSTORE_STORAGE_SERVICE(BLOCKSTORE_HANDLE_REQUEST, TEvService)
        BLOCKSTORE_SERVICE_REQUESTS(BLOCKSTORE_HANDLE_REQUEST, TEvService)

        default:
            return false;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TServiceActor::StateWork)
{
    UpdateActorStatsSampled(ActorContext());
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleWakeup);
        HFunc(TEvServicePrivate::TEvSelfPing, HandleSelfPing);

        HFunc(NMon::TEvHttpInfo, HandleHttpInfo);

        HFunc(TEvService::TEvVolumeMountStateChanged, HandleVolumeMountStateChanged);
        HFunc(TEvService::TEvRegisterVolume, HandleRegisterVolume);
        HFunc(TEvService::TEvUnregisterVolume, HandleUnregisterVolume);
        HFunc(TEvService::TEvVolumeConfigUpdated, HandleVolumeConfigUpdated);

        HFunc(TEvServicePrivate::TEvSessionActorDied, HandleSessionActorDied);

        HFunc(
            TEvServicePrivate::TEvUpdateManuallyPreemptedVolume,
            HandleUpdateManuallyPreemptedVolume);
        HFunc(
            TEvServicePrivate::TEvSyncManuallyPreemptedVolumesComplete,
            HandleSyncManuallyPreemptedVolumesComplete);
        HFunc(
            TEvService::TEvRunVolumesLivenessCheckResponse,
            HandleRunVolumesLivenessCheckResponse);

        default:
            if (!HandleRequests(ev)) {
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::SERVICE,
                    __PRETTY_FUNCTION__);
            }
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
