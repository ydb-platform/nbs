#include "stats_service_actor.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/storage/stats_service/actors/service_statistics_collector_actor.h>

#include <cloud/storage/core/libs/api/user_stats.h>

#include <contrib/ydb/core/base/appdata.h>
#include <contrib/ydb/core/mon/mon.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace NCloud::NStorage::NUserStats;

////////////////////////////////////////////////////////////////////////////////

TStatsServiceActor::TStatsServiceActor(
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticsConfig,
        NYdbStats::IYdbVolumesStatsUploaderPtr uploader,
        IStatsAggregatorPtr clientStatsAggregator)
    : Config(std::move(config))
    , DiagnosticsConfig(std::move(diagnosticsConfig))
    , StatsUploader(std::move(uploader))
    , ClientStatsAggregator(std::move(clientStatsAggregator))
    , State(*Config, *DiagnosticsConfig)
    , UserCounters(CreateUserCounterSupplier())
{}

void TStatsServiceActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    LOG_WARN(ctx, TBlockStoreComponents::STATS_SERVICE,
        "Stats service running");

    RegisterCounters(ctx);
    ScheduleCleanupBackgroundSources(ctx);
}

void TStatsServiceActor::RegisterCounters(const TActorContext& ctx)
{
    auto counters = AppData(ctx)->Counters;
    auto rootGroup = counters->GetSubgroup("counters", "blockstore");
    auto totalCounters = rootGroup->GetSubgroup("component", "service");
    auto hddCounters = totalCounters->GetSubgroup("type", "hdd");
    auto ssdCounters = totalCounters->GetSubgroup("type", "ssd");
    auto ssdNonreplCounters = totalCounters->GetSubgroup("type", "ssd_nonrepl");
    auto hddNonreplCounters = totalCounters->GetSubgroup("type", "hdd_nonrepl");
    auto ssdMirror2Counters = totalCounters->GetSubgroup("type", "ssd_mirror2");
    auto ssdMirror3Counters = totalCounters->GetSubgroup("type", "ssd_mirror3");
    auto ssdSystemCounters = totalCounters->GetSubgroup("type", "ssd_system");
    auto hddSystemCounters = totalCounters->GetSubgroup("type", "hdd_system");
    auto hddLocalCounters = totalCounters->GetSubgroup("type", "hdd_local");
    auto ssdLocalCounters = totalCounters->GetSubgroup("type", "sdd_local");
    auto localCounters = totalCounters->GetSubgroup("binding", "local");
    auto nonlocalCounters = totalCounters->GetSubgroup("binding", "remote");

    State.GetTotalCounters().Register(totalCounters);
    State.GetHddCounters().Register(hddCounters);
    State.GetSsdCounters().Register(ssdCounters);
    State.GetSsdNonreplCounters().Register(ssdNonreplCounters);
    State.GetHddNonreplCounters().Register(hddNonreplCounters);
    State.GetSsdMirror2Counters().Register(ssdMirror2Counters);
    State.GetSsdMirror3Counters().Register(ssdMirror3Counters);
    State.GetSsdSystemCounters().Register(ssdSystemCounters);
    State.GetHddSystemCounters().Register(hddSystemCounters);
    State.GetSsdLocalCounters().Register(ssdLocalCounters);
    State.GetHddLocalCounters().Register(hddLocalCounters);
    State.GetLocalVolumesCounters().Register(localCounters);
    State.GetNonlocalVolumesCounters().Register(nonlocalCounters);
    State.GetSsdBlobCounters().Register(ssdCounters);
    State.GetHddBlobCounters().Register(hddCounters);

    YDbFailedRequests = totalCounters->GetCounter("Ydb/FailedRequests", true);
    FailedPartitionBoots = totalCounters->GetCounter("FailedBoots", true);

    UpdateVolumeSelfCounters(ctx);

    ScheduleCountersUpdate(ctx);
    ScheduleStatsUpload(ctx);

    auto request = std::make_unique<
        NCloud::NStorage::TEvUserStats::TEvUserStatsProviderCreate>(
            UserCounters);

    NCloud::Send(
        ctx,
        NCloud::NStorage::MakeStorageUserStatsId(),
        std::move(request));
}

void TStatsServiceActor::ScheduleCountersUpdate(const TActorContext& ctx)
{
    ctx.Schedule(UpdateCountersInterval, new TEvents::TEvWakeup());
}

void TStatsServiceActor::RegisterStatisticsCollectorActor(
    const TActorContext& ctx) const
{
    TVector<TActorId> volumeActorIds;
    for (const auto& [_, volumeInfo]: State.GetVolumes()) {
        volumeActorIds.push_back(volumeInfo.VolumeActorId);
    }

    NCloud::Register<TServiceStatisticsCollectorActor>(
        ctx,
        SelfId(),
        std::move(volumeActorIds));
}

////////////////////////////////////////////////////////////////////////////////

void TStatsServiceActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    if (Config->GetUsePullSchemeForVolumeStatistics() &&
        State.GetVolumes().size())
    {
        ScheduleCountersUpdate(ctx);
        RegisterStatisticsCollectorActor(ctx);
        return;
    }

    UpdateVolumeSelfCounters(ctx);
    ScheduleCountersUpdate(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TStatsServiceActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleWakeup);

        HFunc(TEvService::TEvUploadClientMetricsRequest, HandleUploadClientMetrics);

        HFunc(TEvStatsService::TEvRegisterVolume, HandleRegisterVolume);
        HFunc(TEvStatsService::TEvUnregisterVolume, HandleUnregisterVolume);
        HFunc(TEvStatsService::TEvVolumeConfigUpdated, HandleVolumeConfigUpdated);
        HFunc(
            TEvStatsService::TEvPartitionBootExternalCompleted,
            HandlePartitionBootExternalCompleted);
        HFunc(TEvStatsService::TEvVolumePartCounters, HandleVolumePartCounters);
        HFunc(TEvStatsService::TEvVolumeSelfCounters, HandleVolumeSelfCounters);
        HFunc(TEvStatsService::TEvGetVolumeStatsRequest, HandleGetVolumeStats);

        HFunc(TEvStatsServicePrivate::TEvUploadDisksStats, HandleUploadDisksStats);
        HFunc(TEvStatsServicePrivate::TEvUploadDisksStatsCompleted, HandleUploadDisksStatsCompleted);

        HFunc(TEvStatsServicePrivate::TEvStatsUploadRetryTimeout, HandleStatsUploadRetryTimeout);

        HFunc(
            TEvStatsServicePrivate::TEvRegisterTrafficSourceRequest,
            HandleRegisterTrafficSource);
        HFunc(
            TEvStatsServicePrivate::TEvCleanupBackgroundSources,
            HandleCleanupBackgroundSources);

        HFunc(
            TEvStatsService::TEvServiceStatisticsCombined,
            HandleServiceStatisticsCombined);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::STATS_SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
