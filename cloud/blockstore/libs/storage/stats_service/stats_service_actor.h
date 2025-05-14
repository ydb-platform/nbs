#pragma once

#include "stats_service_events_private.h"
#include "stats_service_state.h"

#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/diagnostics/stats_aggregator.h>
#include <cloud/blockstore/libs/diagnostics/user_counter.h>
#include <cloud/blockstore/libs/discovery/discovery.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/stats_service.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/metrics.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/core/tablet_counters.h>
#include <cloud/blockstore/libs/ydbstats/ydbstats.h>
#include <cloud/blockstore/libs/ydbstats/ydbrow.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>
#include <contrib/ydb/library/actors/core/mon.h>

#include <util/stream/output.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TStatsServiceActor final
    : public NActors::TActorBootstrapped<TStatsServiceActor>
{
    struct TActorSystemHolder;

    using TStatsUploadRequest = std::pair<NYdbStats::TYdbRowData, TInstant>;

    struct TBackgroundBandwidthInfo
    {
        ui32 DesiredBandwidthMiBs = 0;

        // The time of the last registration
        TInstant LastRegistrationAt;
    };
    using TBackgroundBandwidth = TMap<TString, TBackgroundBandwidthInfo>;

private:
    const TStorageConfigPtr Config;
    const TDiagnosticsConfigPtr DiagnosticsConfig;
    const NYdbStats::IYdbVolumesStatsUploaderPtr StatsUploader;
    const IStatsAggregatorPtr ClientStatsAggregator;

    TStatsServiceState State;

    std::shared_ptr<TActorSystemHolder> ActorSystemHolder;

    struct TLoadCounters {
        ui64 Bytes = 0;
        ui64 Requests = 0;
    };

    TLoadCounters ReadWriteCounters;

    TDeque<TString> VolumeIdQueueForYdbStatsUpload;
    TDeque<TStatsUploadRequest> YdbStatsRequests;
    TInstant YdbStatsRequestSentTs;
    TInstant YdbMetricsRequestSentTs;

    NBlobMetrics::TBlobLoadMetrics CurrentBlobMetrics;

    NMonitoring::TDynamicCounters::TCounterPtr YDbFailedRequests;
    NMonitoring::TDynamicCounters::TCounterPtr FailedPartitionBoots;

    bool StatsUploadScheduled = false;

    std::shared_ptr<NCloud::NStorage::NUserStats::IUserCounterSupplier> UserCounters;

    TBackgroundBandwidth BackgroundBandwidth;

public:
    TStatsServiceActor(
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticsConfig,
        NYdbStats::IYdbVolumesStatsUploaderPtr statsUploader,
        IStatsAggregatorPtr clientStatsAggregator);
    ~TStatsServiceActor() override = default;

    void Bootstrap(const NActors::TActorContext& ctx);

    void Registered(NActors::TActorSystem* sys, const NActors::TActorId& owner) override;

private:
    void RegisterCounters(const NActors::TActorContext& ctx);

    void ScheduleCountersUpdate(const NActors::TActorContext& ctx);

    void UpdateVolumeSelfCounters(const NActors::TActorContext& ctx);

    void ScheduleStatsUpload(const NActors::TActorContext& ctx);

    void RenderHtmlInfo(IOutputStream& out) const;
    void RenderVolumeList(IOutputStream& out) const;

    template <typename TResponse, typename TRequestPtr>
    void HandleNotSupportedRequest(
        const TRequestPtr& ev,
        const NActors::TActorContext& ctx);

    void PushYdbStats(const NActors::TActorContext& ctx);

    void SplitRowsIntoRequests(
        NYdbStats::TYdbRowData rows,
        const NActors::TActorContext& ctx);

    [[nodiscard]] ui32 CalcBandwidthLimit(const TString& sourceId) const;
    void ScheduleCleanupBackgroundSources(
        const NActors::TActorContext& ctx) const;

private:
    STFUNC(StateWork);

    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUploadClientMetrics(
        const TEvService::TEvUploadClientMetricsRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleVolumeConfigUpdated(
        const TEvStatsService::TEvVolumeConfigUpdated::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleRegisterVolume(
        const TEvStatsService::TEvRegisterVolume::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUnregisterVolume(
        const TEvStatsService::TEvUnregisterVolume::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePartitionBootExternalCompleted(
        const TEvStatsService::TEvPartitionBootExternalCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleVolumePartCounters(
        const TEvStatsService::TEvVolumePartCounters::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleVolumeSelfCounters(
        const TEvStatsService::TEvVolumeSelfCounters::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleGetVolumeStats(
        const TEvStatsService::TEvGetVolumeStatsRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUploadDisksStats(
        const TEvStatsServicePrivate::TEvUploadDisksStats::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUploadDisksStatsCompleted(
        const TEvStatsServicePrivate::TEvUploadDisksStatsCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleStatsUploadRetryTimeout(
        const TEvStatsServicePrivate::TEvStatsUploadRetryTimeout::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleRegisterTrafficSource(
        const TEvStatsServicePrivate::TEvRegisterTrafficSourceRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleCleanupBackgroundSources(
        const TEvStatsServicePrivate::TEvCleanupBackgroundSources::TPtr& ev,
        const NActors::TActorContext& ctx);

    bool HandleRequests(STFUNC_SIG);
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
