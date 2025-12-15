#include "stats_service_actor.h"

#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/core/disk_counters.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/storage/core/libs/diagnostics/histogram.h>
#include <cloud/storage/core/libs/diagnostics/weighted_percentile.h>

#include <contrib/ydb/core/base/appdata.h>

#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NMonitoring;
using namespace NProto;
using namespace NYdbStats;

namespace {

////////////////////////////////////////////////////////////////////////////////

bool IsRecentlyStarted(TInstant now, const TVolumeStatsInfo& v)
{
    return now <= v.ApproximateStartTs + TDuration::Minutes(5);
}

std::vector<std::pair<TString, TString>> BuildVolumeChain(
    const NProto::TVolume& volumeInfo)
{
    return {
        {"volume", volumeInfo.GetDiskId()},
        {"cloud", volumeInfo.GetCloudId()},
        {"folder", volumeInfo.GetFolderId()}};
}

TIntrusivePtr<TDynamicCounters> RegisterChain(
    const NMonitoring::TDynamicCounterPtr& counters,
    const std::vector<std::pair<TString, TString>>& chain)
{
    auto subgroup = counters;
    for (const auto& [name, value]: chain) {
        subgroup = subgroup->GetSubgroup(name, value);
    }
    return subgroup;
}

void RegisterIsLocalMountCounter(
    const NMonitoring::TDynamicCounterPtr& counters,
    TVolumeStatsInfo& volume)
{
    if (!counters) {
        return;
    }
    if (volume.IsLocalMountCounter) {
        return;
    }

    auto head = counters->GetSubgroup("counters", "blockstore")
                    ->GetSubgroup("component", "service_volume");

    auto volumeCounters =
        RegisterChain(head, BuildVolumeChain(volume.VolumeInfo));

    volume.IsLocalMountCounter =
        volumeCounters->GetCounter("IsLocalMount", false);
}

void UnregisterIsLocalMountCounter(
    const NMonitoring::TDynamicCounterPtr& counters,
    TVolumeStatsInfo& volume)
{
    if (!counters) {
        return;
    }
    if (!volume.IsLocalMountCounter) {
        return;
    }

    auto head = counters->GetSubgroup("counters", "blockstore")
                    ->GetSubgroup("component", "service_volume");

    head->RemoveSubgroupChain(BuildVolumeChain(volume.VolumeInfo));

    volume.IsLocalMountCounter = nullptr;
}

}    // namespace

////////////////////////////////////////////////////////////////////////////////

void TStatsServiceActor::RegisterServiceVolumeCounters(
    const NMonitoring::TDynamicCounterPtr& counters,
    TVolumeStatsInfo& volume)
{
    if (!counters) {
        return;
    }
    if (volume.ServiceVolumeCounters) {
        return;
    }

    auto head =
        counters->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "service_volume")
            ->GetSubgroup("host", "cluster");

    volume.ServiceVolumeCounters =
        RegisterChain(head, BuildVolumeChain(volume.VolumeInfo));

    volume.PerfCounters.Register(volume.ServiceVolumeCounters);

    NUserCounter::RegisterServiceVolume(
        *UserCounters,
        volume.VolumeInfo.GetCloudId(),
        volume.VolumeInfo.GetFolderId(),
        volume.VolumeInfo.GetDiskId(),
        DiagnosticsConfig->GetHistogramCounterOptions(),
        volume.ServiceVolumeCounters);
}

void TStatsServiceActor::UnregisterServiceVolumeCounters(
    const NMonitoring::TDynamicCounterPtr& counters,
    TVolumeStatsInfo& volume)
{
    if (!counters) {
        return;
    }
    if (!volume.ServiceVolumeCounters) {
        return;
    }

    auto head =
        counters->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "service_volume")
            ->GetSubgroup("host", "cluster");

    head->RemoveSubgroupChain(BuildVolumeChain(volume.VolumeInfo));

    NUserCounter::UnregisterServiceVolume(
        *UserCounters,
        volume.VolumeInfo.GetCloudId(),
        volume.VolumeInfo.GetFolderId(),
        volume.VolumeInfo.GetDiskId());
}

void TStatsServiceActor::UpdateVolumeSelfCounters(const TActorContext& ctx)
{
    TVector<TTotalCounters*> totalCounters{
        &State.GetTotalCounters(),
        &State.GetSsdCounters(),
        &State.GetHddCounters(),
        &State.GetSsdNonreplCounters(),
        &State.GetHddNonreplCounters(),
        &State.GetSsdMirror2Counters(),
        &State.GetSsdMirror3Counters(),
        &State.GetSsdSystemCounters(),
        &State.GetHddSystemCounters(),
        &State.GetHddLocalCounters(),
        &State.GetSsdLocalCounters(),
    };

    for (auto* tc: totalCounters) {
        tc->TotalDiskCount.Reset();
        tc->TotalDiskCountLast15Min.Reset();
        tc->TotalDiskCountLastHour.Reset();
        tc->TotalPartitionCount.Reset();
        tc->VolumeLoadTimeUnder1Sec.Reset();
        tc->VolumeLoadTime1To5Sec.Reset();
        tc->VolumeLoadTimeOver5Sec.Reset();
        tc->VolumeStartTimeUnder1Sec.Reset();
        tc->VolumeStartTime1To5Sec.Reset();
        tc->VolumeStartTimeOver5Sec.Reset();
    }

    NBlobMetrics::TBlobLoadMetrics tempBlobMetrics;

    auto& serviceTotal = State.GetTotalCounters();

    for (auto& p: State.GetVolumes()) {
        auto& vol = p.second;
        auto& tc = State.GetCounters(vol.VolumeInfo);

        if (IsRecentlyStarted(ctx.Now(), vol)) {
            const auto& selfSimple = vol.PerfCounters.VolumeSelfCounters.Simple;
            const auto loadTime =
                TDuration::MicroSeconds(selfSimple.LastVolumeLoadTime.Value);
            if (loadTime >= TDuration::Seconds(1)) {
                if (loadTime < TDuration::Seconds(5)) {
                    tc.VolumeLoadTime1To5Sec.Increment(1);
                    serviceTotal.VolumeLoadTime1To5Sec.Increment(1);
                } else {
                    tc.VolumeLoadTimeOver5Sec.Increment(1);
                    serviceTotal.VolumeLoadTimeOver5Sec.Increment(1);
                }
            } else {
                tc.VolumeLoadTimeUnder1Sec.Increment(1);
                serviceTotal.VolumeLoadTimeUnder1Sec.Increment(1);
            }

            const auto startTime =
                TDuration::MicroSeconds(selfSimple.LastVolumeStartTime.Value);
            if (startTime >= TDuration::Seconds(1)) {
                if (startTime < TDuration::Seconds(5)) {
                    tc.VolumeStartTime1To5Sec.Increment(1);
                    serviceTotal.VolumeStartTime1To5Sec.Increment(1);
                } else {
                    tc.VolumeStartTimeOver5Sec.Increment(1);
                    serviceTotal.VolumeStartTimeOver5Sec.Increment(1);
                }
            } else {
                tc.VolumeStartTimeUnder1Sec.Increment(1);
                serviceTotal.VolumeStartTimeUnder1Sec.Increment(1);
            }
        }

        RegisterIsLocalMountCounter(AppData(ctx)->Counters, vol);
        *vol.IsLocalMountCounter = vol.IsLocalMount;

        const bool shouldPublishServiceVolumeCounters =
            vol.PerfCounters.HasCheckpoint || vol.PerfCounters.HasClients;
        if (shouldPublishServiceVolumeCounters) {
            RegisterServiceVolumeCounters(AppData(ctx)->Counters, vol);
            vol.PerfCounters.Publish(ctx.Now());
        } else {
            UnregisterServiceVolumeCounters(AppData(ctx)->Counters, vol);
        }

        tc.TotalDiskCount.Increment(1);
        tc.TotalDiskCountLast15Min.Increment(1);
        tc.TotalDiskCountLastHour.Increment(1);
        tc.TotalPartitionCount.Increment(vol.VolumeInfo.GetPartitionsCount());

        serviceTotal.TotalDiskCount.Increment(1);
        serviceTotal.TotalDiskCountLast15Min.Increment(1);
        serviceTotal.TotalDiskCountLastHour.Increment(1);
        serviceTotal.TotalPartitionCount.Increment(vol.VolumeInfo.GetPartitionsCount());
        tempBlobMetrics += vol.OffsetBlobMetrics;
    }

    for (const auto& rv: State.UpdateAndGetRecentVolumes(ctx.Now())) {
        auto& tc = State.GetCounters(
            rv.IsSystem,
            rv.StorageMediaKind);
        if (rv.RemoveTs + TDuration::Minutes(15) >= ctx.Now()) {
            tc.TotalDiskCountLast15Min.Increment(1);
            serviceTotal.TotalDiskCountLast15Min.Increment(1);
        }
        if (rv.RemoveTs + TDuration::Hours(1) >= ctx.Now()) {
            tc.TotalDiskCountLastHour.Increment(1);
            serviceTotal.TotalDiskCountLastHour.Increment(1);
        }
    }

    for (auto* tc: totalCounters) {
        tc->Publish(ctx.Now());
    }

    State.GetSsdBlobCounters().Publish(tempBlobMetrics, ctx.Now());
    State.GetHddBlobCounters().Publish(tempBlobMetrics, ctx.Now());

    CurrentBlobMetrics += tempBlobMetrics;

    State.GetLocalVolumesCounters().Publish(ctx.Now());
    State.GetNonlocalVolumesCounters().Publish(ctx.Now());
}

void TStatsServiceActor::HandleRegisterVolume(
    const TEvStatsService::TEvRegisterVolume::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);
    const auto* msg = ev->Get();

    auto *volume = State.GetOrAddVolume(msg->DiskId, msg->Config);
    volume->VolumeTabletId = msg->TabletId;

    if (volume->IsDiskRegistryBased()) {
        volume->PerfCounters = TDiskPerfData(
            EPublishingPolicy::DiskRegistryBased,
            DiagnosticsConfig->GetHistogramCounterOptions());
    } else {
        volume->PerfCounters = TDiskPerfData(
            EPublishingPolicy::Repl,
            DiagnosticsConfig->GetHistogramCounterOptions());
    }

    if (volume->ServiceVolumeCounters) {
        volume->PerfCounters.Register(volume->ServiceVolumeCounters);
    }
}

void TStatsServiceActor::HandleVolumeConfigUpdated(
    const TEvStatsService::TEvVolumeConfigUpdated::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto *volume = State.GetVolume(msg->DiskId);
    if (!volume) {
        LOG_WARN(ctx, TBlockStoreComponents::STATS_SERVICE,
            "Volume %s not found",
            msg->DiskId.Quote().data());
        return;
    }

    const bool updateCounters =
        (volume->VolumeInfo.GetCloudId() != msg->Config.GetCloudId() ||
            volume->VolumeInfo.GetFolderId() != msg->Config.GetFolderId());

    if (updateCounters) {
        UnregisterIsLocalMountCounter(AppData(ctx)->Counters, *volume);
        UnregisterServiceVolumeCounters(AppData(ctx)->Counters, *volume);
    }

    volume->VolumeInfo = msg->Config;

    if (updateCounters) {
        RegisterIsLocalMountCounter(AppData(ctx)->Counters, *volume);
        RegisterServiceVolumeCounters(AppData(ctx)->Counters, *volume);
    }
}

void TStatsServiceActor::HandleUnregisterVolume(
    const TEvStatsService::TEvUnregisterVolume::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto& volumes = State.GetVolumes();

    if (auto it = volumes.find(msg->DiskId); it != volumes.end()) {
        UnregisterIsLocalMountCounter(AppData(ctx)->Counters, it->second);
        UnregisterServiceVolumeCounters(AppData(ctx)->Counters, it->second);

        State.RemoveVolume(ctx.Now(), it->first);
    }
}

void TStatsServiceActor::HandlePartitionBootExternalCompleted(
    const TEvStatsService::TEvPartitionBootExternalCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto *volume = State.GetVolume(msg->DiskId);
    if (!volume) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::STATS_SERVICE,
            "Volume %s not found",
            msg->DiskId.Quote().data());
        return;
    }

    volume->ChannelInfos[msg->PartitionTabletId] =
        std::move(msg->ChannelInfos);
}

void TStatsServiceActor::HandleVolumePartCounters(
    const TEvStatsService::TEvVolumePartCounters::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto* volume = State.GetVolume(msg->DiskId);

    if (!volume) {
        LOG_DEBUG(ctx, TBlockStoreComponents::STATS_SERVICE,
            "Volume %s for counters not found",
            msg->DiskId.Quote().data());
        return;
    }

    volume->PerfCounters.VolumeSystemCpu += msg->VolumeSystemCpu;
    volume->PerfCounters.VolumeUserCpu += msg->VolumeUserCpu;
    volume->PerfCounters.HasCheckpoint = msg->HasCheckpoint;

    volume->PerfCounters.DiskCounters.Add(*msg->DiskCounters);
    volume->PerfCounters.YdbDiskCounters.Add(*msg->DiskCounters);
    volume->OffsetBlobMetrics = msg->BlobLoadMetrics;

    State.GetTotalCounters().UpdatePartCounters(*msg->DiskCounters);

    State.GetCounters(volume->VolumeInfo).UpdatePartCounters(*msg->DiskCounters);

    if (ev->Sender.NodeId() == SelfId().NodeId()) {
        State.GetLocalVolumesCounters().UpdateCounters(*msg->DiskCounters);
    } else {
        State.GetNonlocalVolumesCounters().UpdateCounters(*msg->DiskCounters);
    }
}

void TStatsServiceActor::HandleVolumeSelfCounters(
    const TEvStatsService::TEvVolumeSelfCounters::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto* volume = State.GetVolume(msg->DiskId);

    if (!volume) {
        LOG_DEBUG(ctx, TBlockStoreComponents::STATS_SERVICE,
            "Volume %s for counters not found",
            msg->DiskId.Quote().data());
        return;
    }

    volume->IsLocalMount = msg->IsLocalMount;

    if (!volume->ApproximateStartTs) {
        volume->ApproximateStartTs = ctx.Now();
    }

    auto& selfNew = *msg->VolumeSelfCounters;

    auto& selfSimpleNew = selfNew.Simple;
    auto& loadTimeNew = selfSimpleNew.LastVolumeLoadTime.Value;
    auto& startTimeNew = selfSimpleNew.LastVolumeStartTime.Value;
    const auto bootstrapTimeNew =
        TDuration::MicroSeconds(loadTimeNew + startTimeNew);
    if (volume->ApproximateBootstrapTime != bootstrapTimeNew)
    {
        // it's the first time we are getting stats for this volume or the
        // volume restarted recently
        volume->ApproximateStartTs = ctx.Now();
        volume->ApproximateBootstrapTime = bootstrapTimeNew;
    }

    volume->PerfCounters.VolumeSelfCounters.Add(selfNew);
    volume->PerfCounters.YdbVolumeSelfCounters.Add(selfNew);
    volume->PerfCounters.HasClients = msg->HasClients;
    volume->PerfCounters.IsPreempted = msg->IsPreempted;

    FailedPartitionBoots->Add(msg->FailedBoots);

    if (!IsRecentlyStarted(ctx.Now(), *volume)) {
        loadTimeNew = 0;
        startTimeNew = 0;
    }

    State.GetTotalCounters().UpdateVolumeSelfCounters(selfNew);
    State.GetCounters(volume->VolumeInfo).UpdateVolumeSelfCounters(selfNew);
}

}   // namespace NCloud::NBlockStore::NStorage
