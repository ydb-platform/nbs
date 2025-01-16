#include "stats_service_state.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TTotalCounters::Register(NMonitoring::TDynamicCountersPtr counters)
{
    PartAcc.Register(counters, true);
    VolumeAcc.Register(counters, true);
    TotalDiskCount.Register(counters, "TotalDiskCount");
    TotalDiskCountLast15Min.Register(counters, "TotalDiskCountLast15Min");
    TotalDiskCountLastHour.Register(counters, "TotalDiskCountLastHour");
    TotalPartitionCount.Register(counters, "TotalPartitionCount");
    VolumeLoadTimeUnder1Sec.Register(counters, "VolumeLoadTimeUnder1Sec");
    VolumeLoadTime1To5Sec.Register(counters, "VolumeLoadTime1To5Sec");
    VolumeLoadTimeOver5Sec.Register(counters, "VolumeLoadTimeOver5Sec");
    VolumeStartTimeUnder1Sec.Register(counters, "VolumeStartTimeUnder1Sec");
    VolumeStartTime1To5Sec.Register(counters, "VolumeStartTime1To5Sec");
    VolumeStartTimeOver5Sec.Register(counters, "VolumeStartTimeOver5Sec");
}

void TTotalCounters::Reset()
{
    PartAcc.Reset();
    VolumeAcc.Reset();
}

void TTotalCounters::Publish(TInstant now)
{
    PartAcc.Publish(now);
    VolumeAcc.Publish(now);
    TotalDiskCount.Publish(now);
    TotalDiskCountLast15Min.Publish(now);
    TotalDiskCountLastHour.Publish(now);
    TotalPartitionCount.Publish(now);
    VolumeLoadTimeUnder1Sec.Publish(now);
    VolumeLoadTime1To5Sec.Publish(now);
    VolumeLoadTimeOver5Sec.Publish(now);
    VolumeStartTimeUnder1Sec.Publish(now);
    VolumeStartTime1To5Sec.Publish(now);
    VolumeStartTimeOver5Sec.Publish(now);
}

void TTotalCounters::UpdatePartCounters(const TPartitionDiskCounters& source)
{
    PartAcc.AggregateWith(source);
}

void TTotalCounters::UpdateVolumeSelfCounters(const TVolumeSelfCounters& source)
{
    VolumeAcc.AggregateWith(source);
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeRequestCounters::Register(NMonitoring::TDynamicCountersPtr counters)
{
    ReadCount.Register(counters->GetSubgroup("request", "ReadBlocks"), "Count");
    ReadBytes.Register(counters->GetSubgroup("request", "ReadBlocks"), "RequestBytes");
    ReadVoidBytes.Register(counters->GetSubgroup("request", "ReadBlocks"), "RequestVoidBytes");
    ReadNonVoidBytes.Register(counters->GetSubgroup("request", "ReadBlocks"), "RequestNonVoidBytes");

    WriteCount.Register(counters->GetSubgroup("request", "WriteBlocks"), "Count");
    WriteBytes.Register(counters->GetSubgroup("request", "WriteBlocks"), "RequestBytes");

    ZeroCount.Register(counters->GetSubgroup("request", "ZeroBlocks"), "Count");
    ZeroBytes.Register(counters->GetSubgroup("request", "ZeroBlocks"), "RequestBytes");
}

void TVolumeRequestCounters::Publish(TInstant now)
{
    ReadCount.Publish(now);
    ReadBytes.Publish(now);
    ReadVoidBytes.Publish(now);
    ReadNonVoidBytes.Publish(now);

    WriteCount.Publish(now);
    WriteBytes.Publish(now);

    ZeroCount.Publish(now);
    ZeroBytes.Publish(now);

    Reset();
}

void TVolumeRequestCounters::Reset()
{
    ReadCount.Reset();
    ReadBytes.Reset();
    ReadVoidBytes.Reset();
    ReadNonVoidBytes.Reset();

    WriteCount.Reset();
    WriteBytes.Reset();

    ZeroCount.Reset();
    ZeroBytes.Reset();
}

void TVolumeRequestCounters::UpdateCounters(const TPartitionDiskCounters& source)
{
    ReadCount.Increment(source.RequestCounters.ReadBlocks.GetCount());
    ReadBytes.Increment(source.RequestCounters.ReadBlocks.GetRequestBytes());
    ReadVoidBytes.Increment(
        source.RequestCounters.ReadBlocks.GetRequestVoidBytes());
    ReadNonVoidBytes.Increment(
        source.RequestCounters.ReadBlocks.GetRequestNonVoidBytes());

    WriteCount.Increment(source.RequestCounters.WriteBlocks.GetCount());
    WriteBytes.Increment(source.RequestCounters.WriteBlocks.GetRequestBytes());

    ZeroCount.Increment(source.RequestCounters.ZeroBlocks.GetCount());
    ZeroBytes.Increment(source.RequestCounters.ZeroBlocks.GetRequestBytes());
}

////////////////////////////////////////////////////////////////////////////////

TBlobLoadCounters::TBlobLoadCounters(
        const TString& mediaKind,
        ui64 maxGroupReadIops,
        ui64 maxGroupWriteIops,
        ui64 maxGroupReadThroughput,
        ui64 maxGroupWriteThroughput)
    : MediaKind(mediaKind)
    , MaxGroupReadIops(maxGroupReadIops)
    , MaxGroupWriteIops(maxGroupWriteIops)
    , MaxGroupReadThroughput(maxGroupReadThroughput)
    , MaxGroupWriteThroughput(maxGroupWriteThroughput)
{}

void TBlobLoadCounters::Register(NMonitoring::TDynamicCountersPtr counters)
{
    UsedGroupsCount.Init(counters, "UsedGroupsHundredthsCount");
}

void TBlobLoadCounters::Publish(
    const NBlobMetrics::TBlobLoadMetrics& metrics,
    TInstant now)
{
    NBlobMetrics::TBlobLoadMetrics::TTabletMetric metricsAggregate;

    for (const auto& [kind, ops]: metrics.PoolKind2TabletOps) {
        if (kind.Contains(MediaKind)) {
            for (const auto& [key, metrics]: ops) {
                metricsAggregate += metrics;
            }
        }
    }

    const double groupReadIops =
        metricsAggregate.ReadOperations.Iops;
    const double groupWriteIops =
        metricsAggregate.WriteOperations.Iops;
    const double groupReadByteCounts =
        metricsAggregate.ReadOperations.ByteCount;
    const double groupWriteByteCounts =
        metricsAggregate.WriteOperations.ByteCount;

    const ui64 groupsCount = 100 *
        ( groupReadIops / MaxGroupReadIops +
          groupWriteIops / MaxGroupWriteIops +
          groupReadByteCounts / MaxGroupReadThroughput +
          groupWriteByteCounts / MaxGroupWriteThroughput ) /
        UpdateCountersInterval.SecondsFloat();

    UsedGroupsCount.SetCounterValue(now, groupsCount);
}

////////////////////////////////////////////////////////////////////////////////

void TStatsServiceState::RemoveVolume(TInstant now, const TString& diskId)
{
    auto it = VolumesById.find(diskId);
    if (it == VolumesById.end()) {
        return;
    }

    RecentVolumes.push_back({
        diskId,
        it->second.VolumeInfo.GetIsSystem(),
        it->second.VolumeInfo.GetStorageMediaKind(),
        now,
    });
    auto last = std::prev(RecentVolumes.end());
    RecentVolumesById[last->DiskId] = last;

    VolumesById.erase(it);
}

TVolumeStatsInfo* TStatsServiceState::GetVolume(const TString& diskId)
{
    auto it = VolumesById.find(diskId);
    return it != VolumesById.end() ? &it->second : nullptr;
}

TVolumeStatsInfo* TStatsServiceState::GetOrAddVolume(
    const TString& diskId,
    NProto::TVolume config)
{
    TVolumesMap::insert_ctx ctx;
    auto it = VolumesById.find(diskId, ctx);
    if (it == VolumesById.end()) {
        it = VolumesById.emplace_direct(
            ctx,
            diskId,
            TVolumeStatsInfo(std::move(config), HistCounterOptions));

        auto rit = RecentVolumesById.find(diskId);
        if (rit != RecentVolumesById.end()) {
            auto listIt = rit->second;
            // we should delete from RecentVolumesById first because it holds
            // TStringBufs pointing to RecentVolumes entries
            RecentVolumesById.erase(rit);
            RecentVolumes.erase(listIt);
        }
    }

    return &it->second;
}

const TStatsServiceState::TRecentVolumesList&
TStatsServiceState::UpdateAndGetRecentVolumes(TInstant now)
{
    const TDuration ttl = TDuration::Hours(1);

    while (RecentVolumes.size()) {
        auto& rv = RecentVolumes.front();

        if (rv.RemoveTs + ttl >= now) {
            break;
        }

        RecentVolumesById.erase(rv.DiskId);
        RecentVolumes.pop_front();
    }

    return RecentVolumes;
}

}   // namespace NCloud::NBlockStore::NStorage
