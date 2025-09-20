#include "part_mirror_actor.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/volume/actors/disk_registry_based_partition_statistics_collector_actor.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionActor::UpdateCounters(
    const TActorContext& ctx,
    const TActorId& sender,
    TPartNonreplCountersData partCountersData)
{
    const ui32 replicaIndex = State.GetReplicaIndex(sender);
    if (replicaIndex < ReplicaCounters.size()) {
        ReplicaCounters[replicaIndex] =
            std::move(partCountersData.DiskCounters);
        NetworkBytes += partCountersData.NetworkBytes;
        CpuUsage += partCountersData.CpuUsage;
    } else {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::PARTITION,
            "Partition %s for disk %s counters not found",
            ToString(sender).c_str(),
            State.GetReplicaInfos()[0].Config->GetName().Quote().c_str());

        Y_DEBUG_ABORT_UNLESS(0);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionActor::HandlePartCounters(
    const TEvVolume::TEvDiskRegistryBasedPartitionCounters::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    TPartNonreplCountersData partCountersData(
        msg->NetworkBytes,
        msg->CpuUsage,
        std::move(msg->DiskCounters));

    UpdateCounters(ctx, ev->Sender, std::move(partCountersData));
}

////////////////////////////////////////////////////////////////////////////////

TPartNonreplCountersData TMirrorPartitionActor::ExtractPartCounters(
    const TActorContext& ctx)
{
    auto stats = CreatePartitionDiskCounters(
        EPublishingPolicy::DiskRegistryBased,
        DiagnosticsConfig->GetHistogramCounterOptions());

    for (const auto& counters: ReplicaCounters) {
        if (counters) {
            stats->AggregateWith(*counters);
        }
    }

    // for some counters default AggregateWith logic is suboptimal for mirrored
    // partitions
    stats->Simple.BytesCount.Reset();
    stats->Simple.IORequestsInFlight.Reset();
    for (const auto& counters: ReplicaCounters) {
        if (counters) {
            stats->Simple.BytesCount.Value =
                Max(stats->Simple.BytesCount.Value,
                    counters->Simple.BytesCount.Value);
            stats->Simple.IORequestsInFlight.Value =
                Max(stats->Simple.IORequestsInFlight.Value,
                    counters->Simple.IORequestsInFlight.Value);
        }
    }

    stats->Simple.ChecksumMismatches.Value = ChecksumMismatches;
    stats->Simple.ScrubbingProgress.Value =
        100 * GetScrubbingRange().Start / State.GetBlockCount();
    stats->Cumulative.ScrubbingThroughput.Value = ScrubbingThroughput;

    TPartNonreplCountersData partCounters(
        NetworkBytes,
        CpuUsage,
        std::move(stats));

    NetworkBytes = 0;
    CpuUsage = {};
    ScrubbingThroughput = 0;

    const bool scrubbingEnabled =
        Config->GetDataScrubbingEnabled() && !ResyncActorId;
    auto scrubberCounters = std::make_unique<TEvVolume::TEvScrubberCounters>(
        MakeIntrusive<TCallContext>(),
        scrubbingEnabled,
        GetScrubbingRange(),
        std::move(Minors),
        std::move(Majors),
        std::move(Fixed),
        std::move(FixedPartial));
    NCloud::Send(ctx, StatActorId, std::move(scrubberCounters));

    return partCounters;
}

void TMirrorPartitionActor::SendStats(const TActorContext& ctx)
{
    if (!StatActorId) {
        return;
    }

    auto&& [networkBytes, cpuUsage, diskCounters] = ExtractPartCounters(ctx);

    auto request =
        std::make_unique<TEvVolume::TEvDiskRegistryBasedPartitionCounters>(
            MakeIntrusive<TCallContext>(),
            std::move(diskCounters),
            DiskId,
            networkBytes,
            cpuUsage);

    NCloud::Send(
        ctx,
        StatActorId,
        std::move(request));

}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionActor::HandleGetDiskRegistryBasedPartCounters(
    const TEvNonreplPartitionPrivate::
        TEvGetDiskRegistryBasedPartCountersRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    ++StatisticSeqNo;

    if (StatisticRequestInfo) {
        NCloud::Reply(
            ctx,
            *StatisticRequestInfo,
            std::make_unique<TEvNonreplPartitionPrivate::
                                 TEvGetDiskRegistryBasedPartCountersResponse>(
                MakeError(E_REJECTED, "Mirror actor gets new request"),
                CreatePartitionDiskCounters(
                    EPublishingPolicy::DiskRegistryBased,
                    DiagnosticsConfig
                        ->GetHistogramCounterOptions()),   // diskCounters
                0,                                         // networkBytes
                TDuration{},                               // cpuUsage
                SelfId(),
                DiskId,
                msg->VolumeStatisticSeqNo));
    }

    auto statActorIds = State.GetReplicaActorsBypassingProxies();

    if (statActorIds.empty()) {
        auto&& [networkBytes, cpuUsage, diskCounters] =
            ExtractPartCounters(ctx);

        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvNonreplPartitionPrivate::
                                 TEvGetDiskRegistryBasedPartCountersResponse>(
                MakeError(E_INVALID_STATE, "Mirror actor hasn't replicas"),
                std::move(diskCounters),
                networkBytes,
                cpuUsage,
                SelfId(),
                DiskId,
                msg->VolumeStatisticSeqNo));
        return;
    }

    StatisticRequestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    NCloud::Register<TDiskRegistryBasedPartitionStatisticsCollectorActor>(
        ctx,
        SelfId(),
        std::move(statActorIds),
        StatisticSeqNo,
        msg->VolumeStatisticSeqNo);
}

void TMirrorPartitionActor::HandleDiskRegistryBasedPartCountersCombined(
    const TEvNonreplPartitionPrivate::TEvDiskRegistryBasedPartCountersCombined::
        TPtr& ev,
    const TActorContext& ctx)
{
    if (!StatisticRequestInfo) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::PARTITION_NONREPL,
            "Failed to send mirror actor statistics due to empty "
            "StatisticRequestInfo");
        return;
    }

    auto* msg = ev->Get();

    if (msg->SeqNo < StatisticSeqNo) {
        return;
    }

    for (auto& counters: msg->Counters) {
        TPartNonreplCountersData partCountersData(
            counters.NetworkBytes,
            counters.CpuUsage,
            std::move(counters.DiskCounters));

        UpdateCounters(ctx, counters.SelfId, std::move(partCountersData));
    }

    auto&& [networkBytes, cpuUsage, diskCounters] = ExtractPartCounters(ctx);

    NCloud::Reply(
        ctx,
        *StatisticRequestInfo,
        std::make_unique<TEvNonreplPartitionPrivate::
                             TEvGetDiskRegistryBasedPartCountersResponse>(
            msg->Error,
            std::move(diskCounters),
            networkBytes,
            cpuUsage,
            SelfId(),
            DiskId,
            msg->VolumeStatisticSeqNo));

    StatisticRequestInfo = nullptr;
}

}   // namespace NCloud::NBlockStore::NStorage
