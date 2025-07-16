#include "part_mirror_actor.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/volume/actors/disk_registry_based_partition_statistics_collector_actor.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionActor::UpdateCounters(
    const TActorContext& ctx,
    TUpdateCounters& args)
{
    const ui32 replicaIndex = State.GetReplicaIndex(args.Sender);
    if (replicaIndex < ReplicaCounters.size()) {
        ReplicaCounters[replicaIndex] = std::move(args.DiskCounters);
        NetworkBytes += args.NetworkBytes;
        CpuUsage += args.CpuUsage;
    } else {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::PARTITION,
            "Partition %s for disk %s counters not found",
            ToString(args.Sender).c_str(),
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

    TUpdateCounters args(
        ev->Sender,
        msg->NetworkBytes,
        msg->CpuUsage,
        std::move(msg->DiskCounters));

    UpdateCounters(ctx, args);
}

////////////////////////////////////////////////////////////////////////////////

TDiskRegistryBasedPartCounters TMirrorPartitionActor::GetStats(
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

    TDiskRegistryBasedPartCounters partCounters(
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

    auto&& [networkBytes, cpuUsage, stats] = GetStats(ctx);

    auto request =
        std::make_unique<TEvVolume::TEvDiskRegistryBasedPartitionCounters>(
            MakeIntrusive<TCallContext>(),
            std::move(stats),
            DiskId,
            networkBytes,
            cpuUsage);

    NCloud::Send(
        ctx,
        StatActorId,
        std::move(request));

}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionActor::HandleGetDiskRegistryBasedPartCountersRequest(
    const TEvNonreplPartitionPrivate::
        TEvGetDiskRegistryBasedPartCountersRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto statActorIds = State.GetAllActors();

    if (statActorIds.empty()) {
        auto&& [networkBytes, cpuUsage, stats] = GetStats(ctx);

        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvNonreplPartitionPrivate::
                                 TEvGetDiskRegistryBasedPartCountersResponse>(
                MakeError(E_INVALID_STATE, "Mirror actor hasn't replicas"),
                std::move(stats),
                networkBytes,
                cpuUsage,
                SelfId(),
                DiskId));
        return;
    }

    StatActorIdInPullScheme = ev->Sender;

    DiskRegistryBasedPartitionStatisticsCollectorActorId =
        NCloud::Register<TDiskRegistryBasedPartitionStatisticsCollectorActor>(
            ctx,
            SelfId(),
            std::move(statActorIds));
}

void TMirrorPartitionActor::HandleDiskRegistryBasedPartCountersCombined(
    const TEvNonreplPartitionPrivate::TEvDiskRegistryBasedPartCountersCombined::
        TPtr& ev,
    const TActorContext& ctx)
{
    auto* record = ev->Get();

    for (auto& counters: record->Counters) {
        TUpdateCounters args(
            counters.SelfId,
            counters.NetworkBytes,
            counters.CpuUsage,
            std::move(counters.DiskCounters));

        UpdateCounters(ctx, args);
    }

    auto&& [networkBytes, cpuUsage, stats] = GetStats(ctx);

    NCloud::Send(
        ctx,
        StatActorIdInPullScheme,
        std::make_unique<TEvNonreplPartitionPrivate::
                             TEvGetDiskRegistryBasedPartCountersResponse>(
            record->Error,
            std::move(stats),
            networkBytes,
            cpuUsage,
            SelfId(),
            DiskId));
}

}   // namespace NCloud::NBlockStore::NStorage
