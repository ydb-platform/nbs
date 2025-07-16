#include "part_nonrepl_migration_common_actor.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/volume/actors/disk_registry_based_partition_statistics_collector_actor.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationCommonActor::UpdateCounters(
    const TActorContext& ctx,
    TUpdateCounters& args)
{
    if (args.Sender == SrcActorId) {
        SrcCounters = std::move(args.DiskCounters);
    } else if (args.Sender == DstActorId) {
        DstCounters = std::move(args.DiskCounters);
    } else {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::PARTITION,
            "Partition %s for disk %s counters not found",
            ToString(args.Sender).c_str(),
            DiskId.Quote().c_str());

        Y_DEBUG_ABORT_UNLESS(0);
    }
    NetworkBytes += args.NetworkBytes;
    CpuUsage += args.CpuUsage;
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationCommonActor::HandlePartCounters(
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

TDiskRegistryBasedPartCounters
TNonreplicatedPartitionMigrationCommonActor::GetStats()
{
    auto stats = CreatePartitionDiskCounters(
        EPublishingPolicy::DiskRegistryBased,
        DiagnosticsConfig->GetHistogramCounterOptions());

    if (SrcCounters) {
        stats->AggregateWith(*SrcCounters);
    }

    if (DstActorId && DstCounters) {
        stats->AggregateWith(*DstCounters);
    }

    if (SrcCounters && DstActorId && DstCounters) {
        // for some counters default AggregateWith logic is suboptimal for
        // mirrored partitions
        stats->Simple.BytesCount.Value =
            Max(SrcCounters->Simple.BytesCount.Value,
                DstCounters->Simple.BytesCount.Value);
        stats->Simple.IORequestsInFlight.Value =
            Max(SrcCounters->Simple.IORequestsInFlight.Value,
                DstCounters->Simple.IORequestsInFlight.Value);
    }

    stats->AggregateWith(*MigrationCounters);
    MigrationCounters = CreatePartitionDiskCounters(
        EPublishingPolicy::DiskRegistryBased,
        DiagnosticsConfig->GetHistogramCounterOptions());

    TDiskRegistryBasedPartCounters counters(NetworkBytes, CpuUsage, std::move(stats));

    NetworkBytes = 0;
    CpuUsage = {};

    return counters;
}

void TNonreplicatedPartitionMigrationCommonActor::SendStats(
    const TActorContext& ctx)
{
    if (!StatActorId) {
        return;
    }

    auto&& [networkBytes, cpuUsage, stats] = GetStats();

    auto request =
        std::make_unique<TEvVolume::TEvDiskRegistryBasedPartitionCounters>(
            MakeIntrusive<TCallContext>(),
            std::move(stats),
            DiskId,
            networkBytes,
            cpuUsage);

    NCloud::Send(ctx, StatActorId, std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationCommonActor::
    HandleGetDiskRegistryBasedPartCountersRequest(
        const TEvNonreplPartitionPrivate::
            TEvGetDiskRegistryBasedPartCountersRequest::TPtr& ev,
        const TActorContext& ctx)
{
    TVector<TActorId> statActorIds;

    if (SrcActorId) {
        statActorIds.push_back(SrcActorId);
    }

    if (DstActorId) {
        statActorIds.push_back(DstActorId);
    }

    if (statActorIds.empty()) {
        auto&& [networkBytes, cpuUsage, stats] = GetStats();

        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvNonreplPartitionPrivate::
                                 TEvGetDiskRegistryBasedPartCountersResponse>(
                MakeError(
                    E_INVALID_STATE,
                    "Nonreplicated migration actor hasn't src and dst "
                    "actors"),
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

    PoisonPillHelper.TakeOwnership(
        ctx,
        DiskRegistryBasedPartitionStatisticsCollectorActorId);
}

void TNonreplicatedPartitionMigrationCommonActor::
    HandleDiskRegistryBasedPartCountersCombined(
        const TEvNonreplPartitionPrivate::
            TEvDiskRegistryBasedPartCountersCombined::TPtr& ev,
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

    auto&& [networkBytes, cpuUsage, stats] = GetStats();

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

    PoisonPillHelper.ReleaseOwnership(
        ctx,
        DiskRegistryBasedPartitionStatisticsCollectorActorId);
}

}   // namespace NCloud::NBlockStore::NStorage
