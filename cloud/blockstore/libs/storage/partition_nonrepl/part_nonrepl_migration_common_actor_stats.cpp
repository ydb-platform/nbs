#include "part_nonrepl_migration_common_actor.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/volume/actors/disk_registry_based_partition_statistics_collector_actor.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationCommonActor::UpdateCounters(
    const TActorContext& ctx,
    const TActorId& sender,
    TPartNonreplCountersData partCountersData)
{
    if (sender == SrcActorId) {
        SrcCounters = std::move(partCountersData.DiskCounters);
    } else if (sender == DstActorId) {
        DstCounters = std::move(partCountersData.DiskCounters);
    } else {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::PARTITION,
            "Partition %s for disk %s counters not found",
            ToString(sender).c_str(),
            DiskId.Quote().c_str());

        Y_DEBUG_ABORT_UNLESS(0);
    }
    NetworkBytes += partCountersData.NetworkBytes;
    CpuUsage += partCountersData.CpuUsage;
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationCommonActor::HandlePartCounters(
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

TPartNonreplCountersData
TNonreplicatedPartitionMigrationCommonActor::ExtractPartCounters()
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

    TPartNonreplCountersData counters(NetworkBytes, CpuUsage, std::move(stats));

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

    auto&& [networkBytes, cpuUsage, diskCounters] = ExtractPartCounters();

    auto request =
        std::make_unique<TEvVolume::TEvDiskRegistryBasedPartitionCounters>(
            MakeIntrusive<TCallContext>(),
            std::move(diskCounters),
            DiskId,
            networkBytes,
            cpuUsage);

    NCloud::Send(ctx, StatActorId, std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationCommonActor::
    HandleGetDiskRegistryBasedPartCounters(
        const TEvNonreplPartitionPrivate::
            TEvGetDiskRegistryBasedPartCountersRequest::TPtr& ev,
        const TActorContext& ctx)
{
    if (StatisticRequestInfo) {
        NCloud::Reply(
            ctx,
            *StatisticRequestInfo,
            std::make_unique<TEvNonreplPartitionPrivate::
                                 TEvGetDiskRegistryBasedPartCountersResponse>(
                MakeError(E_REJECTED, "Migration actor got new request"),
                CreatePartitionDiskCounters(
                    EPublishingPolicy::DiskRegistryBased,
                    DiagnosticsConfig
                        ->GetHistogramCounterOptions()),   // diskCounters
                0,                                         // networkBytes
                TDuration{},                               // cpuUsage
                SelfId(),
                DiskId));
        StatisticRequestInfo.Reset();
    }

    TVector<TActorId> statActorIds;

    if (SrcActorId) {
        statActorIds.push_back(SrcActorId);
    }

    if (DstActorId) {
        statActorIds.push_back(DstActorId);
    }

    if (statActorIds.empty()) {
        auto&& [networkBytes, cpuUsage, diskCounters] = ExtractPartCounters();

        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvNonreplPartitionPrivate::
                                 TEvGetDiskRegistryBasedPartCountersResponse>(
                MakeError(
                    E_INVALID_STATE,
                    "Nonreplicated migration actor hasn't src and dst "
                    "actors"),
                std::move(diskCounters),
                networkBytes,
                cpuUsage,
                SelfId(),
                DiskId));

        return;
    }

    StatisticRequestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, ev->Get()->CallContext);

    NCloud::Register<TDiskRegistryBasedPartitionStatisticsCollectorActor>(
        ctx,
        SelfId(),
        std::move(statActorIds),
        ++StatisticSeqNo);
}

void TNonreplicatedPartitionMigrationCommonActor::
    HandleDiskRegistryBasedPartCountersCombined(
        const TEvNonreplPartitionPrivate::
            TEvDiskRegistryBasedPartCountersCombined::TPtr& ev,
        const TActorContext& ctx)
{
    if (!StatisticRequestInfo) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::PARTITION_NONREPL,
            "[%s] Failed to send migration actor statistics due to empty "
            "StatisticRequestInfo.",
            DiskId.Quote().c_str());
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

        UpdateCounters(ctx, counters.ActorId, std::move(partCountersData));
    }

    auto&& [networkBytes, cpuUsage, diskCounters] = ExtractPartCounters();

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
            DiskId));

    StatisticRequestInfo.Reset();
}

}   // namespace NCloud::NBlockStore::NStorage
