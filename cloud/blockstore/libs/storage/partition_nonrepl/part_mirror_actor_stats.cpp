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
    UpdateCounters(ctx, ev->Sender, std::move(msg->CountersData));
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
    stats->Cumulative.ScrubbingThroughput.Value =
        std::exchange(ScrubbingThroughput, {});

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

    return {
        .DiskCounters = std::move(stats),
        .NetworkBytes = std::exchange(NetworkBytes, {}),
        .CpuUsage = std::exchange(CpuUsage, {}),
    };
}

void TMirrorPartitionActor::SendStats(const TActorContext& ctx)
{
    if (!StatActorId) {
        return;
    }

    auto request =
        std::make_unique<TEvVolume::TEvDiskRegistryBasedPartitionCounters>(
            MakeIntrusive<TCallContext>(),
            DiskId,
            ExtractPartCounters(ctx));

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
    if (StatisticRequestInfo) {
        NCloud::Reply(
            ctx,
            *StatisticRequestInfo,
            std::make_unique<TEvNonreplPartitionPrivate::
                                 TEvGetDiskRegistryBasedPartCountersResponse>(
                MakeError(E_REJECTED, "Mirror actor got new request"),
                SelfId(),
                DiskId,
                TPartNonreplCountersData{
                    .DiskCounters = CreatePartitionDiskCounters(
                        EPublishingPolicy::DiskRegistryBased,
                        DiagnosticsConfig->GetHistogramCounterOptions()),
                }));
        StatisticRequestInfo.Reset();
    }

    auto statActorIds = State.GetReplicaActorsBypassingProxies();

    if (statActorIds.empty()) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvNonreplPartitionPrivate::
                                 TEvGetDiskRegistryBasedPartCountersResponse>(
                MakeError(E_INVALID_STATE, "Mirror actor hasn't replicas"),
                SelfId(),
                DiskId,
                ExtractPartCounters(ctx)));
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

void TMirrorPartitionActor::HandleDiskRegistryBasedPartCountersCombined(
    const TEvNonreplPartitionPrivate::TEvDiskRegistryBasedPartCountersCombined::
        TPtr& ev,
    const TActorContext& ctx)
{
    if (!StatisticRequestInfo) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::PARTITION_NONREPL,
            "[%s] Failed to send mirror actor statistics due to empty "
            "StatisticRequestInfo.",
            DiskId.Quote().c_str());
        return;
    }

    auto* msg = ev->Get();

    for (auto& counters: msg->Counters) {
        UpdateCounters(ctx, counters.ActorId, std::move(counters.CountersData));
    }

    NCloud::Reply(
        ctx,
        *StatisticRequestInfo,
        std::make_unique<TEvNonreplPartitionPrivate::
                             TEvGetDiskRegistryBasedPartCountersResponse>(
            msg->Error,
            SelfId(),
            DiskId,
            ExtractPartCounters(ctx)));

    StatisticRequestInfo.Reset();
}

}   // namespace NCloud::NBlockStore::NStorage
