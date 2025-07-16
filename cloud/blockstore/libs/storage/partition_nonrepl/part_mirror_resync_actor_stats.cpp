#include "part_mirror_resync_actor.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/volume/actors/disk_registry_based_partition_statistics_collector_actor.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionResyncActor::UpdateCounters(
    const TActorContext& ctx,
    TUpdateCounters& args)
{
    bool knownSender = args.Sender == MirrorActorId;
    for (const auto& replica: Replicas) {
        knownSender |= replica.ActorId == args.Sender;
    }

    if (!knownSender) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::PARTITION,
            "Partition %s for disk %s counters not found",
            ToString(args.Sender).c_str(),
            PartConfig->GetName().Quote().c_str());

        Y_DEBUG_ABORT_UNLESS(0);
        return;
    }

    if (!MirrorCounters) {
        MirrorCounters = std::move(args.DiskCounters);
    } else {
        MirrorCounters->AggregateWith(*args.DiskCounters);
    }

    NetworkBytes += args.NetworkBytes;
    CpuUsage += args.CpuUsage;
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionResyncActor::HandlePartCounters(
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

void TMirrorPartitionResyncActor::HandleScrubberCounters(
    const TEvVolume::TEvScrubberCounters::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    ForwardMessageToActor(ev, ctx, StatActorId);
}

void TMirrorPartitionResyncActor::HandleGetDiskRegistryBasedPartCountersRequest(
    const TEvNonreplPartitionPrivate::
        TEvGetDiskRegistryBasedPartCountersRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (!MirrorActorId && Replicas.empty()) {
        auto&& [networkBytes, cpuUsage, stats] = GetStats();

        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvNonreplPartitionPrivate::
                                 TEvGetDiskRegistryBasedPartCountersResponse>(
                MakeError(
                    E_INVALID_STATE,
                    "Part mirror resync actor hasn't replicas and mirror "
                    "actor"),
                std::move(stats),
                networkBytes,
                cpuUsage,
                SelfId(),
                PartConfig->GetName()));
        return;
    }

    TVector<TActorId> statActorIds;

    if (MirrorActorId) {
        statActorIds.emplace_back(MirrorActorId);
    }

    for (const auto& replica: Replicas) {
        statActorIds.emplace_back(replica.ActorId);
    }

    DiskRegistryBasedPartitionStatisticsCollectorActorId =
        NCloud::Register<TDiskRegistryBasedPartitionStatisticsCollectorActor>(
            ctx,
            SelfId(),
            std::move(statActorIds));
}

void TMirrorPartitionResyncActor::HandleDiskRegistryBasedPartCountersCombined(
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

    auto&& [networkBytes, cpuUsage, stats] = GetStats();

    NCloud::Send(
        ctx,
        StatActorId,
        std::make_unique<TEvNonreplPartitionPrivate::
                             TEvGetDiskRegistryBasedPartCountersResponse>(
            record->Error,
            std::move(stats),
            networkBytes,
            cpuUsage,
            SelfId(),
            PartConfig->GetName()));
}

////////////////////////////////////////////////////////////////////////////////

TDiskRegistryBasedPartCounters TMirrorPartitionResyncActor::GetStats()
{
    auto stats = CreatePartitionDiskCounters(
        EPublishingPolicy::DiskRegistryBased,
        DiagnosticsConfig->GetHistogramCounterOptions());

    if (MirrorCounters) {
        stats->AggregateWith(*MirrorCounters);
        MirrorCounters.reset();
    }

    TDiskRegistryBasedPartCounters counters(NetworkBytes, CpuUsage, std::move(stats));

    NetworkBytes = 0;
    CpuUsage = TDuration();

    return counters;
}

void TMirrorPartitionResyncActor::SendStats(const TActorContext& ctx)
{
    auto&& [networkBytes, cpuUsage, stats] = GetStats();

    auto request =
        std::make_unique<TEvVolume::TEvDiskRegistryBasedPartitionCounters>(
            MakeIntrusive<TCallContext>(),
            std::move(stats),
            PartConfig->GetName(),
            networkBytes,
            cpuUsage);

    NCloud::Send(ctx, StatActorId, std::move(request));
}

}   // namespace NCloud::NBlockStore::NStorage
