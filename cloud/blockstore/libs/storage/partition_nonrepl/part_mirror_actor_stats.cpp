#include "part_mirror_actor.h"

#include <cloud/blockstore/libs/storage/api/volume.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionActor::HandlePartCounters(
    const TEvVolume::TEvDiskRegistryBasedPartitionCounters::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    const ui32 replicaIndex = State.GetReplicaIndex(ev->Sender);
    if (replicaIndex < ReplicaCounters.size()) {
        ReplicaCounters[replicaIndex] = std::move(msg->DiskCounters);
        NetworkBytes += msg->NetworkBytes;
        CpuUsage += CpuUsage;
    } else {
        LOG_INFO(ctx, TBlockStoreComponents::PARTITION,
            "Partition %s for disk %s counters not found",
            ToString(ev->Sender).c_str(),
            State.GetReplicaInfos()[0].Config->GetName().Quote().c_str());

        Y_DEBUG_ABORT_UNLESS(0);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionActor::SendStats(const TActorContext& ctx)
{
    if (!StatActorId) {
        return;
    }

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
            stats->Simple.BytesCount.Value = Max(
                stats->Simple.BytesCount.Value,
                counters->Simple.BytesCount.Value);
            stats->Simple.IORequestsInFlight.Value = Max(
                stats->Simple.IORequestsInFlight.Value,
                counters->Simple.IORequestsInFlight.Value);
        }
    }

    stats->Simple.ChecksumMismatches.Value = ChecksumMismatches;
    stats->Simple.ScrubbingProgress.Value =
        100 * GetScrubbingRange().Start / State.GetBlockCount();
    stats->Cumulative.ScrubbingThroughput.Value = ScrubbingThroughput;
    auto request =
        std::make_unique<TEvVolume::TEvDiskRegistryBasedPartitionCounters>(
            MakeIntrusive<TCallContext>(),
            std::move(stats),
            DiskId,
            NetworkBytes,
            CpuUsage);

    NetworkBytes = 0;
    CpuUsage = {};
    ScrubbingThroughput = 0;

    NCloud::Send(
        ctx,
        StatActorId,
        std::move(request));
}

}   // namespace NCloud::NBlockStore::NStorage
