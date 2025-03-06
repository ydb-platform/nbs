#include "part_nonrepl_migration_common_actor.h"

#include <cloud/blockstore/libs/storage/api/volume.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationCommonActor::HandlePartCounters(
    const TEvVolume::TEvDiskRegistryBasedPartitionCounters::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (ev->Sender == SrcActorId) {
        SrcCounters = std::move(msg->DiskCounters);
    } else if (ev->Sender == DstActorId) {
        DstCounters = std::move(msg->DiskCounters);
    } else {
        LOG_INFO(ctx, TBlockStoreComponents::PARTITION,
            "Partition %s for disk %s counters not found",
            ToString(ev->Sender).c_str(),
            DiskId.Quote().c_str());

        Y_DEBUG_ABORT_UNLESS(0);
    }
    NetworkBytes += msg->NetworkBytes;
    CpuUsage += msg->CpuUsage;
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationCommonActor::SendStats(
    const TActorContext& ctx)
{
    if (!StatActorId) {
        return;
    }

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
        stats->Simple.BytesCount.Value = Max(
            SrcCounters->Simple.BytesCount.Value,
            DstCounters->Simple.BytesCount.Value);
        stats->Simple.IORequestsInFlight.Value = Max(
            SrcCounters->Simple.IORequestsInFlight.Value,
            DstCounters->Simple.IORequestsInFlight.Value);
    }

    stats->AggregateWith(*MigrationCounters);
    MigrationCounters = CreatePartitionDiskCounters(
        EPublishingPolicy::DiskRegistryBased,
        DiagnosticsConfig->GetHistogramCounterOptions());

    auto request =
        std::make_unique<TEvVolume::TEvDiskRegistryBasedPartitionCounters>(
            MakeIntrusive<TCallContext>(),
            std::move(stats),
            DiskId,
            NetworkBytes,
            CpuUsage);

    NetworkBytes = 0;
    CpuUsage = {};

    NCloud::Send(ctx, StatActorId, std::move(request));
}

}   // namespace NCloud::NBlockStore::NStorage
