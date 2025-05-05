#include "part_mirror_resync_actor.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionResyncActor::HandlePartCounters(
    const TEvVolume::TEvDiskRegistryBasedPartitionCounters::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    bool knownSender = ev->Sender == MirrorActorId;
    for (const auto& replica: Replicas) {
        knownSender |= replica.ActorId == ev->Sender;
    }

    if (!knownSender) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::PARTITION,
            "Partition %s for disk %s counters not found",
            ToString(ev->Sender).c_str(),
            PartConfig->GetName().Quote().c_str());

        Y_DEBUG_ABORT_UNLESS(0);
        return;
    }

    if (!MirrorCounters) {
        MirrorCounters = std::move(msg->DiskCounters);
    } else {
        MirrorCounters->AggregateWith(*msg->DiskCounters);
    }

    NetworkBytes += msg->NetworkBytes;
    CpuUsage += msg->CpuUsage;
}

void TMirrorPartitionResyncActor::HandleScrubberCounters(
    const TEvVolume::TEvScrubberCounters::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    ForwardMessageToActor(ev, ctx, StatActorId);
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionResyncActor::SendStats(const TActorContext& ctx)
{
    auto stats = CreatePartitionDiskCounters(
        EPublishingPolicy::DiskRegistryBased,
        DiagnosticsConfig->GetHistogramCounterOptions());

    if (MirrorCounters) {
        stats->AggregateWith(*MirrorCounters);
        MirrorCounters.reset();
    }

    auto request =
        std::make_unique<TEvVolume::TEvDiskRegistryBasedPartitionCounters>(
            MakeIntrusive<TCallContext>(),
            std::move(stats),
            PartConfig->GetName(),
            NetworkBytes,
            CpuUsage);

    NCloud::Send(ctx, StatActorId, std::move(request));

    NetworkBytes = 0;
    CpuUsage = TDuration();
}

}   // namespace NCloud::NBlockStore::NStorage
