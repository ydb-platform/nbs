#include "part_mirror_resync_actor.h"

#include <cloud/blockstore/libs/storage/api/volume.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionResyncActor::HandlePartCounters(
    const TEvVolume::TEvDiskRegistryBasedPartitionCounters::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (ev->Sender == MirrorActorId) {
        MirrorCounters = std::move(msg->DiskCounters);
    } else {
        LOG_INFO(ctx, TBlockStoreComponents::PARTITION,
            "Partition %s for disk %s counters not found",
            ToString(ev->Sender).c_str(),
            PartConfig->GetName().Quote().c_str());

        Y_VERIFY_DEBUG(0);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionResyncActor::SendStats(const TActorContext& ctx)
{
    auto stats = CreatePartitionDiskCounters(EPublishingPolicy::NonRepl);

    if (MirrorCounters) {
        stats->AggregateWith(*MirrorCounters);
    }

    auto request = std::make_unique<TEvVolume::TEvDiskRegistryBasedPartitionCounters>(
        MakeIntrusive<TCallContext>(),
        std::move(stats));

    NCloud::Send(ctx, StatActorId, std::move(request));
}

}   // namespace NCloud::NBlockStore::NStorage
