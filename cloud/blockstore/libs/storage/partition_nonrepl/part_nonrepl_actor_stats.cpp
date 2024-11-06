#include "part_nonrepl_actor.h"

#include <cloud/blockstore/libs/storage/api/stats_service.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionActor::UpdateStats(
    const NProto::TPartitionStats& update)
{
    auto blockSize = PartConfig->GetBlockSize();
    PartCounters->Cumulative.BytesWritten.Increment(
        update.GetUserWriteCounters().GetBlocksCount() * blockSize);

    PartCounters->Cumulative.BytesRead.Increment(
        update.GetUserReadCounters().GetBlocksCount() * blockSize);
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionActor::SendStats(const TActorContext& ctx)
{
    if (!StatActorId) {
        return;
    }

    PartCounters->Simple.IORequestsInFlight.Set(
        RequestsInProgress.GetRequestCount()
    );
    PartCounters->Simple.BytesCount.Set(
        PartConfig->GetBlockCount() * PartConfig->GetBlockSize()
    );

    PartCounters->Simple.HasBrokenDevice.Set(
        CalculateHasBrokenDeviceCounterValue(ctx, false));
    PartCounters->Simple.HasBrokenDeviceSilent.Set(
        CalculateHasBrokenDeviceCounterValue(ctx, true));

    auto request =
        std::make_unique<TEvVolume::TEvDiskRegistryBasedPartitionCounters>(
            MakeIntrusive<TCallContext>(),
            std::move(PartCounters),
            PartConfig->GetName(),
            NetworkBytes,
            CpuUsage);

    NetworkBytes = 0;
    CpuUsage = {};

    PartCounters =
        CreatePartitionDiskCounters(EPublishingPolicy::DiskRegistryBased);

    NCloud::Send(ctx, StatActorId, std::move(request));
}

}   // namespace NCloud::NBlockStore::NStorage
