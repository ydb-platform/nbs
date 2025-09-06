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

TPartNonreplCountersData TNonreplicatedPartitionActor::ExtractPartCounters(
    const TActorContext& ctx)
{
    PartCounters->Simple.IORequestsInFlight.Set(
        RequestsInProgress.GetRequestCount());
    PartCounters->Simple.BytesCount.Set(
        PartConfig->GetBlockCount() * PartConfig->GetBlockSize());

    PartCounters->Simple.HasBrokenDevice.Set(
        CalculateHasBrokenDeviceCounterValue(ctx, false));
    PartCounters->Simple.HasBrokenDeviceSilent.Set(
        CalculateHasBrokenDeviceCounterValue(ctx, true));

    TPartNonreplCountersData counters(
        NetworkBytes,
        CpuUsage,
        std::move(PartCounters));

    NetworkBytes = 0;
    CpuUsage = {};

    PartCounters = CreatePartitionDiskCounters(
        EPublishingPolicy::DiskRegistryBased,
        DiagnosticsConfig->GetHistogramCounterOptions());

    return counters;
}

void TNonreplicatedPartitionActor::SendStats(const TActorContext& ctx)
{
    if (!StatActorId) {
        return;
    }

    auto&& [networkBytes, cpuUsage, diskCounters] = ExtractPartCounters(ctx);

    auto request =
        std::make_unique<TEvVolume::TEvDiskRegistryBasedPartitionCounters>(
            MakeIntrusive<TCallContext>(),
            std::move(diskCounters),
            PartConfig->GetName(),
            networkBytes,
            cpuUsage);

    NCloud::Send(ctx, StatActorId, std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionActor::
    HandleGetDiskRegistryBasedPartCounters(
        const TEvNonreplPartitionPrivate::
            TEvGetDiskRegistryBasedPartCountersRequest::TPtr& ev,
        const TActorContext& ctx)
{
    auto&& [networkBytes, cpuUsage, diskCounters] = ExtractPartCounters(ctx);

    NCloud::Reply(
        ctx,
        *ev,
        std::make_unique<TEvNonreplPartitionPrivate::
                             TEvGetDiskRegistryBasedPartCountersResponse>(
            MakeError(S_OK),
            std::move(diskCounters),
            networkBytes,
            cpuUsage,
            SelfId(),
            PartConfig->GetName()));
}

}   // namespace NCloud::NBlockStore::NStorage
