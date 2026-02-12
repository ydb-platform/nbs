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

    auto partCounters = CreatePartitionDiskCounters(
        EPublishingPolicy::DiskRegistryBased,
        DiagnosticsConfig->GetHistogramCounterOptions());

    return {
        .DiskCounters = std::exchange(PartCounters, std::move(partCounters)),
        .NetworkBytes = std::exchange(NetworkBytes, {}),
        .CpuUsage = std::exchange(CpuUsage, {}),
    };
}

void TNonreplicatedPartitionActor::SendStats(const TActorContext& ctx)
{
    if (!StatActorId) {
        return;
    }

    auto request =
        std::make_unique<TEvVolume::TEvDiskRegistryBasedPartitionCounters>(
            MakeIntrusive<TCallContext>(),
            PartConfig->GetName(),
            ExtractPartCounters(ctx));

    NCloud::Send(ctx, StatActorId, std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionActor::
    HandleGetDiskRegistryBasedPartCounters(
        const TEvNonreplPartitionPrivate::
            TEvGetDiskRegistryBasedPartCountersRequest::TPtr& ev,
        const TActorContext& ctx)
{
    NCloud::Reply(
        ctx,
        *ev,
        std::make_unique<TEvNonreplPartitionPrivate::
                             TEvGetDiskRegistryBasedPartCountersResponse>(
            SelfId(),
            PartConfig->GetName(),
            ExtractPartCounters(ctx)));
}

void TNonreplicatedPartitionActor::
    RejectGetDiskRegistryBasedPartCounters(
        const TEvNonreplPartitionPrivate::
            TEvGetDiskRegistryBasedPartCountersRequest::TPtr& ev,
        const TActorContext& ctx)
{
    NCloud::Reply(
        ctx,
        *ev,
        std::make_unique<TEvNonreplPartitionPrivate::
                             TEvGetDiskRegistryBasedPartCountersResponse>(
            MakeError(E_REJECTED),
            SelfId(),
            PartConfig->GetName(),
            ExtractPartCounters(ctx)));
}

}   // namespace NCloud::NBlockStore::NStorage
