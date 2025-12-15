#include "part_mirror_resync_actor.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/volume/actors/disk_registry_based_partition_statistics_collector_actor.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionResyncActor::UpdateCounters(
    const TActorContext& ctx,
    const TActorId& sender,
    TPartNonreplCountersData partCountersData)
{
    bool knownSender = sender == MirrorActorId;
    for (const auto& replica: Replicas) {
        knownSender |= replica.ActorId == sender;
    }

    if (!knownSender) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::PARTITION,
            "Partition %s for disk %s counters not found",
            ToString(sender).c_str(),
            PartConfig->GetName().Quote().c_str());

        Y_DEBUG_ABORT_UNLESS(0);
        return;
    }

    if (!MirrorCounters) {
        MirrorCounters = std::move(partCountersData.DiskCounters);
    } else {
        MirrorCounters->AggregateWith(*partCountersData.DiskCounters);
    }

    NetworkBytes += partCountersData.NetworkBytes;
    CpuUsage += partCountersData.CpuUsage;
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionResyncActor::HandlePartCounters(
    const TEvVolume::TEvDiskRegistryBasedPartitionCounters::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    UpdateCounters(ctx, ev->Sender, std::move(msg->CountersData));
}

void TMirrorPartitionResyncActor::HandleScrubberCounters(
    const TEvVolume::TEvScrubberCounters::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    ForwardMessageToActor(ev, ctx, StatActorId);
}

void TMirrorPartitionResyncActor::HandleGetDiskRegistryBasedPartCounters(
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
                MakeError(E_REJECTED, "Mirror resync actor got new request"),
                SelfId(),
                PartConfig->GetName(),
                TPartNonreplCountersData{
                    .DiskCounters = CreatePartitionDiskCounters(
                        EPublishingPolicy::DiskRegistryBased,
                        DiagnosticsConfig->GetHistogramCounterOptions())}));
        StatisticRequestInfo.Reset();
    }

    if (!MirrorActorId && Replicas.empty()) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvNonreplPartitionPrivate::
                                 TEvGetDiskRegistryBasedPartCountersResponse>(
                MakeError(
                    E_INVALID_STATE,
                    "Part mirror resync actor hasn't replicas and mirror "
                    "actor"),
                SelfId(),
                PartConfig->GetName(),
                ExtractPartCounters()));
        return;
    }

    TVector<TActorId> statActorIds;

    if (MirrorActorId) {
        statActorIds.push_back(MirrorActorId);
    }

    for (const auto& replica: Replicas) {
        statActorIds.push_back(replica.ActorId);
    }

    StatisticRequestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, ev->Get()->CallContext);

    NCloud::Register<TDiskRegistryBasedPartitionStatisticsCollectorActor>(
        ctx,
        SelfId(),
        std::move(statActorIds),
        ++StatisticSeqNo);
}

void TMirrorPartitionResyncActor::HandleDiskRegistryBasedPartCountersCombined(
    const TEvNonreplPartitionPrivate::TEvDiskRegistryBasedPartCountersCombined::
        TPtr& ev,
    const TActorContext& ctx)
{
    if (!StatisticRequestInfo) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::PARTITION_NONREPL,
            "[%s] Failed to send mirror resync actor statistics due to empty "
            "StatisticRequestInfo.",
            PartConfig->GetName().Quote().c_str());
        return;
    }

    auto* msg = ev->Get();

    if (msg->SeqNo < StatisticSeqNo) {
        return;
    }

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
            PartConfig->GetName(),
            ExtractPartCounters()));

    StatisticRequestInfo.Reset();;
}

////////////////////////////////////////////////////////////////////////////////

TPartNonreplCountersData TMirrorPartitionResyncActor::ExtractPartCounters()
{
    auto stats = CreatePartitionDiskCounters(
        EPublishingPolicy::DiskRegistryBased,
        DiagnosticsConfig->GetHistogramCounterOptions());

    if (MirrorCounters) {
        stats->AggregateWith(*MirrorCounters);
        MirrorCounters.reset();
    }

    return {
        .DiskCounters = std::exchange(stats, {}),
        .NetworkBytes = std::exchange(NetworkBytes, {}),
        .CpuUsage = std::exchange(CpuUsage, {}),
    };
}

void TMirrorPartitionResyncActor::SendStats(const TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvVolume::TEvDiskRegistryBasedPartitionCounters>(
            MakeIntrusive<TCallContext>(),
            PartConfig->GetName(),
            ExtractPartCounters());

    NCloud::Send(ctx, StatActorId, std::move(request));
}

}   // namespace NCloud::NBlockStore::NStorage
