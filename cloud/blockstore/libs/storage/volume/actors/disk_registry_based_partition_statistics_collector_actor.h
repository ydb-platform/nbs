#pragma once

#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_events_private.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

class TDiskRegistryBasedPartitionStatisticsCollectorActor final
    : public NActors::TActorBootstrapped<
          TDiskRegistryBasedPartitionStatisticsCollectorActor>
{
private:
    const NActors::TActorId Owner;

    const TVector<NActors::TActorId> StatActorIds;

    TEvNonreplPartitionPrivate::TDiskRegistryBasedPartCountersCombined Response;

    NProto::TError LastError;

public:
    TDiskRegistryBasedPartitionStatisticsCollectorActor(
        const NActors::TActorId& owner,
        TVector<NActors::TActorId> statActorIds);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void SendStatistics(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleTimeout(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleGetDiskRegistryBasedPartCountersResponse(
        TEvNonreplPartitionPrivate::
            TEvGetDiskRegistryBasedPartCountersResponse::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
