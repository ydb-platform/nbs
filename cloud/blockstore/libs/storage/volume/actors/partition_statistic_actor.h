#pragma once

#include <cloud/blockstore/libs/storage/api/stats_service.h>
#include <cloud/blockstore/libs/storage/volume/partition_info.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>

#include <util/generic/hash.h>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

class TPartitionStatisticActor final
    : public NActors::TActorBootstrapped<TPartitionStatisticActor>
{
private:
    const NActors::TActorId Owner;

    const TPartitionInfoList& Partitions;

    THashMap<NActors::TActorId, bool> IsUpdatedPartitionCounters;

    TVector<TEvStatsService::TUpdatePartCountersResponse> PartCounters;

public:
    TPartitionStatisticActor(
        const NActors::TActorId& owner,
        const TPartitionInfoList& partitions);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void SendStatToVolume(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleTimeout(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdatePartCountersResponse(
        TEvStatsService::TEvUpdatePartCountersResponse::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
