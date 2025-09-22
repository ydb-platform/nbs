#pragma once

#include <cloud/blockstore/libs/storage/api/stats_service.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

class TServiceStatisticsCollectorActor final
    : public NActors::TActorBootstrapped<TServiceStatisticsCollectorActor>
{
private:
    const NActors::TActorId Owner;

    const TVector<NActors::TActorId> VolumeActorIds;

    TEvStatsService::TServiceStatisticsCombined Response;

    NProto::TError LastError;

public:
    TServiceStatisticsCollectorActor(
        const NActors::TActorId& owner,
        TVector<NActors::TActorId> volumeActorIds);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void SendStatistics(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleTimeout(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleGetServiceStatisticsResponse(
        TEvStatsService::TEvGetServiceStatisticsResponse::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
