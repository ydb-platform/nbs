#pragma once

#include "public.h"

#include "part_nonrepl_events_private.h"

#include <cloud/blockstore/libs/storage/core/config.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// This actor is used to monitor the availability of a lagging agent by
// periodically sending ReadBlocks requests to it.
class TAgentAvailabilityMonitoringActor final
    : public NActors::TActorBootstrapped<TAgentAvailabilityMonitoringActor>
{
private:
    const TStorageConfigPtr Config;
    const TNonreplicatedPartitionConfigPtr PartConfig;
    const NActors::TActorId NonreplPartitionActorId;
    const NActors::TActorId ParentActorId;
    const TString AgentId;

    ui64 ReadBlockIndex = 0;

public:
    TAgentAvailabilityMonitoringActor(
        TStorageConfigPtr config,
        TNonreplicatedPartitionConfigPtr partConfig,
        NActors::TActorId nonreplPartitionActorId,
        NActors::TActorId parentActorId,
        TString agentId);

    ~TAgentAvailabilityMonitoringActor() override;

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void ScheduleCheckAvailabilityRequest(const NActors::TActorContext& ctx);
    void CheckAgentAvailability(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleChecksumBlocksUndelivery(
        const TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleChecksumBlocksResponse(
        const TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
