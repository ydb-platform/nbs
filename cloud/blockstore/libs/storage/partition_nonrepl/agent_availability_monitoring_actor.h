#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/core/config.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>

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
    const google::protobuf::RepeatedPtrField<NProto::TDeviceMigration>
        Migrations;
    const NActors::TActorId NonreplPartitionActorId;
    const NActors::TActorId ParentActorId;
    const NProto::TLaggingAgent LaggingAgent;

    ui32 LaggingAgentNodeId = 0;

public:
    TAgentAvailabilityMonitoringActor(
        TStorageConfigPtr config,
        TNonreplicatedPartitionConfigPtr partConfig,
        google::protobuf::RepeatedPtrField<NProto::TDeviceMigration> migrations,
        NActors::TActorId nonreplPartitionActorId,
        NActors::TActorId parentActorId,
        NProto::TLaggingAgent laggingAgent);

    ~TAgentAvailabilityMonitoringActor() override;

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void ScheduleCheckAvailabilityRequest(const NActors::TActorContext& ctx);
    void CheckAgentAvailability(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleChecksumBlocksUndelivery(
        const TEvDiskAgent::TEvChecksumDeviceBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleChecksumBlocksResponse(
        const TEvDiskAgent::TEvChecksumDeviceBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
