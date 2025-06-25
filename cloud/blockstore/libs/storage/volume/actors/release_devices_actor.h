#pragma once

#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/public.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/interconnect/types.h>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

class TReleaseDevicesActor final
    : public NActors::TActorBootstrapped<TReleaseDevicesActor>
{
    using TAgentId = TString;

    using TNodeId = ui32;

private:
    const TActorId Owner;
    const TString DiskId;
    const TString ClientId;
    const ui32 VolumeGeneration;
    const TDuration RequestTimeout;
    const bool MuteIOErrors;

    TVector<NProto::TDeviceConfig> Devices;
    THashMap<TNodeId, TAgentId> PendingAgents;

public:
    TReleaseDevicesActor(
        const TActorId& owner,
        TString diskId,
        TString clientId,
        ui32 volumeGeneration,
        TDuration requestTimeout,
        TVector<NProto::TDeviceConfig> devices,
        bool muteIOErrors);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void PrepareRequest(NProto::TReleaseDevicesRequest& request);
    void ReplyAndDie(const NActors::TActorContext& ctx, NProto::TError error);

    TString LogTargets() const;

    TString LogPendingAgents() const;

    void OnReleaseResponse(
        const NActors::TActorContext& ctx,
        TNodeId nodeId,
        NProto::TError error);

private:
    STFUNC(StateWork);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReleaseDevicesResponse(
        const TEvDiskAgent::TEvReleaseDevicesResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReleaseDevicesUndelivery(
        const TEvDiskAgent::TEvReleaseDevicesRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleTimeout(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
