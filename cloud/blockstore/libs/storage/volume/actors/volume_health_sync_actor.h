#pragma once

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/model/log_title.h>
#include <cloud/blockstore/libs/storage/protos/volume.pb.h>

#include <cloud/storage/core/libs/common/backoff_delay_provider.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>

#include <util/datetime/base.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Reliable delivery of TVolumeActor's health state to DiskRegistry.
// Lives one Volume generation; idempotency on DR side (seqNo) lets the new
// generation simply re-push current health without persisting in-flight state.
class TVolumeHealthSyncActor final
    : public NActors::TActorBootstrapped<TVolumeHealthSyncActor>
{
private:
    const NActors::TActorId Owner;
    const TString DiskId;
    TChildLogTitle LogTitle;
    const TDuration InitialBackoff;
    const TDuration MaxBackoff;

    NProto::EVolumeHealth DesiredHealth = NProto::VOLUME_HEALTH_HEALTHY;
    ui64 DesiredSeqNo = 0;
    ui64 LastConfirmedSeqNo = 0;

    TBackoffDelayProvider Backoff;
    ui64 WakeupCookie = 0;

public:
    TVolumeHealthSyncActor(
        const NActors::TActorId& owner,
        TString diskId,
        TChildLogTitle logTitle,
        TDuration initialBackoff,
        TDuration maxBackoff);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);

    void SendUpdate(const NActors::TActorContext& ctx);
    void ScheduleRetry(const NActors::TActorContext& ctx);

    void HandleSetDesiredHealth(
        const TEvVolumePrivate::TEvSetDesiredVolumeHealth::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateVolumeHealthResponse(
        const TEvDiskRegistry::TEvUpdateVolumeHealthResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
