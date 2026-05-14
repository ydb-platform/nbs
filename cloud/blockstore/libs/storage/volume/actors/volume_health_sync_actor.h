#pragma once

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/model/composite_id.h>
#include <cloud/blockstore/libs/storage/model/log_title.h>
#include <cloud/blockstore/libs/storage/protos/volume.pb.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>

#include <cloud/storage/core/libs/common/backoff_delay_provider.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/events.h>

#include <util/datetime/base.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TVolumeHealthSyncActor final
    : public NActors::TActor<TVolumeHealthSyncActor>
{
    struct TUpdate
    {
        NProto::EVolumeHealth Health = NProto::VOLUME_HEALTH_HEALTHY;
        ui64 SeqNo = 0;
        TInstant Deadline;
    };

private:
    const TString DiskId;
    TCompositeId SeqNoGen;
    TChildLogTitle LogTitle;

    TBackoffDelayProvider DelayProvider;

    std::optional<TUpdate> UpdateInProgress;

public:
    TVolumeHealthSyncActor(
        TString diskId,
        ui32 generation,
        TChildLogTitle logTitle,
        TBackoffDelayProvider delayProvider);

private:
    STFUNC(StateWork);

    void SendUpdate(const NActors::TActorContext& ctx, TUpdate update);

    void HandleUpdateVolumeHealth(
        const TEvDiskRegistry::TEvUpdateVolumeHealthRequest::TPtr& ev,
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

    void ProcessError(
        const NActors::TActorContext& ctx,
        ui64 seqNo,
        const NProto::TError& error);

    TInstant CalcRequestDeadline(TInstant now) const;
};

}   // namespace NCloud::NBlockStore::NStorage
