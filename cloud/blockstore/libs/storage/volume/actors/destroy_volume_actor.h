#pragma once

#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/model/log_title.h>

#include <cloud/storage/core/libs/common/backoff_delay_provider.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TDestroyVolumeActor final
    : public NActors::TActorBootstrapped<TDestroyVolumeActor>
{
private:
    const TChildLogTitle LogTitle;
    const TRequestInfoPtr RequestInfo;
    const TString DiskId;

    TBackoffDelayProvider DelayProvider{
        TDuration::Seconds(5),
        TDuration::Seconds(60)};
    size_t TryCount = 0;

public:
    TDestroyVolumeActor(
        TChildLogTitle logTitle,
        TRequestInfoPtr requestInfo,
        TString diskId);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void DestroyDisk(const NActors::TActorContext& ctx);

    void ReplyAndDie(const NActors::TActorContext& ctx, NProto::TError error);

private:
    STFUNC(StateWork);

    void HandleDestroyVolumeResponse(
        const TEvService::TEvDestroyVolumeResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage

////////////////////////////////////////////////////////////////////////////////
