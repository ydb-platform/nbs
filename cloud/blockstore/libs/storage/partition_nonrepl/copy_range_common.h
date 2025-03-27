#pragma once

#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>
#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/actorid.h>
#include <contrib/ydb/library/actors/core/events.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class ICopyRangeOwner
{
public:
    virtual void ReadyToCopy(
        const NActors::TActorContext& ctx,
        ui64 volumerequestId) = 0;

    virtual bool OnMessage(
        const NActors::TActorContext& ctx,
        TAutoPtr<NActors::IEventHandle>& ev) = 0;

    virtual void BeforeDie(
        const NActors::TActorContext& ctx,
        NProto::TError error) = 0;

    virtual ~ICopyRangeOwner() = default;
};

class TCopyRangeActorCommon
    : public NActors::TActorBootstrapped<TCopyRangeActorCommon>
{
    ICopyRangeOwner* Owner;
    const NActors::TActorId VolumeActorId;

public:
    TCopyRangeActorCommon(
            ICopyRangeOwner* owner,
            NActors::TActorId volumeActorId)
        : Owner(owner)
        , VolumeActorId(volumeActorId)
    {}

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void GetVolumeRequestId(const NActors::TActorContext& ctx);

protected:
    void Done(const NActors::TActorContext& ctx, NProto::TError error);

private:
    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleVolumeRequestId(
        const TEvVolumePrivate::TEvTakeVolumeRequestIdResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    STFUNC(StateWork);
};

}   // namespace NCloud::NBlockStore::NStorage
