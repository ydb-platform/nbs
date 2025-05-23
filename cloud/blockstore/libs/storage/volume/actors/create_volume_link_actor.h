
#pragma once

#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/volume/model/follower_disk.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////
//
//    TCreateVolumeLinkActor diagram
//
///////////////////////////////////////////////////////////////////////////////
// Describe Leader ---------
//      | Success          | Fail
//      |                  V
//      |                Response(Error);
//      V
//  Describe Follower ------
//      | Success          | Fail
//      |                  V
//      |                Response(Error);
//      V
//   Can link ? ----------
//      | Yes            | No
//      |                V
//      |             Response(E_INVALID_STATE);
//      V
//  Persist on Leader (state=Created) ----
//      |                                | Fail
//      | Success                        V
//      |                              Response(E_FAIL);
//      V
//  Persist on Follower (with retry)----
//      |                              | Fail
//      | Success                      V
//      |                     Persist on Leader(state=Error) ----
//      |                       | Success                       | Fail
//      |                       V                               V
//      |                     Response(Error);                Response(E_FAIL);
//      V
//   Persist on Leader(state=Preparing) ----
//      |                                  | Fail
//      | Success                          V
//      |                              Response(E_FAIL);
//      V
//    Response(S_OK);

class TCreateVolumeLinkActor final
    : public NActors::TActorBootstrapped<TCreateVolumeLinkActor>
{
private:
    const ui64 TabletID;
    const NActors::TActorId VolumeActorId;

    TFollowerDiskInfo Follower;
    NProto::TVolume LeaderVolume;
    NProto::TVolume FollowerVolume;

public:
    TCreateVolumeLinkActor(
        ui64 tabletID,
        NActors::TActorId volumeActorId,
        TLeaderFollowerLink link);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void LinkVolumes(const NActors::TActorContext& ctx);
    void PersistOnLeader(const NActors::TActorContext& ctx);
    void PersistOnFollower(const NActors::TActorContext& ctx);

    void HandleDescribeVolumeResponse(
        const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePersistedOnLeader(
        const TEvVolumePrivate::TEvUpdateFollowerStateResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePersistedOnFollower(
        const TEvVolumePrivate::TEvLinkOnFollowerCreated::TPtr& ev,
        const NActors::TActorContext& ctx);

    void ReplyAndDie(
        const NActors::TActorContext& ctx,
        const NProto::TError& error);

private:
    STFUNC(StateWork);
};

////////////////////////////////////////////////////////////////////////////////
}   // namespace NCloud::NBlockStore::NStorage
