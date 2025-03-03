#include "volume_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleLinkLeaderVolumeToFollower(
    const TEvVolume::TEvLinkLeaderVolumeToFollowerRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Reply(
        ctx,
        *ev,
        std::make_unique<TEvVolume::TEvLinkLeaderVolumeToFollowerResponse>(
            MakeError(E_NOT_IMPLEMENTED)));
}

void TVolumeActor::HandleUnlinkLeaderVolumeFromFollower(
    const TEvVolume::TEvUnlinkLeaderVolumeFromFollowerRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Reply(
        ctx,
        *ev,
        std::make_unique<TEvVolume::TEvUnlinkLeaderVolumeFromFollowerResponse>(
            MakeError(E_NOT_IMPLEMENTED)));
}

}   // namespace NCloud::NBlockStore::NStorage
